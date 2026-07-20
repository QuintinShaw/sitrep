package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// LocalAutomation is the machine-local half of an automation. In v1 the
// server's automation state (AutomationState — automation_id, name,
// executor_kind, schedule, run state) is the shared control plane every
// device can see and edit, but the executor **command** is deliberately NOT
// part of it: "scheduling truth lives on the server; execution truth lives
// here, so credentials and code never leave the machine" (see cmd/sitrep's
// agent doc comment; v1-architecture.md folds only the control-plane fields
// into `automations`). This local store is where the command argv lives,
// keyed by the server-assigned automation_id, so the resident agent can
// join the server's schedule/state with the local command to actually run
// it. LastRunAt is also tracked here because the v1 automations PATCH route
// has no client-writable last_run field (the daemon no longer stamps runs
// server-side).
// Run-now (POST /v1/automations/:id/run) is at-least-once, not
// exactly-once (external review round 3, P0): the resident agent's
// last-consumed run_request_id is persisted HERE, defaulting to 0 the first
// time an automation is seen locally — never adopted from the server's
// current value — so a run-now tap that fires while the agent is offline is
// not silently swallowed the moment the agent finally starts (it observes
// run_request_id > 0 = last_consumed and correctly treats it as due).
// ClaimedRunRequestID/CompletedRunRequestID implement a persisted two-phase
// claim around actually executing a run-requested automation: claimed is
// written BEFORE the executor starts, completed (together with an advance
// of LastConsumedRunRequestID) AFTER it finishes. If the process crashes
// between the two, ClaimedRunRequestID > CompletedRunRequestID survives on
// disk and the next agent startup re-runs that run_request_id — see
// cmd/sitrep/agent.go. This is an at-least-once bias (prefer a possible
// re-run over a possible silent skip); automations are expected to be
// idempotent, exactly like the pause/resume/stop reverse-control commands
// this same principle already applies to (v1-architecture.md §1.4).
type LocalAutomation struct {
	Command      []string `json:"command"`
	ExecutorKind string   `json:"executor_kind"`
	Name         string   `json:"name"`
	LastRunAt    int64    `json:"last_run_at,omitempty"` // unix ms; 0 = never

	// LastConsumedRunRequestID is the run_request_id value compared against
	// the server's current one to decide whether a run-now tap is due (>).
	// Advanced to the just-executed id only AFTER that run completes
	// (schedule-driven runs never touch this field — only run_request_id
	// advances trip it). 0 (the zero value / omitted) is the correct
	// "never consumed anything" default for an automation this device has
	// never locally recorded a run for.
	LastConsumedRunRequestID int64 `json:"last_consumed_run_request_id,omitempty"`
	// ClaimedRunRequestID is written just before starting execution of a
	// run-requested run; see the type doc comment's two-phase claim.
	ClaimedRunRequestID int64 `json:"claimed_run_request_id,omitempty"`
	// CompletedRunRequestID is written just after that execution finishes
	// (successfully or not — a normal executor failure still completed;
	// only a process crash/kill mid-run leaves this behind claimed).
	CompletedRunRequestID int64 `json:"completed_run_request_id,omitempty"`
}

// localAutomationsMu serializes the read-modify-write cycles below so two
// concurrent CLI/agent operations on the same file don't clobber each
// other. The store is small (a handful of automations) and touched rarely,
// so a single process-wide lock plus whole-file rewrite is more than
// adequate.
var localAutomationsMu sync.Mutex

// LocalAutomationsPath is where the machine-local automation commands live,
// alongside config.json.
func LocalAutomationsPath() string {
	dir := filepath.Dir(Path())
	if dir == "" || dir == "." {
		return "automations.json"
	}
	return filepath.Join(dir, "automations.json")
}

// LoadLocalAutomations reads the local automation-command store (keyed by
// automation_id). A missing or unreadable file yields an empty map, not an
// error — the store is best-effort local cache, and an agent that finds no
// local command for an automation simply skips executing it.
func LoadLocalAutomations() map[string]LocalAutomation {
	localAutomationsMu.Lock()
	defer localAutomationsMu.Unlock()
	return loadLocalAutomationsLocked()
}

func loadLocalAutomationsLocked() map[string]LocalAutomation {
	out := map[string]LocalAutomation{}
	data, err := os.ReadFile(LocalAutomationsPath())
	if err != nil {
		return out
	}
	_ = json.Unmarshal(data, &out)
	if out == nil {
		out = map[string]LocalAutomation{}
	}
	return out
}

// SaveLocalAutomation upserts one automation's local command by id.
func SaveLocalAutomation(id string, la LocalAutomation) error {
	localAutomationsMu.Lock()
	defer localAutomationsMu.Unlock()
	m := loadLocalAutomationsLocked()
	m[id] = la
	return writeLocalAutomationsLocked(m)
}

// DeleteLocalAutomation removes one automation's local command by id.
// Deleting an absent id is a no-op, not an error.
func DeleteLocalAutomation(id string) error {
	localAutomationsMu.Lock()
	defer localAutomationsMu.Unlock()
	m := loadLocalAutomationsLocked()
	if _, ok := m[id]; !ok {
		return nil
	}
	delete(m, id)
	return writeLocalAutomationsLocked(m)
}

// StampLocalRun records that automation id last ran at unix-ms `whenMS`,
// leaving its command/executor/name untouched. It is a no-op for an id with
// no local entry (nothing to schedule from anyway).
func StampLocalRun(id string, whenMS int64) error {
	localAutomationsMu.Lock()
	defer localAutomationsMu.Unlock()
	m := loadLocalAutomationsLocked()
	la, ok := m[id]
	if !ok {
		return nil
	}
	la.LastRunAt = whenMS
	m[id] = la
	return writeLocalAutomationsLocked(m)
}

// MarkRunClaimed persists that automation id is about to execute
// run_request_id runID, BEFORE the executor actually starts — the first
// half of a two-phase claim/complete pair (see LocalAutomation's doc
// comment and MarkRunCompleted). A crash between this write and the
// matching MarkRunCompleted leaves ClaimedRunRequestID > CompletedRunRequestID
// on disk, which cmd/sitrep/agent.go's startup recovery detects and re-runs.
// No-op for an id with no local entry (nothing to schedule from anyway,
// same convention as StampLocalRun).
func MarkRunClaimed(id string, runID int64) error {
	localAutomationsMu.Lock()
	defer localAutomationsMu.Unlock()
	m := loadLocalAutomationsLocked()
	la, ok := m[id]
	if !ok {
		return nil
	}
	la.ClaimedRunRequestID = runID
	m[id] = la
	return writeLocalAutomationsLocked(m)
}

// MarkRunCompleted persists that automation id finished executing
// run_request_id runID: it advances CompletedRunRequestID (pairing with an
// earlier MarkRunClaimed to prove the run was not interrupted) and
// LastConsumedRunRequestID (the value automationDue compares the server's
// run_request_id against) to runID — never backward, in case an older
// call races a newer one — and stamps LastRunAt, all in one atomic write.
// A no-op for an id with no local entry.
func MarkRunCompleted(id string, runID, whenMS int64) error {
	localAutomationsMu.Lock()
	defer localAutomationsMu.Unlock()
	m := loadLocalAutomationsLocked()
	la, ok := m[id]
	if !ok {
		return nil
	}
	if runID > la.LastConsumedRunRequestID {
		la.LastConsumedRunRequestID = runID
	}
	if runID > la.CompletedRunRequestID {
		la.CompletedRunRequestID = runID
	}
	la.LastRunAt = whenMS
	m[id] = la
	return writeLocalAutomationsLocked(m)
}

func writeLocalAutomationsLocked(m map[string]LocalAutomation) error {
	path := LocalAutomationsPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o600)
}
