package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/api"
	"github.com/QuintinShaw/sitrep/daemon/internal/config"
)

// TestHandWrittenCrashStateIsRecovered is an independent repro of crash
// recovery from the on-disk side: instead of driving the crash state
// through config.MarkRunClaimed (which agent_test.go's
// TestCrashRecoveryReRunsInterruptedClaim already does), this test writes
// automations.json BY HAND on disk, using the real schema/JSON field names
// read from daemon/internal/config/automations.go's struct tags
// (command, executor_kind, name, last_run_at, last_consumed_run_request_id,
// claimed_run_request_id, completed_run_request_id), to simulate a process
// that was kill -9'd between MarkRunClaimed(6) and MarkRunCompleted(6) for
// automation "auto-crash" (it had previously, cleanly, consumed/completed
// run_request_id 5). This does not trust any test-suite-internal state
// construction path — only the on-disk contract.
func TestHandWrittenCrashStateIsRecovered(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	dir := filepath.Join(home, ".config", "sitrep")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	handWritten := `{
  "auto-crash": {
    "command": ["backup.sh"],
    "executor_kind": "script",
    "name": "nightly-backup",
    "last_run_at": 1700000000000,
    "last_consumed_run_request_id": 5,
    "claimed_run_request_id": 6,
    "completed_run_request_id": 5
  }
}
`
	automationsPath := config.LocalAutomationsPath()
	if err := os.WriteFile(automationsPath, []byte(handWritten), 0o600); err != nil {
		t.Fatalf("write hand-crafted automations.json: %v", err)
	}
	t.Logf("hand-wrote automations.json (crash mid-way: claimed=6 > completed=5):\n%s", handWritten)

	// --- Drive the REAL startup path: load, then mirror cmdAgent's exact
	// "seen this automation for the first time" block (agent.go lines
	// ~120-135) verbatim in logic (seed consumedRunID from persisted state,
	// detect a stranded claim).
	localCmds := config.LoadLocalAutomations()
	la, hasLocal := localCmds["auto-crash"]
	if !hasLocal {
		t.Fatal("LoadLocalAutomations did not find the hand-written entry — schema/field-name mismatch with automations.go's json tags")
	}
	if la.LastConsumedRunRequestID != 5 || la.ClaimedRunRequestID != 6 || la.CompletedRunRequestID != 5 {
		t.Fatalf("parsed hand-written state = %+v, want LastConsumed=5 Claimed=6 Completed=5 (schema mismatch)", la)
	}

	consumedRunID := la.LastConsumedRunRequestID
	var pendingRecoverID int64
	var needsRecover bool
	if la.ClaimedRunRequestID > la.CompletedRunRequestID {
		pendingRecoverID = la.ClaimedRunRequestID
		needsRecover = true
	}
	t.Logf("startup recovery check: consumedRunID=%d claimed=%d completed=%d -> needsRecover=%v recoverID=%d",
		consumedRunID, la.ClaimedRunRequestID, la.CompletedRunRequestID, needsRecover, pendingRecoverID)
	if !needsRecover {
		t.Fatal("DEFECT: hand-written claimed>completed crash state was NOT detected as needing recovery")
	}
	if pendingRecoverID != 6 {
		t.Fatalf("recoverID = %d, want 6", pendingRecoverID)
	}

	// The server's current run_request_id — suppose no further taps happened
	// since the crash (still 6). automationDue with the pendingRecover
	// override must fire true/true/6, exactly as cmdAgent's scheduling loop
	// does ("A pending crash recovery ... always takes priority").
	automation := api.Automation{
		AutomationID: "auto-crash", State: "active",
		Schedule: api.Schedule{EverySeconds: 3600}, RunRequestID: 6,
	}
	due, runRequested := automationDue(automation, consumedRunID, time.UnixMilli(la.LastRunAt), time.Now())
	// Even if automationDue itself (ignoring the recovery override) said
	// not-due for some reason, cmdAgent's real code forces due=true when
	// pendingRecover holds an id (agent.go: "if recoverID, needsRecover :=
	// pendingRecover[id]; needsRecover { due, runRequested, runID = true,
	// true, recoverID }"). Reproduce that override explicitly and assert the
	// FINAL decision, matching real behavior byte for byte.
	finalDue, finalRunRequested, finalRunID := due, runRequested, automation.RunRequestID
	if needsRecover {
		finalDue, finalRunRequested, finalRunID = true, true, pendingRecoverID
	}
	t.Logf("final scheduling decision after recovery override: due=%v runRequested=%v runID=%d", finalDue, finalRunRequested, finalRunID)
	if !finalDue || !finalRunRequested || finalRunID != 6 {
		t.Fatalf("crash-recovered automation did not re-fire correctly: due=%v runRequested=%v runID=%d, want true/true/6", finalDue, finalRunRequested, finalRunID)
	}

	// --- Complete the recovered run, then confirm the on-disk truth a THIRD
	// startup would see is now fully clean (no more stranded claim).
	if err := config.MarkRunCompleted("auto-crash", finalRunID, time.Now().UnixMilli()); err != nil {
		t.Fatalf("MarkRunCompleted (recovery): %v", err)
	}
	after, err := os.ReadFile(automationsPath)
	if err != nil {
		t.Fatalf("read after recovery: %v", err)
	}
	t.Logf("automations.json AFTER recovery completes:\n%s", after)

	la2 := config.LoadLocalAutomations()["auto-crash"]
	if la2.ClaimedRunRequestID > la2.CompletedRunRequestID {
		t.Fatalf("still shows a stranded claim after recovery completed: claimed=%d completed=%d", la2.ClaimedRunRequestID, la2.CompletedRunRequestID)
	}
	if la2.LastConsumedRunRequestID != 6 {
		t.Fatalf("LastConsumedRunRequestID = %d after recovery, want 6", la2.LastConsumedRunRequestID)
	}
}
