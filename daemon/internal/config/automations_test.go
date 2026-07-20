package config

import (
	"reflect"
	"testing"
)

// TestLocalAutomationsRoundTrip pins the machine-local executor-command
// store the v1 automations migration introduced: the command argv lives
// locally (it is deliberately absent from the server's shared automation
// state), keyed by the server-assigned automation_id, with last-run stamped
// locally (the v1 PATCH route has no client-writable last_run field).
func TestLocalAutomationsRoundTrip(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	if m := LoadLocalAutomations(); len(m) != 0 {
		t.Fatalf("fresh store should be empty, got %+v", m)
	}

	a := LocalAutomation{Command: []string{"backup.sh", "--full"}, ExecutorKind: "script", Name: "nightly"}
	if err := SaveLocalAutomation("auto-1", a); err != nil {
		t.Fatalf("SaveLocalAutomation: %v", err)
	}
	if err := SaveLocalAutomation("auto-2", LocalAutomation{Command: []string{"echo", "hi"}, ExecutorKind: "script", Name: "greet"}); err != nil {
		t.Fatalf("SaveLocalAutomation 2: %v", err)
	}

	m := LoadLocalAutomations()
	if got := m["auto-1"]; !reflect.DeepEqual(got.Command, a.Command) || got.ExecutorKind != "script" || got.Name != "nightly" {
		t.Fatalf("auto-1 round-trip mismatch: %+v", got)
	}
	if len(m) != 2 {
		t.Fatalf("want 2 stored automations, got %d", len(m))
	}

	// Stamp a run; command/executor/name are preserved.
	if err := StampLocalRun("auto-1", 1784476800000); err != nil {
		t.Fatalf("StampLocalRun: %v", err)
	}
	got := LoadLocalAutomations()["auto-1"]
	if got.LastRunAt != 1784476800000 || got.Name != "nightly" || len(got.Command) != 2 {
		t.Fatalf("after StampLocalRun: %+v", got)
	}

	// Stamping an unknown id is a no-op (no entry created).
	if err := StampLocalRun("nope", 123); err != nil {
		t.Fatalf("StampLocalRun unknown: %v", err)
	}
	if _, ok := LoadLocalAutomations()["nope"]; ok {
		t.Fatal("StampLocalRun created an entry for an unknown id")
	}

	// Delete removes only the targeted entry; deleting an absent id is fine.
	if err := DeleteLocalAutomation("auto-1"); err != nil {
		t.Fatalf("DeleteLocalAutomation: %v", err)
	}
	if err := DeleteLocalAutomation("auto-1"); err != nil {
		t.Fatalf("DeleteLocalAutomation (absent): %v", err)
	}
	m = LoadLocalAutomations()
	if _, ok := m["auto-1"]; ok {
		t.Fatal("auto-1 not deleted")
	}
	if _, ok := m["auto-2"]; !ok {
		t.Fatal("delete removed the wrong entry")
	}
}

// TestRunRequestIDPersistenceDefaultsToZero pins the P0 fix (external review
// round 3): a freshly-seen automation's LastConsumedRunRequestID defaults to
// 0, never to the server's current run_request_id — this is what makes an
// offline run-now tap execute once the agent starts, instead of being
// silently adopted as "already handled."
func TestRunRequestIDPersistenceDefaultsToZero(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	if err := SaveLocalAutomation("auto-1", LocalAutomation{Command: []string{"backup.sh"}, ExecutorKind: "script", Name: "nightly"}); err != nil {
		t.Fatalf("SaveLocalAutomation: %v", err)
	}
	got := LoadLocalAutomations()["auto-1"]
	if got.LastConsumedRunRequestID != 0 || got.ClaimedRunRequestID != 0 || got.CompletedRunRequestID != 0 {
		t.Fatalf("freshly-saved automation should have all run-request fields at 0, got %+v", got)
	}
}

// TestMarkRunClaimedThenCompleted pins the two-phase claim/complete
// persistence: MarkRunClaimed records a claim BEFORE execution;
// MarkRunCompleted advances both CompletedRunRequestID and
// LastConsumedRunRequestID (and stamps LastRunAt) AFTER. A claim with no
// matching completion (simulated by only calling MarkRunClaimed) is what
// cmd/sitrep/agent.go's startup recovery detects as an interrupted run.
func TestMarkRunClaimedThenCompleted(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	if err := SaveLocalAutomation("auto-1", LocalAutomation{Command: []string{"backup.sh"}, ExecutorKind: "script", Name: "nightly"}); err != nil {
		t.Fatalf("SaveLocalAutomation: %v", err)
	}

	// Claim run_request_id 5.
	if err := MarkRunClaimed("auto-1", 5); err != nil {
		t.Fatalf("MarkRunClaimed: %v", err)
	}
	got := LoadLocalAutomations()["auto-1"]
	if got.ClaimedRunRequestID != 5 {
		t.Fatalf("ClaimedRunRequestID = %d, want 5", got.ClaimedRunRequestID)
	}
	if got.ClaimedRunRequestID <= got.CompletedRunRequestID {
		t.Fatal("expected claimed > completed immediately after MarkRunClaimed — simulates a crash-recoverable state")
	}

	// Complete it.
	if err := MarkRunCompleted("auto-1", 5, 1784476800000); err != nil {
		t.Fatalf("MarkRunCompleted: %v", err)
	}
	got = LoadLocalAutomations()["auto-1"]
	if got.CompletedRunRequestID != 5 || got.LastConsumedRunRequestID != 5 || got.LastRunAt != 1784476800000 {
		t.Fatalf("after MarkRunCompleted: %+v", got)
	}
	if got.ClaimedRunRequestID > got.CompletedRunRequestID {
		t.Fatal("claimed > completed after a successful completion — recovery would wrongly re-run this")
	}

	// MarkRunClaimed/MarkRunCompleted on an unknown id is a no-op (same
	// convention as StampLocalRun).
	if err := MarkRunClaimed("nope", 1); err != nil {
		t.Fatalf("MarkRunClaimed unknown: %v", err)
	}
	if err := MarkRunCompleted("nope", 1, 1); err != nil {
		t.Fatalf("MarkRunCompleted unknown: %v", err)
	}
	if _, ok := LoadLocalAutomations()["nope"]; ok {
		t.Fatal("MarkRunClaimed/MarkRunCompleted created an entry for an unknown id")
	}
}
