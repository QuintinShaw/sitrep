package config

import (
	"os"
	"path/filepath"
	"testing"
)

// TestMarkRunClaimedWriteFailureIsHandled is fault injection against the
// real on-disk write path: what happens when the automations.json WRITE
// fails mid-claim (e.g. disk full, permissions revoked, config dir made
// read-only)? This exercises the actual writeLocalAutomationsLocked path
// (os.WriteFile), not a mock.
func TestMarkRunClaimedWriteFailureIsHandled(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	dir := filepath.Join(home, ".config", "sitrep")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// Seed a normal, writable automation entry first.
	if err := SaveLocalAutomation("auto-x", LocalAutomation{
		Command: []string{"echo", "hi"}, ExecutorKind: "script", Name: "job-x",
	}); err != nil {
		t.Fatalf("seed SaveLocalAutomation: %v", err)
	}
	before, err := os.ReadFile(LocalAutomationsPath())
	if err != nil {
		t.Fatalf("read seeded file: %v", err)
	}
	t.Logf("BEFORE claim, automations.json:\n%s", before)

	// os.WriteFile(O_WRONLY|O_CREATE|O_TRUNC) against an EXISTING file only
	// needs write permission on the FILE itself, not the directory (making
	// the directory read-only, tried first, is a no-op: truncating an
	// already-open-able file doesn't need a directory write). Chmod the FILE
	// read-only to force the actual os.WriteFile call inside
	// writeLocalAutomationsLocked to fail with permission denied.
	automationsPath := LocalAutomationsPath()
	if err := os.Chmod(automationsPath, 0o400); err != nil {
		t.Fatalf("chmod file read-only: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(automationsPath, 0o600) })

	// chmod-based permission denial doesn't actually block writes when the
	// test runs as root (or in some container/CI setups where the effective
	// uid bypasses file-mode checks) — os.WriteFile would silently succeed
	// despite the 0o400 mode, making the rest of this test meaningless.
	// Probe for that up front and skip rather than asserting on a premise
	// that doesn't hold in this environment.
	if probeErr := os.WriteFile(automationsPath, before, 0o400); probeErr == nil {
		t.Skip("skipping: chmod 0o400 did not actually block writes in this environment (likely running as root) — write-failure fault injection is not exercised")
	}

	claimErr := MarkRunClaimed("auto-x", 42)
	t.Logf("MarkRunClaimed while automations.json is read-only -> error: %v", claimErr)
	if claimErr == nil {
		t.Fatal("DEFECT: MarkRunClaimed returned nil error despite the write being impossible (file read-only) — the caller (cmd/sitrep/agent.go) cannot distinguish a persisted claim from a lost one")
	}

	// Restore permissions so we can inspect the actual on-disk truth a NEXT
	// process would observe.
	if err := os.Chmod(automationsPath, 0o600); err != nil {
		t.Fatalf("restore perms: %v", err)
	}
	after, err := os.ReadFile(LocalAutomationsPath())
	if err != nil {
		t.Fatalf("read file after failed claim: %v", err)
	}
	t.Logf("AFTER failed claim attempt, automations.json on disk:\n%s", after)

	la := LoadLocalAutomations()["auto-x"]
	t.Logf("reloaded from disk: ClaimedRunRequestID=%d CompletedRunRequestID=%d", la.ClaimedRunRequestID, la.CompletedRunRequestID)
	if la.ClaimedRunRequestID != 0 {
		t.Fatalf("DEFECT: on-disk state shows ClaimedRunRequestID=%d even though the write errored — a claim appears to have landed when it didn't", la.ClaimedRunRequestID)
	}
	// The critical safety property: cmdAgent's caller in agent.go does
	// `if err := config.MarkRunClaimed(id, runID); err != nil { fmt.Fprintf(stderr, ...) }`
	// and proceeds to execute the automation regardless of the error (it only
	// logs). If the write genuinely failed, the on-disk state never recorded
	// the claim, so if the process crashes DURING the subsequent execution,
	// startup recovery (comparing Claimed>Completed) will not detect a
	// stranded claim (since Claimed was never persisted) and will not re-run
	// it. This is a silent-skip risk on write failure, not a crash — see the
	// hardening comment at the MarkRunClaimed call site in
	// cmd/sitrep/agent.go for the accepted-tradeoff rationale (at-least-once
	// automations must tolerate re-execution; a permanently unwritable disk
	// degrades that guarantee for the specific case of local-disk failure at
	// claim time).
	t.Log("FINDING: agent.go's MarkRunClaimed call site only logs the error to stderr and continues executing anyway (see cmd/sitrep/agent.go around 'persist run claim') — a write failure here means a crash-during-execution afterward leaves no on-disk evidence of the attempt, so crash recovery cannot re-run it.")
}
