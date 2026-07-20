package health

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestComponentFileContract pins the on-disk contract the menubar reads for
// ONE component: shape {"ok":bool,"reason":string} at
// health.d/<component>.json, absence == healthy, and a degraded state
// carries a reason (v1-architecture.md §14).
func TestComponentFileContract(t *testing.T) {
	dir := t.TempDir()
	SetDirForTest(dir)
	path := filepath.Join(dir, "device_seq.json")

	// Fresh: no file → healthy.
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected no component file initially, stat err = %v", err)
	}
	if s := Load(); !s.OK {
		t.Fatal("missing health.d/ must Load() as ok:true")
	}

	// Degraded: ok:false with a reason, written to disk atomically.
	if err := Report("device_seq", "local telemetry storage unwritable"); err != nil {
		t.Fatalf("Report: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read component file: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("component file is not valid JSON: %v", err)
	}
	if raw["ok"] != false {
		t.Fatalf("component file ok = %v, want false", raw["ok"])
	}
	if raw["reason"] != "local telemetry storage unwritable" {
		t.Fatalf("component file reason = %v, want the degraded cause", raw["reason"])
	}
	for k := range raw {
		if k != "ok" && k != "reason" {
			t.Fatalf("unexpected key %q in component file (contract is {ok, reason})", k)
		}
	}

	// Recovery: clearing the component removes ITS file (absence == healthy).
	if err := Clear("device_seq"); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("cleared component must remove its file, stat err = %v", err)
	}
	if s := Load(); !s.OK {
		t.Fatal("after Clear, Load() must be ok:true")
	}
}

// TestIndependentComponentsDoNotClobber is the P0/pre-launch regression the
// health.d migration exists to fix: two components (touched by different
// processes, in this test simulated as two independent Report/Clear
// sequences) live in DIFFERENT files and never clobber each other — unlike
// the old single shared health.json, where one process's whole-file rewrite
// could erase another's currently-reported issue.
func TestIndependentComponentsDoNotClobber(t *testing.T) {
	dir := t.TempDir()
	SetDirForTest(dir)

	_ = Report("device_seq", "storage unwritable")
	_ = Report("outbox", "storage full")
	if s := Load(); s.OK {
		t.Fatal("two issues → must be ok:false")
	}

	// Clearing one leaves the OTHER component's file, and its issue,
	// completely untouched.
	_ = Clear("device_seq")
	if _, err := os.Stat(filepath.Join(dir, "outbox.json")); err != nil {
		t.Fatalf("clearing device_seq must not touch outbox.json: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "device_seq.json")); !os.IsNotExist(err) {
		t.Fatalf("device_seq.json should be gone after Clear, stat err = %v", err)
	}
	if s := Load(); s.OK {
		t.Fatal("outbox issue remaining → still ok:false")
	}

	_ = Clear("outbox")
	if s := Load(); !s.OK {
		t.Fatal("all cleared → ok:true")
	}
}

// TestConcurrentWritesToDifferentComponentsDoNotRace drives concurrent
// Report/Clear calls against N distinct components from multiple goroutines
// (standing in for independent daemon processes touching this shared
// directory) and confirms every component's file ends up correct — proving
// the per-component-file design is safe under real concurrency, not just
// sequential calls. Run with -race.
func TestConcurrentWritesToDifferentComponentsDoNotRace(t *testing.T) {
	dir := t.TempDir()
	SetDirForTest(dir)

	components := []string{"outbox", "device_seq", "auth", "outbox_open"}
	done := make(chan struct{})
	for _, c := range components {
		go func(component string) {
			defer func() { done <- struct{}{} }()
			for i := 0; i < 20; i++ {
				if err := Report(component, "transient issue"); err != nil {
					t.Errorf("Report(%s): %v", component, err)
				}
			}
			if err := Clear(component); err != nil {
				t.Errorf("Clear(%s): %v", component, err)
			}
		}(c)
	}
	for range components {
		<-done
	}

	// Every component ended cleared — its file must be gone, and no
	// component's write leaked into another's file.
	for _, c := range components {
		if _, err := os.Stat(filepath.Join(dir, c+".json")); !os.IsNotExist(err) {
			t.Fatalf("%s.json still present after Clear, stat err = %v", c, err)
		}
	}
	if s := Load(); !s.OK {
		t.Fatalf("all components cleared → want ok:true, got %+v", s)
	}
}

// TestStaleIssueResolvesWithoutRecoveryWrite pins §14's staleness rule: a
// component file that is ok:false but older than StaleAfter is treated as
// resolved by Load() even though no recovery write ever happened — this is
// what lets a short-lived process's failure self-heal.
func TestStaleIssueResolvesWithoutRecoveryWrite(t *testing.T) {
	dir := t.TempDir()
	SetDirForTest(dir)

	if err := Report("outbox_open", "failed to open local outbox"); err != nil {
		t.Fatalf("Report: %v", err)
	}
	if s := Load(); s.OK {
		t.Fatal("freshly-reported issue must be live (ok:false)")
	}

	// Back-date the file's mtime past StaleAfter — simulating time passing
	// with no follow-up write from the (already-exited) short-lived process.
	path := filepath.Join(dir, "outbox_open.json")
	old := time.Now().Add(-StaleAfter - time.Minute)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}

	s := Load()
	if !s.OK {
		t.Fatalf("stale ok:false file must be treated as resolved, got %+v", s)
	}
	// The file itself is untouched (still says ok:false on disk) — only
	// AGGREGATION treats it as resolved, per §14 ("even though its on-disk
	// content still says otherwise").
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("stale file should still be on disk: %v", err)
	}
	var raw map[string]any
	_ = json.Unmarshal(data, &raw)
	if raw["ok"] != false {
		t.Fatal("staleness must not rewrite the file's own content")
	}
}

// TestMissingDirectoryIsHealthy pins that the whole health.d/ directory
// being absent (a fresh daemon that has never reported anything) is healthy,
// same as one specific component's file being absent.
func TestMissingDirectoryIsHealthy(t *testing.T) {
	SetDirForTest(filepath.Join(t.TempDir(), "does-not-exist", "health.d"))
	if s := Load(); !s.OK {
		t.Fatalf("missing health.d/ directory must Load() as ok:true, got %+v", s)
	}
}

// TestUnwritableDirDoesNotPanic pins that a health.d directory this process
// cannot write to (e.g. its parent's permissions were revoked out from under
// it) degrades Report/Load to a returned error or a healthy no-op — never a
// panic. health is a side channel: a failure reporting health must never
// itself take down the caller's real work.
func TestUnwritableDirDoesNotPanic(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "health.d")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.Chmod(base, 0o000); err != nil {
		t.Fatalf("chmod parent unwritable: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(base, 0o700) }) // so t.TempDir() cleanup can remove it

	// A revoked parent directory mode doesn't actually block access when
	// this process runs as root (or under some container/CI setups whose
	// effective uid bypasses file-mode checks) — the writes below would
	// silently succeed despite the 0o000 mode, making the rest of this test
	// meaningless. Probe for that up front and skip rather than asserting on
	// a premise that doesn't hold in this environment.
	if probeErr := os.WriteFile(filepath.Join(dir, ".probe"), []byte("x"), 0o600); probeErr == nil {
		t.Skip("skipping: chmod 0o000 did not actually block access in this environment (likely running as root)")
	}

	SetDirForTest(dir)

	var reportErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Report PANICKED on unwritable dir: %v", r)
			}
		}()
		reportErr = Report("outbox", "disk full")
	}()
	t.Logf("Report() error on unwritable parent dir: %v", reportErr)
	if reportErr == nil {
		t.Fatal("expected Report() to surface a real filesystem error on an unwritable dir, got nil")
	}

	// Load() over the same broken dir must also degrade gracefully, not panic.
	var s Status
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Load PANICKED on unwritable dir: %v", r)
			}
		}()
		s = Load()
	}()
	t.Logf("Load() over unwritable dir: %+v", s)
}
