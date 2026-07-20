package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/api"
	"github.com/QuintinShaw/sitrep/daemon/internal/config"
)

// TestAutomationDueRunRequestID pins the pure comparison automationDue makes:
// the resident agent runs an automation at most once per distinct
// run_request_id observation, keyed off a freshly-compared last-consumed
// value (not a stale, 10s-refreshed local timestamp). This is what makes it
// robust to the 2s-poll / 10s-refresh skew that double-fired the old
// wall-clock scheme. Whether the OVERALL system (across process restarts) is
// exactly-once or at-least-once depends on how the caller seeds/persists
// consumedRunID — see cmd/sitrep/agent.go's cmdAgent (persisted, defaults to
// 0, at-least-once with crash recovery — external review round 3, P0), not
// this pure function.
func TestAutomationDueRunRequestID(t *testing.T) {
	now := time.UnixMilli(2_000_000)
	// Schedule is far from due: last ran 1s ago, interval 3600s.
	lastRun := now.Add(-time.Second)

	a := api.Automation{
		AutomationID: "a1",
		State:        "active",
		Schedule:     api.Schedule{EverySeconds: 3600},
		RunRequestID: 0,
	}

	// run_request_id not advanced beyond consumed (0) → not due.
	if due, _ := automationDue(a, 0, lastRun, now); due {
		t.Fatal("should not be due: run_request_id == consumed and schedule not elapsed")
	}

	// A tap advances run_request_id to 1 → due, and flagged as run-requested.
	a.RunRequestID = 1
	due, runRequested := automationDue(a, 0, lastRun, now)
	if !due || !runRequested {
		t.Fatalf("run_request_id 1 > consumed 0 should be due+runRequested, got due=%v runRequested=%v", due, runRequested)
	}

	// Once consumed (==1), the SAME id must NOT re-fire — even across many
	// polls before the next config refresh (the old double-fire bug).
	for i := 0; i < 5; i++ {
		if due, _ := automationDue(a, 1, lastRun, now); due {
			t.Fatal("must not re-fire: run_request_id 1 already consumed")
		}
	}

	// A second tap (id 2) re-arms the trigger.
	a.RunRequestID = 2
	if due, rr := automationDue(a, 1, lastRun, now); !due || !rr {
		t.Fatal("a newer run_request_id (2 > 1) should re-arm the trigger")
	}

	// The schedule path still works and is NOT flagged as run-requested.
	a.RunRequestID = 2
	elapsed := now.Add(-2 * time.Hour)
	if due, rr := automationDue(a, 2, elapsed, now); !due || rr {
		t.Fatalf("schedule-elapsed should be due but not run-requested, got due=%v rr=%v", due, rr)
	}
}

// TestRunAutomationHitsV1RunRoute pins that issuing a run is the field-stamp
// HTTP call POST /v1/automations/:id/run (NOT a command mint), with no body.
func TestRunAutomationHitsV1RunRoute(t *testing.T) {
	var gotPath, gotMethod string
	var gotLen int64 = -1
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod, gotLen = r.URL.Path, r.Method, r.ContentLength
		w.WriteHeader(http.StatusOK) // no body
	}))
	defer srv.Close()

	if err := api.New(srv.URL, "tok").RunAutomation("a1"); err != nil {
		t.Fatalf("RunAutomation: %v", err)
	}
	if gotMethod != http.MethodPost || gotPath != "/v1/automations/a1/run" {
		t.Fatalf("hit %s %s, want POST /v1/automations/a1/run", gotMethod, gotPath)
	}
	if gotLen > 0 {
		t.Fatalf("run request carried a body (len %d), want none", gotLen)
	}
}

// TestOfflineRunNowExecutesOnceAfterRestart is the external-review-round-3
// P0 repro: an automation's run_request_id advances (a viewer tapped "run
// now") WHILE THE AGENT IS STOPPED. When the agent finally starts, it must
// see the request and run the automation exactly once — not silently adopt
// the server's current run_request_id as "already consumed" (the bug), and
// not re-run once it has recorded consuming it.
//
// This drives the exact same building blocks cmdAgent's scheduling loop
// uses per iteration (config.LoadLocalAutomations/MarkRunClaimed/
// MarkRunCompleted, automationDue) directly, rather than running cmdAgent's
// infinite for{} loop (which has no clean stop signal to unit-test around).
func TestOfflineRunNowExecutesOnceAfterRestart(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	// The automation is configured locally (a prior `sitrep automation add`)
	// but has never actually run — a fresh, all-zero LocalAutomation entry,
	// exactly what a never-started or newly-installed agent would see.
	if err := config.SaveLocalAutomation("auto-1", config.LocalAutomation{
		Command: []string{"backup.sh"}, ExecutorKind: "script", Name: "nightly",
	}); err != nil {
		t.Fatalf("SaveLocalAutomation: %v", err)
	}

	// --- Agent is now "offline." A viewer taps run-now three times while it
	// is down; the server's run_request_id ends at 3 (POST .../run is not
	// simulated here — v1-architecture.md §5.1 is server-side; what matters
	// for this daemon-side repro is the value the agent observes on its
	// first poll after restart).
	automation := api.Automation{
		AutomationID: "auto-1",
		Name:         "nightly",
		State:        "active",
		Schedule:     api.Schedule{EverySeconds: 3600},
		RunRequestID: 3,
	}
	t.Logf("REPRO: server run_request_id=%d while agent was offline (0 taps observed by agent so far)", automation.RunRequestID)

	// --- Agent "starts fresh": first sight of this automation. Per the P0
	// fix, consumedRunID seeds from PERSISTED local state (defaulting to 0
	// for a never-run automation) — never from automation.RunRequestID.
	la := config.LoadLocalAutomations()["auto-1"]
	consumedRunID := la.LastConsumedRunRequestID
	t.Logf("REPRO: agent restart — seeded consumedRunID=%d from local state (must be 0, not adopted from server's %d)", consumedRunID, automation.RunRequestID)
	if consumedRunID != 0 {
		t.Fatalf("consumedRunID seeded to %d, want 0 (must default to 0, not adopt the server's current run_request_id)", consumedRunID)
	}

	// --- First poll: due, and correctly flagged as run-requested.
	due, runRequested := automationDue(automation, consumedRunID, time.Time{}, time.Now())
	t.Logf("REPRO: automationDue(run_request_id=%d, consumed=%d) = due=%v runRequested=%v", automation.RunRequestID, consumedRunID, due, runRequested)
	if !due || !runRequested {
		t.Fatalf("offline run-now tap not detected: due=%v runRequested=%v, want true/true", due, runRequested)
	}

	// --- Execute it (claim, run, complete — the two-phase persisted claim).
	runCount := 0
	if err := config.MarkRunClaimed("auto-1", automation.RunRequestID); err != nil {
		t.Fatalf("MarkRunClaimed: %v", err)
	}
	runCount++ // the automation actually executes here in cmdAgent
	if err := config.MarkRunCompleted("auto-1", automation.RunRequestID, time.Now().UnixMilli()); err != nil {
		t.Fatalf("MarkRunCompleted: %v", err)
	}
	consumedRunID = automation.RunRequestID
	t.Logf("REPRO: executed automation (runCount=%d), persisted last_consumed_run_request_id=%d", runCount, consumedRunID)
	if runCount != 1 {
		t.Fatalf("automation executed %d times, want exactly 1", runCount)
	}

	// --- Subsequent polls (server's run_request_id unchanged at 3) must NOT
	// re-fire — neither in this same process (in-memory consumedRunID) nor
	// after ANOTHER restart (persisted LastConsumedRunRequestID).
	for i := 0; i < 3; i++ {
		if due, _ := automationDue(automation, consumedRunID, time.Now(), time.Now()); due {
			t.Fatalf("poll %d: re-fired on the same run_request_id within the same process", i)
		}
	}
	la2 := config.LoadLocalAutomations()["auto-1"]
	t.Logf("REPRO: after a SECOND restart, persisted LastConsumedRunRequestID=%d", la2.LastConsumedRunRequestID)
	if due, _ := automationDue(automation, la2.LastConsumedRunRequestID, time.Now(), time.Now()); due {
		t.Fatal("re-fired after a second restart — LastConsumedRunRequestID was not durably persisted")
	}
}

// TestCrashRecoveryReRunsInterruptedClaim is the crash-recovery repro: a
// process claims a run_request_id (persists ClaimedRunRequestID) and is then
// killed before it can persist the matching MarkRunCompleted — e.g. the
// machine lost power mid-automation. On the next agent startup,
// claimed > completed must be detected and the automation re-run
// (at-least-once bias — automations are expected to be idempotent).
func TestCrashRecoveryReRunsInterruptedClaim(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	if err := config.SaveLocalAutomation("auto-1", config.LocalAutomation{
		Command: []string{"backup.sh"}, ExecutorKind: "script", Name: "nightly",
	}); err != nil {
		t.Fatalf("SaveLocalAutomation: %v", err)
	}

	// --- A process claims run_request_id 7 and is killed before completing.
	if err := config.MarkRunClaimed("auto-1", 7); err != nil {
		t.Fatalf("MarkRunClaimed: %v", err)
	}
	t.Log("REPRO: process claimed run_request_id=7, then crashed (no MarkRunCompleted)")

	// --- Next agent startup: first-sight detection, mirroring cmdAgent's
	// crash-recovery block exactly.
	la := config.LoadLocalAutomations()["auto-1"]
	needsRecover := la.ClaimedRunRequestID > la.CompletedRunRequestID
	t.Logf("REPRO: on restart, claimed=%d completed=%d -> needsRecover=%v", la.ClaimedRunRequestID, la.CompletedRunRequestID, needsRecover)
	if !needsRecover {
		t.Fatalf("crash was not detected: claimed=%d completed=%d", la.ClaimedRunRequestID, la.CompletedRunRequestID)
	}
	recoverID := la.ClaimedRunRequestID
	if recoverID != 7 {
		t.Fatalf("recoverID = %d, want 7", recoverID)
	}

	// --- Re-run it (at-least-once): claim again, execute, complete.
	if err := config.MarkRunClaimed("auto-1", recoverID); err != nil {
		t.Fatalf("MarkRunClaimed (recovery): %v", err)
	}
	if err := config.MarkRunCompleted("auto-1", recoverID, time.Now().UnixMilli()); err != nil {
		t.Fatalf("MarkRunCompleted (recovery): %v", err)
	}
	t.Logf("REPRO: re-ran run_request_id=%d to completion", recoverID)

	// --- A THIRD startup (no further crash) must not detect a recoverable
	// claim anymore.
	la2 := config.LoadLocalAutomations()["auto-1"]
	if la2.ClaimedRunRequestID > la2.CompletedRunRequestID {
		t.Fatalf("still shows an unrecovered claim after a clean completion: claimed=%d completed=%d", la2.ClaimedRunRequestID, la2.CompletedRunRequestID)
	}
	t.Logf("REPRO: after recovery, claimed=%d completed=%d — no longer flagged as interrupted", la2.ClaimedRunRequestID, la2.CompletedRunRequestID)
}
