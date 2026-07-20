package main

import (
	"fmt"
	"os"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/api"
	"github.com/QuintinShaw/sitrep/daemon/internal/config"
	"github.com/QuintinShaw/sitrep/daemon/internal/health"
	"github.com/QuintinShaw/sitrep/daemon/internal/protocol"
	rtclient "github.com/QuintinShaw/sitrep/daemon/internal/realtime/client"
	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/outbox"
	"github.com/QuintinShaw/sitrep/daemon/internal/runner"
	"github.com/QuintinShaw/sitrep/daemon/internal/uplink"
)

// cmdAgent is the resident scheduler: it executes the space's automation
// registry. The menu bar app supervises this process; headless machines run
// it under launchd/systemd directly. Scheduling truth lives on the server
// (so any device can edit intervals); execution truth lives here (so
// credentials and code never leave the machine).
func cmdAgent(args []string) {
	client, err := api.FromConfig()
	if err != nil {
		fatal(err)
	}
	fmt.Fprintf(os.Stderr, "sitrep agent: scheduling automations from %s\n", client.Server)

	cfg := config.Load()

	// Shared persistent device_seq allocator (the realtime outbox's
	// space_seq_counters). Opened once for the process and shared by BOTH the
	// realtime Client (via Enqueue) and every automation's best-effort
	// /v1/events uplink (via AllocSeq), so device_seq is strictly monotonic
	// across all of this device's reliable events and never resets per
	// automation run.
	seqStore := openAgentSeqStore(cfg)
	if seqStore != nil {
		defer seqStore.Close()
	}
	nextSeq := seqAllocator(seqStore, cfg.Space)

	// Realtime uplink: opt-in feature flag (SITREP_REALTIME=1 or
	// config.json's realtime_enabled). One shared connection for the
	// whole resident agent process, mirroring the protocol's "at most one
	// connection per device" model (proto/realtime/SPEC.md section 9.4) —
	// unlike the one-shot `sitrep run` CLI, this process lives long
	// enough for a persistent WebSocket (or, when that transport is
	// unavailable, HTTP POST /v1/events — see internal/realtime/client)
	// connection to make sense. Default (flag off) leaves every
	// automation run on the HTTP POST /v1/events uplink, byte-for-byte as
	// before this feature existed.
	rt := newAgentRealtimeUplink(cfg, seqStore)

	lastRun := map[string]time.Time{}
	running := map[string]bool{}
	// consumedRunID is the last run_request_id this process has ALREADY acted
	// on, per automation, seeded from the machine-local, PERSISTED
	// LastConsumedRunRequestID (config.LocalAutomation) — never the stale
	// 10s-refreshed local config, and, per external review round 3's P0 fix,
	// never adopted from the server's current value on first sight either
	// (that adoption silently swallowed an offline run-now tap the moment
	// the agent finally started). Comparing against fresh in-memory state
	// (seeded from disk once per automation, then advanced in-process) is
	// what avoids the poll-skew double-fire; persisting it across restarts
	// is what makes an offline run-now tap correctly execute once the agent
	// comes back online — see automationDue and the two-phase claim/complete
	// recovery below.
	//
	// Run-now is at-least-once, not exactly-once: a crash between claiming
	// and completing a run_request_id causes a re-run on restart (see
	// pendingRecover below and config.MarkRunClaimed/MarkRunCompleted).
	// Automations are expected to be idempotent — the same assumption the
	// pause/resume/stop command-ack fix relies on (v1-architecture.md §1.4).
	consumedRunID := map[string]int64{}
	// pendingRecover holds, per automation, a claimed-but-not-completed
	// run_request_id discovered in local state at the moment this process
	// first sees that automation — evidence a previous process crashed
	// mid-run. Consumed (deleted) the first time the scheduling loop below
	// re-fires that automation.
	pendingRecover := map[string]int64{}
	seenAuto := map[string]bool{}
	finished := make(chan string, 32)
	var automations []api.Automation
	localCmds := map[string]config.LocalAutomation{}
	nextFetch := time.Time{}

	for {
	DrainFinished:
		for {
			select {
			case id := <-finished:
				running[id] = false
			default:
				break DrainFinished
			}
		}
		if time.Now().After(nextFetch) {
			// Polling GET /v1/automations as a source device IS the v1 agent
			// presence heartbeat (server stamps agent_last_seen) — there is no
			// separate ?agent=1 ping.
			if current, err := client.AutomationsAsAgent(); err == nil {
				automations = current
				// The executor command is machine-local in v1; join it in by
				// automation_id.
				localCmds = config.LoadLocalAutomations()
				for _, automation := range current {
					if seenAuto[automation.AutomationID] {
						continue
					}
					seenAuto[automation.AutomationID] = true
					la, hasLocal := localCmds[automation.AutomationID]
					// Seed the schedule's in-memory last-run from the local store
					// so a fresh agent process doesn't re-fire everything on
					// restart.
					if hasLocal && la.LastRunAt > 0 {
						lastRun[automation.AutomationID] = time.UnixMilli(la.LastRunAt)
					}
					// Seed last-consumed from PERSISTED local state, defaulting to 0
					// (the zero value of an absent/never-seen entry) — NEVER adopted
					// from the server's current run_request_id (P0, external review
					// round 3): that adoption treated any run-now tap that happened
					// while this device was offline as "already handled," silently
					// swallowing it forever. Defaulting to 0 means a server
					// run_request_id > 0 for an automation this device has no local
					// record of running is correctly seen as due below.
					consumedRunID[automation.AutomationID] = la.LastConsumedRunRequestID
					// Crash recovery: a persisted claim with no matching completion
					// means a previous process was interrupted mid-run (killed,
					// crashed) — re-run it (at-least-once bias; see the doc comment
					// on consumedRunID above).
					if hasLocal && la.ClaimedRunRequestID > la.CompletedRunRequestID {
						pendingRecover[automation.AutomationID] = la.ClaimedRunRequestID
					}
				}
			} else {
				fmt.Fprintf(os.Stderr, "sitrep agent: fetch automations: %v\n", err)
			}
			nextFetch = time.Now().Add(10 * time.Second)
		}

		for _, automation := range automations {
			id := automation.AutomationID
			la, hasCmd := localCmds[id]
			if !automation.Active() || !hasCmd || len(la.Command) == 0 || running[id] {
				continue
			}
			due, runRequested := automationDue(automation, consumedRunID[id], lastRun[id], time.Now())
			runID := automation.RunRequestID
			// A pending crash recovery (claimed but never completed by a
			// previous, interrupted process) always takes priority and is
			// treated exactly like a fresh run-now request — see
			// pendingRecover's doc comment above.
			if recoverID, needsRecover := pendingRecover[id]; needsRecover {
				due, runRequested, runID = true, true, recoverID
			}
			if !due {
				continue
			}
			if runRequested {
				// Two-phase claim (persisted, P0): write BEFORE executing so a
				// crash mid-run is detected and retried on the next startup —
				// see config.MarkRunClaimed / the crash-recovery block above.
				// Consuming runID up front (in memory) also stops a short run
				// from re-tripping the same id on the next 2s poll.
				//
				// If the persist itself fails (e.g. automations.json is on a
				// permanently unwritable disk), we log and proceed in-memory
				// anyway: consumedRunID/lastRun/running are still updated
				// below, so this process won't re-run it. But since the claim
				// never landed on disk, a crash during the run that follows
				// leaves no on-disk evidence, so the next process restart
				// won't detect a stranded claim either — this run_request_id
				// re-executes on every agent restart until the disk recovers.
				// Accepted under the documented at-least-once/idempotent-
				// automations contract: automations must tolerate redundant
				// re-execution, so retrying on restart is the safe failure
				// mode here, not a silently dropped run.
				if err := config.MarkRunClaimed(id, runID); err != nil {
					fmt.Fprintf(os.Stderr, "sitrep agent: automation %q: persist run claim: %v\n", automation.Name, err)
				}
				consumedRunID[id] = runID
				delete(pendingRecover, id)
			}
			lastRun[id] = time.Now()
			running[id] = true
			command := la.Command
			go func(automation api.Automation, command []string, runRequested bool, runID int64) {
				runAutomation(automation, command, rt, nextSeq, runRequested, runID)
				finished <- automation.AutomationID
			}(automation, command, runRequested, runID)
		}

		time.Sleep(2 * time.Second)
	}
}

// automationDue reports whether an automation should run now, and whether
// that decision was driven by a viewer "run now" request (vs. the schedule).
// A run is requested when the server's monotonic run_request_id is strictly
// greater than the last value THIS process has consumed
// (v1-architecture.md §5.1) — no wall-clock, no stale-config comparison, so
// a given run_request_id fires at most once per fresh comparison. The
// caller (cmdAgent) is what makes the overall system at-least-once rather
// than exactly-once: consumedRunID is seeded from PERSISTED local state
// (defaulting to 0, never adopted from the server) rather than in-memory
// state alone, and a process crash between claiming and completing a run
// causes a re-run on the next startup — see config.LocalAutomation and
// cmd/sitrep/agent.go's crash-recovery block (P0, external review round 3).
// Otherwise it fires on the schedule interval elapsing since the in-memory
// lastRun.
func automationDue(a api.Automation, consumedRunID int64, lastRun, now time.Time) (due, runRequested bool) {
	if a.RunRequestID > consumedRunID {
		return true, true
	}
	if now.Sub(lastRun) >= time.Duration(a.Schedule.EverySeconds)*time.Second {
		return true, false
	}
	return false, false
}

// openAgentSeqStore opens the shared persistent device_seq allocator for the
// resident agent, or returns nil when the device isn't provisioned to send
// reliable events (no device_id/space).
func openAgentSeqStore(cfg config.Config) *outbox.Store {
	if cfg.DeviceID == "" || cfg.Space == "" {
		return nil
	}
	store, err := outbox.OpenWithMaxRows(config.RealtimeOutboxPath(), cfg.OutboxMaxRows)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sitrep agent: device_seq outbox: %v; reliable telemetry may be dropped\n", err)
		// NEW (pre-launch, v1-architecture.md §14): surface the open failure
		// to health.d/outbox_open.json in addition to stderr — previously
		// this was a stderr-only gap the menubar could never see.
		_ = health.Report("outbox_open", "open local outbox: "+err.Error())
		return nil
	}
	_ = health.Clear("outbox_open")
	wireOutboxHealth(store)
	return store
}

// newAgentRealtimeUplink builds the shared realtime client for the resident
// agent process when config.RealtimeEnabled is set (and a device_seq store
// opened), or returns nil (the safe, fully-backward-compatible default). It
// reuses the caller's shared store rather than opening its own, so the
// Client's Enqueue and the best-effort uplinks' AllocSeq draw device_seq
// from the same counter. A nil *rtclient.Client is nil-safe everywhere it's
// passed.
func newAgentRealtimeUplink(cfg config.Config, store *outbox.Store) *rtclient.Client {
	if !cfg.RealtimeEnabled {
		return nil
	}
	if cfg.Server == "" || cfg.DeviceID == "" || cfg.Space == "" || store == nil {
		fmt.Fprintln(os.Stderr, "sitrep agent: realtime uplink enabled but server/device_id/space is not configured; staying on HTTP /v1/events")
		return nil
	}
	url := cfg.RealtimeURLFor()
	eventsURL := cfg.EventsURLFor()
	fmt.Fprintf(os.Stderr, "sitrep agent: realtime uplink enabled (%s, HTTP fallback %s)\n", url, eventsURL)
	return rtclient.New(rtclient.Config{
		URL:       url,
		EventsURL: eventsURL,
		Token:     cfg.Token,
		DeviceID:  cfg.DeviceID,
		Space:     cfg.Space,
		Logf: func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, "sitrep agent: realtime: "+format+"\n", args...)
		},
		Outbox:      store,
		OnCommand:   agentIgnoreTaskCommand,
		OnAuthState: authHealthHook,
	})
}

// agentIgnoreTaskCommand handles reverse-control commands (pause/resume/stop)
// that arrive over the resident agent's realtime WS. The agent runs
// AUTOMATIONS, not tasks — a task's pause/resume/stop is directed at the
// HTTP-only `sitrep run` process that actually owns that task's PID, which
// the agent cannot control. So the agent IGNORES the command cleanly WITHOUT
// acking or consuming it: with the server no longer marking a WS-relayed
// command delivered, an ignored command correctly stays available for the
// task owner's HTTP `for_task_id` drain to pick up. This makes the (correct)
// non-execution EXPLICIT and non-lossy, rather than a silent nil-handler
// drop. (If a future design has the agent itself execute tasks, this would
// apply the command and ack it instead — out of scope today.)
func agentIgnoreTaskCommand(cmd rtclient.CommandAction) {
	fmt.Fprintf(os.Stderr, "sitrep agent: ignoring task command %s (%s, task %q) — the agent runs automations, not tasks; leaving it for the task owner's HTTP drain\n",
		cmd.CommandID, cmd.Action, cmd.TaskID)
}

// runAutomation executes one round with a stable automation identity. Task
// lifecycle belongs to bounded `sitrep run`; scheduled work emits metrics and
// messages. command is the machine-local executor argv (v1 keeps it out of
// the server's shared automation state); rt is the shared realtime uplink
// (nil unless the feature flag is on); nextSeq is the shared persistent
// device_seq allocator used by the best-effort /v1/events uplink when
// realtime is off. runRequested/runID identify whether this execution is
// completing a run-now claim the caller already persisted with
// config.MarkRunClaimed (see cmdAgent) — if so, runID's matching
// config.MarkRunCompleted call here is the second half of the two-phase
// claim that makes crash recovery possible.
func runAutomation(automation api.Automation, command []string, rt *rtclient.Client, nextSeq func() (int64, error), runRequested bool, runID int64) {
	cfg := uplinkConfig("")
	cfg.Realtime = rt
	cfg.NextDeviceSeq = nextSeq
	up := uplink.New(cfg)
	defer up.Close()
	sourceID := "a" + automation.AutomationID
	emitAll := runner.MakeEmitter(up, sourceID, os.Getenv("SITREP_DEBUG") != "")
	emit := func(ev protocol.Event) {
		if ev.Kind == protocol.TaskStart || ev.Kind == protocol.TaskProgress || ev.Kind == protocol.TaskStep || ev.Kind == protocol.TaskDone || ev.Kind == protocol.TaskFail {
			return
		}
		emitAll(ev)
	}

	if _, err := runner.RunOnce(command, false, emit, nil); err != nil {
		fmt.Fprintf(os.Stderr, "sitrep agent: automation %q: %v\n", automation.Name, err)
	}
	whenMS := time.Now().UnixMilli()
	if runRequested {
		// Second half of the two-phase claim (config.MarkRunClaimed was
		// written before RunOnce started, in cmdAgent's scheduling loop).
		// Reached whether the executor succeeded or failed — a failed
		// executor still RAN TO COMPLETION, it did not crash mid-flight, so
		// it must not be retried forever; only a process crash/kill between
		// the claim and this call leaves claimed > completed for the next
		// startup's recovery to find and re-run.
		if err := config.MarkRunCompleted(automation.AutomationID, runID, whenMS); err != nil {
			fmt.Fprintf(os.Stderr, "sitrep agent: automation %q: persist run completion: %v\n", automation.Name, err)
		}
		return
	}
	// Schedule-driven run (not run-request-driven): only LastRunAt needs
	// stamping. Last-run is stamped machine-locally in v1 (the automations
	// PATCH route has no client-writable last_run field).
	_ = config.StampLocalRun(automation.AutomationID, whenMS)
}
