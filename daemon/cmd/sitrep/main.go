// Command sitrep is the agent-facing CLI. Humans use the menu bar / phone
// apps; agents and headless machines use this.
//
//	sitrep run [--title t] [--icon i] [--tint c] [--template t] -- <cmd>...
//	    one-shot task with a Live Activity
//	sitrep automation add|list|rm   register scheduled local executors
//	sitrep agent                    resident scheduler (supervised by the
//	    menu bar app; run under systemd on servers)
//	sitrep space create             mint an anonymous space (first run)
//	sitrep invite [--role source]   mint a device invite code
//	sitrep join --code c             join a space (headless machines);
//	    --space is optional — a self-routing connect code embeds it
package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/config"
	"github.com/QuintinShaw/sitrep/daemon/internal/health"
	"github.com/QuintinShaw/sitrep/daemon/internal/proc"
	"github.com/QuintinShaw/sitrep/daemon/internal/protocol"
	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/outbox"
	"github.com/QuintinShaw/sitrep/daemon/internal/runner"
	"github.com/QuintinShaw/sitrep/daemon/internal/uplink"
)

// wireOutboxHealth points the store's backpressure signal at the health file
// the menubar reads (5d): the durable footprint hitting its cap becomes a
// visible ok:false, and recovery clears it.
func wireOutboxHealth(store *outbox.Store) {
	if store == nil {
		return
	}
	store.SetHealthHook(func(healthy bool, reason string) {
		if healthy {
			_ = health.Clear("outbox")
			return
		}
		_ = health.Report("outbox", reason)
	})
}

// authHealthHook routes a persistent authentication failure (a revoked/
// expired token, surfaced by the realtime Client or the /v1/events uplink as
// a 401) to the health file, so a revoked device is visible in the menubar
// rather than failing silently (MINOR). Shared by both transports under one
// "auth" component so they don't clobber each other.
func authHealthHook(ok bool, reason string) {
	if ok {
		_ = health.Clear("auth")
		return
	}
	_ = health.Report("auth", reason)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "run":
		cmdRun(os.Args[2:])
	case "report":
		cmdReport(os.Args[2:])
	case "automation":
		cmdAutomation(os.Args[2:])
	case "agent":
		cmdAgent(os.Args[2:])
	case "space":
		if len(os.Args) > 2 && os.Args[2] == "create" {
			cmdSpaceCreate(os.Args[3:])
		} else {
			usage()
		}
	case "invite":
		cmdInvite(os.Args[2:])
	case "join":
		cmdJoin(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  sitrep run [--title t] [--icon i] [--tint c] [--template t] -- <command> [args...]
  sitrep report --task <id> --kind <started|progress|step|done|failed> [...]
  sitrep automation add|list|rm|run ...
  sitrep agent
  sitrep space create [--server url]
  sitrep invite [--role viewer|source]
  sitrep join [--server url] [--space <id>] --code <invite>`)
	os.Exit(2)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "sitrep:", err)
	os.Exit(1)
}

func uplinkConfig(commandSource string) uplink.Config {
	cfg := config.Load()
	return uplink.Config{
		ServerURL:     cfg.Server,
		Token:         cfg.Token,
		DeviceID:      cfg.DeviceID,
		Space:         cfg.Space,
		CommandSource: commandSource,
		OnAuthState:   authHealthHook,
	}
}

// openSeqStore opens the shared, persistent device_seq allocator — the
// realtime outbox's space_seq_counters, at the per-machine
// config.RealtimeOutboxPath(). Every uplink from this device (the realtime
// Client and the best-effort /v1/events uplink alike) draws its device_seq
// from this one counter, so sequence numbers are strictly monotonic across
// processes and never reset. Returns (nil, nil) when the device isn't
// provisioned to send reliable events (no device_id); the caller must Close
// a non-nil store.
func openSeqStore(cfg config.Config) (*outbox.Store, error) {
	if cfg.DeviceID == "" {
		return nil, nil
	}
	store, err := outbox.OpenWithMaxRows(config.RealtimeOutboxPath(), cfg.OutboxMaxRows)
	if err != nil {
		// NEW (pre-launch, v1-architecture.md §14): previously this failure
		// was only ever logged to stderr by each caller — no health signal at
		// all, so the menubar had no way to surface "the local outbox
		// couldn't even open." Report it to its own component file
		// (health.d/outbox_open.json) in addition to, not instead of, the
		// caller's stderr log, before the caller decides whether to exit or
		// continue degraded.
		_ = health.Report("outbox_open", "open local outbox: "+err.Error())
		return nil, err
	}
	_ = health.Clear("outbox_open")
	wireOutboxHealth(store)
	return store, nil
}

// seqAllocator returns a NextDeviceSeq func bound to store+space, or nil if
// store is nil (offline/unprovisioned — the uplink then can't send reliable
// events anyway, since their bodies need a device_id). The returned func
// surfaces an AllocSeq failure as an explicit, persistent health state (N1):
// when the device_seq DB becomes unwritable the daemon keeps the safe
// degraded mode (the uplink skips the event rather than sending a colliding
// seq) but writes ok:false so the menubar shows it — not just a log line.
func seqAllocator(store *outbox.Store, space string) func() (int64, error) {
	if store == nil {
		return nil
	}
	var degraded atomic.Bool
	return func() (int64, error) {
		seq, err := store.AllocSeq(context.Background(), space)
		if err != nil {
			if degraded.CompareAndSwap(false, true) {
				_ = health.Report("device_seq", "local telemetry storage unwritable")
			}
			return 0, err
		}
		if degraded.CompareAndSwap(true, false) {
			_ = health.Clear("device_seq")
		}
		return seq, nil
	}
}

type runFlags struct {
	title    string
	icon     string
	tint     string
	template string
	argv     []string
}

func parseRunFlags(args []string) runFlags {
	f := runFlags{}
	for len(args) > 0 {
		switch {
		case args[0] == "--title" && len(args) > 1:
			f.title = args[1]
			args = args[2:]
		case strings.HasPrefix(args[0], "--icon="):
			f.icon = strings.TrimPrefix(args[0], "--icon=")
			args = args[1:]
		case strings.HasPrefix(args[0], "--tint="):
			f.tint = strings.TrimPrefix(args[0], "--tint=")
			args = args[1:]
		case strings.HasPrefix(args[0], "--template="):
			f.template = strings.TrimPrefix(args[0], "--template=")
			args = args[1:]
		case args[0] == "--":
			f.argv = args[1:]
			args = nil
		default:
			f.argv = args
			args = nil
		}
	}
	if len(f.argv) == 0 {
		usage()
	}
	if f.title == "" {
		f.title = f.argv[0]
	}
	return f
}

func cmdRun(args []string) {
	f := parseRunFlags(args)
	sourceID := uplink.NewSourceID()

	// Persistent, cross-process device_seq: without it, this one-shot
	// process would restart the counter at 1 and its task.start would collide
	// with a previous `sitrep run`'s (the server's dedup is never pruned),
	// silently never appearing to any viewer.
	cfg := config.Load()
	seqStore, err := openSeqStore(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sitrep: device_seq store: %v; telemetry may be dropped\n", err)
	}
	if seqStore != nil {
		defer seqStore.Close()
	}

	ucfg := uplinkConfig(sourceID)
	ucfg.ForTaskID = sourceID // only drain/apply this task's reverse-control commands
	ucfg.NextDeviceSeq = seqAllocator(seqStore, cfg.Space)

	up := uplink.New(ucfg)
	emit := runner.MakeEmitter(up, sourceID, os.Getenv("SITREP_DEBUG") != "")

	emit(protocol.Event{
		Kind: protocol.TaskStart, Title: f.title,
		Icon: f.icon, Tint: f.tint, Template: f.template,
	})

	// Reverse control: commands arrive on the uplink's piggyback channel and
	// map onto the platform process controller.
	var stoppedByPhone atomic.Bool
	onStart := func(pid int) {
		if up == nil {
			return
		}
		go func() {
			ctl := proc.New()
			for cmd := range up.Commands {
				switch cmd {
				case "pause":
					if ctl.Pause(pid) == nil {
						emit(protocol.Event{Kind: protocol.TaskStep, Step: "⏸ paused from phone"})
					}
				case "resume":
					if ctl.Resume(pid) == nil {
						emit(protocol.Event{Kind: protocol.TaskStep, Step: "▶ resumed"})
					}
				case "stop":
					stoppedByPhone.Store(true)
					emit(protocol.Event{Kind: protocol.TaskFail, Text: "stopped from phone"})
					_ = ctl.Resume(pid) // a paused process can't handle SIGTERM
					_ = ctl.Stop(pid, 10)
				}
			}
		}()
	}

	explicitEnd, err := runner.RunFull(f.argv, nil, true, emit, onStart, func(line string) {
		up.LogLine(sourceID, line)
	})
	if stoppedByPhone.Load() {
		explicitEnd = true // task.fail already emitted by the stop handler
	}
	if !explicitEnd {
		if err != nil {
			emit(protocol.Event{Kind: protocol.TaskFail, Text: err.Error()})
		} else {
			emit(protocol.Event{Kind: protocol.TaskDone})
		}
	}
	up.Close()
	if err != nil && !stoppedByPhone.Load() {
		if ee, isExit := err.(*exec.ExitError); isExit {
			os.Exit(ee.ExitCode())
		}
		fmt.Fprintln(os.Stderr, "sitrep:", err)
		os.Exit(1)
	}
	_ = time.Now // keep time imported for future use
}
