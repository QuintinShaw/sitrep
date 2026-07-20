package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/config"
	"github.com/QuintinShaw/sitrep/daemon/internal/protocol"
	rtclient "github.com/QuintinShaw/sitrep/daemon/internal/realtime/client"
	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/outbox"
	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/wire"
	"github.com/QuintinShaw/sitrep/daemon/internal/uplink"
)

// cmdReport emits a single task lifecycle/progress event and exits. It is
// the reporting entry point external adapters (the Claude Code hook) call
// instead of hand-rolling raw HTTP: going through this command means
// device_seq allocation, the durable outbox, and the health signal are all
// owned by one place, not reimplemented (and silently broken) in shell.
//
// The event is routed through the SAME durable local outbox + persistent
// device_seq allocator `sitrep run`'s realtime uplink and `sitrep agent`
// share (internal/realtime/outbox, internal/realtime/client) — regardless of
// config.RealtimeEnabled — so a network blip, a 5xx, or this process being
// killed mid-flush cannot silently drop a hook-reported event (pre-launch
// fix, external review round 3): the event is durable the instant
// outbox.Store.Enqueue returns, before this process ever touches the
// network. See reportDurable.
//
//	sitrep report --task <id> --kind <started|progress|step|done|failed> \
//	    [--title s] [--step s] [--text s] [--percent n]
func cmdReport(args []string) {
	taskID, kind, title, step, text := "", "", "", "", ""
	percent := -1
	for len(args) > 0 {
		switch {
		case args[0] == "--task" && len(args) > 1:
			taskID, args = args[1], args[2:]
		case args[0] == "--kind" && len(args) > 1:
			kind, args = args[1], args[2:]
		case args[0] == "--title" && len(args) > 1:
			title, args = args[1], args[2:]
		case args[0] == "--step" && len(args) > 1:
			step, args = args[1], args[2:]
		case args[0] == "--text" && len(args) > 1:
			text, args = args[1], args[2:]
		case args[0] == "--percent" && len(args) > 1:
			n, err := strconv.Atoi(args[1])
			if err != nil {
				fatal(fmt.Errorf("bad --percent %q", args[1]))
			}
			percent, args = n, args[2:]
		default:
			reportUsage()
		}
	}
	if taskID == "" {
		reportUsage()
	}
	protoKind := reportKind(kind)
	if protoKind == "" {
		reportUsage()
	}

	cfg := config.Load()
	if cfg.Server == "" {
		return // not configured — never an error (mirrors the hook's non-blocking contract)
	}

	seqStore, err := openSeqStore(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sitrep report: device_seq store: %v\n", err)
	}
	if seqStore != nil {
		defer seqStore.Close()
	}

	if seqStore != nil && cfg.DeviceID != "" && cfg.Space != "" {
		reportDurable(cfg, seqStore, taskID, protoKind, title, step, text, percent)
		return
	}

	// Fallback: no local outbox to enqueue into (either it failed to open —
	// already logged/health-reported above — or this device isn't
	// provisioned with a device_id/space yet). Best-effort HTTP only, same
	// as before this fix, so an unconfigured/degraded device doesn't hard
	// fail; there is nothing durable to route through in either case.
	//
	// The durable-outbox guarantee (report.go/reportDurable, above) only
	// holds while the outbox is openable in the first place; this fallback
	// is the documented degraded mode when it isn't. That degradation is
	// never silent: openSeqStore already reported the open failure to
	// health.d/outbox_open.json (see main.go's openSeqStore), so the
	// menubar/health surface reflects reduced durability even though this
	// call site itself just does a best-effort HTTP send.
	ucfg := uplinkConfig("") // one-shot report: no reverse-control channel
	ucfg.ForTaskID = taskID
	ucfg.NextDeviceSeq = seqAllocator(seqStore, cfg.Space)

	up := uplink.New(ucfg)
	ev := uplink.Event{
		Event:    protocol.Event{Kind: protoKind, Title: title, Step: step, Text: text},
		SourceID: taskID,
		TS:       time.Now().UTC().Format(time.RFC3339),
	}
	if protoKind == protocol.TaskProgress && percent >= 0 {
		ev.Percent = percent
	}
	up.Offer(ev)
	up.Close() // flushes the pending event before returning
}

// reportDurable is cmdReport's durable path (pre-launch fix, external review
// round 3): it durably enqueues the hook-reported event directly into the
// local outbox (outbox.Store.Enqueue — the SAME store and device_seq counter
// `sitrep run`'s realtime uplink and `sitrep agent` share), THEN makes one
// prompt HTTP delivery attempt (rtclient.FlushOnce) before returning.
//
// Unlike the pre-fix best-effort path (uplink.Uplink's bounded in-memory
// HTTP retry, silently dropped on final failure), a network blip or this
// process being killed mid-flush leaves the event safely durable on disk —
// by the time Enqueue returns, it survives a crash. It is picked up by the
// next process that touches this device's outbox (another `sitrep report`,
// `sitrep run`, or a `sitrep agent` flush), never silently lost.
func reportDurable(cfg config.Config, store *outbox.Store, taskID string, kind protocol.Kind, title, step, text string, percent int) {
	ctx := context.Background()
	realtimeKind := reportRealtimeKind(kind)
	if realtimeKind == "" {
		return // unreachable: cmdReport already validated kind via reportKind
	}
	var pct *int
	if kind == protocol.TaskProgress && percent >= 0 {
		p := percent
		pct = &p
	}
	// task.progress is continuous/last-value-wins, same coalescing policy
	// the realtime Client's SendTaskEvent applies (internal/realtime/client),
	// so successive progress reports for the same task never pile up in
	// overflow if the outbox is ever at capacity.
	coalesceKey := ""
	if realtimeKind == "progress" {
		coalesceKey = "task.progress:" + taskID
	}
	_, err := store.EnqueueCoalesce(ctx, cfg.Space, wire.TypeTaskEvent, coalesceKey, func(seq int64) (json.RawMessage, error) {
		body := wire.TaskEventBody{
			DeviceID:   cfg.DeviceID,
			DeviceSeq:  seq,
			TaskID:     taskID,
			Kind:       realtimeKind,
			OccurredAt: time.Now().UnixMilli(),
			Title:      title,
			Percent:    pct,
			Step:       step,
			Message:    text,
		}
		if verr := body.Validate(); verr != nil {
			return nil, fmt.Errorf("%w: %v", rtclient.ErrInvalidBody, verr)
		}
		return json.Marshal(body)
	})
	if err != nil && !errors.Is(err, outbox.ErrOverflowed) {
		// Enqueue itself failed (its own bounded internal retries exhausted —
		// a persistent local-disk failure): nothing was durably stored
		// anywhere. Nothing to flush.
		fmt.Fprintf(os.Stderr, "sitrep report: enqueue: %v\n", err)
		return
	}
	// ErrOverflowed is informational (§ outbox.ErrOverflowed doc comment):
	// the event is already durable, just not yet seq-bearing. Either way,
	// attempt one prompt delivery — FlushOnce also promotes overflow first.
	rtclient.FlushOnce(ctx, rtclient.Config{
		EventsURL:   cfg.EventsURLFor(),
		Token:       cfg.Token,
		DeviceID:    cfg.DeviceID,
		Space:       cfg.Space,
		Outbox:      store,
		OnAuthState: authHealthHook,
		Logf: func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, "sitrep report: "+format+"\n", args...)
		},
	})
}

// reportRealtimeKind maps a protocol.Kind (this CLI's task-event kinds) to
// the realtime-protocol task.event `kind` string
// (internal/realtime/wire.TaskEventBody.Kind).
func reportRealtimeKind(kind protocol.Kind) string {
	switch kind {
	case protocol.TaskStart:
		return "started"
	case protocol.TaskProgress:
		return "progress"
	case protocol.TaskStep:
		return "step"
	case protocol.TaskDone:
		return "done"
	case protocol.TaskFail:
		return "failed"
	default:
		return ""
	}
}

func reportKind(kind string) protocol.Kind {
	switch kind {
	case "started", "start":
		return protocol.TaskStart
	case "progress":
		return protocol.TaskProgress
	case "step":
		return protocol.TaskStep
	case "done":
		return protocol.TaskDone
	case "failed", "fail":
		return protocol.TaskFail
	default:
		return ""
	}
}

func reportUsage() {
	fmt.Fprintln(os.Stderr, `usage:
  sitrep report --task <id> --kind <started|progress|step|done|failed> [--title s] [--step s] [--text s] [--percent n]`)
	os.Exit(2)
}
