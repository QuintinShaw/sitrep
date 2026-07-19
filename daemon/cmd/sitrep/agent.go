package main

import (
	"fmt"
	"os"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/api"
	"github.com/QuintinShaw/sitrep/daemon/internal/config"
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

	// Realtime uplink: opt-in feature flag (SITREP_REALTIME=1 or
	// config.json's realtime_enabled). One shared connection for the
	// whole resident agent process, mirroring the protocol's "at most one
	// connection per device" model (proto/realtime/SPEC.md section 9.4) —
	// unlike the one-shot `sitrep run` CLI, this process lives long
	// enough for a persistent WebSocket connection to make sense. Default
	// (flag off) leaves every automation run on the existing HTTP-only
	// path, byte-for-byte as before this feature existed.
	rt := newAgentRealtimeUplink()

	lastRun := map[string]time.Time{}
	running := map[string]bool{}
	finished := make(chan string, 32)
	var automations []api.Automation
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
			if current, err := client.AutomationsAsAgent(); err == nil {
				automations = current
				for _, automation := range current {
					if _, seen := lastRun[automation.ID]; !seen && automation.LastRun != "" {
						if t, err := time.Parse(time.RFC3339, automation.LastRun); err == nil {
							lastRun[automation.ID] = t
						}
					}
				}
			} else {
				fmt.Fprintf(os.Stderr, "sitrep agent: fetch automations: %v\n", err)
			}
			nextFetch = time.Now().Add(10 * time.Second)
		}

		for _, automation := range automations {
			if !automation.Enabled || len(automation.Command) == 0 || running[automation.ID] {
				continue
			}
			due := time.Since(lastRun[automation.ID]) >= time.Duration(automation.EveryS)*time.Second
			// Viewer-requested immediate run ("立即执行" on the phone).
			if !due && automation.RunRequestedAt != "" {
				if req, err := time.Parse(time.RFC3339, automation.RunRequestedAt); err == nil && req.After(lastRun[automation.ID]) {
					due = true
				}
			}
			if !due {
				continue
			}
			lastRun[automation.ID] = time.Now()
			running[automation.ID] = true
			go func(automation api.Automation) {
				runAutomation(client, automation, rt)
				finished <- automation.ID
			}(automation)
		}

		time.Sleep(2 * time.Second)
	}
}

// newAgentRealtimeUplink builds the shared realtime client for the resident
// agent process when config.RealtimeEnabled is set, or returns nil (the
// safe, fully-backward-compatible default). A nil *rtclient.Client is
// nil-safe everywhere it's passed (uplink.Config.Realtime nil disables the
// feature entirely, same as today).
func newAgentRealtimeUplink() *rtclient.Client {
	cfg := config.Load()
	if !cfg.RealtimeEnabled {
		return nil
	}
	if cfg.Server == "" || cfg.DeviceID == "" || cfg.Space == "" {
		fmt.Fprintln(os.Stderr, "sitrep agent: realtime uplink enabled but server/device_id/space is not configured; staying on HTTP ingest")
		return nil
	}
	store, err := outbox.OpenWithMaxRows(config.RealtimeOutboxPath(), cfg.OutboxMaxRows)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sitrep agent: realtime outbox: %v; staying on HTTP ingest\n", err)
		return nil
	}
	url := cfg.RealtimeURLFor()
	fmt.Fprintf(os.Stderr, "sitrep agent: realtime uplink enabled (%s)\n", url)
	return rtclient.New(rtclient.Config{
		URL:      url,
		Token:    cfg.Token,
		DeviceID: cfg.DeviceID,
		Space:    cfg.Space,
		Logf: func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, "sitrep agent: realtime: "+format+"\n", args...)
		},
		Outbox: store,
	})
}

// runAutomation executes one round with a stable automation identity. Task
// lifecycle belongs to bounded `sitrep run`; scheduled work emits metrics and
// messages. rt is the shared realtime uplink (nil unless the feature flag
// is on).
func runAutomation(client *api.Client, automation api.Automation, rt *rtclient.Client) {
	cfg := uplinkConfig("")
	cfg.Realtime = rt
	up := uplink.New(cfg)
	defer up.Close()
	sourceID := "a" + automation.ID
	emitAll := runner.MakeEmitter(up, sourceID, os.Getenv("SITREP_DEBUG") != "")
	emit := func(ev protocol.Event) {
		if ev.Kind == protocol.TaskStart || ev.Kind == protocol.TaskProgress || ev.Kind == protocol.TaskStep || ev.Kind == protocol.TaskDone || ev.Kind == protocol.TaskFail {
			return
		}
		emitAll(ev)
	}

	if _, err := runner.RunOnce(automation.Command, false, emit, nil); err != nil {
		fmt.Fprintf(os.Stderr, "sitrep agent: automation %q: %v\n", automation.Name, err)
	}
	_ = client.StampAutomationRun(automation.ID, time.Now())
}
