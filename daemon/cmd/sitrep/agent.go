package main

import (
	"fmt"
	"os"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/api"
	"github.com/QuintinShaw/sitrep/daemon/internal/protocol"
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
				runAutomation(client, automation)
				finished <- automation.ID
			}(automation)
		}

		time.Sleep(2 * time.Second)
	}
}

// runAutomation executes one round with a stable automation identity. Task
// lifecycle belongs to bounded `sitrep run`; scheduled work emits metrics and
// messages.
func runAutomation(client *api.Client, automation api.Automation) {
	up := uplink.New(uplinkConfig(""))
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
