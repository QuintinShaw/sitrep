package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/api"
	"github.com/QuintinShaw/sitrep/daemon/internal/config"
)

// cmdAutomation registers scheduled local executors. The server stores only
// the shared control plane; execution still happens on this computer.
func cmdAutomation(args []string) {
	if len(args) == 0 {
		automationUsage()
	}
	switch args[0] {
	case "add":
		automationAdd(args[1:])
	case "list", "ls":
		automationList()
	case "rm", "remove":
		if len(args) < 2 {
			automationUsage()
		}
		client, err := api.FromConfig()
		if err != nil {
			fatal(err)
		}
		if err := client.DeleteAutomation(args[1]); err != nil {
			fatal(err)
		}
		// Drop the machine-local executor command too (v1 keeps the command
		// out of the server's shared automation state).
		_ = config.DeleteLocalAutomation(args[1])
		fmt.Println("removed", args[1])
	case "run":
		if len(args) < 2 {
			automationUsage()
		}
		client, err := api.FromConfig()
		if err != nil {
			fatal(err)
		}
		// Field-stamp trigger (NOT a command): sets run_requested_at; the
		// resident agent runs it on its next poll (v1-architecture.md §5.1).
		if err := client.RunAutomation(args[1]); err != nil {
			fatal(err)
		}
		fmt.Println("run requested for", args[1])
	default:
		automationUsage()
	}
}

func automationUsage() {
	fmt.Fprintln(os.Stderr, `usage:
  sitrep automation add --name <n> --executor <script|agent|hybrid> --every <30s|5m|1h> -- <command> [args...]
  sitrep automation list
  sitrep automation rm <id>
  sitrep automation run <id>`)
	os.Exit(2)
}

func automationAdd(args []string) {
	name := ""
	executorKind := "script"
	every := time.Minute
	var argv []string
	for len(args) > 0 {
		switch {
		case args[0] == "--name" && len(args) > 1:
			name = args[1]
			args = args[2:]
		case args[0] == "--executor" && len(args) > 1:
			executorKind = args[1]
			args = args[2:]
		case args[0] == "--every" && len(args) > 1:
			d, err := parseEvery(args[1])
			if err != nil {
				fatal(err)
			}
			every = d
			args = args[2:]
		case args[0] == "--":
			argv = args[1:]
			args = nil
		default:
			argv = args
			args = nil
		}
	}
	if executorKind != "script" && executorKind != "agent" && executorKind != "hybrid" {
		fatal(fmt.Errorf("bad --executor %q", executorKind))
	}
	if len(argv) == 0 {
		automationUsage()
	}
	if name == "" {
		name = argv[0]
	}
	client, err := api.FromConfig()
	if err != nil {
		fatal(err)
	}
	automation, err := client.AddAutomation(name, executorKind, int(every.Seconds()))
	if err != nil {
		fatal(err)
	}
	// The executor command is machine-local in v1 (not part of the server's
	// shared automation state) — store it keyed by the server-assigned id so
	// the resident agent can join schedule/state with the local command.
	if err := config.SaveLocalAutomation(automation.AutomationID, config.LocalAutomation{
		Command:      argv,
		ExecutorKind: executorKind,
		Name:         name,
	}); err != nil {
		fatal(fmt.Errorf("save local command: %w", err))
	}
	fmt.Printf("registered %q (id %s) · %s · every %s\n", automation.Name, automation.AutomationID, executorKind, every)
}

func parseEvery(s string) (time.Duration, error) {
	if n, err := strconv.Atoi(s); err == nil {
		return time.Duration(n) * time.Second, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < 5*time.Second {
		return 0, fmt.Errorf("bad --every %q (min 5s)", s)
	}
	return d, nil
}

func automationList() {
	client, err := api.FromConfig()
	if err != nil {
		fatal(err)
	}
	automations, err := client.Automations()
	if err != nil {
		fatal(err)
	}
	if len(automations) == 0 {
		fmt.Println("no automations registered")
		return
	}
	// Last-run is tracked machine-locally in v1 (the daemon no longer stamps
	// it server-side); prefer the local value, falling back to the server's
	// last_run_at when present.
	local := config.LoadLocalAutomations()
	for _, automation := range automations {
		state := automation.State
		if state == "" {
			state = "active"
		}
		lastRunMS := automation.LastRunAt
		if la, ok := local[automation.AutomationID]; ok && la.LastRunAt > lastRunMS {
			lastRunMS = la.LastRunAt
		}
		last := "never"
		if lastRunMS > 0 {
			last = time.Since(time.UnixMilli(lastRunMS)).Round(time.Second).String() + " ago"
		}
		fmt.Printf("%s  %-20s %-7s every %-6s %-8s last run %s\n",
			automation.AutomationID, automation.Name, automation.ExecutorKind,
			(time.Duration(automation.Schedule.EverySeconds) * time.Second).String(), state, last)
	}
}
