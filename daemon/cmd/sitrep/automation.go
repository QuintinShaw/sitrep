package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/api"
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
		fmt.Println("removed", args[1])
	default:
		automationUsage()
	}
}

func automationUsage() {
	fmt.Fprintln(os.Stderr, `usage:
  sitrep automation add --name <n> --executor <script|agent|hybrid> --every <30s|5m|1h> -- <command> [args...]
  sitrep automation list
  sitrep automation rm <id>`)
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
	automation, err := client.AddAutomation(name, executorKind, argv, int(every.Seconds()))
	if err != nil {
		fatal(err)
	}
	fmt.Printf("registered %q (id %s) · %s · every %s\n", automation.Name, automation.ID, executorKind, every)
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
	for _, automation := range automations {
		state := "active"
		if !automation.Enabled {
			state = "paused"
		}
		last := "never"
		if automation.LastRun != "" {
			if t, err := time.Parse(time.RFC3339, automation.LastRun); err == nil {
				last = time.Since(t).Round(time.Second).String() + " ago"
			}
		}
		fmt.Printf("%s  %-20s %-7s every %-6s %-8s last run %s\n",
			automation.ID, automation.Name, automation.ExecutorKind,
			(time.Duration(automation.EveryS) * time.Second).String(), state, last)
	}
}
