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
//	sitrep join --space s --code c  join a space (headless machines)
package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/config"
	"github.com/QuintinShaw/sitrep/daemon/internal/proc"
	"github.com/QuintinShaw/sitrep/daemon/internal/protocol"
	"github.com/QuintinShaw/sitrep/daemon/internal/runner"
	"github.com/QuintinShaw/sitrep/daemon/internal/uplink"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "run":
		cmdRun(os.Args[2:])
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
  sitrep automation add|list|rm ...
  sitrep agent
  sitrep space create [--server url]
  sitrep invite [--role viewer|source]
  sitrep join [--server url] --space <id> --code <invite>`)
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
		CommandSource: commandSource,
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
	up := uplink.New(uplinkConfig(sourceID))
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
