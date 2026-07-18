// Package runner executes commands and streams parsed protocol events.
package runner

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/protocol"
	"github.com/QuintinShaw/sitrep/daemon/internal/uplink"
)

// Emitter forwards a wire event; implementations decide transport.
type Emitter func(ev protocol.Event)

// RunOnce executes the command, passing stdout through while emitting parsed
// ::sitrep events. onStart (optional) receives the child pid once running —
// used to wire up reverse control. Returns the command's exit error (nil on
// success) and whether the script emitted an explicit task.done/task.fail.
func RunOnce(argv []string, passthrough bool, emit Emitter, onStart func(pid int)) (explicitEnd bool, err error) {
	return RunFull(argv, nil, passthrough, emit, onStart, nil)
}

// RunOnceEnv is RunOnce with explicit extra environment variables (KEY=VALUE).
func RunOnceEnv(argv, extraEnv []string, passthrough bool, emit Emitter, onStart func(pid int)) (explicitEnd bool, err error) {
	return RunFull(argv, extraEnv, passthrough, emit, onStart, nil)
}

// RunFull adds onOutput, which receives every NON-protocol stdout line —
// the task detail view's log tail.
func RunFull(argv, extraEnv []string, passthrough bool, emit Emitter, onStart func(pid int), onOutput func(string)) (explicitEnd bool, err error) {
	cmd := exec.Command(argv[0], argv[1:]...)
	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
	cmd.Stdin = os.Stdin
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return false, err
	}
	if err := cmd.Start(); err != nil {
		return false, err
	}
	if onStart != nil {
		onStart(cmd.Process.Pid)
	}
	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if passthrough {
			fmt.Println(line)
		}
		ev, ok, perr := protocol.ParseLine(line)
		if !ok {
			if onOutput != nil {
				onOutput(line)
			}
			continue
		}
		if perr != nil {
			continue
		}
		if ev.Kind == protocol.TaskDone || ev.Kind == protocol.TaskFail {
			explicitEnd = true
		}
		emit(ev)
	}
	return explicitEnd, cmd.Wait()
}

// MakeEmitter binds an uplink and source id into an Emitter, stamping
// timestamps and optionally echoing wire events to stderr for debugging.
func MakeEmitter(up *uplink.Uplink, sourceID string, debug bool) Emitter {
	return func(ev protocol.Event) {
		wire := uplink.Event{Event: ev, SourceID: sourceID, TS: time.Now().UTC().Format(time.RFC3339)}
		up.Offer(wire)
		if debug {
			fmt.Fprintf(os.Stderr, "sitrep> %+v\n", wire)
		}
	}
}
