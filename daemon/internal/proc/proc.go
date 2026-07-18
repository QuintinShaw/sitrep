// Package proc abstracts platform-specific process control.
//
// Pause semantics differ by OS: Unix uses SIGSTOP/SIGCONT; Windows has no
// equivalent signal and will use NtSuspendProcess / Job Objects. Callers
// depend only on Controller so v1 can ship darwin-only without painting the
// codebase into a corner.
package proc

import "errors"

// ErrUnsupported is returned on platforms where an operation is not yet
// implemented.
var ErrUnsupported = errors.New("proc: operation not supported on this platform")

// Controller pauses, resumes, and stops a running task process tree.
type Controller interface {
	// Pause freezes the process. Held network connections and locks may time
	// out while frozen; this is documented, accepted behavior.
	Pause(pid int) error
	// Resume unfreezes a paused process.
	Resume(pid int) error
	// Stop terminates gracefully (SIGTERM or platform equivalent), escalating
	// to a hard kill after gracePeriodSeconds.
	Stop(pid int, gracePeriodSeconds int) error
}
