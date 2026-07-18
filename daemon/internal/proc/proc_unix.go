//go:build unix

package proc

import (
	"os"
	"syscall"
	"time"
)

// New returns the Unix controller (darwin, linux).
func New() Controller { return unixController{} }

type unixController struct{}

func (unixController) Pause(pid int) error  { return syscall.Kill(pid, syscall.SIGSTOP) }
func (unixController) Resume(pid int) error { return syscall.Kill(pid, syscall.SIGCONT) }

func (unixController) Stop(pid int, gracePeriodSeconds int) error {
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		return err
	}
	deadline := time.Now().Add(time.Duration(gracePeriodSeconds) * time.Second)
	for time.Now().Before(deadline) {
		// Signal 0 probes for existence without sending anything.
		if err := syscall.Kill(pid, 0); err != nil {
			return nil // already gone
		}
		time.Sleep(200 * time.Millisecond)
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return nil
	}
	return p.Kill()
}
