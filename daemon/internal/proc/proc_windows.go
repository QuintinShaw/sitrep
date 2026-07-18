//go:build windows

package proc

// New returns the Windows controller. Pause/Resume will be implemented with
// NtSuspendProcess/NtResumeProcess or Job Objects; not yet supported.
func New() Controller { return windowsController{} }

type windowsController struct{}

func (windowsController) Pause(pid int) error                       { return ErrUnsupported }
func (windowsController) Resume(pid int) error                      { return ErrUnsupported }
func (windowsController) Stop(pid int, gracePeriodSeconds int) error { return ErrUnsupported }
