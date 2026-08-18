//go:build !windows

package applyinator

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// configureProcessGroup puts the command in its own process group so that all of its
// descendants can be signaled with a single call. It must be called before cmd.Start(),
// because SysProcAttr is only read when the process is forked.
//
// Signaling only the direct child is not enough. Plan instructions almost always invoke
// a run.sh script, which in turn runs an installer or package manager. Killing the shell
// can therefore leave the actual work running.
func configureProcessGroup(cmd *exec.Cmd) error {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return nil
}

// assignProcessTree is a no-op on Unix because configureProcessGroup already creates the process
// group during the fork. It exists to keep execute identical across platforms and is only meaningful
// on Windows.
func assignProcessTree(cmd *exec.Cmd) error {
	return nil
}

// terminateProcessTree asks the command's whole process group to shut down cleanly.
func terminateProcessTree(cmd *exec.Cmd) error {
	return signalProcessTree(cmd, syscall.SIGTERM)
}

// killProcessTree kills the command's whole process group unconditionally.
func killProcessTree(cmd *exec.Cmd) error {
	return signalProcessTree(cmd, syscall.SIGKILL)
}

// releaseProcessTree is a no-op on Unix because a process group holds no resources that need releasing.
func releaseProcessTree(cmd *exec.Cmd) {}

// signalProcessTree sends sig to the command's process group, falling back to the direct child
// if the process group cannot be safely signaled.
func signalProcessTree(cmd *exec.Cmd, sig syscall.Signal) error {
	if cmd.Process == nil {
		// Start failed or was never called, so there is nothing to signal.
		return nil
	}

	// DO NOT simplify this to `if err != nil`. The pgid check is the load-bearing part.
	//
	// Only signal the group when the child is actually its own group leader, which is what Setpgid
	// guarantees. If configureProcessGroup failed, the child inherits the daemon's process group.
	// In that case Getpgid succeeds and returns the agent's pgid, so kill(-pgid, SIGKILL) would signal
	// rancher-system-agent and every other process it started. Cancelling one plan could therefore
	// kill the agent. Falling back to a direct-child signal is far safer than letting the root daemon
	// kill itself.
	//
	// Deliberately not handled: Getpgid and Kill are raw syscalls, so unlike os.Process.Signal they
	// provide no pid-reuse protection. The watchdog can be in this function while cmd.Wait() reaps
	// the child on another goroutine; stop() closes done only afterward and then waits on <-finished.
	// A pid could therefore be recycled in that window and receive the signal instead. Closing that
	// race would require wrapping the entire pid-space operation in microseconds; the group-wide
	// signaling that requires these raw syscalls is considered worth the trade-off.
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil || pgid != cmd.Process.Pid {
		return ignoreProcessGone(cmd.Process.Signal(sig))
	}
	return ignoreProcessGone(syscall.Kill(-pgid, sig))
}

// ignoreProcessGone treats "no such process" as success. The watchdog can race with the instruction
// exiting on its own, and an already-terminated process tree means the requested outcome has already
// been achieved rather than representing an error worth reporting.
func ignoreProcessGone(err error) error {
	if errors.Is(err, syscall.ESRCH) || errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}
