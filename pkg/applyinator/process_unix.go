//go:build !windows

package applyinator

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// configureProcessGroup puts the command into a process group of its own so that everything it
// spawns can be signalled in one call. It must be called before cmd.Start(): SysProcAttr is only
// read at fork time.
//
// Signalling the direct child alone is not enough. Plan instructions are near-universally a run.sh
// that shells out to an installer or a package manager, and killing the shell leaves the real work
// running.
func configureProcessGroup(cmd *exec.Cmd) error {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return nil
}

// assignProcessTree is a no-op on Unix: configureProcessGroup already established the process group
// as part of the fork. It exists so that the body of execute is identical on every platform, and is
// load-bearing only on Windows.
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

// releaseProcessTree is a no-op on Unix: a process group holds no resources that need releasing.
func releaseProcessTree(cmd *exec.Cmd) {}

// signalProcessTree delivers sig to the command's process group, falling back to the direct child
// when the group cannot be established as safe to signal.
func signalProcessTree(cmd *exec.Cmd, sig syscall.Signal) error {
	if cmd.Process == nil {
		// Start failed or was never called, so there is nothing to signal.
		return nil
	}

	// DO NOT simplify this to `if err != nil`. The pgid check is the load-bearing half.
	//
	// Only signal the group when the child is genuinely its own group leader, which is exactly what
	// Setpgid guarantees. If configureProcessGroup failed, the child inherited *this daemon's*
	// process group -- and in that case Getpgid does not fail, it *succeeds* and returns the
	// agent's own pgid. kill(-pgid, SIGKILL) would then deliver the signal to rancher-system-agent
	// itself and to every other process it started, so cancelling one plan would kill the agent.
	// Degrading to a direct-child signal is far preferable to a root daemon killing itself.
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil || pgid != cmd.Process.Pid {
		return ignoreProcessGone(cmd.Process.Signal(sig))
	}
	return ignoreProcessGone(syscall.Kill(-pgid, sig))
}

// ignoreProcessGone maps "no such process" onto success. The watchdog races with the instruction
// exiting under its own power, and a process tree that is already gone is the outcome the caller
// asked for rather than a failure worth reporting.
func ignoreProcessGone(err error) error {
	if errors.Is(err, syscall.ESRCH) || errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}
