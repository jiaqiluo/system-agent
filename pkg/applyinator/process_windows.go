//go:build windows

package applyinator

import (
	"errors"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"unsafe"

	"github.com/sirupsen/logrus"
	"golang.org/x/sys/windows"
)

// processJob is the per-command Windows state the process-tree helpers share.
type processJob struct {
	handle windows.Handle
	// assigned records whether the child actually made it into the job. If assignProcessTree
	// failed the job is empty, and terminating it would kill nothing, so killProcessTree has to
	// fall back to the direct child instead.
	assigned bool
}

// processJobs holds the Job Object created for each running command, keyed by the *exec.Cmd it was
// created for. *exec.Cmd has nowhere to carry a platform handle, and the five process-tree helpers
// have to keep identical signatures on both platforms so that the body of execute stays
// platform-independent, so the state is parked here for the window between configureProcessGroup
// and releaseProcessTree.
var processJobs sync.Map

// configureProcessGroup creates a Job Object for the command so that everything it spawns can be
// terminated in one call. It must be called before cmd.Start(), because Windows requires the job to
// exist before there is a process to assign to it; assignProcessTree does the assignment afterwards.
//
// Signalling the direct child alone is not enough. Plan instructions are near-universally a script
// that shells out to an installer or a package manager, and killing the script leaves the real work
// running.
func configureProcessGroup(cmd *exec.Cmd) error {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return err
	}

	// KILL_ON_JOB_CLOSE means nothing outlives the job: when releaseProcessTree closes the last
	// handle to it, any process still inside is terminated. Note that this also applies on the
	// success path, so a Windows instruction cannot leave a detached background process behind.
	info := &windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	info.BasicLimitInformation.LimitFlags |= windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	_, err = windows.SetInformationJobObject(
		job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(info)),
		uint32(unsafe.Sizeof(*info)),
	)
	// info reaches the kernel only as a uintptr, which the garbage collector does not treat as a
	// live reference, so it has to be kept alive explicitly across the call.
	runtime.KeepAlive(info)
	if err != nil {
		_ = windows.CloseHandle(job)
		return err
	}

	processJobs.Store(cmd, processJob{handle: job})
	return nil
}

// assignProcessTree puts the started child into the Job Object that configureProcessGroup created.
// It must be called after a successful cmd.Start().
//
// Accepted caveat: os/exec exposes no pre-Start hook and no handle to the child's primary thread,
// so the CREATE_SUSPENDED -> assign -> ResumeThread sequence that would close the gap is
// unavailable. A descendant spawned in the microseconds between cmd.Start() and this call escapes
// the job and will survive a cancellation. Accepted; out of scope.
func assignProcessTree(cmd *exec.Cmd) error {
	job, ok := lookupProcessJob(cmd)
	if !ok || cmd.Process == nil {
		// configureProcessGroup failed, or Start never produced a process. Either way there is
		// nothing to assign, and cancel degrades to the direct-child kill in killProcessTree.
		return nil
	}

	var assignErr error
	// WithHandle rather than OpenProcess(pid): the handle is guaranteed to refer to this process
	// for the duration of the callback, so it cannot race with pid reuse, and it needs no access
	// rights of its own.
	if err := cmd.Process.WithHandle(func(handle uintptr) {
		assignErr = windows.AssignProcessToJobObject(job.handle, windows.Handle(handle))
	}); err != nil {
		return err
	}
	if assignErr != nil {
		return assignErr
	}

	// Replacing the whole value rather than mutating it in place keeps the sync.Map the only
	// synchronisation this state needs.
	job.assigned = true
	processJobs.Store(cmd, job)
	return nil
}

// terminateProcessTree does nothing on Windows.
//
// Accepted caveat: Windows has no SIGTERM. There is no way to ask a process tree to shut down
// cleanly, so no graceful signal is sent and the instruction is never given the chance to clean up
// after itself. The watchdog's grace period therefore elapses without anything being asked of the
// instruction, and killProcessTree then terminates the job outright.
func terminateProcessTree(cmd *exec.Cmd) error {
	return nil
}

// killProcessTree terminates every process in the command's Job Object.
func killProcessTree(cmd *exec.Cmd) error {
	if job, ok := lookupProcessJob(cmd); ok && job.assigned {
		return windows.TerminateJobObject(job.handle, 1)
	}

	// There is no job, or the child never made it into one, so terminating the job would kill
	// nothing. Fall back to the direct child so that cancel degrades to a single-process kill
	// rather than doing nothing at all.
	if cmd.Process == nil {
		return nil
	}
	return ignoreProcessGone(cmd.Process.Kill())
}

// releaseProcessTree closes the command's Job Object handle and forgets it. It is safe to call when
// no job was ever recorded, and safe to call repeatedly: LoadAndDelete makes the close happen at
// most once.
func releaseProcessTree(cmd *exec.Cmd) {
	value, ok := processJobs.LoadAndDelete(cmd)
	if !ok {
		return
	}
	job, ok := value.(processJob)
	if !ok {
		return
	}
	if err := windows.CloseHandle(job.handle); err != nil {
		logrus.Warnf("[applyinator] error closing the job object handle: %v", err)
	}
}

func lookupProcessJob(cmd *exec.Cmd) (processJob, bool) {
	value, ok := processJobs.Load(cmd)
	if !ok {
		return processJob{}, false
	}
	job, ok := value.(processJob)
	return job, ok
}

// ignoreProcessGone maps "the process is already gone" onto success. The watchdog races with the
// instruction exiting under its own power, and a process tree that is already gone is the outcome
// the caller asked for rather than a failure worth reporting.
func ignoreProcessGone(err error) error {
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}
