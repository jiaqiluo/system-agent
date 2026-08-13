//go:build windows

package applyinator

import (
	"errors"
	"os"
	"os/exec"
	"sync"

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
// The job exists for exactly one purpose: to give killProcessTree a handle through which
// TerminateJobObject can reach the whole tree. It deliberately does NOT set
// JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE. That flag fires when the last handle closes, and
// releaseProcessTree closes this one at the end of *every* execute, successful ones included -- so
// setting it would silently stop a Windows instruction from leaving a background process behind,
// while a Unix one still can. Orphan reaping on the success path is not what this change is for,
// and the asymmetry would be a Windows-only behaviour change no test in this repo can exercise.
//
// Signalling the direct child alone is not enough. Plan instructions are near-universally a script
// that shells out to an installer or a package manager, and killing the script leaves the real work
// running.
func configureProcessGroup(cmd *exec.Cmd) error {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
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

// terminateProcessTree terminates the command's Job Object, exactly as killProcessTree does.
//
// Accepted caveat: Windows has no SIGTERM. There is no way to ask a process tree to shut down
// cleanly, so there is nothing a distinct graceful step could do and the instruction is never given
// the chance to clean up after itself. Cancelling therefore terminates the tree immediately, with
// no grace period: this is the previously settled "Windows: direct kill" behaviour, widened from
// the direct child to the whole tree. watchForTermination's grace wait has a process-exit arm, so
// terminating here short-circuits it rather than stalling on it.
func terminateProcessTree(cmd *exec.Cmd) error {
	return killProcessTree(cmd)
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
