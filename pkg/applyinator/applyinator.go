package applyinator

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	planapi "github.com/rancher/rancher/pkg/plan"
	"github.com/rancher/system-agent/pkg/image"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// Applyinator coordinates plan application and execution.
// It holds configuration and resources used during an apply.
type Applyinator struct {
	mu              *sync.Mutex
	workDir         string
	preserveWorkDir bool
	appliedPlanDir  string
	interlockDir    string
	imageUtil       *image.Utility
}

// CalculatedPlan holds a Plan and its checksum, and is passed into Applyinator.
type CalculatedPlan struct {
	Plan     planapi.Plan
	Checksum string
}

const appliedPlanFileSuffix = "-applied.plan"
const applyinatorDateCodeLayout = "20060102-150405"
const defaultCommand = "/run.sh"
const cattleAgentExecutionPwdEnvKey = "CATTLE_AGENT_EXECUTION_PWD"
const cattleAgentAttemptKey = "CATTLE_AGENT_ATTEMPT_NUMBER"
const planRetentionPolicyCount = 64
const restartPendingInterlockFile = "restart-pending"
const applyinatorActiveInterlockFile = "applyinator-active"
const restartPendingTimeout = 5 * time.Minute // Wait a maximum of 5 minutes before force-applying a plan if a restart is pending.
const deleteFileAction = "delete"

const defaultEffectivePeriod = 600 // 10 minutes
const defaultFailureCooldown = 6

// instructionTerminationGrace is how long a cancelled instruction's process tree is given to exit
// after a graceful signal before it is killed outright.
//
// A var rather than a const so that tests can shorten it: the escalation path, which is where the
// pipe close lives, is otherwise only reachable after a real ten-second wait.
var instructionTerminationGrace = 10 * time.Second

func NewApplyinator(workDir string, preserveWorkDir bool, appliedPlanDir, interlockDir string, imageUtil *image.Utility) *Applyinator {
	return &Applyinator{
		mu:              &sync.Mutex{},
		workDir:         workDir,
		preserveWorkDir: preserveWorkDir,
		appliedPlanDir:  appliedPlanDir,
		interlockDir:    interlockDir,
		imageUtil:       imageUtil,
	}
}

func CalculatePlan(rawPlan []byte) (CalculatedPlan, error) {
	p, err := planapi.Parse(rawPlan)
	if err != nil {
		return CalculatedPlan{}, err
	}
	return CalculatedPlan{
		Plan:     p,
		Checksum: planapi.Checksum(rawPlan),
	}, nil
}

// Interruption reports why an apply stopped short of completing normally.
type Interruption string

const (
	// InterruptionNone means the apply was not interrupted.
	InterruptionNone Interruption = ""
	// InterruptionPaused means the apply stopped at an instruction boundary and may be resumed.
	InterruptionPaused Interruption = "paused"
	// InterruptionCanceled means the apply was abandoned; the in-flight instruction was signaled.
	InterruptionCanceled Interruption = "canceled"
)

// checkInterruption reports which interruption, if any, is already pending. It never blocks, and
// a nil channel is never ready. Cancel is tested first: cancel wins over pause.
func checkInterruption(cancel, pause <-chan struct{}) Interruption {
	// A select over two ready channels picks pseudo-randomly, so cancel's precedence over pause
	// has to be an explicit prior check rather than case ordering.
	select {
	case <-cancel:
		return InterruptionCanceled
	default:
	}

	select {
	case <-pause:
		return InterruptionPaused
	default:
	}

	return InterruptionNone
}

type ApplyOutput struct {
	OneTimeOutput          []byte
	OneTimeApplySucceeded  bool
	PeriodicOutput         []byte
	PeriodicApplySucceeded bool
	// Interruption is InterruptionNone unless the apply stopped early.
	Interruption Interruption
	// CompletedOneTimeInstructions is an ABSOLUTE count over the plan's one-time instructions,
	// not a count of what this apply ran: the loop starts at
	// ApplyInput.ResumeFromOneTimeInstruction and reports index+1. A plan paused after
	// instruction 2, resumed, and paused again three instructions later therefore reports 5, not
	// 3 — successive pause/resume cycles compose instead of resetting.
	CompletedOneTimeInstructions int
}

type ApplyInput struct {
	CalculatedPlan             CalculatedPlan
	RunOneTimeInstructions     bool
	OneTimeInstructionAttempts int
	ReconcileFiles             bool
	ExistingOneTimeOutput      []byte
	ExistingPeriodicOutput     []byte
	// Cancel, when closed, abandons the apply as promptly as possible: the in-flight
	// instruction's context is cancelled and no further instruction is started. A nil channel
	// is never ready, so the zero value means "never cancelled".
	Cancel <-chan struct{}
	// Pause, when closed, stops the apply at the next instruction boundary. It never interrupts
	// a running instruction: a checkpoint is only trustworthy if every instruction below
	// ApplyOutput.CompletedOneTimeInstructions ran to completion. A nil channel is never ready.
	Pause <-chan struct{}
	// ResumeFromOneTimeInstruction is the index of the first one-time instruction to execute.
	// Instructions below it are treated as already complete and are not re-run. Zero (the zero
	// value) starts from the beginning.
	ResumeFromOneTimeInstruction int
}

// Apply reconciles the local system against input.CalculatedPlan: it honors the interlock, archives the plan,
// reconciles files, optionally runs one-time instructions, and always runs due periodic instructions. It returns
// the updated one-time and periodic outputs (gzip+JSON encoded) alongside their success flags. Notably,
// ApplyOutput.OneTimeApplySucceeded will be false if ApplyInput.RunOneTimeInstructions is false.
//
// An apply can be interrupted by the operator through input.Cancel and input.Pause. Cancel is prompt: it cancels
// the in-flight instruction's context and starts nothing further. Pause is a boundary: it never interrupts a
// running instruction, it only stops before the next one starts. Either way ApplyOutput.Interruption reports why
// the apply stopped and ApplyOutput.CompletedOneTimeInstructions records the resume checkpoint — the absolute
// number of one-time instructions known to have run to completion. Passing that value back as
// ApplyInput.ResumeFromOneTimeInstruction on a later apply resumes the plan without re-running them. An
// interrupted apply is a reported outcome, not a failure: the returned error stays nil.
func (a *Applyinator) Apply(ctx context.Context, input ApplyInput) (ApplyOutput, error) {
	logrus.Debugf("[applyinator] applying plan with checksum %s", input.CalculatedPlan.Checksum)
	output := ApplyOutput{
		OneTimeOutput:                input.ExistingOneTimeOutput,
		PeriodicOutput:               input.ExistingPeriodicOutput,
		CompletedOneTimeInstructions: input.ResumeFromOneTimeInstruction,
	}

	// This check has to happen before the lock, not merely before the file reconciliation: an apply that is
	// already cancelled must not queue behind an in-flight apply on the mutex, and must not sit in
	// checkInterlock's restart-pending wait for up to restartPendingTimeout only to return an error instead of
	// a clean InterruptionCanceled.
	//
	// It is a contract on ApplyInput, not a path production exercises today: pkg/k8splan's only caller hands
	// over channels its interrupt watch has just created and cannot have closed yet, and pkg/localplan passes
	// nil. Its coverage is the unit tests, so do not read production reachability into it — and do not delete
	// it on the strength of that, either. It is what makes "an already-interrupted ApplyInput is a reported
	// outcome, not a wait" true for any caller.
	if interruption := checkInterruption(input.Cancel, input.Pause); interruption != InterruptionNone {
		logrus.Infof("[applyinator] not applying plan with checksum %s: %s before the apply started", input.CalculatedPlan.Checksum, interruption)
		output.Interruption = interruption
		return output, nil
	}

	logrus.Tracef("[applyinator] applying plan - attempting to get lock")
	a.mu.Lock()
	logrus.Tracef("[applyinator] applying plan - lock achieved")
	defer a.mu.Unlock()
	now := time.Now()
	nowString := now.Format(applyinatorDateCodeLayout)

	cleanupInterlock, err := a.checkInterlock(now)
	if err != nil {
		return output, err
	}
	defer cleanupInterlock()

	// execCtx is the context handed to instruction execution. It is cancelled when input.Cancel closes, which
	// is what makes a cancel prompt rather than a boundary.
	execCtx, cancelExec := context.WithCancel(ctx)
	defer cancelExec()
	if input.Cancel != nil {
		go func() {
			select {
			case <-input.Cancel:
				cancelExec()
			case <-execCtx.Done():
				// Apply returned (or ctx was cancelled); exit so this goroutine cannot leak.
			}
		}()
	}

	executionDir := filepath.Join(a.workDir, nowString)
	logrus.Tracef("[applyinator] applying calculated node plan contents %v", input.CalculatedPlan.Checksum)
	logrus.Tracef("[applyinator] using %s as execution directory", executionDir)
	if a.appliedPlanDir != "" {
		logrus.Debugf("[applyinator] writing applied calculated plan contents to historical plan directory %s", a.appliedPlanDir)
		if err := os.MkdirAll(a.appliedPlanDir, 0700); err != nil {
			logrus.Errorf("[applyinator] error creating applied plan directory: %v", err)
		}
		if err := a.writePlanToDisk(now, &input.CalculatedPlan); err != nil {
			logrus.Errorf("[applyinator] error writing applied plan to disk: %v", err)
		}
		if err := a.appliedPlanRetentionPolicy(planRetentionPolicyCount); err != nil {
			logrus.Errorf("[applyinator] error while applying plan retention policy: %v", err)
		}
	}

	if input.ReconcileFiles {
		if err := reconcileFiles(input.CalculatedPlan.Plan.Files); err != nil {
			return output, err
		}
	}

	if !a.preserveWorkDir {
		logrus.Debugf("[applyinator] cleaning working directory before applying %s", a.workDir)
		if err := os.RemoveAll(a.workDir); err != nil {
			return output, err
		}
	}

	if input.RunOneTimeInstructions {
		oneTime, err := a.runOneTimeInstructions(execCtx, executionDir, input.CalculatedPlan, input.ExistingOneTimeOutput,
			input.OneTimeInstructionAttempts, input.ResumeFromOneTimeInstruction, input.Cancel, input.Pause)
		if err != nil {
			return output, err
		}
		output.OneTimeOutput = oneTime.Output
		output.OneTimeApplySucceeded = oneTime.Succeeded
		output.CompletedOneTimeInstructions = oneTime.Completed
		if oneTime.Interruption != InterruptionNone {
			// An interrupt suppresses execution: running periodic instructions after abandoning the
			// one-time set would execute work the operator asked to stop.
			output.Interruption = oneTime.Interruption
			return output, nil
		}
	}

	periodicOutput, periodicSucceeded, err := a.runPeriodicInstructions(execCtx, executionDir, input.CalculatedPlan,
		input.ExistingPeriodicOutput, input.RunOneTimeInstructions, now, input.Cancel, input.Pause)
	if err != nil {
		return output, err
	}
	output.PeriodicOutput = periodicOutput
	output.PeriodicApplySucceeded = periodicSucceeded
	// Periodic instructions have no checkpoint, so their interruption is only observable here.
	if output.Interruption == InterruptionNone {
		output.Interruption = checkInterruption(input.Cancel, input.Pause)
	}

	return output, nil
}

// parseUnixTimeOrZero parses s using time.UnixDate.
// It returns ok false when s is empty or unparsable.
// Callers treat that as no recorded time, not an error.
func parseUnixTimeOrZero(label, s string) (t time.Time, ok bool) {
	if s == "" {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.UnixDate, s)
	if err != nil {
		logrus.Errorf("[applyinator] error parsing %s %q: %v", label, s, err)
		return time.Time{}, false
	}
	return parsed, true
}

// decodeGzipJSON gunzips data and unmarshals into out.
// It returns nil when data is empty.
func decodeGzipJSON(data []byte, out any) error {
	if len(data) == 0 {
		return nil
	}
	objectBuffer, err := generateByteBufferFromBytes(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(objectBuffer.Bytes(), out)
}

// encodeGzipJSON marshals v to JSON and gzips the result.
func encodeGzipJSON(v any) ([]byte, error) {
	marshalled, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return gzipByteSlice(marshalled)
}

// periodicInstructionDue determines if a periodic instruction should run now.
// It uses the previously recorded output for the instruction.
// It treats an unset or unparsable last-successful timestamp as no history (always due).
// When forced is true, bypass the period and the failure cooldown.
func periodicInstructionDue(now time.Time, prev planapi.PeriodicInstructionOutput, periodSeconds int, forced bool) (due bool, failures int) {
	if t, ok := parseUnixTimeOrZero("last successful run time", prev.LastSuccessfulRunTime); ok {
		effectivePeriod := periodSeconds
		if effectivePeriod == 0 {
			effectivePeriod = defaultEffectivePeriod
		}
		if now.Before(t.Add(time.Second*time.Duration(effectivePeriod))) && !forced {
			logrus.Debugf("[applyinator] not running periodic instruction as period duration has not elapsed since last successful run")
			return false, failures
		}
	}

	if prev.LastFailedRunTime != "" {
		if t, ok := parseUnixTimeOrZero("last failed run time", prev.LastFailedRunTime); ok {
			failures = prev.Failures
			failureCooldown := failures
			if failureCooldown > defaultFailureCooldown {
				failureCooldown = defaultFailureCooldown
			} else if failureCooldown == 0 {
				failureCooldown = 1
			}
			if now.Before(t.Add(time.Second*time.Duration(30*failureCooldown))) && !forced {
				logrus.Debugf("[applyinator] not running periodic instruction as failure cooldown has not elapsed since last failed run")
				return false, failures
			}
		}
	}

	return true, failures
}

// reconcileFiles applies a plan's Files.
// It writes regular files, creates directories, and deletes marked paths.
func reconcileFiles(files []planapi.File) error {
	for _, file := range files {
		if file.Action == deleteFileAction {
			if err := removeFile(file); err != nil {
				return err
			}
		} else if file.Directory {
			logrus.Debugf("[applyinator] creating directory %s", file.Path)
			if err := createDirectory(file); err != nil {
				return err
			}
		} else {
			logrus.Debugf("[applyinator] writing file %s", file.Path)
			if err := writeBase64ContentToFile(file); err != nil {
				return err
			}
		}
	}
	return nil
}

// instructionExecutionDir returns the per-instruction execution directory and log prefix.
// The values derive from the plan checksum and the instruction index.
func instructionExecutionDir(baseDir, checksum string, index int) (dir, prefix string) {
	prefix = checksum + "_" + strconv.Itoa(index)
	return filepath.Join(baseDir, prefix), prefix
}

// oneTimeResult is the outcome of one pass over a plan's one-time instructions.
type oneTimeResult struct {
	Output    []byte
	Succeeded bool
	// Completed is absolute: the index of the last instruction that returned, plus one.
	Completed int
	// Interruption is InterruptionNone unless the pass stopped early.
	Interruption Interruption
}

// runOneTimeInstructions executes one-time instructions in order.
// It stops at the first failure.
// It returns the updated gzip+JSON encoded saved-output map and a success flag.
func (a *Applyinator) runOneTimeInstructions(ctx context.Context, executionDir string, cp CalculatedPlan, existingOutput []byte,
	attempts, resumeFrom int, cancel, pause <-chan struct{},
) (oneTimeResult, error) {
	logrus.Infof("[applyinator] applying one-time instructions for plan with checksum %s starting at instruction %d", cp.Checksum, resumeFrom)
	executionOutputs := map[string][]byte{}
	if err := decodeGzipJSON(existingOutput, &executionOutputs); err != nil {
		return oneTimeResult{}, err
	}

	if resumeFrom < 0 {
		// Defensive: a negative resume index would panic on the instruction lookup below. There is no
		// meaningful checkpoint before the first instruction, so start from the beginning.
		logrus.Warnf("[applyinator] negative resume index %d for plan %s, starting from the first instruction", resumeFrom, cp.Checksum)
		resumeFrom = 0
	}

	result := oneTimeResult{Succeeded: true, Completed: min(resumeFrom, len(cp.Plan.OneTimeInstructions))}
	for index := resumeFrom; index < len(cp.Plan.OneTimeInstructions); index++ {
		if interruption := checkInterruption(cancel, pause); interruption != InterruptionNone {
			logrus.Infof("[applyinator] plan %s %s before instruction %d; not executing it", cp.Checksum, interruption, index)
			result.Interruption = interruption
			break
		}

		instruction := cp.Plan.OneTimeInstructions[index]
		logrus.Debugf("[applyinator] executing instruction %d attempt %d for plan %s", index, attempts, cp.Checksum)
		instructionDir, prefix := instructionExecutionDir(executionDir, cp.Checksum, index)
		executeOutput, _, exitCode, err := a.execute(ctx, prefix, instructionDir, instruction.CommonInstruction, true, attempts)
		failed := err != nil || exitCode != 0
		if failed {
			logrus.Errorf("[applyinator] error executing instruction %d: %v", index, err)
			result.Succeeded = false
		}
		// Output from a killed or failed instruction is still worth saving, so this stays ahead of the break.
		if instruction.Name == "" && instruction.SaveOutput {
			logrus.Errorf("[applyinator] instruction does not have a name set, cannot save output data")
		} else if instruction.SaveOutput {
			executionOutputs[instruction.Name] = executeOutput
		}
		// If we have failed to apply our one-time instructions, we need to break in order to stop subsequent
		// instructions from executing.
		if failed {
			// A cancel kills the in-flight instruction, so re-check: a cancel-induced kill must be reported
			// as an interruption rather than as a plan failure. A pause never interrupts a running
			// instruction, so a pause observed here did not cause the failure -- the instruction genuinely
			// failed and Succeeded stays false -- but it still stops the loop.
			result.Interruption = checkInterruption(cancel, pause)
			// Completed does not advance past a failed instruction.
			break
		}
		result.Completed = index + 1
	}

	output, err := encodeGzipJSON(executionOutputs)
	if err != nil {
		return oneTimeResult{}, err
	}
	result.Output = output
	return result, nil
}

// runPeriodicInstructions executes each due periodic instruction.
// It returns the updated gzip+JSON encoded periodic-output map and a success flag.
// Set ranOneTime to force every instruction to run regardless of period and cooldown.
func (a *Applyinator) runPeriodicInstructions(ctx context.Context, executionDir string, cp CalculatedPlan, existingOutput []byte,
	ranOneTime bool, now time.Time, cancel, pause <-chan struct{},
) ([]byte, bool, error) {
	nowUnixTimeString := now.Format(time.UnixDate)

	periodicOutputs := map[string]planapi.PeriodicInstructionOutput{}
	if err := decodeGzipJSON(existingOutput, &periodicOutputs); err != nil {
		return nil, false, err
	}

	periodicApplySucceeded := true
	for index, instruction := range cp.Plan.PeriodicInstructions {
		if interruption := checkInterruption(cancel, pause); interruption != InterruptionNone {
			logrus.Infof("[applyinator] plan %s %s before periodic instruction %d; not executing it", cp.Checksum, interruption, index)
			break
		}

		if instruction.Name == "" {
			logrus.Errorf("[applyinator] periodic instruction %d did not have name, unable to run", index)
			continue
		}

		prev := periodicOutputs[instruction.Name]
		due, failures := periodicInstructionDue(now, prev, instruction.PeriodSeconds, ranOneTime)
		if !due {
			logrus.Debugf("[applyinator] not running periodic instruction %s; not yet due", instruction.Name)
			continue
		}

		previousRunTime := ""
		if _, ok := parseUnixTimeOrZero("last successful run time", prev.LastSuccessfulRunTime); ok {
			previousRunTime = prev.LastSuccessfulRunTime
		}

		logrus.Debugf("[applyinator] executing periodic instruction %d for plan %s", index, cp.Checksum)
		instructionDir, prefix := instructionExecutionDir(executionDir, cp.Checksum, index)
		stdout, stderr, exitCode, err := a.execute(ctx, prefix, instructionDir, instruction.CommonInstruction, false, failures+1)
		if err != nil || exitCode != 0 {
			periodicApplySucceeded = false
		}

		lsrt := nowUnixTimeString
		lastFailureTime := ""
		if exitCode != 0 {
			lsrt = previousRunTime
			lastFailureTime = nowUnixTimeString
			failures++
		} else {
			// reset last failure time and failure count
			failures = 0
		}
		if !instruction.SaveStderrOutput {
			stderr = []byte{}
		}
		periodicOutputs[instruction.Name] = planapi.PeriodicInstructionOutput{
			Name:                  instruction.Name,
			Stdout:                stdout,
			Stderr:                stderr,
			ExitCode:              exitCode,
			LastSuccessfulRunTime: lsrt,
			LastFailedRunTime:     lastFailureTime,
			Failures:              failures,
		}
		if !periodicApplySucceeded {
			break
		}
	}

	output, err := encodeGzipJSON(periodicOutputs)
	if err != nil {
		return nil, false, err
	}
	return output, periodicApplySucceeded, nil
}

// checkInterlock enforces the interlock directory protocol used by install.sh during agent upgrade.
// A restart-pending file blocks applies for restartPendingTimeout, then it is removed and ignored.
// On success return a cleanup func. The caller must defer that func to remove applyinator-active file.
func (a *Applyinator) checkInterlock(now time.Time) (func(), error) {
	noop := func() {}
	if a.interlockDir == "" {
		return noop, nil
	}

	nowUnixTimeString := now.Format(time.UnixDate)
	restartPendingInterlockFilePath := filepath.Join(a.interlockDir, restartPendingInterlockFile)
	applyinatorActiveInterlockFilePath := filepath.Join(a.interlockDir, applyinatorActiveInterlockFile)

	// First off, remove check and remove the active interlock as the applyinator is not actually active
	if _, err := os.Stat(applyinatorActiveInterlockFilePath); err == nil {
		if err := os.Remove(applyinatorActiveInterlockFilePath); err != nil {
			logrus.Errorf("[applyinator] unable to remove applyinator active interlock file %s: %v", applyinatorActiveInterlockFilePath, err)
		}
	}

	if _, err := os.Stat(restartPendingInterlockFilePath); err == nil {
		// check the restart pending interlock file to see if we've passed our threshold for blocking
		fileContents, err := os.ReadFile(restartPendingInterlockFilePath)
		if err != nil {
			return noop, fmt.Errorf("unable to read restart pending interlock file %s: %w", restartPendingInterlockFilePath, err)
		}
		// Parse the time out of the file and determine if we have passed our time threshold
		t, err := time.Parse(time.UnixDate, string(fileContents))
		if err != nil {
			// If we are unable to parse the first observed time out of the file, write "now" as the first observed time of the file.
			if err := os.WriteFile(restartPendingInterlockFilePath, []byte(nowUnixTimeString), 0600); err != nil {
				return noop, fmt.Errorf("unable to write first-observed time to restart pending interlock file %s: %w", restartPendingInterlockFilePath, err)
			}
			return noop, fmt.Errorf("restart is pending for system-agent, waiting %s until ignoring pending restart", restartPendingTimeout.String())
		}
		if now.Before(t.Add(restartPendingTimeout)) {
			return noop, fmt.Errorf("restart is pending for system-agent, waiting %s until ignoring pending restart", t.Add(restartPendingTimeout).Sub(now).String())
		}
		// remove the restart pending file
		if err := os.Remove(restartPendingInterlockFilePath); err != nil {
			logrus.Errorf("[applyinator] error encountered while removing restart pending interlock file %s: %v", restartPendingInterlockFilePath, err)
		}
	}

	// At this point, there is no restart-pending and we can continue with applyinator reconciliation, so create the applyinator-active file
	if err := os.WriteFile(applyinatorActiveInterlockFilePath, []byte(nowUnixTimeString), 0600); err != nil {
		logrus.Errorf("[applyinator] unable to write applyinator active interlock file %s: %v", applyinatorActiveInterlockFilePath, err)
	}

	return func() {
		// Remove the Applyinator Active Interlock File
		if err := os.Remove(applyinatorActiveInterlockFilePath); err != nil {
			logrus.Errorf("[applyinator] unable to remove applyinator active interlock file %s: %v", applyinatorActiveInterlockFilePath, err)
		}
	}, nil
}

func gzipByteSlice(input []byte) ([]byte, error) {
	var gzOutput bytes.Buffer

	gzWriter := gzip.NewWriter(&gzOutput)

	if _, err := gzWriter.Write(input); err != nil {
		logrus.Errorf("[applyinator] error writing gzipped byte slice: %v", err)
	}

	if err := gzWriter.Close(); err != nil {
		return []byte{}, err
	}
	return gzOutput.Bytes(), nil
}

func generateByteBufferFromBytes(input []byte) (*bytes.Buffer, error) {
	buffer := bytes.NewBuffer(input)
	gzReader, err := gzip.NewReader(buffer)
	if err != nil {
		return nil, err
	}
	defer gzReader.Close()

	var objectBuffer bytes.Buffer
	_, err = io.Copy(&objectBuffer, gzReader)
	if err != nil {
		return nil, err
	}
	return &objectBuffer, nil
}

func (a *Applyinator) appliedPlanRetentionPolicy(retention int) error {
	planFiles, err := a.getAppliedPlanFiles()
	if err != nil {
		return err
	}

	if len(planFiles) <= retention {
		return nil
	}

	sort.Slice(planFiles, func(i, j int) bool {
		return planFiles[i].Name() < planFiles[j].Name()
	})

	delCount := len(planFiles) - retention
	for _, df := range planFiles[:delCount] {
		historicalPlanFile := filepath.Join(a.appliedPlanDir, df.Name())
		logrus.Infof("[applyinator] removing historical applied plan (retention policy count: %d) %s", retention, historicalPlanFile)
		if err := os.Remove(historicalPlanFile); err != nil {
			return err
		}
	}
	return nil
}

func (a *Applyinator) getAppliedPlanFiles() ([]os.DirEntry, error) {
	var planFiles []os.DirEntry
	dirListedPlanFiles, err := os.ReadDir(a.appliedPlanDir)
	if err != nil {
		return nil, err
	}

	for _, f := range dirListedPlanFiles {
		if strings.HasSuffix(f.Name(), appliedPlanFileSuffix) && !f.IsDir() {
			planFiles = append(planFiles, f)
		}
	}
	return planFiles, nil
}

func (a *Applyinator) writePlanToDisk(now time.Time, plan *CalculatedPlan) error {
	planFiles, err := a.getAppliedPlanFiles()
	if err != nil {
		return err
	}

	file := now.Format(applyinatorDateCodeLayout) + appliedPlanFileSuffix
	anpString, err := json.Marshal(plan)
	if err != nil {
		return err
	}

	if len(planFiles) != 0 {
		sort.Slice(planFiles, func(i, j int) bool {
			return planFiles[i].Name() > planFiles[j].Name()
		})
		existingFileContent, err := os.ReadFile(filepath.Join(a.appliedPlanDir, planFiles[0].Name()))
		if err != nil {
			return err
		}
		if bytes.Equal(existingFileContent, anpString) {
			logrus.Debugf("[applyinator] not writing applied plan to file %s as the last file written (%s) had identical contents", file, planFiles[0].Name())
			return nil
		}
	}

	return writeContentToFile(filepath.Join(a.appliedPlanDir, file), os.Getuid(), os.Getgid(), 0600, anpString)
}

// execute stages the instruction's execution directory, runs its command, and returns the captured
// stdout, stderr, exit code and wait error.
//
// The command is put into a process group (Unix) or Job Object (Windows) of its own, and a watchdog
// is armed on ctx: when ctx is cancelled the whole process tree is signalled to terminate and, if
// it has not exited within instructionTerminationGrace, killed. Signalling only the direct child
// would leave the installer or package manager that a typical run.sh shells out to still running.
func (a *Applyinator) execute(ctx context.Context, prefix, executionDir string, instruction planapi.CommonInstruction, combinedOutput bool, attempt int) ([]byte, []byte, int, error) {
	if instruction.Image == "" {
		logrus.Infof("[applyinator] no image provided, creating empty working directory %s", executionDir)
		// UID/GID -1 means "don't change ownership" (a no-op chown). Without this, the directory
		// defaults to UID/GID 0 (root) — harmless in production, where the agent always runs as
		// root, but it makes this code unusable from a non-root test process (os.Chown to a
		// different owner than the caller returns "operation not permitted").
		if err := createDirectory(planapi.File{Directory: true, Path: executionDir, UID: -1, GID: -1}); err != nil {
			logrus.Errorf("[applyinator] error while creating empty working directory: %v", err)
			return nil, nil, -1, err
		}
	} else {
		logrus.Infof("[applyinator] extracting image %s to directory %s", instruction.Image, executionDir)
		if err := a.imageUtil.Stage(executionDir, instruction.Image); err != nil {
			logrus.Errorf("[applyinator] error while staging: %v", err)
			return nil, nil, -1, err
		}
	}

	command := instruction.Command

	if command == "" {
		logrus.Debugf("[applyinator] command was not specified, defaulting to %s%s", executionDir, defaultCommand)
		command = executionDir + defaultCommand
	}

	cmd := exec.Command(command, instruction.Args...)
	logrus.Infof("[applyinator] running command: %s %v", instruction.Command, instruction.Args)
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, instruction.Env...)
	cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", cattleAgentExecutionPwdEnvKey, executionDir))
	cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%d", cattleAgentAttemptKey, attempt))
	cmd.Env = append(cmd.Env, "PATH="+os.Getenv("PATH")+":"+executionDir)
	cmd.Dir = executionDir

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		logrus.Errorf("[applyinator] error setting up stdout pipe: %v", err)
		return nil, nil, -1, err
	}
	defer stdout.Close()

	stderr, err := cmd.StderrPipe()
	if err != nil {
		logrus.Errorf("[applyinator] error setting up stderr pipe: %v", err)
		return nil, nil, -1, err
	}
	defer stderr.Close()

	// Before Start: SysProcAttr is only read at fork time. A failure here must not stop the
	// instruction from running at all, it only degrades cancellation to a direct-child signal.
	if err := configureProcessGroup(cmd); err != nil {
		logrus.Errorf("[applyinator] error configuring the process group for %s: %v; cancelling it will only signal its direct child", command, err)
	}

	var (
		eg           = errgroup.Group{}
		stdoutBuffer bytes.Buffer
		stderrBuffer bytes.Buffer
	)

	stdoutTarget := &stdoutBuffer
	stderrTarget := &stderrBuffer
	stdoutLock := &sync.Mutex{}
	stderrLock := stdoutLock

	if combinedOutput {
		// Share one buffer (and therefore the one lock already assigned above) so stdout and
		// stderr genuinely interleave into a single combined result. Previously this assigned
		// stderrBuffer = stdoutBuffer, which copies an empty bytes.Buffer by value: the two
		// goroutines below still wrote into two independent buffers, so combinedOutput silently
		// did nothing, and one-time instructions (which call execute with combinedOutput=true and
		// only keep the first return value) never captured stderr in SaveOutput results.
		stderrTarget = stdoutTarget
	} else {
		stderrLock = &sync.Mutex{}
	}

	eg.Go(func() error {
		return streamLogs("["+prefix+":stdout]", stdoutTarget, stdout, stdoutLock)
	})
	eg.Go(func() error {
		return streamLogs("["+prefix+":stderr]", stderrTarget, stderr, stderrLock)
	})

	if err := cmd.Start(); err != nil {
		// The watchdog is never armed on this path, so nothing else would release the platform
		// handle configureProcessGroup may have created. releaseProcessTree is idempotent and a
		// no-op when no handle was recorded.
		releaseProcessTree(cmd)
		return nil, nil, -1, err
	}

	// After Start: Windows can only assign a process to a Job Object once it exists. Degrades the
	// same way as configureProcessGroup above.
	if err := assignProcessTree(cmd); err != nil {
		logrus.Errorf("[applyinator] error assigning %s to its process tree: %v; cancelling it will only signal its direct child", command, err)
	}

	stop := watchForTermination(ctx, cmd, stdout, stderr)
	defer stop()

	// Wait for I/O to complete before calling cmd.Wait() because cmd.Wait() will close the I/O pipes.
	_ = eg.Wait()
	exitCode := 0
	waitErr := cmd.Wait()
	if waitErr != nil {
		// A non-ExitError wait failure (the process never produced an exit status) must not be
		// reported as exit code 0: runPeriodicInstructions branches on the exit code rather than
		// the error, and would otherwise persist a failed run as a success.
		exitCode = -1
		var ee *exec.ExitError
		if errors.As(waitErr, &ee) {
			exitCode = ee.ExitCode()
		}
	}
	logrus.Infof("[applyinator] command %s %v finished with err: %v and exit code: %d", instruction.Command, instruction.Args, waitErr, exitCode)
	return stdoutTarget.Bytes(), stderrTarget.Bytes(), exitCode, waitErr
}

// watchForTermination signals cmd's process tree once ctx is done: a graceful signal first,
// escalating to an unconditional kill after instructionTerminationGrace. The pipes are closed
// alongside the kill so streamLogs cannot block forever on a descendant that inherited them. The
// returned func stops the watchdog and releases any platform handles; callers must defer it.
func watchForTermination(ctx context.Context, cmd *exec.Cmd, pipes ...io.Closer) func() {
	// done is closed by the returned func; finished is closed by the watchdog on its way out, so
	// the returned func can be sure nothing is still signalling before it releases the handles.
	done := make(chan struct{})
	finished := make(chan struct{})

	go func() {
		defer close(finished)

		select {
		case <-done:
			// The instruction finished on its own; there is nothing to terminate.
			return
		case <-ctx.Done():
		}

		pid := -1
		if cmd.Process != nil {
			pid = cmd.Process.Pid
		}

		logrus.Infof("[applyinator] apply was cancelled, terminating the process tree of pid %d", pid)
		if err := terminateProcessTree(cmd); err != nil {
			logrus.Warnf("[applyinator] error terminating the process tree of pid %d: %v", pid, err)
		}

		// This wait has a process-exit arm, not just the timer: execute defers stop(), so done is
		// closed as soon as cmd.Wait() returns. A terminateProcessTree that actually terminates the
		// tree rather than asking it nicely — which is what Windows does, having no graceful signal
		// to send — therefore short-circuits the grace period instead of stalling on it.
		select {
		case <-done:
			// The tree is gone: either it took the hint, or terminateProcessTree killed it outright.
			return
		case <-time.After(instructionTerminationGrace):
		}

		logrus.Warnf("[applyinator] process tree of pid %d did not exit within %s of being asked, killing it", pid, instructionTerminationGrace)
		if err := killProcessTree(cmd); err != nil {
			logrus.Warnf("[applyinator] error killing the process tree of pid %d: %v", pid, err)
		}

		// execute calls eg.Wait() before cmd.Wait(), and eg.Wait() only returns once both pipes
		// reach EOF. A killed shell's grandchild inherits the write ends of those pipes, so without
		// an explicit close here the apply hangs forever on a descendant the kill did not reach.
		// This deliberately does not happen on the graceful path above, so a well-behaved
		// instruction's final output is not truncated. Close errors are ignored: cmd.Wait() closes
		// its own copies and a double close returns os.ErrClosed, which is expected.
		for _, pipe := range pipes {
			_ = pipe.Close()
		}
	}()

	var once sync.Once
	return func() {
		once.Do(func() {
			close(done)
			<-finished
			releaseProcessTree(cmd)
		})
	}
}

// streamLogs reads lines from reader and appends them to outputBuffer.
// Log each line with prefix. Protect writes with lock.
func streamLogs(prefix string, outputBuffer *bytes.Buffer, reader io.Reader, lock *sync.Mutex) error {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		logrus.Infof("%s: %s", prefix, scanner.Text())
		lock.Lock()
		outputBuffer.Write(append(scanner.Bytes(), []byte("\n")...))
		lock.Unlock()
	}
	return nil
}
