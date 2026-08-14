package k8splan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"time"

	planapi "github.com/rancher/rancher/pkg/plan"
	"github.com/rancher/system-agent/pkg/applyinator"
	"github.com/rancher/system-agent/pkg/prober"
	corecontrollers "github.com/rancher/wrangler/v3/pkg/generated/controllers/core/v1"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
)

// interruptedEnqueuePeriod is the re-enqueue period used while a plan is held or cancelled.
// Removing the annotation arrives as a watch event, so this is a slow-poll safety net rather than
// the mechanism for noticing an unpause; at probePeriod (5s default) it would churn the workqueue
// on every node for the whole duration of a pause to no purpose.
const interruptedEnqueuePeriod = time.Minute

// reconcileSecret handles Secret change events.
// It decides whether to apply the plan, runs the apply, and writes outcomes back to the Secret.
func (w *watcher) reconcileSecret(ctx context.Context, sc corecontrollers.SecretController, secret *corev1.Secret, cooldownPeriod time.Duration) (*corev1.Secret, error) {
	if secret == nil {
		logrus.Debugf("[k8splan] received nil secret (object deleted from cache), skipping")
		return nil, nil
	}
	originalSecret := secret.DeepCopy()
	secret = secret.DeepCopy()

	var err error
	currentTime := time.Now()
	lastApplyTime := parseLastApplyTime(secret.Data, currentTime)
	w.probePeriod = parseProbePeriodOverride(secret.Data, w.probePeriod)

	logrus.Debugf("[k8splan] processing secret %s in namespace %s at generation %d with resource version %s",
		secret.Name, secret.Namespace, secret.Generation, secret.ResourceVersion)
	// needsApplied indicates whether the one-time instructions should be run. It is always set by
	// decidePlanStateAction or decideChecksumFlowAction below before being read.
	var needsApplied bool

	uidChanged := w.secretUID != "" && w.secretUID != string(secret.UID)
	rvIsOlder := toInt(w.lastAppliedResourceVersion) > toInt(secret.ResourceVersion)

	switch {
	case uidChanged:
		// Secret was deleted and recreated with a new UID; reset state so the new secret is force-applied.
		logrus.Infof("[k8splan] received secret with new UID (%s, previously %s); secret was recreated — resetting agent state",
			secret.UID, w.secretUID)
		w.secretUID = ""
		w.lastAppliedResourceVersion = ""
		w.hasRunOnce = false

	case rvIsOlder:
		logrus.Errorf("[k8splan] received secret to process that was older than the last secret operated on (%s vs %s)",
			secret.ResourceVersion, w.lastAppliedResourceVersion)
		return secret, errors.New("secret received was too old")
	}

	planData, ok := secret.Data[PlanKey]
	if !ok {
		sc.EnqueueAfter(w.connInfo.Namespace, w.connInfo.SecretName, w.probePeriod)
		return secret, nil
	}

	logrus.Tracef("[k8splan] byte data: %v", planData)
	logrus.Tracef("[k8splan] plan string was %s", string(planData))

	probeStatuses := parseProbeStatuses(secret.Data)

	cp, err := applyinator.CalculatePlan(planData)
	if err != nil {
		return secret, err
	}
	logrus.Tracef("[k8splan] calculated checksum to be %s", cp.Checksum)

	wasFailedPlan := false

	// currentPlanState is non-empty when Rancher supports plan-state.
	// If absent, fall back to checksum-based logic for backward compatibility.
	currentPlanState := planapi.PlanState(secret.Data[planapi.PlanStateKey])

	// Parse failureCount unconditionally — needed for OneTimeInstructionAttempts in both flows.
	failureCount, planAttempt := parseFailureCount(secret.Data)

	// Step A: branch by flow, and settle the interrupt question before anything else reads the
	// plan. In the checksum flow the annotations are unsupported and have no effect at all; in
	// the plan-state flow they decide whether this reconcile is allowed to execute anything.
	if currentPlanState == "" {
		for _, key := range []string{PlanCancelledAnnotation, PlanPausedAnnotation} {
			if value, ok := secret.Annotations[key]; ok {
				logrus.Warnf("[k8splan] ignoring unsupported annotation in checksum flow key=%s value=%s", key, value)
			}
		}
	} else {
		interrupt, interruptErr := readInterrupt(secret.Annotations)
		// The two returns below are a SAFETY PROPERTY, not an early-exit optimisation, and
		// moving anything above them breaks the feature silently. In the plan-state flow the
		// agent executes a plan only when both interrupt annotations read as an explicit
		// false — absent, or the literal "false". Everything capable of starting work
		// (resolveResume, the resume commit, decidePlanStateAction, the pending -> in-progress
		// pre-commit, Apply) sits below them, so there is no ordering in which a held plan
		// reaches any of it — including the first reconcile after an agent restart, which is
		// what stops "pause works until the agent restarts, and then the plan runs anyway".
		if interruptErr != nil {
			// Deliberately narrow: no Secret write (so resourceVersion is stable and the error
			// cannot amplify into a write loop), no probes, no Apply, no EnqueueAfter. The
			// workqueue's exponential rate limiter owns the retry, and correcting the
			// annotation arrives as a watch event. In particular this does NOT write
			// plan-state: failed — nothing ran, so there is no failure to report.
			logrus.Errorf("[k8splan] refusing to act on plan secret %s/%s: %v", w.connInfo.Namespace, w.connInfo.SecretName, interruptErr)
			return secret, interruptErr
		}
		if interrupt != applyinator.InterruptionNone {
			return w.recordInterruptAtEntry(sc, secret, cp, currentPlanState, interrupt, probeStatuses)
		}
	}

	// Step B: everything below runs only in the checksum flow, or in the plan-state flow with
	// both annotations read as an explicit false. effectiveState is the state this reconcile
	// acts on; resumeFrom positions an apply some other condition has already decided to run.
	effectiveState, resumeFrom := resolveResume(currentPlanState, secret.Data, cp.Checksum)
	resumeFrom = clampResumeFrom(resumeFrom, len(cp.Plan.OneTimeInstructions))

	// Step C: release the suspension before anything is applied. Reaching this line is itself
	// the proof that the annotation is gone, so "was the plan suspended" is the only condition.
	// Three reasons this write exists, in order of importance:
	//  1. A plan that is executing must not report paused. Without it the wire state stays
	//     paused for the whole resumed apply — and where ResumeState is terminal, forever, since
	//     no outcome write follows. Pausing a succeeded plan that only runs periodic
	//     instructions is the likeliest operator action of all, so that is not a corner case.
	//  2. It re-arms the write-once guard: a lingering Paused: true would make handleInterrupt
	//     treat a second pause as already recorded and keep the stale Completed.
	//  3. It bounds the checkpoint's authority to the hold, so a crash during a resumed apply
	//     falls back to re-executing from instruction 0 — the ordinary contract — rather than
	//     trusting a record whose plan is no longer parked at an instruction boundary.
	resumeUpdates := resumeCommitUpdates(currentPlanState, effectiveState, secret.Data, cp.Checksum)

	if effectiveState != "" {
		psResult := decidePlanStateAction(effectiveState)
		needsApplied = psResult.NeedsApplied
		if psResult.ResetPlanAttempt {
			planAttempt = 1
		}
		if !w.hasRunOnce {
			w.hasRunOnce = true
		}
	} else {
		// Backward compatibility: old checksum-based needsApplied decision.
		csResult := decideChecksumFlowAction(secret.Data, cp.Checksum, w.hasRunOnce, failureCount, currentTime,
			lastApplyTime, cooldownPeriod, w.lastAppliedResourceVersion == secret.ResourceVersion, w.lastAppliedResourceVersion)

		needsApplied = csResult.NeedsApplied
		wasFailedPlan = csResult.WasFailedPlan
		w.hasRunOnce = csResult.HasRunOnce
		if csResult.ClearAppliedChecksum {
			secret.Data[AppliedChecksumKey] = []byte("")
		}
	}

	// The pending pre-commit below already writes plan-state, so the resume folds into it: one
	// write, not two.
	foldResumeIntoPreCommit := resumeUpdates != nil && effectiveState == planapi.PlanStatePending && needsApplied
	if resumeUpdates != nil && !foldResumeIntoPreCommit {
		// If this fails the reconcile returns and no apply runs until it lands.
		committed, resumeErr := w.writeInterruptOutcome(sc, cp.Checksum, resumeUpdates)
		if resumeErr != nil {
			return secret, fmt.Errorf("failed to commit the resume into plan-state:%s: %w", effectiveState, resumeErr)
		}
		maps.Copy(secret.Data, resumeUpdates)
		if committed != nil {
			secret.ResourceVersion = committed.ResourceVersion
		}
	}

	output := selectExistingOutput(secret.Data, wasFailedPlan)

	periodicOutput := secret.Data[AppliedPeriodicOutputKey]

	// Transition pending -> in-progress before executing so the state is durable
	// in the event of a crash. On restart the agent will see in-progress and re-execute.
	if effectiveState == planapi.PlanStatePending && needsApplied {
		secret.Data[planapi.PlanStateKey] = []byte(planapi.PlanStateInProgress)
		secret.Data[planapi.PlanRevisionKey] = incrementCount(secret.Data[planapi.PlanRevisionKey])
		if foldResumeIntoPreCommit {
			// Only the checkpoint's Paused: false clear is folded in; plan-state is not taken
			// from resumeUpdates because in-progress supersedes it. plan-revision is bumped
			// here because this is the pending -> in-progress transition, not because of the
			// resume: the resume commit never bumps it, the plan content has not changed.
			secret.Data[PlanProgressKey] = resumeUpdates[PlanProgressKey]
		}
		// Commit to the API server now, before Apply runs. This makes the transition
		// durable: if the agent crashes mid-apply, the next startup sees in-progress
		// and re-executes from the beginning.
		var inProgressErr error
		if secret, inProgressErr = w.updateSecret(sc, secret); inProgressErr != nil {
			return nil, fmt.Errorf("failed to commit plan-state:%s to API server: %w", planapi.PlanStateInProgress, inProgressErr)
		}
	}

	// Step E: the interrupt watch is started only in the plan-state flow. In the checksum flow
	// both channels stay nil, and applyinator.checkInterruption treats a nil channel as never
	// ready, so the interrupted-outcome path below is structurally unreachable there.
	var cancelCh, pauseCh <-chan struct{}
	if effectiveState != "" {
		var stopWatch func()
		cancelCh, pauseCh, stopWatch = w.startInterruptWatch(ctx, sc)
		defer stopWatch()
	}

	input := applyinator.ApplyInput{
		CalculatedPlan:               cp,
		ReconcileFiles:               needsApplied,
		ExistingOneTimeOutput:        output,
		ExistingPeriodicOutput:       periodicOutput,
		RunOneTimeInstructions:       needsApplied,
		OneTimeInstructionAttempts:   planAttempt,
		Cancel:                       cancelCh,
		Pause:                        pauseCh,
		ResumeFromOneTimeInstruction: resumeFrom,
	}

	applyOutput, err := w.applyinator.Apply(ctx, input)
	if err != nil {
		return secret, fmt.Errorf("error encountered when running apply: %w", err)
	}

	// Step F: Interruption is tested BEFORE OneTimeApplySucceeded, and that order is the
	// contract: a cancel-killed instruction reports OneTimeApplySucceeded: false alongside
	// Interruption: InterruptionCanceled, so routing this through buildSecretDataUpdates would
	// record plan-state: failed for a plan the operator stopped on purpose.
	if applyOutput.Interruption != applyinator.InterruptionNone {
		return w.recordInterruptAfterApply(sc, secret, cp, effectiveState, needsApplied, applyOutput, probeStatuses)
	}

	output = applyOutput.OneTimeOutput
	periodicOutput = applyOutput.PeriodicOutput

	outcomeUpdates := buildSecretDataUpdates(applyOutcomeInput{
		Checksum:              cp.Checksum,
		CurrentTime:           currentTime,
		NeedsApplied:          needsApplied,
		WasFailedPlan:         wasFailedPlan,
		UsesPlanState:         effectiveState != "",
		OneTimeOutput:         output,
		OneTimeApplySucceeded: applyOutput.OneTimeApplySucceeded,
		PeriodicOutput:        periodicOutput,
		PriorFailureCount:     secret.Data[FailureCountKey],
		PriorSuccessCount:     secret.Data[SuccessCountKey],
	})
	maps.Copy(secret.Data, outcomeUpdates)

	prober.DoProbes(cp.Plan.Probes, probeStatuses, needsApplied)

	marshalledProbeStatus, err := json.Marshal(probeStatuses)
	if err != nil {
		logrus.Errorf("[k8splan] error while marshalling probe statuses: %v", err)
	} else {
		secret.Data[ProbeStatusesKey] = marshalledProbeStatus
	}

	if applyOutput.OneTimeApplySucceeded == needsApplied {
		// If the one-time instructions were successfully applied,
		// we should enqueue the secret for the period of a probe to attempt to guarantee timeliness on probe reactivity.
		logrus.Debugf("[k8splan] enqueueing after %f seconds", w.probePeriod.Seconds())
		sc.EnqueueAfter(w.connInfo.Namespace, w.connInfo.SecretName, w.probePeriod)
	}

	if reflect.DeepEqual(originalSecret.Data, secret.Data) && reflect.DeepEqual(originalSecret.StringData, secret.StringData) {
		logrus.Debugf("[k8splan] secret data/string-data did not change, not updating secret")
		return originalSecret, nil
	}
	secret, err = w.updateSecret(sc, secret)
	if err != nil {
		logrus.Fatalf("[k8splan] encountered an error while attempting to update the secret: %v", err)
		return nil, nil
	}
	return secret, nil
}

// recordInterruptAtEntry persists an interrupt observed at reconcile entry — before any decision
// to execute was taken — and returns without applying anything. It never sets w.hasRunOnce: that
// is mutable state belonging below the safety returns in reconcileSecret.
func (w *watcher) recordInterruptAtEntry(sc corecontrollers.SecretController, secret *corev1.Secret, cp applyinator.CalculatedPlan,
	currentPlanState planapi.PlanState, interrupt applyinator.Interruption, probeStatuses map[string]planapi.ProbeStatus,
) (*corev1.Secret, error) {
	total := len(cp.Plan.OneTimeInstructions)
	updates := handleInterrupt(interrupt, currentPlanState, secret.Data, cp.Checksum, total)
	if interrupt == applyinator.InterruptionCanceled && len(updates) > 0 {
		// An empty map means the write-once guard suppressed the record, so there is no fresh
		// cancellation to warn about.
		warnOnPartialCancellation(parsePlanProgress(secret.Data, cp.Checksum).Completed, total)
	}

	// An interrupt suppresses execution, never observation. Merged into the same map so one
	// write persists both; a steady-state probe changes nothing and writeInterruptOutcome's
	// DeepEqual guard then skips the Update entirely.
	mergeProbeStatuses(updates, cp.Plan.Probes, probeStatuses)

	committed, err := w.writeInterruptOutcome(sc, cp.Checksum, updates)
	if err != nil {
		return secret, fmt.Errorf("failed to record the %s interrupt: %w", interrupt, err)
	}
	sc.EnqueueAfter(w.connInfo.Namespace, w.connInfo.SecretName, interruptedEnqueuePeriod)
	if committed != nil {
		return committed, nil
	}
	return secret, nil
}

// recordInterruptAfterApply persists the outcome of an apply that was interrupted mid-flight. It
// deliberately replaces buildSecretDataUpdates rather than running alongside it: an interrupted
// apply reports OneTimeApplySucceeded: false, which that function would record as
// plan-state: failed. It writes neither applied-checksum (the plan is not applied) nor the
// failure and success counters (nothing failed and nothing succeeded), and it is never fatal.
func (w *watcher) recordInterruptAfterApply(sc corecontrollers.SecretController, secret *corev1.Secret, cp applyinator.CalculatedPlan,
	effectiveState planapi.PlanState, needsApplied bool, applyOutput applyinator.ApplyOutput, probeStatuses map[string]planapi.ProbeStatus,
) (*corev1.Secret, error) {
	progress := planProgress{Checksum: cp.Checksum}
	if needsApplied {
		// The one-time set was running, so the Secret was already transitioned to in-progress
		// before Apply ran; that is the state to resume into.
		progress.ResumeState = planapi.PlanStateInProgress
		progress.Completed = applyOutput.CompletedOneTimeInstructions
		progress.Total = len(cp.Plan.OneTimeInstructions)
	} else {
		// Periodic-only: restore the state the monitoring reconcile was in. Defaulting this to
		// in-progress would re-execute a completed Day 2 operation in full on unpause. There is
		// nothing to resume, so Completed and Total stay 0.
		progress.ResumeState = effectiveState
	}

	state := PlanStatePaused
	progress.Paused = true
	if applyOutput.Interruption == applyinator.InterruptionCanceled {
		// A cancellation is a report, not a suspension: nothing resumes from it.
		state = planapi.PlanStateCancelled
		progress.Paused = false
		progress.ResumeState = ""
		warnOnPartialCancellation(progress.Completed, progress.Total)
	}

	updates := map[string][]byte{
		planapi.PlanStateKey: []byte(state),
		PlanProgressKey:      marshalPlanProgress(progress),
		// applied-output is what selectExistingOutput feeds back as ExistingOneTimeOutput next
		// time, so the SaveOutput results of the instructions that did complete survive the hold.
		AppliedOutputKey: applyOutput.OneTimeOutput,
	}
	mergeProbeStatuses(updates, cp.Plan.Probes, probeStatuses)

	committed, err := w.writeInterruptOutcome(sc, cp.Checksum, updates)
	if err != nil {
		return secret, fmt.Errorf("failed to record the %s outcome of the interrupted apply: %w", applyOutput.Interruption, err)
	}
	sc.EnqueueAfter(w.connInfo.Namespace, w.connInfo.SecretName, interruptedEnqueuePeriod)
	if committed != nil {
		return committed, nil
	}
	return secret, nil
}

// resumeCommitUpdates returns the Secret writes that release a suspension, or nil when the plan
// was not suspended. The caller has already established that neither annotation is set, so "was
// the plan suspended" is the only question left: plan-state says paused, or a checkpoint for this
// plan claims it.
//
// Completed and Total are kept as a record of how far the plan got; Paused: false is what stops
// the checkpoint granting any further resume, and ResumeState is cleared because it has just been
// consumed into effectiveState.
func resumeCommitUpdates(currentPlanState, effectiveState planapi.PlanState, data map[string][]byte, checksum string) map[string][]byte {
	progress := parsePlanProgress(data, checksum)
	if currentPlanState != PlanStatePaused && !progress.Paused {
		return nil
	}
	// Set explicitly: parsePlanProgress returns the zero value when plan-state said paused with
	// no checkpoint beneath it, and an unscoped checkpoint would be discarded on the next read.
	progress.Checksum = checksum
	progress.Paused = false
	progress.ResumeState = ""
	return map[string][]byte{
		planapi.PlanStateKey: []byte(effectiveState),
		PlanProgressKey:      marshalPlanProgress(progress),
	}
}

// mergeProbeStatuses runs the plan's probes and merges the marshalled statuses into updates, so
// an interrupt suppresses execution but never observation. Freezing probe statuses would feed
// stale health data to Rancher's MachineHealthCheck on exactly the nodes most likely to be
// unhealthy: a plan stopped mid-flight leaves the node in a partial state.
//
// initial is false: the InitialDelaySeconds sleep exists to give a freshly applied plan time to
// settle, and no plan was applied on either interrupt path.
func mergeProbeStatuses(updates map[string][]byte, probes map[string]planapi.Probe, probeStatuses map[string]planapi.ProbeStatus) {
	prober.DoProbes(probes, probeStatuses, false)
	marshalled, err := json.Marshal(probeStatuses)
	if err != nil {
		logrus.Errorf("[k8splan] error while marshalling probe statuses: %v", err)
		return
	}
	updates[ProbeStatusesKey] = marshalled
}

// warnOnPartialCancellation reports a cancellation that landed between instructions, which is the
// one interrupt outcome an operator has to act on: the plan is terminal, so nothing will finish
// what it started.
func warnOnPartialCancellation(completed, total int) {
	if completed <= 0 || completed >= total {
		return
	}
	logrus.Warnf("[k8splan] the plan was cancelled after %d of %d one-time instructions; this node may be left in an inconsistent state",
		completed, total)
}

// clampResumeFrom bounds a checkpoint's instruction index to the plan it will be applied to.
// resolveResume returns the stored Completed verbatim, and a hand-edited or truncated checkpoint
// could carry 999 or a negative. pkg/applyinator clamps internally too; this is defence in depth
// at the boundary where an operator-controllable value enters.
func clampResumeFrom(resumeFrom, total int) int {
	if resumeFrom < 0 {
		logrus.Warnf("[k8splan] resume checkpoint %d is negative; resuming from the first one-time instruction instead", resumeFrom)
		return 0
	}
	if resumeFrom > total {
		logrus.Warnf("[k8splan] resume checkpoint %d exceeds the plan's %d one-time instructions; resuming from %d instead", resumeFrom, total, total)
		return total
	}
	return resumeFrom
}
