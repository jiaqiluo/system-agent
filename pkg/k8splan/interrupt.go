package k8splan

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"sync"
	"time"

	planapi "github.com/rancher/rancher/pkg/plan"
	"github.com/rancher/system-agent/pkg/applyinator"
	corecontrollers "github.com/rancher/wrangler/v3/pkg/generated/controllers/core/v1"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

// The suppression invariant this file exists to uphold:
//
//	In the plan-state flow, the agent executes a plan only when both PlanPausedAnnotation and
//	PlanCancelledAnnotation are unambiguously not set — absent, or present with the value "false".
//	Anything else stops execution, no matter what plan-state says, no matter what the checkpoint
//	says, no matter how the agent arrived at this reconcile.
//
// That is a whitelist of the two ways a plan may run, not a blacklist of the ways it is held: a
// value the agent does not recognise falls on the stop side by construction rather than by an
// explicit rule, which is what makes the invariant hold for values nobody anticipated. The failure
// mode it guards against is not "pause does not work" — an operator sees that immediately — but
// "pause works until the agent restarts, and then the plan runs anyway".
//
// An interrupt suppresses execution, never observation: while either annotation is set the agent
// skips Apply but still runs probes and still persists probe statuses, because freezing probe
// statuses would feed stale health data to Rancher's MachineHealthCheck on exactly the nodes most
// likely to be unhealthy. handleInterrupt therefore returns only the lifecycle-key updates; the
// caller merges the probe statuses into the same map.

// parseInterruptAnnotation reports whether key requests its interrupt. The only valid values are
// "true" and "false"; an absent annotation is "false". Any other value is an operator
// misconfiguration and is returned as an error — never silently coerced in either direction.
//
// Deliberately not strconv.ParseBool: that accepts "True", "TRUE", "t", "1", "0" and friends, so
// it would quietly accept eleven spellings of each value and reject the twelfth. The point of an
// exact match is that the accepted set is the set that can be written down in documentation and
// validated by an admission check — one spelling each, and everything else is visibly wrong at the
// moment it is set rather than subtly wrong later. Do not "simplify" this to ParseBool.
func parseInterruptAnnotation(annotations map[string]string, key string) (bool, error) {
	v, ok := annotations[key]
	if !ok {
		return false, nil
	}
	switch v {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("annotation %s has invalid value %q, must be \"true\" or \"false\"", key, v)
	}
}

// readInterrupt interprets both interrupt annotations and reports what the agent should do.
//
// Precedence:
//  1. a valid cancelled == "true" wins outright, even when the pause value is invalid — an
//     operator stopping a runaway installer is not made to fix a typo on an unrelated annotation
//     first;
//  2. otherwise any invalid value is returned as an error, and the returned Interruption is
//     meaningless to the caller;
//  3. otherwise a valid paused == "true" holds the plan;
//  4. otherwise the plan may run.
//
// An invalid cancel value does not let a valid pause take effect: pause is the weaker request, and
// honoring it while the stronger one is unreadable would let the agent act on a guess about what
// the operator meant.
func readInterrupt(annotations map[string]string) (applyinator.Interruption, error) {
	cancelled, cancelErr := parseInterruptAnnotation(annotations, PlanCancelledAnnotation)
	paused, pauseErr := parseInterruptAnnotation(annotations, PlanPausedAnnotation)

	if cancelErr == nil && cancelled {
		return applyinator.InterruptionCanceled, nil
	}
	if cancelErr != nil || pauseErr != nil {
		// Joined rather than returned one at a time so a doubly-mistyped Secret reports both
		// mistakes in one message instead of one per reconcile.
		return applyinator.InterruptionNone, errors.Join(cancelErr, pauseErr)
	}
	if paused {
		return applyinator.InterruptionPaused, nil
	}
	return applyinator.InterruptionNone, nil
}

// interruptPollInterval is how often the interrupt watch re-reads the plan Secret while an apply
// is in flight. A var rather than a const so tests can shorten it; see withInterruptPollInterval.
var interruptPollInterval = 2 * time.Second

// startInterruptWatch polls the plan Secret while an apply is in flight and closes the returned
// channel matching the first interrupt it observes. The controller cannot deliver the annotation
// change itself: Applyinator.Apply runs synchronously inside the OnChange handler with
// DefaultWorkers: 1, so while an apply is running the workqueue worker is busy. The informer's
// indexer is updated by its own goroutine and is not blocked by that worker, so a cache read stays
// fresh during an apply.
//
// The returned stop func must be deferred by the caller.
func (w *watcher) startInterruptWatch(ctx context.Context, sc corecontrollers.SecretController) (cancelCh, pauseCh <-chan struct{}, stop func()) {
	cancel := make(chan struct{})
	pause := make(chan struct{})
	stopCh := make(chan struct{})
	done := make(chan struct{})

	go w.pollInterrupts(ctx, sc, cancel, pause, stopCh, done)

	var once sync.Once
	return cancel, pause, func() {
		once.Do(func() { close(stopCh) })
		// Wait for the goroutine to exit so a returned stop() is a guarantee that nothing is
		// still reading the Secret. Receiving from a closed channel never blocks, so calling
		// stop() again is free.
		<-done
	}
}

// pollInterrupts is startInterruptWatch's goroutine body. It is the only writer of cancel and
// pause, so the "closed at most once" guards are plain bools rather than sync.Once.
func (w *watcher) pollInterrupts(ctx context.Context, sc corecontrollers.SecretController, cancel, pause chan struct{}, stopCh, done chan struct{}) {
	defer close(done)

	ticker := time.NewTicker(interruptPollInterval)
	defer ticker.Stop()

	var cancelClosed, pauseClosed, errorLogged bool
	for {
		select {
		case <-ctx.Done():
			return
		case <-stopCh:
			return
		case <-ticker.C:
		}

		annotations, ok := w.readInterruptAnnotations(sc)
		if !ok {
			continue // a transient read failure must not interrupt anything
		}

		// Delegated to readInterrupt so validity is decided in exactly one place.
		interrupt, err := readInterrupt(annotations)
		if err != nil {
			// This is the one place this file's two paths diverge, and the divergence is
			// deliberate. Interrupting an in-flight apply is destructive — for cancel,
			// irreversibly so — and doing it on input the agent could not parse would be acting
			// on a guess. The reconcile-entry path can afford to be strict because refusing to
			// *start* costs nothing; the watch cannot. So an invalid value that appears mid-apply
			// is reported and otherwise ignored until the operator corrects it, at which point
			// the next poll sees a value it understands. The divergence is between "do not start"
			// and "do not kill" — never between "hold" and "run".
			if !errorLogged {
				// Rate-limited to once per watch: the alternative is this line every 2s for the
				// whole duration of the apply.
				errorLogged = true
				logrus.Errorf("[k8splan] not interrupting the in-flight apply: %v", err)
			}
			continue
		}

		switch interrupt {
		case applyinator.InterruptionCanceled:
			if !cancelClosed {
				logrus.Infof("[k8splan] %s observed during an in-flight apply; cancelling it", PlanCancelledAnnotation)
				close(cancel)
				cancelClosed = true
			}
		case applyinator.InterruptionPaused:
			if !pauseClosed {
				logrus.Infof("[k8splan] %s observed during an in-flight apply; pausing at the next instruction boundary", PlanPausedAnnotation)
				close(pause)
				pauseClosed = true
			}
		case applyinator.InterruptionNone:
		}

		if cancelClosed && pauseClosed {
			return // nothing left to observe
		}
	}
}

// readInterruptAnnotations reads the plan Secret's annotations, preferring the informer cache and
// falling back to the live client. Both failing is reported as !ok and logged at debug: the caller
// keeps polling rather than treating a transient read failure as an interrupt.
//
// Cache objects are shared and must be treated as read-only. Only Annotations is read, the object
// is never mutated, and neither it nor the returned map is retained past the poll that read it.
func (w *watcher) readInterruptAnnotations(sc corecontrollers.SecretController) (map[string]string, bool) {
	secret, err := sc.Cache().Get(w.connInfo.Namespace, w.connInfo.SecretName)
	if err == nil {
		return secret.Annotations, true
	}
	logrus.Debugf("[k8splan] interrupt watch could not read secret %s/%s from cache, falling back to the API server: %v",
		w.connInfo.Namespace, w.connInfo.SecretName, err)

	secret, err = sc.Get(w.connInfo.Namespace, w.connInfo.SecretName, metav1.GetOptions{})
	if err != nil {
		logrus.Debugf("[k8splan] interrupt watch could not read secret %s/%s: %v", w.connInfo.Namespace, w.connInfo.SecretName, err)
		return nil, false
	}
	return secret.Annotations, true
}

// handleInterrupt computes the Secret data writes for an interrupt observed at reconcile entry. It
// returns an empty map when the interrupt is already recorded — see the write-once rule below.
// The caller merges the returned map into the Secret and emits the returned logs.
//
// The write-once rule: if the interrupt is already recorded, write nothing at all. Without it the
// periodic re-enqueue re-enters this path every minute for the entire duration of the pause,
// rewriting the Secret — and, worse, recomputing Completed from a reconcile where no apply is in
// flight, which would silently reset a checkpoint that had just recorded real progress. Because
// the checkpoint is durable, the rule also covers a restart: an agent that comes back up while the
// annotation is still set finds the suspension already recorded and keeps its predecessor's
// progress. It is also what makes ResumeState captured exactly once, at the moment the suspension
// is first recorded.
//
// The agent never edits the annotations; the orchestrator owns them.
func handleInterrupt(interrupt applyinator.Interruption, currentPlanState planapi.PlanState, data map[string][]byte, checksum string,
	totalOneTimeInstructions int,
) (map[string][]byte, []decisionLog) {
	switch interrupt {
	case applyinator.InterruptionCanceled:
		return handleCancellation(currentPlanState, data, checksum, totalOneTimeInstructions)
	case applyinator.InterruptionPaused:
		return handlePause(currentPlanState, data, checksum, totalOneTimeInstructions)
	case applyinator.InterruptionNone:
		// Unreachable by contract: the caller only reaches this function for a real interrupt.
		// Writing nothing is the safe response to an input that was not supposed to arrive.
		return map[string][]byte{}, []decisionLog{debugDecision("handleInterrupt called with no interruption; nothing to record")}
	}
	return map[string][]byte{}, []decisionLog{debugDecision("handleInterrupt called with unknown interruption %q; nothing to record", interrupt)}
}

// handleCancellation records a cancellation as a terminal plan-state plus a report of how far the
// plan got. The report is not a suspension: it carries Paused: false and an empty ResumeState,
// because there is nothing to resume into.
func handleCancellation(currentPlanState planapi.PlanState, data map[string][]byte, checksum string,
	totalOneTimeInstructions int,
) (map[string][]byte, []decisionLog) {
	if currentPlanState.IsTerminal() {
		// This also implements cancel's write-once guard, which keys off plan-state rather than
		// the checkpoint: PlanStateCancelled is terminal, so an already-recorded cancellation
		// lands here and rewrites nothing. Cancel keys off plan-state — unlike pause, which keys
		// off the checkpoint — because it writes no resumable checkpoint to key off.
		return map[string][]byte{}, []decisionLog{debugDecision("plan-state is %q (terminal); not recording the cancellation", currentPlanState)}
	}

	completed := parsePlanProgress(data, checksum).Completed
	updates := map[string][]byte{
		planapi.PlanStateKey: []byte(planapi.PlanStateCancelled),
		PlanProgressKey: marshalPlanProgress(planProgress{
			Checksum:  checksum,
			Completed: completed,
			Total:     totalOneTimeInstructions,
			// ResumeState and Paused are deliberately left zero: a cancellation is a report, and
			// only a suspended checkpoint ever grants a resume.
		}),
	}
	logs := []decisionLog{infoDecision("%s is set; recording plan-state %q after %d of %d one-time instructions",
		PlanCancelledAnnotation, planapi.PlanStateCancelled, completed, totalOneTimeInstructions)}
	return updates, logs
}

// handlePause records a suspension: a non-terminal plan-state plus the checkpoint the plan resumes
// from once the annotation is removed.
func handlePause(currentPlanState planapi.PlanState, data map[string][]byte, checksum string,
	totalOneTimeInstructions int,
) (map[string][]byte, []decisionLog) {
	existing := parsePlanProgress(data, checksum)
	if existing.Paused {
		// Pause's write-once guard keys off the checkpoint, not off plan-state. The checkpoint is
		// the thing that must not be recomputed, so it is the thing to test; and it is the more
		// precise signal, because plan-state == paused with no checkpoint beneath it is a state
		// this guard must NOT suppress — there is a suspension to record for the first time.
		logs := []decisionLog{debugDecision("suspension already recorded for checksum %s at %d of %d one-time instructions; not rewriting it",
			checksum, existing.Completed, existing.Total)}
		return map[string][]byte{}, logs
	}

	resumeState := currentPlanState
	if resumeState == PlanStatePaused {
		// plan-state already says paused but no checkpoint vouches for it — a hand-edited Secret,
		// or a checkpoint that was lost. "paused" is not a state to resume *into*: resolveResume
		// would hand it straight back to decidePlanStateAction, which treats every state it does
		// not know as terminal, and the plan would stall for good the moment it was unpaused.
		// Leaving it empty lets resolveResume's in-progress default take over — re-executing from
		// instruction 0 is always safe, stalling silently is not.
		resumeState = ""
	}

	updates := map[string][]byte{
		planapi.PlanStateKey: []byte(PlanStatePaused),
		PlanProgressKey: marshalPlanProgress(planProgress{
			Checksum:    checksum,
			Completed:   existing.Completed,
			Total:       totalOneTimeInstructions,
			ResumeState: resumeState,
			Paused:      true,
		}),
	}
	logs := []decisionLog{infoDecision("%s is set; holding the plan at %d of %d one-time instructions, to resume into plan-state %q",
		PlanPausedAnnotation, existing.Completed, totalOneTimeInstructions, resumeState)}
	return updates, logs
}

// writeInterruptOutcome re-reads the Secret from the API server, verifies it still carries the plan
// that was interrupted, merges updates into that fresh copy and Updates it, retrying the whole
// read-modify-write on conflict. It skips the Update entirely when nothing changed, and it returns
// errors to the caller rather than treating them as fatal.
//
// It exists instead of updateSecret because updateSecret's conflict retry only merges when
// ck.Checksum == string(secret.Data[AppliedChecksumKey]) — it carries data over only if the
// *already applied* checksum matches the plan now on the server. The interrupted path deliberately
// does not write applied-checksum, so for the common case (a new Day 2 plan being cancelled) that
// comparison is against the previous plan's checksum and fails, updateSecret returns the error, and
// reconcileSecret calls logrus.Fatalf.
//
// And the conflict is not a rare race, it is the normal path: the operator's annotation write bumps
// the Secret's resourceVersion while the agent holds a copy read before the apply started, so the
// outcome Update is guaranteed to 409. Under updateSecret's behaviour cancel would self-heal by
// crashing and pause would not — the checkpoint and the accumulated applied-output would be lost,
// so unpause would re-run from instruction 0, silently defeating the feature.
//
// An empty updates map is legal and common (it is what the write-once guard produces); it becomes a
// no-op with no Update call. The Get still happens, which is what makes the staleness check honest.
func (w *watcher) writeInterruptOutcome(sc corecontrollers.SecretController, checksum string, updates map[string][]byte) (*corev1.Secret, error) {
	var result *corev1.Secret
	// The retry wraps the whole read-modify-write, not just the Update: a conflict means the copy
	// being merged into is stale, so it has to be re-read.
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		latest, err := sc.Get(w.connInfo.Namespace, w.connInfo.SecretName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !secretCarriesPlan(latest, checksum) {
			// A newer plan has landed; that plan's own reconcile owns the state. Not an error.
			logrus.Infof("[k8splan] secret %s/%s no longer carries the plan with checksum %s; abandoning the interrupt write",
				w.connInfo.Namespace, w.connInfo.SecretName, checksum)
			result = latest
			return nil
		}

		// The live client may hand back a shared object, so mutate a copy.
		updated := latest.DeepCopy()
		if len(updates) > 0 && updated.Data == nil {
			// Defensive: secretCarriesPlan means Data is non-nil here today. Only initialise when
			// there is something to write, so an empty updates map cannot turn a nil Data into an
			// empty map and make the DeepEqual below spuriously report a change.
			updated.Data = map[string][]byte{}
		}
		maps.Copy(updated.Data, updates)
		if reflect.DeepEqual(updated.Data, latest.Data) {
			logrus.Debugf("[k8splan] interrupt outcome for secret %s/%s changed nothing, not updating secret", w.connInfo.Namespace, w.connInfo.SecretName)
			result = latest
			return nil
		}

		resulting, updateErr := sc.Update(updated)
		if updateErr != nil {
			return updateErr
		}
		// Recorded exactly as updateSecret does, so the next cache delivery is not rejected as
		// stale by reconcileSecret's rvIsOlder check.
		if w.secretUID == "" {
			w.secretUID = string(resulting.UID)
		}
		w.lastAppliedResourceVersion = resulting.ResourceVersion
		result = resulting
		logrus.Infof("[k8splan] recorded the interrupt outcome on plan secret %s/%s", w.connInfo.Namespace, w.connInfo.SecretName)
		return nil
	})
	// No logrus.Fatalf on this path, ever: reconcileSecret propagates the error and the workqueue
	// retries under its configured exponential rate limiter.
	return result, err
}

// secretCarriesPlan reports whether secret still holds the plan identified by checksum. An absent
// or unparsable plan counts as "not this plan": either way the agent has nothing to attribute an
// interrupt outcome to.
func secretCarriesPlan(secret *corev1.Secret, checksum string) bool {
	planData, ok := secret.Data[PlanKey]
	if !ok {
		return false
	}
	cp, err := applyinator.CalculatePlan(planData)
	if err != nil {
		return false
	}
	return cp.Checksum == checksum
}
