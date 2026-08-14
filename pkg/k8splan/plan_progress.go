package k8splan

import (
	"encoding/json"

	planapi "github.com/rancher/rancher/pkg/plan"
	"github.com/sirupsen/logrus"
)

// planProgress is the resume checkpoint the agent stores under PlanProgressKey. It is scoped to
// the plan checksum and to nothing else: a checkpoint written by a previous agent lifetime is
// honored, one written for a different plan is not. That is deliberate — an agent that restarts
// while a plan is held must resume that plan where it stopped rather than re-run it.
type planProgress struct {
	Checksum    string            `json:"checksum"`
	Completed   int               `json:"completedInstructions"`
	Total       int               `json:"totalInstructions"`
	ResumeState planapi.PlanState `json:"resumeState,omitempty"` // state restored when the pause lifts
	// Paused marks the record as a *suspension* rather than a report, and is the sole gate on
	// Completed being honored: only a suspended checkpoint grants a resume. The cancel paths write
	// Paused: false records, which report how far the plan got and are never resumed from, and the
	// resume commit clears the flag so a checkpoint stops granting a resume the moment the plan is
	// no longer held.
	Paused bool `json:"paused,omitempty"`
}

// parsePlanProgress decodes the checkpoint stored under PlanProgressKey.
//   - key absent, malformed JSON, or a checksum that does not match the plan being reconciled:
//     the zero value.
//   - checksum matches: returned verbatim, whichever agent lifetime wrote it.
func parsePlanProgress(data map[string][]byte, checksum string) planProgress {
	raw, ok := data[PlanProgressKey]
	if !ok || len(raw) == 0 {
		// An empty value is how the checkpoint is cleared, so it is absence, not corruption;
		// decoding it would log a spurious error on every subsequent reconcile. Clearing it must
		// be an empty value rather than a delete — see the comment on secretConflictMergeKeys.
		return planProgress{}
	}
	var p planProgress
	if err := json.Unmarshal(raw, &p); err != nil {
		// Operator-visible corruption, but not fatal: the zero value costs at worst a
		// re-execution, which is always safe.
		logrus.Errorf("[k8splan] error while parsing plan progress: %v", err)
		return planProgress{}
	}
	if p.Checksum != checksum {
		logrus.Debugf("[k8splan] discarding plan progress recorded for checksum %s while reconciling checksum %s", p.Checksum, checksum)
		return planProgress{}
	}
	return p
}

// marshalPlanProgress encodes p for storage under PlanProgressKey.
func marshalPlanProgress(p planProgress) []byte {
	raw, err := json.Marshal(p)
	if err != nil {
		// Unreachable: a struct of strings, ints and a bool cannot fail to marshal.
		logrus.Errorf("[k8splan] error while marshalling plan progress: %v", err)
		return nil
	}
	return raw
}

// resolveResume maps a stored plan-state onto the state the reconcile should act on. A plan is
// treated as suspended if plan-state says so *or* if a valid checkpoint claims it — the latter is
// what keeps resume working across an agent restart, and when an external write has moved
// plan-state out from under a checkpoint. Every other state passes through unchanged with
// resumeFrom 0.
//
// Precondition: the caller has already established that both interrupt annotations read as an
// explicit false. This function describes how to *leave* a suspension, never whether to; a plan
// that is still held — or whose annotation could not be parsed — never reaches it.
func resolveResume(state planapi.PlanState, data map[string][]byte, checksum string) (planapi.PlanState, int) {
	if state == "" { // checksum flow: no checkpoint is ever written, nothing to resolve
		return state, 0
	}
	p := parsePlanProgress(data, checksum)
	if state != PlanStatePaused && !p.Paused {
		return state, 0
	}
	if !p.Paused {
		// plan-state says paused but no suspended checkpoint vouches for any progress: a
		// hand-edited Secret, or a cancel report on a plan someone then paused. Resume the state,
		// not the position.
		return orDefault(sanitizeResumeState(p.ResumeState), planapi.PlanStateInProgress), 0
	}
	return orDefault(sanitizeResumeState(p.ResumeState), planapi.PlanStateInProgress), p.Completed
}

// sanitizeResumeState rejects a stored ResumeState of PlanStatePaused, treating it as unset so
// orDefault's in-progress fallback takes over.
//
// Resuming *into* paused is a silent permanent stall: decidePlanStateAction routes every state it
// does not know to its terminal default branch, so the plan would never run again and never leave
// paused, with no annotation left for an operator to remove. handleInterrupt already refuses to
// write such a record; this is the reading half, for a hand-edited Secret or one written by a
// build that predates that guard. Re-executing from instruction 0 is always safe; stalling
// silently is not.
func sanitizeResumeState(state planapi.PlanState) planapi.PlanState {
	if state != PlanStatePaused {
		return state
	}
	logrus.Warnf("[k8splan] ignoring a stored resume state of %q, which would stall the plan permanently; resuming into %q instead",
		PlanStatePaused, planapi.PlanStateInProgress)
	return ""
}

// orDefault returns v unless it is the zero value, in which case it returns def.
func orDefault[T comparable](v, def T) T {
	var zero T
	if v == zero {
		return def
	}
	return v
}
