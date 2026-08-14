package k8splan

import (
	"time"

	planapi "github.com/rancher/rancher/pkg/plan"
	"github.com/sirupsen/logrus"
)

// applyOutcomeInput packages inputs for buildSecretDataUpdates.
type applyOutcomeInput struct {
	Checksum              string
	CurrentTime           time.Time
	NeedsApplied          bool
	WasFailedPlan         bool
	UsesPlanState         bool // true when currentPlanState != "" in the plan-state flow
	OneTimeOutput         []byte
	OneTimeApplySucceeded bool
	PeriodicOutput        []byte
	PriorFailureCount     []byte // secret.Data[FailureCountKey] before this reconcile
	PriorSuccessCount     []byte // secret.Data[SuccessCountKey] before this reconcile
}

// buildSecretDataUpdates computes the Secret data key writes resulting from one apply.
// The caller merges the returned map into secret.Data.
func buildSecretDataUpdates(in applyOutcomeInput) map[string][]byte {
	updates := map[string][]byte{
		AppliedPeriodicOutputKey: in.PeriodicOutput,
	}
	if in.UsesPlanState {
		// Both outcomes below are terminal for this apply, so the resume checkpoint has served
		// its purpose and must not leak into a later run. Cleared as an empty value rather than
		// a delete — see the comment on secretConflictMergeKeys: the conflict merge loop only
		// carries over keys present in the in-hand copy, so a delete would be silently lost on
		// a retry.
		//
		// Gated on the flow: the checksum flow never writes a checkpoint and nothing in it reads
		// one, so clearing it there would invent a Secret data key on a Secret owned by an
		// orchestrator that knows nothing about this feature. Unlike FailedOutputKey and
		// FailedChecksumKey below, which that flow already owns, plan-progress is not its key.
		updates[PlanProgressKey] = []byte{}
	}

	failed := (in.NeedsApplied && !in.OneTimeApplySucceeded) || (!in.NeedsApplied && in.WasFailedPlan)
	if failed {
		logrus.Debugf("[k8splan] one-time-instructions with checksum (%s) either failed or was already failed"+
			" (and cooldown period hasn't elapsed) during application", in.Checksum)
		// Update the corresponding counts/outputs
		updates[FailedChecksumKey] = []byte(in.Checksum)
		if in.NeedsApplied {
			updates[FailureCountKey] = incrementCount(in.PriorFailureCount)
			updates[FailedOutputKey] = in.OneTimeOutput
			updates[SuccessCountKey] = []byte("0")
			updates[LastApplyTimeKey] = []byte(in.CurrentTime.Format(time.UnixDate))
			if in.UsesPlanState {
				// In the new flow the agent reports failure immediately; the orchestrator
				// decides whether to retry by resetting plan-state to pending.
				updates[planapi.PlanStateKey] = []byte(planapi.PlanStateFailed)
			}
		}
		return updates
	}

	// secret.Data should always have already been initialized because otherwise we would have failed out above.
	logrus.Debugf("[k8splan] writing an applied checksum value of %s to the remote plan", in.Checksum)
	updates[AppliedChecksumKey] = []byte(in.Checksum)
	updates[AppliedOutputKey] = in.OneTimeOutput
	// On a successful application, we should blank out the corresponding failure keys.
	updates[FailureCountKey] = []byte("0")
	updates[FailedOutputKey] = []byte{}
	updates[FailedChecksumKey] = []byte{}
	if in.NeedsApplied {
		updates[LastApplyTimeKey] = []byte(in.CurrentTime.Format(time.UnixDate))
		updates[SuccessCountKey] = incrementCount(in.PriorSuccessCount)
		if in.UsesPlanState {
			updates[planapi.PlanStateKey] = []byte(planapi.PlanStateSucceeded)
		}
	}
	return updates
}
