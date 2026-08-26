# Plan cancellation and pause support in rancher-system-agent

## Context

`github.com/rancher/rancher/pkg/plan` now models an explicit plan lifecycle
(`pending` → `in-progress` → `succeeded` | `failed` | `canceled`), and the agent already drives
those transitions in `pkg/k8splan/reconcile.go`. Two lifecycle controls are still missing on the
node side:

- **Cancellation** — an operator needs to abort a Day 2 operation that is hanging, was triggered by
  accident, or has left the cluster in a partial state. `PlanStateCanceled` exists in the API but
  nothing ever writes it.
- **Pause** — an operator needs to hold execution at an instruction boundary (maintenance window,
  incident) and later resume from where it stopped rather than re-running the whole plan.

Both are signaled by annotations on the plan Secret (`plan.cattle.io/canceled: "true"`,
`plan.cattle.io/paused: "true"`; the only other valid value is `"false"`, and anything else is an
error the operator has to correct). The hard part is that `Applyinator.Apply` runs synchronously
inside the `OnChange` handler with `DefaultWorkers: 1`, so while an apply is in flight the
controller cannot deliver the annotation change. Detecting an interruption mid-apply needs a
separate observer, and stopping cleanly needs the applyinator to grow interruption points.

**Outcome:** an operator can cancel or pause a plan at any point in its lifecycle; the agent stops,
reports the resulting state in the Secret, and — when the operator clears the annotation — resumes
a paused plan at the first instruction that has not yet completed, including when an agent restart,
crash or upgrade intervened while the plan was held.

Resuming is triggered by exactly one event: **the operator clearing the annotation**, by deleting
it or by setting it to `"false"` — the two are indistinguishable to the agent. Nothing else resumes
a plan — not a restart, not a crash, not the periodic re-enqueue, not the plan-state flow's own
crash-recovery rule. See "The suppression invariant" below; it is the safety property this feature
exists to provide, and the design is arranged so that no code path can violate it.

The two controls stop at different granularity, and the difference is load-bearing:

- **Cancel is prompt.** It signals the in-flight instruction's process tree (SIGTERM, then SIGKILL
  after a grace period) and abandons the plan. This is the tool for a hung instruction.
- **Pause is a boundary.** It lets the running instruction finish and stops before the next one.
  Pause latency is therefore the remaining runtime of the current instruction, and pause will
  **never** take effect on an instruction that never returns. That is deliberate: a checkpoint is
  only trustworthy if every instruction it claims is complete actually ran to completion.

That boundary property is also what lets the checkpoint outlive the agent process. A suspended plan
is never parked inside a half-executed instruction, so the record of what completed is exactly as
true after a restart as it was before one. Pause is consequently the one lifecycle state that
survives a crash intact: an interrupted *execution* still re-runs from the beginning, an interrupted
*hold* does not.

## Settled decisions

| Decision                              | Choice                                                                                                                                                                                                                 |
|---------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Where new wire constants live         | Locally in `pkg/k8splan` for now; follow-up to upstream them into `rancher/pkg/plan`                                                                                                                                   |
| Paused representation                 | New non-terminal plan-state value `paused`                                                                                                                                                                             |
| Resume semantics                      | Checkpoint the instruction index in the Secret; completed instructions are not re-run                                                                                                                                  |
| Checkpoint durability                 | Independent of the agent process. Keyed to the plan checksum alone, so it survives restart, crash and agent upgrade                                                                                                    |
| Restart while a plan is paused        | Never executes — nothing at all runs while the annotation is set, whatever the plan-state or checkpoint says. Only the operator clearing it resumes the plan, and then at the first incomplete instruction              |
| Restart while a plan is *executing*   | Unchanged crash-recovery contract: `in-progress` with no suspended checkpoint re-executes from instruction 0                                                                                                           |
| Stopping a running instruction        | Cancel only. SIGTERM → SIGKILL after 10s, to the whole **process group** (Windows: Job Object terminate)                                                                                                               |
| Pause granularity                     | Instruction boundary only; never interrupts a running instruction                                                                                                                                                      |
| Probes during an interrupt            | Keep running. An interrupt suppresses *execution*, never *observation*                                                                                                                                                 |
| Writing the interrupted outcome       | Fresh read-modify-write against the API server, never a merge from the stale in-hand copy                                                                                                                              |
| Valid annotation values               | Plan-state flow: exactly `"true"` and `"false"`; absent means `"false"`. Any other value is a configuration error. Checksum flow: annotations are unsupported and ignored with a warning                                 |
| Checksum-flow annotation behavior     | No effect on execution or state; no checkpoint writes; one warn-level log per reconcile when either annotation is present                                                                          |
| Scope                                 | Remote mode (`pkg/k8splan`) only — annotations are a Secret concept; `pkg/localplan` is unaffected                                                                                                                     |

## Wire format additions

New constants in `pkg/k8splan/watcher.go`:

```go
// TODO: upstream these into github.com/rancher/rancher/pkg/plan alongside PlanStateCanceled,
// then drop the local definitions and bump the dependency.

// PlanCanceledAnnotation, set to "true" on the plan Secret, requests that the agent abort the plan.
// Removing it does not un-cancel: the plan is terminal and waits for new content.
// The only valid values are "true" and "false"; see readInterrupt.
PlanCanceledAnnotation = "plan.cattle.io/canceled"
// PlanPausedAnnotation, set to "true" on the plan Secret, requests that the agent hold execution.
// While it is set the agent executes nothing for this plan, whatever plan-state or the resume
// checkpoint say; clearing it is the only thing that resumes the plan.
// The only valid values are "true" and "false"; see readInterrupt.
PlanPausedAnnotation = "plan.cattle.io/paused"
// PlanProgressKey is the Secret data key holding the resume checkpoint (JSON, see planProgress).
PlanProgressKey = "plan-progress"
// PlanStatePaused is a non-terminal plan-state: execution is held at an instruction boundary and
// will resume into planProgress.ResumeState when the pause annotation is removed.
PlanStatePaused planapi.PlanState = "paused"
```

The `plan.cattle.io/` prefix is intentional and matches the doc comment on `PlanStateCanceled` in
upstream `state.go`, even though the annotations already in use by the planner
(`PlanLastUpdatedAnnotation`, `PlanProbesPassedAnnotation`) carry the `rke.cattle.io/` prefix. The
upstream form of these four constants is spelled out in "Proposed changes in rancher/rancher" §1;
defining them locally first keeps this change independent of a dependency bump.

`PlanProgressKey` is appended to `secretConflictMergeKeys` so that the *clear* performed on a
terminal outcome survives a conflict retry. While doing so, fix the merge loop to skip keys absent
from the in-hand copy — today it writes a nil value for every key in the list, materialising empty
Secret data keys:

```go
for _, key := range secretConflictMergeKeys {
    if v, ok := secret.Data[key]; ok {
        latestSecret.Data[key] = v
    }
}
```

### planProgress

New `pkg/k8splan/plan_progress.go`. The record is scoped to the plan checksum, so a checkpoint
recorded for a different plan is not trusted — and to nothing else, so one recorded by a previous
agent lifetime is:

```go
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
//   - malformed JSON, or a checksum that does not match the plan being reconciled: zero value.
//   - checksum matches: returned verbatim, whichever agent lifetime wrote it.
func parsePlanProgress(data map[string][]byte, checksum string) planProgress
func marshalPlanProgress(p planProgress) []byte
```

#### Why the checkpoint outlives the agent

The plan checksum is the only scope. There is deliberately no per-process identifier: an agent that
restarts while a plan is held must resume that plan where it stopped, not re-run it from the
beginning. Restarts are not exotic here — the interlock exists precisely because `install.sh`
restarts the agent during an upgrade, and a plan paused across that upgrade has to come back
correctly.

This does not weaken the crash-recovery contract, because the two situations are not the same
situation:

|                                        | `in-progress` at startup                          | suspended checkpoint at startup                           |
|----------------------------------------|---------------------------------------------------|-----------------------------------------------------------|
| What was happening when the agent died | an instruction was executing, at an unknown point | nothing was executing; the plan was parked at a boundary  |
| What the agent can honestly claim      | nothing about the last instruction                | every instruction below `Completed` returned successfully |
| Behaviour                              | re-execute from instruction 0                     | hold, then resume at `Completed`                          |

The invariant that makes this safe is one-directional: **the checkpoint can under-report progress,
never over-report it.** It is written only after an instruction has returned and only while no
instruction is in flight. Losing the write — the agent dies between the last instruction returning
and the Secret `Update` landing — leaves an older, lower `Completed`, or none at all, which costs
re-executed work but never skips an instruction that did not run.

The checkpoint's claim is "these instructions ran to completion", not "their effects are still
present on this node". That is the same assumption the applyinator already makes between two
instructions of a single apply; see "Remaining risks" item 4 for where it stops holding.

#### Checkpoint lifecycle

| Event                                     | Effect on `plan-progress`                                                               |
|-------------------------------------------|-----------------------------------------------------------------------------------------|
| Pause observed (entry path or mid-apply)  | written with `Paused: true`, `Completed`, `ResumeState`                                 |
| Cancel observed                           | written with `Paused: false` as a report; never resumed from                            |
| Re-enqueue while still paused             | untouched — the write-once rule                                                         |
| Agent restart or crash                    | untouched; read back verbatim                                                           |
| Pause lifted                              | `Paused` cleared in the same write that resolves `plan-state` — see "Leaving the pause" |
| Plan content changes                      | ignored on read (checksum mismatch); overwritten by the new plan's own outcome          |
| Terminal outcome (`succeeded` / `failed`) | key cleared by `buildSecretDataUpdates`                                                 |

## Behavior rules

### The suppression invariant

> **In the plan-state flow, the agent executes a plan only when both `plan.cattle.io/paused` and
> `plan.cattle.io/canceled` are unambiguously not set — absent, or present with the value
> `"false"`. Anything else stops execution. Full stop — no matter what `plan-state` says, no matter
> what the checkpoint says, no matter how the agent arrived at this reconcile.**

In the checksum flow (`plan-state` absent, legacy orchestrators), these annotations are unsupported
and intentionally have no effect: the agent logs a warning on **every reconcile** where either key
is present and continues checksum reconciliation.

In the plan-state flow, note the shape of that statement: it is a whitelist of the two ways a plan
is permitted to run, not a blacklist of the ways it is held. A value the agent does not recognise
falls on the *stop* side by construction rather than by an explicit rule, which is what makes the
invariant hold for values nobody anticipated. What such a value additionally does — see "Invalid
annotation values" below — is report an error; what it does not do, ever, is let the plan proceed.

Every other rule in this document is subordinate to that one. It is stated separately because the
dangerous failure mode is not "pause does not work" — an operator notices that immediately — but
"pause works until the agent restarts, and then the plan runs anyway", which is silent, arrives
minutes or hours after the operator walked away, and is exactly the situation pause exists to
prevent.

The invariant is upheld structurally rather than by remembering to check for it: in the plan-state
flow, `readInterrupt` is evaluated at the top of the reconcile, and both the interrupt branch and
the invalid-value branch **return**. Everything capable of starting work — `resolveResume`, the
resume commit, `decidePlanStateAction`, the `pending → in-progress` pre-commit, `Apply` — sits
below those returns and therefore runs only when `readInterrupt` reported no error and
`InterruptionNone`. There is no ordering in which a suspended plan reaches them.

Three consequences worth naming, because each is a plausible bug that this ordering rules out:

- **Restart does not resume.** The first reconcile after startup is an ordinary reconcile: the
  annotation is read, the interrupt branch returns, nothing executes. The plan-state flow's
  `in-progress → re-execute` crash-recovery rule is never reached, so a plan that was paused
  mid-apply is not restarted from instruction 0 the way an ordinary crashed plan would be.
- **The checkpoint never triggers execution, only positions it.** `resumeFrom` is an argument to an
  `Apply` that some *other* condition already decided to run. A checkpoint on a Secret that still
  carries the annotation is inert.
- **No configuration can weaken it.** The suppression decision reads the annotation and nothing
  else — not `plan-state`, not the checkpoint, not any config field. There is deliberately no
  setting that turns pause off; an operator who wants the plan to run clears the annotation.

Resuming has exactly one trigger — the operator clearing the annotation, by removing it or by
setting it to `"false"` — and the reconcile that observes the change is the only one that may
execute. The two forms are indistinguishable to the agent and are treated identically everywhere in
this document; "removing the annotation" is shorthand for both.

### Invalid annotation values (plan-state flow only)

The only valid values are `"true"` and `"false"`. An absent annotation is `"false"`. Everything else
— `"True"`, `"1"`, `"yes"`, `""`, a stray trailing space — is a **configuration error**, not a
request, and the operator has to correct it. This section applies only when `plan-state` is present;
checksum flow logs and ignores annotation values.

The agent's response to one is deliberately narrow. It:

- **executes nothing.** This is the suppression invariant, which does not distinguish an invalid
  value from `"true"`;
- **interrupts nothing.** An invalid value is not a pause request and not a cancel request. An apply
  already in flight runs to completion; no process is killed, no checkpoint is written, `plan-state`
  is not moved;
- **writes nothing to the Secret.** No `plan-state`, no checkpoint, not even probe statuses on this
  path — the agent has been handed input it cannot interpret and does not respond by editing the
  object. `resourceVersion` is therefore stable for the whole time the error persists, which also
  means the error cannot amplify into a write loop;
- **reports.** `logrus.Errorf` naming the annotation, the offending value and the two accepted ones,
  and `reconcileSecret` returns the error so the workqueue retries under its existing exponential
  rate limiter (1m–5m). Correcting the annotation arrives as a watch event and is picked up
  immediately; the backoff is only the floor.

Do nothing, say so loudly, retry cheaply. The reason the response is not richer — no `plan-state:
failed`, no invented error key — is that `failed` means "the instructions ran and did not succeed",
which is a claim about the node. Nothing ran. Overloading it would make a typo indistinguishable
from a broken installer in every downstream consumer, and would trip failure-count and max-failures
machinery on a plan that was never attempted.

One exception, and it is the emergency-stop one. Precedence is:

1. `plan.cattle.io/canceled == "true"` → **cancel**, even if the other annotation's value is
   invalid. An operator stopping a runaway installer is not made to fix a typo on an unrelated
   annotation first. The invalid value is still logged.
2. Otherwise, either value invalid → the error path above.
3. Otherwise, `plan.cattle.io/paused == "true"` → **pause**.
4. Otherwise → execute.

Steps 1 and 3 are the existing "cancel wins over pause" rule; the ordering above just says where
validation sits relative to it. Note that an invalid *cancel* value does not permit a valid pause to
take effect — pause is the weaker request, and honoring it while the stronger one is unreadable
would let the agent act on a guess about what the operator meant.

The right long-term fix is upstream of the agent: if Rancher exposes pause and cancel as a UI action
or API field, only the two valid values are constructible and this path is unreachable. It exists
for the `kubectl annotate` escape hatch. See "Proposed changes in rancher/rancher" items 6 and 10.

### Execution versus observation

The governing rule for what is suppressed: **an interrupt suppresses execution, never observation.**
While either annotation is set the agent skips `Apply` entirely — no file reconciliation, no
one-time instructions, no periodic instructions — but it still runs probes and still persists probe
statuses. Freezing probe statuses would feed stale health data to Rancher's MachineHealthCheck on
exactly the nodes most likely to be unhealthy (a plan canceled mid-flight leaves the node in a
partial state). Reporting honestly is the safer default, and it costs nothing: `applyProbeResult`
saturates its counters at the success/failure threshold, so a steady-state probe produces identical
Secret data and the existing `reflect.DeepEqual` guard suppresses the write.

The invalid-value path is the one exception, and it is not really an exception to this rule: an
invalid value is not an interrupt at all, so there is nothing to observe *about* — the reconcile
never reaches the point where it has decided what is happening to the plan. It returns the error
and touches nothing, probes included. Probes resume the moment the value is corrected, at which
point the plan is either held (and observed) or running.

On top of that:

- **Cancel wins over pause.** If the effective state is non-terminal, write `plan-state: canceled`
  plus a `plan-progress` record. If it is already terminal, log at debug and write nothing.
- **Pause writes `plan-state: paused`** and a `plan-progress` record. Only in the plan-state flow —
  see below.
- **State is written once.** If the interrupt is already recorded, the interrupt path writes nothing
  at all. Without this guard the periodic re-enqueue re-enters the interrupt path every few seconds
  for the entire duration of the pause, rewriting the Secret — and, worse, recomputing `Completed`
  and `ResumeState` from a reconcile where no apply is in flight, which would silently reset a
  checkpoint that had just recorded real progress.

  "Already recorded" is read from the **checkpoint**, not from `plan-state`: `plan-progress.Paused`
  for pause, `plan-state == canceled` for cancel. The checkpoint is the thing that must not be
  recomputed, so it is also the thing to test; and it is strictly the more precise signal, because
  `plan-state == paused` with no checkpoint beneath it is a state the guard must *not* suppress —
  there is a suspension to record for the first time. Cancel keys off `plan-state` because it
  writes no resumable checkpoint: its `plan-progress` record has `Paused: false` and exists only to
  report how far the plan got.

  Because the checkpoint is durable, this guard also covers a restart: an agent that comes back up
  while the annotation is still set finds the suspension already recorded, writes nothing, and keeps
  the progress its predecessor recorded.
- **A restart while paused re-enters the hold, not the plan** — the suppression invariant above.
  The two halves of "a paused plan is not executed after a restart" are independent and neither
  relies on the other: the annotation check stops the plan running, and the durable checkpoint makes
  the *eventual* unpause resume rather than restart. If the checkpoint were lost entirely, the plan
  would still be held; it would merely re-run from instruction 0 once released.
- **Canceled is terminal and needs no special-casing.** `decidePlanStateAction` already returns
  `NeedsApplied: false` for every terminal state, which is precisely "monitor only". Once the
  annotation is removed, a canceled plan behaves exactly like a failed one: probes run, nothing
  executes, and the orchestrator must deliver new content with `pending` to move forward. No `Halt`
  flag and no change to terminal-state handling are required.
- **Checksum flow** (`plan-state` absent, legacy orchestrators): annotations are unsupported and
  have **no effect**. The agent logs a warning on **every reconcile** when either annotation is
  present and proceeds with ordinary checksum reconciliation. It does not write `plan-state` or
  `plan-progress`, and it never starts interrupt watches/channels for this flow.

  Recommended warning shape (one line per present key, every reconcile):
  `warn: ignoring unsupported annotation in checksum flow key=<annotation> value=<value>`
- **The agent never edits the annotations**; the orchestrator owns them. A new plan delivered with
  the cancel annotation still set is canceled again — logged at warn level.

### ResumeState

`ResumeState` is the plan-state the plan would carry had the pause not intervened. It is captured
**exactly once**, at the moment the suspension is first recorded, and is never recomputed while the
plan stays paused (guaranteed by the write-once rule above) — including across a restart, since the
agent that comes back finds the suspension already recorded and leaves the record alone.

| Where the pause was observed                        | ResumeState                                                                                                    | Completed                                                                                                           |
|-----------------------------------------------------|----------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| Reconcile entry, before Apply                       | `currentPlanState` — the state being suspended, which may be `pending`, `in-progress`, `succeeded` or `failed` | preserved from an existing checksum-matching checkpoint, including one written by an earlier agent lifetime, else 0 |
| During Apply, one-time set running (`needsApplied`) | `in-progress` — the Secret was already transitioned to `in-progress` before Apply ran                          | `applyOutput.CompletedOneTimeInstructions`                                                                          |
| During Apply, periodic-only (`!needsApplied`)       | `effectiveState` — the state the monitoring reconcile was in                                                   | 0, with `Total` 0: there is nothing to resume                                                                       |

The third row is the one that makes the distinction worth drawing. A `succeeded` plan still runs
periodic instructions on every reconcile; a pause landing in that window must restore `succeeded`.
Defaulting it to `in-progress` would re-execute a completed Day 2 operation in full on unpause.

All three rows set `Paused: true`; the cancel paths write the same record with `Paused: false` and
no `ResumeState`, since there is nothing to resume into.

`Completed` is an absolute count over the plan's one-time instructions, not a count of what the
current apply ran: the loop starts at `ResumeFromOneTimeInstruction` and reports `index + 1`. A plan
paused after instruction 2, resumed, and paused again three instructions later therefore checkpoints
5, not 3 — successive pause/resume cycles compose instead of resetting. This matters more now that a
checkpoint can span several agent lifetimes.

`succeeded` and `failed` restore as no-ops: `decidePlanStateAction` sends them down the terminal
branch.

### Resume path resolution

At the top of the reconcile, after `currentPlanState` is read:

```go
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
        return orDefault(p.ResumeState, planapi.PlanStateInProgress), 0
    }
    return orDefault(p.ResumeState, planapi.PlanStateInProgress), p.Completed
}
```

`resumeFrom` is non-zero only on the reconcile that lifts a pause — the one where the annotation is
gone but the suspended checkpoint is still on the wire. That reconcile may well be the first one
after a restart, which is the point: the operator's unpause is honored identically whether or not
the agent that recorded the checkpoint is still running. Plain `in-progress` with no suspended
checkpoint keeps today's documented crash-recovery contract (re-execute from the beginning), and the
resume commit below is what returns a resumed plan to that contract.

#### Leaving the pause: the resume commit

The reconcile that lifts a pause writes once, before it applies anything: `plan-state` becomes the
resolved state and the checkpoint's `Paused` flag is cleared (`Completed` and `Total` are kept, now
as a record rather than a claim). Three reasons, in order of importance:

1. **A plan that is executing must not report `paused`.** Without this write the wire state stays
   `paused` for the whole duration of the resumed apply — and where `ResumeState` is terminal,
   forever, because no outcome write ever follows. That case is not a corner: pausing a node whose
   plan already `succeeded` and which is only running periodic instructions is the likeliest
   operator action of all.
2. **It re-arms the write-once guard.** A second pause on the same plan must record fresh progress;
   a lingering `Paused: true` would make the entry path treat the new suspension as already
   recorded and keep the stale `Completed`.
3. **It bounds the checkpoint's authority to the hold.** A checkpoint is honored only while the
   plan is suspended, so a crash *during* a resumed apply falls back to re-executing from
   instruction 0 — the ordinary contract — rather than trusting a record whose plan is no longer
   parked at a boundary.

It is one write, not two: where the resolved state is `pending` the flag is cleared in the same
`Update` as the existing `pending → in-progress` pre-commit; where it is terminal it is the only
write the reconcile makes. `plan-revision` is not bumped — the plan content has not changed. The
write goes through `writeInterruptOutcome`, which covers both entering and leaving an interrupt
(fresh read-modify-write, checksum-verified, non-fatal); if it fails the error is returned, the
workqueue retries, and no apply runs until it lands. The checksum flow has no checkpoint and
therefore no resume commit.

### Compatibility with today's Rancher — verified

Writing an unknown `plan-state` value must be inert against a server that predates it. This has
been checked directly against `rancher/rancher` at `73d6cd578`, not assumed.

`SecretToNode` in `pkg/capr/planner/store.go:297-308` switches on the state and routes anything it
does not recognize to a default branch:

```go
switch result.PlanState {
case planapi.PlanStateSucceeded:  result.InSync = true
case planapi.PlanStateFailed:     result.Failed = true
case planapi.PlanStateInProgress: result.InSync = false
default: // Empty or "pending": use checksum comparison
    result.InSync = bytes.Equal(planData, appliedPlanData)
}
```

`paused` lands in `default`. There is no error path, no parse failure, and no misclassification as
succeeded or failed — the planner simply falls back to comparing `plan` against `appliedPlan`.
Since the interrupted path does not write `applied-checksum`, the `plansecret` controller never
copies `plan` into `appliedPlan` (`pkg/controllers/capr/plansecret/plansecret.go:71-72`), so the
comparison yields `InSync = false`. **Writing `paused` is safe against today's server.**

Three consequences follow, and they are properties of the *existing* server rather than of this
change:

1. **`canceled` is in the same default branch.** `PlanStateCanceled` is declared in
   `pkg/plan/state.go:38` and has no `case` anywhere in the planner — it is a dead constant today.
   A canceled plan is therefore also evaluated by checksum and also reports `InSync = false`.
2. **The planner will not fight a pause.** `UpdatePlan` — the only writer of `plan-state: pending`
   (`store.go:407-408`) — is called only when the desired plan actually differs
   (`assignAndCheckPlan`, `store.go:483-486`). A paused plan whose content is unchanged is never
   re-driven, so the pause holds. If the desired plan *does* change mid-pause, the planner writes new content plus
   `pending` while the annotation is still set; the agent then records `ResumeState: pending`
   against the new checksum and holds, which is the correct outcome.
3. **A suspended node is counted as unavailable.** `isUnavailable` (`planner.go:531-534`) is
   `!InSync || ...`, so a paused or canceled node consumes the rolling-update availability budget
   and stalls cluster-wide convergence. The operator-visible symptom is
   `getPlanStatusReasonMessage` (`store.go:150-168`) returning the generic
   `"waiting for plan to be applied"`, and `plansecret.go:184-185` clearing the machine's
   `PlanApplied` condition to nil — i.e. Rancher reports a healthy-looking wait with no indication
   that a human suspended the plan.

Point 3 is the real cost of shipping the agent side alone: the behaviour is safe but opaque. Fixing
the opacity is Rancher-side work, specified in "Proposed changes in rancher/rancher" below.

The other consumer, `pkg/plan/store.go`'s `Store.AssignPlan`, never reads `plan-state` at all — it
decides `Pending` purely by comparing plan bytes, so `paused` is invisible to it. It needs the same
treatment if it supersedes the CAPR path as its doc comment anticipates.

### Mixed-version behavior: new Rancher, old agent — verified

Rancher installs and upgrades the agent, so an upgraded Rancher will deliver plans to nodes still
running the previous agent for the length of the rolling agent upgrade. If such a plan carries
`plan.cattle.io/paused` or `plan.cattle.io/canceled`, what does the old agent do? Checked against
this repository at `HEAD` (the last release before this change):

1. **It never reads annotations.** `grep -rn "Annotation" pkg/ main.go`, excluding tests, returns
   nothing. `reconcileSecret` reads named `secret.Data` keys and the object's `UID` and
   `ResourceVersion`; `secret.Annotations` is not referenced anywhere in the agent.
2. **Unknown data keys are inert and preserved.** The handler deep-copies the informer object,
   writes only the keys it knows, and `Update`s the whole object, so a `plan-progress` key written
   by a newer agent (or left behind by one) survives untouched. The no-op guard
   `reflect.DeepEqual(originalSecret.Data, secret.Data)` is not tripped by a key present in both
   copies, so an unknown key does not cause a write loop either.
3. **Nothing clobbers the annotation.** A plain `Update` from a copy read before the annotation was
   added carries a stale `resourceVersion` and 409s; `updateSecret`'s conflict path then re-`Get`s
   the live object — which has the annotation — and copies only `secretConflictMergeKeys` into it.
   Annotations and unknown data keys on the fresh object are preserved.
4. **An unknown `plan-state` is treated as terminal.** `decidePlanStateAction`'s default branch
   returns `NeedsApplied: false`, and its doc comment already says "including states not yet known
   to this build". So the reverse case — a node downgraded to an older agent while `plan-state` is
   `paused` — is also inert.

**Conclusion: no malfunction.** The annotations cannot crash, corrupt or confuse an old agent; they
are invisible to it.

**But inert is not the same as safe**, and this is the finding that matters operationally: an old
agent **executes the plan normally**. Pause is not "not yet supported" in a way anyone can see — it
is a control that appears to be accepted (the annotation is set, `kubectl get` shows it) and does
nothing, with no error, no event and no status. An operator who pauses a node mid-upgrade to hold a
Day 2 operation will watch the operation proceed. Cancel is worse: the operator believes a hung
installer has been stopped.

Nothing on the agent side can fix this — an old binary cannot be taught to read a new annotation.
The mitigation is Rancher-side and is added as item 8 below: **do not offer pause/cancel for a
machine whose agent predates support for it.** Rancher can determine this; the agent does not
report its own version into the plan Secret today, so the check has to be based on the version
Rancher installed (`settings.SystemAgentVersion`, the `system-agent-version` setting, is the desired
version and the plan that installs it is Rancher's own) or on a new key the agent writes. Recording
the running version in the Secret is the more honest signal and is worth doing regardless, since it
also tells an operator what is actually on the node.

Two smaller findings from the same read of `rancher/rancher` at `73d6cd578`, both of which make the
agent-side design work as written:

- `UpdatePlan` (`pkg/capr/planner/store.go:364+`) mutates the existing `secret.Data` in place and
  deletes only `probe-statuses` when new content is written. A stale `plan-progress` would therefore
  survive a plan change — harmless, because the checkpoint is checksum-scoped and a mismatched one
  decodes to the zero value, but item 9 below adds the matching `delete` for tidiness.
- `CopyPlanMetadataToSecret` → `CopyMap` (`pkg/capr/common.go:503-536`) is additive-only and filters
  on `labelAnnotationMatch` = `^((rke\.cattle\.io)|((?:machine\.)?cluster\.x-k8s\.io))/`. Two
  consequences: an operator-set `plan.cattle.io/paused` on the Secret is **never pruned** by a plan
  update, which is what makes the annotation a stable hold; and any future attempt to propagate the
  annotation from `Machine` metadata down to the Secret would be **silently dropped** by that
  filter. See item 6 — if propagation is ever wanted, either the prefix has to change to
  `rke.cattle.io/` or the regex has to be extended. Annotating the Secret directly, which is what
  this design assumes, is unaffected.

### On not shipping a kill switch

An earlier revision carried a `DisablePausedPlanState` config field plus a
`CATTLE_AGENT_DISABLE_PAUSED_PLAN_STATE` env override, to suppress writing `plan-state: paused` if a
planner mishandled the value. It has been removed. Both legs of its justification are gone:

- **The risk was measured, not assumed.** "Compatibility with today's Rancher — verified" above
  establishes that an unrecognized `plan-state` lands in `SecretToNode`'s default branch and is
  evaluated by checksum, with no error path. There is nothing to hedge against.
- **The two sides ship together.** Rancher owns the agent's version and delivers it, so a node
  running an agent that writes `paused` is by construction managed by a Rancher that was upgraded
  first. The mixed-version window runs in the direction the previous section analyzes — new Rancher,
  old agent — not the direction the kill switch protected.

What is given up is an in-field escape hatch: if `paused` did turn out to be mishandled, the fix is
a patch release of the agent rather than an env var and a restart. That is the normal cost of
shipping a feature, and it is a better trade than carrying a config field, an env var, a
`WatchOptions` refactor and a permanently-tested degraded code path to hedge a risk that has been
verified to be zero.

`planProgress.Paused` stays, but its justification changes. It is no longer "so the wire state can
be suppressed"; it is load-bearing for two reasons that have nothing to do with the kill switch:
it distinguishes a *suspension* from a cancel *report* (both write a checkpoint, only one may be
resumed from), and clearing it is how the resume commit bounds a checkpoint's authority to the
period the plan is actually held.

If something does go wrong, two fallbacks remain and neither needs new configuration, because both
are shipping decisions rather than runtime ones:

| Failure discovered              | Response                                                                     | What still works                                                                    |
|---------------------------------|------------------------------------------------------------------------------|-------------------------------------------------------------------------------------|
| Planner mishandles `canceled`  | Do not ship cancel; pause is independent of it                               | Pause, checkpoint, resume                                                           |
| Checkpointing itself is unsound | Ship cancel only, or set `ResumeFromOneTimeInstruction` to 0 unconditionally | Pause degrades to "suppress and re-run from the start" — the checksum-flow semantic |

Neither requires reverting the applyinator work: the interruption points, the process-tree kill and
the fresh read-modify-write are orthogonal to how the state is reported.

## Changes by package

### 1. `pkg/applyinator` — interruption points

`applyinator.go`:

- New `Interruption` string type: `InterruptionNone` (`""`), `InterruptionPaused`,
  `InterruptionCanceled`.
- `ApplyInput` gains `Cancel <-chan struct{}`, `Pause <-chan struct{}`, and
  `ResumeFromOneTimeInstruction int`. Zero values mean "never interrupted, start at 0", so
  `pkg/localplan/watcher.go` needs no change.
- `ApplyOutput` gains `Interruption Interruption` and `CompletedOneTimeInstructions int`. The latter
  is absolute, not per-apply: the loop starts at `ResumeFromOneTimeInstruction` and reports
  `index + 1`, so it composes across pause/resume cycles instead of restarting at 0.
- `checkInterruption(cancel, pause <-chan struct{}) Interruption` — pure, non-blocking select,
  cancel first. Nil channels are never ready.
- `Apply` short-circuits on a pre-existing interruption **before `a.mu.Lock()`**, not merely before
  touching files. An apply that is already canceled must not queue behind an in-flight apply, and
  must not sit in `checkInterlock`'s `restart-pending` wait for up to five minutes only to return
  an error instead of a clean `InterruptionCanceled`.
- After the interlock, `Apply` derives `execCtx` from `ctx` and cancels it when `input.Cancel`
  closes, so exec'd process trees are signaled.
- `runOneTimeInstructions` takes a start index and the channels and returns a `oneTimeResult`
  struct (`Output`, `Succeeded`, `Completed`, `Interruption`) rather than growing to five return
  values — matching the existing `planStateResult`/`checksumFlowResult` style. It checks
  `checkInterruption` before each instruction, and re-checks after a failed instruction so a
  cancel-induced kill is reported as canceled rather than failed. Output from a killed instruction
  is still saved when `SaveOutput` is set.
- `runPeriodicInstructions` gets the same per-iteration check.

#### `execute()` — graceful termination of the process tree

Replace `exec.CommandContext` with `exec.Command` plus an explicit watchdog, so operator
cancellation and agent shutdown take the same path:

```go
const instructionTerminationGrace = 10 * time.Second

// watchForTermination signals cmd's process tree once ctx is done: a graceful signal first,
// escalating to an unconditional kill after instructionTerminationGrace. The pipes are closed
// alongside the kill so streamLogs cannot block forever on a descendant that inherited them. The
// returned func stops the watchdog and releases any platform handles.
func watchForTermination(ctx context.Context, cmd *exec.Cmd, pipes ...io.Closer) func()
```

Signaling the **direct child only** is not sufficient, and this is promoted from a follow-up to
part of the change. Plan instructions are near-universally a `run.sh` that shells out to an
installer, a package manager or `systemctl`; SIGTERM to the shell leaves the real work running.
Reporting `plan-state: canceled` while the operation the operator wanted stopped is still running
is the worst available outcome for a safety control, so cancel must target the tree.

New `pkg/applyinator/process_unix.go` (`//go:build !windows`) and `process_windows.go`, matching the
existing `file_unix.go`/`file_windows.go` split:

| Function                     | Unix                                                                                                  | Windows                                                                                                         |
|------------------------------|-------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
| `configureProcessGroup(cmd)` | `cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}`                                               | create a Job Object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`; assign the child immediately after `cmd.Start()` |
| `terminateProcessTree(cmd)`  | `syscall.Kill(-pgid, syscall.SIGTERM)`, falling back to `cmd.Process.Signal` if the pgid lookup fails | no graceful equivalent; skipped                                                                                 |
| `killProcessTree(cmd)`       | `syscall.Kill(-pgid, syscall.SIGKILL)`                                                                | `TerminateJobObject`                                                                                            |
| `releaseProcessTree(cmd)`    | no-op                                                                                                 | `CloseHandle` on the job                                                                                        |

All of `CreateJobObject`, `SetInformationJobObject`, `AssignProcessToJobObject` and
`TerminateJobObject` are available in the pinned `golang.org/x/sys` (v0.47.0); the dependency moves
from indirect to direct, so `go mod tidy` is required for `make verify`.

Two accepted platform caveats, to be stated in code comments:

- **Windows has no SIGTERM.** Cancel terminates the job immediately with no grace period. This
  matches the previously-settled "Windows: direct kill" decision, widened to the tree.
- **Windows assignment race.** `os/exec` exposes no pre-`Start` hook and no primary thread handle,
  so the `CREATE_SUSPENDED` → assign → `ResumeThread` sequence is not available. The child is
  assigned to the job immediately after `cmd.Start()`; a descendant spawned in the intervening
  microseconds escapes the job. Accepted.

`Setpgid` does not affect systemd shutdown: the generated unit sets no `KillMode`, so the default
`control-group` applies and stop kills the whole cgroup regardless of process group. The new 10s
grace also fits comfortably inside the default `TimeoutStopSec` of 90s, since the unit sets none.

### 2. `pkg/k8splan` — detection and state writes

New `pkg/k8splan/interrupt.go`:

- `readInterrupt(annotations map[string]string) (applyinator.Interruption, error)` — pure, and the
  single place either annotation is interpreted:

  ```go
  // parseInterruptAnnotation reports whether key requests its interrupt. The only valid values are
  // "true" and "false"; an absent annotation is "false". Any other value is an operator
  // misconfiguration and is returned as an error — never silently coerced in either direction.
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
  ```

  `readInterrupt` calls it for both keys and applies the precedence from "Invalid annotation values":
  a valid `canceled == "true"` returns `InterruptionCanceled` with a nil error even when the pause
  value is invalid; otherwise any error is returned and the `Interruption` is meaningless to the
  caller; otherwise a valid `paused == "true"` returns `InterruptionPaused`; otherwise
  `InterruptionNone`. Errors from both keys are joined with `errors.Join` so a doubly-mistyped
  Secret reports both in one message rather than one per reconcile.

  Deliberately **not** `strconv.ParseBool`: it accepts `"True"`, `"TRUE"`, `"t"`, `"1"`, `"0"` and
  friends, so it would quietly accept eleven spellings of each value and reject the twelfth. The
  point of an exact match is that the accepted set is the set that can be written down in
  documentation and validated by an admission check — one spelling each, and everything else is
  visibly wrong at the moment it is set rather than subtly wrong later.
- `interruptWatch` + `(w *watcher) startInterruptWatch(ctx, sc)` — a goroutine polling every
  `interruptPollInterval` (2s) that closes `Cancel`/`Pause` the first time the matching annotation
  is observed. It reads `sc.Cache().Get(ns, name)` (the informer's indexer is updated by its own
  goroutine and is not blocked by the busy workqueue worker, so it stays fresh during an apply),
  falling back to `sc.Get(...)` if the cache read errors. Cache objects are shared and must be
  treated as read-only — the watch only reads `Annotations` and never mutates or retains the
  object. Started around the `Apply` call and stopped via `defer`.

  It calls the same `readInterrupt`, so validity is decided in one place, but its response to an
  error differs: it closes nothing and lets the apply continue, rate-limiting the log to once per
  watch rather than every 2s. Interrupting an in-flight apply is destructive — for cancel,
  irreversibly so — and doing it on input the agent could not parse would be acting on a guess. The
  entry path can afford to be strict because refusing to *start* costs nothing; the watch cannot,
  so an invalid value that appears mid-apply is reported and otherwise ignored until the operator
  corrects it, at which point the next poll sees a value it understands. This is the one place the
  two paths diverge, and the divergence is between "do not start" and "do not kill" — never between
  "hold" and "run".
- `handleInterrupt(...)` — the reconcile-entry path: emits the decision logs and computes the
  `plan-state`/`plan-progress` updates per the rules above, returning an empty update map when the
  state is already what it would write.

#### Writing the outcome: fresh read-modify-write

Both interrupt paths write through a new helper rather than through `updateSecret`:

```go
// writeInterruptOutcome re-reads the Secret from the API server, verifies it still carries the plan
// that was interrupted, merges updates into that fresh copy and Updates it, retrying the whole
// read-modify-write on conflict. It skips the Update entirely when nothing changed, and it returns
// errors to the caller rather than treating them as fatal.
func (w *watcher) writeInterruptOutcome(sc corecontrollers.SecretController, checksum string, updates map[string][]byte) (*corev1.Secret, error)
```

This is the single most important correction to the original design. `updateSecret`'s conflict
retry only merges when `ck.Checksum == string(secret.Data[AppliedChecksumKey])` — it carries data
over only if the *already applied* checksum matches the plan now on the server. The interrupted
path deliberately does not write `applied-checksum`, so for the common case (a new Day 2 plan being
canceled) that comparison is against the *previous* plan's checksum and fails. The predicate
returns false, `updateSecret` returns the error, and `reconcile.go` calls `logrus.Fatalf`.

And the conflict is not a rare race — it is the normal path. The operator's annotation write bumps
the Secret's resourceVersion while the agent holds a copy read before the apply started, so the
outcome `Update` is guaranteed to 409. Consequences under the original design:

- Cancel self-heals by crashing: the agent exits, restarts, and eventually writes `canceled` from
  a fresh object.
- Pause does not: the `plan-progress` checkpoint and the accumulated `applied-output` are lost, so
  unpause re-runs from instruction 0 — silently defeating the feature.

`writeInterruptOutcome` behaviour:

1. `retry.RetryOnConflict(retry.DefaultBackoff, ...)` around the whole read-modify-write.
2. `sc.Get(ns, name, metav1.GetOptions{})` — the live client, as already used inside
   `updateSecret`'s conflict predicate, not the cache.
3. If `CalculatePlan(latest.Data[PlanKey]).Checksum != checksum`, a newer plan has landed; log at
   info and abandon the write. That plan's own reconcile owns the state.
4. `maps.Copy` the updates in; skip `Update` when the result is `reflect.DeepEqual` to what was
   read.
5. On success, record `w.lastAppliedResourceVersion` and `w.secretUID` exactly as `updateSecret`
   does, so the next cache delivery is not rejected as stale.
6. On failure, return the error. `reconcileSecret` propagates it and the workqueue retries under
   its configured exponential rate limiter (1m–5m). **No `logrus.Fatalf` on this path.** The
   existing fatal on the normal success path is left alone; changing it is a separate question.

#### `reconcile.go` — the resulting flow

1. Existing preamble, unchanged, through `CalculatePlan`, `parseProbeStatuses`, `currentPlanState`
   and `parseFailureCount`.
2. Branch by flow first:

   - If `currentPlanState == ""` (checksum flow), annotations are unsupported. When either
     annotation key is present, log `warn` on every reconcile (include key/value) and continue as
     ordinary checksum reconcile. No interrupt decision, no lifecycle-key writes, no checkpoint
     logic.
   - Otherwise (plan-state flow), evaluate
     `interrupt, err := readInterrupt(secret.Annotations)` **before any decision to execute is
     taken**.

   If `err != nil`: `logrus.Errorf` the message and `return secret, err`. No Secret write, no
   probes, no `Apply`, no `EnqueueAfter` — the workqueue's own rate limiter owns the retry. This is
   the shortest branch in the reconcile and it comes first, because "the input cannot be
   interpreted" has to be settled before anything reads that input.

   Otherwise, if `interrupt != InterruptionNone`:
   - `handleInterrupt` computes the state updates (possibly none),
   - lifecycle-key writes (`plan-state`, `plan-progress`) are applied,
   - `prober.DoProbes(cp.Plan.Probes, probeStatuses, false)` runs and the marshaled statuses join
     the same update map,
   - one `writeInterruptOutcome` call persists both,
   - `sc.EnqueueAfter(..., interruptedEnqueuePeriod)`; **return**.

   Those two returns are the enforcement point for the suppression invariant in the plan-state
   flow, and they are why steps 3-8 need no interrupt handling of their own there: they are
   unreachable unless both annotations read as an explicit false. Worth a comment in the code
   saying so, because the branches look like early-exit optimisations and are in fact a safety
   property.
3. Everything below here runs only when either:
   - checksum flow is active (`currentPlanState == ""`), or
   - plan-state flow has `err == nil && interrupt == InterruptionNone`.
   `effectiveState, resumeFrom := resolveResume(currentPlanState, secret.Data, cp.Checksum)`.
   Every downstream use of `currentPlanState` becomes `effectiveState`, including `UsesPlanState`
   in `applyOutcomeInput`.
4. **Plan-state flow only** (`currentPlanState != ""`): if the plan *was* suspended and no longer is
   — `currentPlanState == paused`, or the checkpoint carried `Paused: true` — issue the resume
   commit described above: `plan-state` =
   `effectiveState`, checkpoint `Paused: false`. Reaching this step is itself the proof that the
   annotation is gone, so "was suspended" is the only condition that has to be tested here. Where
   `effectiveState` is `pending` this folds into the pre-commit in the next step; where it is
   terminal it is the only write this reconcile makes. This is the step that fires on the first
   reconcile after a restart when the operator unpaused while the agent was down.
5. Existing `decidePlanStateAction` / checksum-flow branch, and the `pending` → `in-progress`
   pre-commit — unchanged apart from reading `effectiveState`.
6. Start the interrupt watch; pass `Cancel`, `Pause`, `ResumeFromOneTimeInstruction: resumeFrom`
   into `ApplyInput`.
7. If `applyOutput.Interruption != InterruptionNone`, take the interrupted outcome path instead of
   `buildSecretDataUpdates` (which would otherwise write `plan-state: failed`, since an interrupted
   apply has `OneTimeApplySucceeded == false`):
   - `plan-state` = `canceled`/`paused` and the `plan-progress` checkpoint, per the ResumeState
     table (this path is plan-state flow only),
   - `applied-output` with the accumulated one-time output — this is what `selectExistingOutput`
     feeds back as `ExistingOneTimeOutput`, so `SaveOutput` results from already-completed
     instructions survive the pause,
   - **not** `applied-checksum` (the plan is not applied),
   - failure-count / success-count untouched,
   - probes run and their statuses are persisted, as on every other path,
   - all of it written through `writeInterruptOutcome`,
   - on cancel with `0 < completed < total`, `logrus.Warnf` that the plan was canceled after
     partial execution and the node may be in an inconsistent state.

   In checksum flow this branch is not entered, because annotations are ignored there.
8. Otherwise the existing `buildSecretDataUpdates` path, unchanged.

`interruptedEnqueuePeriod` (1 minute) replaces `probePeriod` for the interrupt path's re-enqueue.
Removing the annotation arrives as a watch event, so this is a slow-poll safety net rather than the
mechanism for noticing an unpause; at `probePeriod` (5s default) it would churn the workqueue on
every node for the whole duration of a pause to no purpose.

`plan_decision.go`: **no changes.** The original design added a `Halt` flag on `planStateResult` for
`PlanStateCanceled` and a defensive `PlanStatePaused` case. Neither is needed once interrupts
preserve observation: `canceled` falls through the existing terminal default branch to
`NeedsApplied: false`, and `paused` is resolved to its `ResumeState` before the function is
reached.

`secret_outcome.go`: `buildSecretDataUpdates` clears `PlanProgressKey` on both the success and
failure paths so a stale checkpoint cannot leak into a later run.

## Remaining risks

The compatibility question that used to head this section is settled — see "Compatibility with
today's Rancher — verified" above. What is left is not verifiable by reading code:

1. **Cancel is a one-way door for CAPR-managed machines.** Confirmed by inspection:
   `assignAndCheckPlan` (`store.go:483-486`) re-drives a plan only when its content changes, so a
   canceled plan is never returned to `pending` on its own, and `isUnavailable` keeps counting the
   node against the rolling-update budget while it sits there. This is a product decision, not a bug — the agent's
   rule that a terminal plan waits for new content is correct. But it means cancel is not an
   "undo" until the Rancher-side recovery path below lands, and shipping it earlier hands
   operators a control that can strand a machine. Recommend gating the cancel half of this work on
   that Rancher change; pause has no such dependency.

   **Release gate:** do not expose cancel through UI/API defaults until "Proposed changes in
   rancher/rancher" item 5 (re-drive from `canceled` when the annotation is cleared) has landed.
2. **MachineHealthCheck interaction.** Keeping probes running on a canceled plan is the right
   default, but a node left in a partial state will start reporting unhealthy and may be
   remediated. Confirm that is the desired behavior with whoever owns the MHC configuration.
3. **Pause has no expiry.** Nothing reaps a forgotten `plan.cattle.io/paused` annotation, and the
   node it is on silently blocks cluster convergence for as long as it stays. Out of scope here,
   but worth a Rancher-side alert or a max-pause-duration policy. A durable checkpoint extends the
   window in which this can go unnoticed: the hold now survives node reboots and agent upgrades, so
   it will not quietly resolve itself the way a process-scoped one eventually did.

   A mistyped value stalls the plan the same way and is *more* likely to be forgotten, because the
   operator who set it believes they either paused the node or did nothing — not that they left it
   wedged. The agent logs it every reconcile, but that is on the node. This is the reason item 10
   is in the Rancher list rather than filed as a nicety: the annotation reaper, the stall alert and
   the invalid-value report all want the same Rancher-side inspection of the same two keys.
4. **A durable checkpoint asserts history, not present state.** `Completed` records that
   instructions ran to completion, not that their effects still exist. Within a node's lifetime that
   is the same assumption the applyinator already makes between two instructions of a single apply,
   and the boundary property of pause makes it sound across a restart. It stops holding if the
   node's disk is rolled back or reimaged while the plan is held, because the plan Secret is
   per-machine and outlives a re-installation: the rebuilt node would resume past instructions whose
   effects are gone. In CAPR a re-provisioned machine is normally a new `Machine` with a new plan
   Secret, which sidesteps this; an in-place OS rollback or a snapshot restore does not. The
   operator remedy is to have the planner deliver new content — any checksum change invalidates the
   checkpoint — or to delete the `plan-progress` key from the Secret.

## Proposed changes in rancher/rancher

The agent side is complete without any of this — the states are inert against today's planner. What
these buy is (a) an operator-legible reason for a stalled rollout, (b) a way back from a cancel,
(c) not offering a control on a node that cannot honor it and (d) not letting an invalid annotation
value reach the node in the first place. Line references are against `73d6cd578`.

### 1. Upstream the wire constants — `pkg/plan/state.go`

Add alongside `PlanStateCanceled`, then bump the dependency in system-agent and delete the local
copies:

```go
// PlanStatePaused means the agent is holding execution at an instruction boundary in response to
// the plan.cattle.io/paused annotation. Non-terminal: the agent resumes into the state recorded in
// plan-progress when the annotation is removed.
PlanStatePaused PlanState = "paused"

// PlanCanceledAnnotation / PlanPausedAnnotation request that the agent abort or hold the plan.
// The only valid values are "true" and "false"; an absent annotation is "false". The agent rejects
// anything else rather than guessing, so producers must not write other spellings.
PlanCanceledAnnotation = "plan.cattle.io/canceled"
PlanPausedAnnotation    = "plan.cattle.io/paused"

// PlanProgressKey is the Secret data key holding the agent's resume checkpoint.
PlanProgressKey = "plan-progress"
```

`IsTerminal` needs no change: `paused` is correctly non-terminal by omission.

### 2. Recognise the states — `pkg/capr/planner/store.go`

`SecretToNode` (lines 297-308) gains explicit cases so the intent is not inferred from a checksum:

```go
case planapi.PlanStatePaused:
    result.InSync = false // suspended by an operator, not converged
case planapi.PlanStateCanceled:
    result.InSync = false
```

The computed `InSync` is unchanged from what the default branch produces today, which is the point:
this is a readability and intent change that unblocks the message work below, and it is safe to
merge independently of everything else. `plan.Node` already carries `PlanState`
(`pkg/apis/rke.cattle.io/v1/plan/plan.go:37`), so no API type change is required.

### 3. Say why the rollout is stalled — `getPlanStatusReasonMessage`, `store.go:150-168`

Today a paused or canceled node reports `WaitingPlanStatusMessage` — `"waiting for plan to be
applied"` — which is indistinguishable from a slow node. Add ahead of the `InSync` case:

```go
case entry.Plan.PlanState == planapi.PlanStatePaused:
    return PausedPlanStatusMessage    // "plan execution paused by operator"
case entry.Plan.PlanState == planapi.PlanStateCanceled:
    return CanceledPlanStatusMessage // "plan execution canceled by operator"
```

This is the highest-value item in the list and the cheapest. `Message` in `pkg/plan/store.go`
buckets nodes for the cluster-level summary and should grow matching buckets, so a stalled upgrade
names the paused machines instead of reporting a generic wait.

### 4. Surface it on the machine — `pkg/controllers/capr/plansecret/plansecret.go:153-186`

The handler currently branches on failure and otherwise clears `PlanApplied` to nil (line 185), so
a suspended plan looks condition-clean. Add a branch before that which sets `PlanApplied` to a
distinct non-error reason (`Paused` / `Canceled`) carrying the same message as above. This is what
the UI and `kubectl describe machine` read, and it is where an operator will actually look.

### 5. A way back from cancel — `assignAndCheckPlan`, `store.go:483-486`

Without this, cancel strands the machine. The existing guard re-drives a plan only when
`!store.equalities.DeepEqual(entry.Plan.Plan, newPlan)`. Widen it: when the cancel annotation has
been removed and `plan-state` is still `canceled`, call `UpdatePlan` to rewrite
`plan-state: pending` even though the plan bytes are unchanged, so the operator's act of clearing
the annotation is what re-arms the node:

```go
v, ok := entry.Metadata.Annotations[planapi.PlanCanceledAnnotation]
if entry.Plan.PlanState == planapi.PlanStateCanceled && (!ok || v == "false") {
    // operator cleared the cancellation; re-drive the existing plan
}
```

Two properties worth stating explicitly in review, because they are the whole risk of this item:
re-arming must be driven by the *annotation transition* and not by the mere presence of
`canceled`, or the planner will immediately undo every cancellation; and the agent re-runs the
plan from instruction 0, since a canceled plan keeps no checkpoint. The alternative — require a
plan content change — is simpler and needs no code, but leaves the operator with no obvious action
to take.

### 6. Setting the annotations

Nothing in Rancher writes either annotation today. Whether that lands as a UI action, an API field
on the machine, or a documented `kubectl annotate` is a product question. The agent treats the
annotations as operator-owned and never edits them, so any of the three works, and a documented
`kubectl annotate` is enough to ship the pause half.

The valid values are exactly `"true"` and `"false"`. A UI action or API field makes anything else
unconstructible, which is the argument for one; the `kubectl annotate` route is the only one that
can produce the invalid-value error path described above, so if that is what ships first the
accepted values belong in the documented command, not just in the reference.

One constraint on the shape: the annotations must be set **on the plan Secret**. Setting them on
the `Machine` and expecting them to reach the Secret does not work — `CopyPlanMetadataToSecret`
filters on `labelAnnotationMatch` = `^((rke\.cattle\.io)|((?:machine\.)?cluster\.x-k8s\.io))/`
(`pkg/capr/common.go:224`), so a `plan.cattle.io/` key is dropped silently, with no error to
distinguish it from a hold that was accepted. If a `Machine`-level control is wanted, either the
annotation prefix changes to `rke.cattle.io/` or the regex is extended — deliberately, and with a
test. The same filter is why a Secret-level annotation is durable: being additive-only, `CopyMap`
never prunes it, so a plan update cannot silently release a hold.

### 7. The successor store — `pkg/plan/store.go`

`Store.AssignPlan` (line ~370) is documented as superseding the CAPR path once its CAPI dependency
is unravelled, and it does not read `plan-state` at all — it infers `InProgress` from plan bytes
matching. Whenever it takes over it needs items 2-5 reapplied, or pause and cancel become invisible
again. Worth a tracking issue now rather than a rediscovery later.

### 8. Do not offer pause or cancel to an agent that cannot honor it

This is the mitigation for the mixed-version hazard established above: on a node running an agent
that predates this change, setting the annotation is accepted and does nothing, and the operator
has no way to tell. The control must therefore be gated on the node, not on the Rancher version.

Two ways to know the node's agent version, in increasing order of honesty:

- **What Rancher installed.** `settings.SystemAgentVersion` (`pkg/settings/setting.go:125`, the
  `system-agent-version` setting) is the version Rancher *intends* the node to run, and the plan
  that installs it is Rancher's own, so the planner can reason about it without the node's help.
  The weakness is that it is a desired value: during the rolling agent upgrade — precisely the
  window this item exists for — the desired version is new and the node's is not.
- **What is actually running.** Have the agent write its own version into the plan Secret (a new
  `agent-version` data key, written on every reconcile alongside `probe-statuses`). This is the
  signal that is correct during the upgrade window, it is one line on the agent side, and it is
  independently useful: today nothing tells an operator what binary a node is running. It is,
  however, a chicken-and-egg signal — an old agent does not write the key, so its *absence* is what
  identifies an old agent, and the gate has to treat absent as unsupported.

Recommendation: write the key, and gate on `absent or < first supporting version → do not offer`.
Until the gate exists, treat pause and cancel as documented-but-unguarded and say so in the docs —
the failure mode is a control that appears to work, which is worse than one that is missing.

### 9. Clear the checkpoint when the plan content changes — `UpdatePlan`

`UpdatePlan` already does `delete(secret.Data, "probe-statuses")` when it writes new plan content
(`pkg/capr/planner/store.go:405`, "so their healthy status will be reported as healthy only when
they pass"). Add `delete(secret.Data, "plan-progress")` next to it, for the same reason.

This is tidiness, not a correctness fix: the checkpoint carries the checksum of the plan it
describes, and `parsePlanProgress` returns the zero value on mismatch, so a stale record from a
superseded plan is already ignored by the agent. But leaving it in the Secret means `kubectl
describe` shows a progress record that does not describe the current plan, which is exactly the
kind of thing someone debugging a stuck rollout will believe.

### 10. Validate the annotation values, and surface an invalid one

The agent refuses to act on a value other than `"true"` or `"false"` and returns an error, but its
only channel for saying so is its own log. From Rancher the machine simply stops progressing — the
same silent-hazard shape as item 8. Two halves, either useful alone:

- **Reject at the source.** If items 6's UI action or API field lands, validate there. If the
  annotation stays operator-set, a validating admission webhook on the plan Secret is the place —
  the value is wrong at the instant it is written, and that is the only moment the operator is
  still looking.
- **Report it if it gets through.** `getPlanStatusReasonMessage` (item 3) and the `plansecret`
  condition (item 4) already exist to explain a stalled rollout, and the annotation is on the object
  they are handed, so the check is local: if either annotation is present with a value that is
  neither `"true"` nor `"false"`, report that instead of `waiting for plan to be applied`. This
  needs nothing from the agent — Rancher can revalidate the same two strings independently, which
  is also why the two implementations must agree exactly, and why the accepted set is two literals
  rather than a parser.

Worth doing even though the path should be unreachable once the source validates: the escape hatch
is `kubectl annotate` on a Secret, and nothing prevents that.

### Suggested sequencing

Items 1-4 are additive, independently mergeable, and carry no behaviour change for existing plans;
they can land before or after the agent work in any order. Item 5 changes planner behaviour and
should land before cancel is exposed to operators. Item 6 gates any of this being reachable from
the UI, and **item 8 gates it being reachable safely** — item 6 without item 8 ships a control that
silently does nothing on a node mid-upgrade. Item 10's first half rides along with item 6 (validate
wherever the value is set) and its second half with items 3-4 (report it wherever a stall is
already explained), so it costs little if taken with them and is awkward to retrofit later. Item 9
is cosmetic and can land whenever.

## Tests

### Unit (`make test`, build tag `test`), table-driven and parallel per repo conventions

`pkg/applyinator/applyinator_test.go` (extend; POSIX-shell cases guarded by the existing
`runtime.GOOS == "windows"` skip):

- `checkInterruption` precedence table.
- Pre-closed `Cancel` runs nothing — and short-circuits without blocking on a held mutex.
- `Pause` closed after instruction 0 stops before instruction 1 with `Completed == 1`.
- `ResumeFromOneTimeInstruction: 1` leaves instruction 0's sentinel file absent.
- `CompletedOneTimeInstructions` is absolute across a resume: pause after instruction 1, then apply
  again with `ResumeFromOneTimeInstruction: 2` and pause after the next one, asserting `Completed`
  is 3 and not 1. This is what makes a checkpoint composable across pause/resume cycles.
- Closing `Cancel` during a `sleep 60` instruction returns promptly with `InterruptionCanceled`.
- A script trapping TERM proves SIGTERM precedes the kill.
- A script that backgrounds a child writing to a sentinel file proves the **grandchild** is also
  killed — the specific regression the process-group work exists to prevent.

`pkg/k8splan/interrupt_test.go`, `readInterrupt` table — the accepted set and the precedence rules,
asserting the returned `Interruption` *and* whether an error came back:

| `canceled` | `paused`  | result                                                        |
|-------------|-----------|---------------------------------------------------------------|
| absent      | absent    | `InterruptionNone`, nil                                       |
| `"false"`   | `"false"` | `InterruptionNone`, nil — the explicit-false form             |
| absent      | `"true"`  | `InterruptionPaused`, nil                                     |
| `"true"`    | absent    | `InterruptionCanceled`, nil                                   |
| `"true"`    | `"true"`  | `InterruptionCanceled`, nil — cancel wins                     |
| `"true"`    | `"yes"`   | `InterruptionCanceled`, nil — a valid cancel is never blocked |
| `"True"`    | absent    | error — case matters                                          |
| `"1"`       | absent    | error — `ParseBool` spellings are rejected on purpose         |
| absent      | `""`      | error — present-and-empty is not absent                       |
| `"maybe"`   | `"nope"`  | error mentioning **both** keys (`errors.Join`)                |
| `"nope"`    | `"true"`  | error — an invalid cancel does not let a valid pause through  |

The `"True"` and `"1"` rows are the ones that would silently pass under a `ParseBool`
implementation, and the `"nope"`/`"true"` row is the one that would silently pause under a
per-annotation "first valid wins" implementation. (No `Halt` case; the flag is gone.)

`pkg/k8splan/plan_progress_test.go`: round-trip; checksum mismatch → zero value; malformed JSON; a
`Paused: false` record (a cancel report) yields `resumeFrom` 0; a `paused` wire state with no
suspended checkpoint resolves the state but not the position. There is deliberately **no**
agent-instance case: a checkpoint decoded from a Secret written by a previous agent lifetime is
indistinguishable from one this process wrote, and the round-trip test is the assertion of that.

`pkg/k8splan/reconcile_test.go` (extend `TestReconcileSecretScenarios`): cancel on pending, cancel
on in-progress, cancel on terminal (ignored), pause on pending, pause on in-progress, unpause
resumes at the checkpoint. Probes are asserted to **keep running** during an interrupt with a
counting `httptest` server.

The suppression invariant gets a dedicated table-driven test rather than a case buried in the
scenario list, because it is the safety property and it has to hold across a matrix of states the
individual scenarios each only touch one cell of. `TestPausedPlanNeverExecutes` constructs the
Secret a previous agent lifetime would have left behind, reconciles it with a
freshly-constructed `watcher` — which is what "restart" means to this package — and asserts for
every row that **`Apply` is never called** and no instruction sentinel file appears:

| `plan-state`           | checkpoint                      | why the row exists                                             |
|------------------------|---------------------------------|----------------------------------------------------------------|
| `paused`               | `Paused: true`, `Completed: 2`  | the ordinary held plan                                         |
| `paused`               | absent                          | paused before the first instruction finished                   |
| `in-progress`          | absent                          | crash mid-apply — the contract that must *not* fire here       |
| `in-progress`          | `Paused: true`, `Completed: 2`  | the write-once guard keys off the checkpoint, not `plan-state` |
| `pending`              | absent                          | a plan delivered already paused                                |
| `succeeded`            | absent                          | terminal; nothing to suppress, asserts no regression           |
| absent (checksum flow) | absent                          | baseline legacy behavior with no annotation influence          |
| `canceled`            | `Paused: false`, `Completed: 2` | a cancel report is not a suspension                            |

All plan-state-flow rows carry `plan.cattle.io/paused: "true"`. The checksum-flow row is run as a
separate baseline fixture (annotation absent), because checksum flow ignores annotations by design.
The plan-state-flow rows are chosen so that at least one would execute if the annotation check were
moved below `resolveResume` or below `decidePlanStateAction` — `in-progress` re-executes on restart
and `pending` starts.

The rows whose suspension is already recorded additionally assert **zero `Update` calls** — the
write-once guard honoring a checkpoint the current process did not write — and the row with no
checkpoint asserts exactly one, recording the suspension for the first time.

The same matrix runs a second time (plan-state flow only) with the value replaced by an invalid one
(`"True"`, `"yes"`, `""`), asserting the stricter outcome: `Apply` never called, **and zero
`Update` calls on every row** including the ones that would otherwise record a suspension, **and** a
non-nil error from `reconcileSecret`. That last assertion is the one that distinguishes this
revision from a fail-closed read — a typo must not be able to masquerade as a working pause, so it
is not enough that the plan is held; the reconcile has to say something is wrong. A third pass
asserts the `resourceVersion` is byte-identical before and after, which is how "writes nothing" is
checked rather than inferred from the `Update` count.

Checksum flow gets its own invalid-value case: annotation present with `"True"`/`"yes"`/`""`
produces a warn log on every reconcile, no interrupt behavior, and ordinary checksum apply
semantics.

Then the resume half:

- **Restart, then unpause, resumes at the checkpoint.** `plan-state: paused` with a `Paused: true`,
  `Completed: 2` checkpoint, the annotation removed
  (the operator unpaused while the agent was down). Assert the resume commit lands first with
  `plan-state: in-progress` and `Paused: false`, that `ApplyInput.ResumeFromOneTimeInstruction` is
  2, and that instruction 0's sentinel is never recreated. This is the case the whole change exists
  for; without process independence it silently re-runs from 0 and every other assertion still
  passes.
- **Restart mid-execution still re-executes from 0.** `plan-state: in-progress`, no annotation, and
  either no checkpoint or one left at `Paused: false` by a resume commit. Assert
  `ResumeFromOneTimeInstruction` is 0 — the crash-recovery contract is unchanged for plans that were
  running rather than held, and a cancel report is never mistaken for a resume point.
- **`"false"` resumes exactly like removal.** The same fixture as the first case, but the operator
  sets `plan.cattle.io/paused: "false"` instead of deleting the key. Assert the resulting Secret is
  equal to the one the removal case produces. Written as a shared table over both release forms
  rather than a copied test, so the two cannot drift.
- **A corrected value self-heals.** Reconcile with `plan.cattle.io/paused: "yes"` and assert the
  error; reconcile again with the value corrected to `"true"` and assert the suspension is recorded
  normally, then again with `"false"` and assert the resume commit fires. The point is that the
  error path leaves no residue — no half-written state for the corrected reconcile to trip over.

Plus one for the exit path itself:

- **The resume commit is issued exactly once and re-arms the guard.** Unpause, let the apply run,
  pause again mid-apply, and assert the second checkpoint reports the newer `Completed` rather than
  the value carried over from the first pause.

Four cases exist specifically for the defects this revision fixes, and should be written first:

- **Checksum-flow annotation is ignored — entry path.** No `plan-state` on the input Secret,
  annotation set (`paused` or `canceled`). Assert a warn log is emitted on every reconcile and
  reconcile follows
  ordinary checksum behavior (including normal apply decision based on checksum), with no
  `plan-state`/`plan-progress` writes. Assert warning text includes checksum-flow context plus
  key/value.
- **Checksum-flow annotation is ignored while apply is running.** Same Secret, annotation appears
  while `Apply` is in flight. Assert no interrupt channels close, no interrupted-outcome path runs,
  and the apply completes under normal checksum semantics. A subsequent reconcile logs warn again if
  the annotation remains.

- **Conflicting write during an interrupted apply.** The fake client returns `Conflict` on the
  first `Update` and a Secret bearing the operator's annotation on the subsequent `Get`. Assert
  `plan-state: paused` and the checkpoint land on the fresh object, that the reconcile returns
  without a fatal, and — the actual regression — that `Completed` is preserved rather than lost.
  A companion case where the `Get` returns a *different* plan asserts the write is abandoned.
- **Interrupt-path re-entry is a no-op.** Reconcile twice with the pause annotation set and
  `plan-state` already `paused`. Assert exactly one `Update` call across both reconciles and that
  `plan-progress` still reports the original `Completed`, not 0.

`pkg/k8splan/interrupt_test.go`, the watch half: `fake.MockCacheInterface[*corev1.Secret]` wired
through `MockControllerInterface.EXPECT().Cache()`, returning the annotation on the second poll;
assert the channel closes and that a cache error falls back to `sc.Get`. Plus the divergence from
the entry path — a poll returning `plan.cattle.io/paused: "yes"` closes **neither** channel and the
watch keeps polling, and a subsequent poll returning `"true"` closes `Pause` normally. Without this
case, "the watch ignores invalid values" is a sentence in a design document rather than a
behaviour.

### E2E (`test/e2e/suites/remote-plan/`, build tag `e2e`, `short` label)

- New `test/framework/secret.go` helpers `SetSecretAnnotation` / `RemoveSecretAnnotation`.
- `pause_test.go`, invalid-value spec: set `plan.cattle.io/paused: "True"` on a pending plan and
  assert the plan does not advance, the Secret's `resourceVersion` is unchanged after a settle
  period, and `plan-state` is still `pending`; then correct the value to `true` and assert the hold
  is recorded. Cheap, and it is the one assertion that catches a real API server behaving
  differently from the fake client on "the agent wrote nothing".
- `cancellation_test.go`: cancel a plan whose first instruction sleeps → `plan-state` reaches
  `canceled`, the later instruction's file never appears, `plan-progress` shows partial execution;
  cancel a pending plan → no side effects at all.
- `pause_test.go`: pause between instructions → `plan-state: paused` and the next instruction's
  file is absent; remove the annotation → `plan-state: succeeded` and an append-once marker file
  proves the completed instruction was not re-executed. Probe statuses are asserted to **continue**
  advancing while paused.
- `pause_test.go`, restart spec: pause between instructions, then restart the agent (delete the
  DaemonSet pod / restart the process) while the annotation is set. Assert nothing executes across
  the restart — the next instruction's file is still absent and the append-once marker has not
  grown — then remove the annotation and assert the plan reaches `succeeded` with the marker still
  showing a single entry per instruction. This is the end-to-end form of "a paused plan is not
  re-executed after a restart", and it is the only place the durable checkpoint is exercised against
  a real API server rather than a fake client.

### Docs

Add a "Cancellation and pause" paragraph to the `pkg/k8splan` section of `CLAUDE.md`, leading with
the suppression invariant in the plan-state flow — a plan runs only when both annotations are absent
or `"false"`, and clearing the annotation is the only thing that resumes it — plus the checksum-flow
rule (annotations are ignored with a warning). Then cover the accepted values (`"true"` and
`"false"`, everything else an error in plan-state flow), the execution-vs-observation rule and the
pause-is-a-boundary semantics. It also needs to qualify the
crash-recovery sentence already there — "`in-progress` on startup → re-apply" — with the pause
exception: a plan held by the pause annotation is not re-applied on startup, and a suspended
checkpoint is honored across agent lifetimes so the eventual unpause resumes rather than restarts.

## Files touched

| File                                                    | Change                                                                                                                                                                         |
|---------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `pkg/applyinator/applyinator.go`                        | `Interruption`, `checkInterruption`, input/output fields, start index and channel checks in both instruction loops, pre-lock short-circuit, `watchForTermination` in `execute` |
| `pkg/applyinator/process_unix.go`, `process_windows.go` | new — process-group / Job Object setup and tree termination                                                                                                                    |
| `pkg/k8splan/watcher.go`                                | new constants, `secretConflictMergeKeys` entry and absent-key fix                                                                                                              |
| `pkg/k8splan/interrupt.go`                              | new — `parseInterruptAnnotation`, `readInterrupt`, `interruptWatch`, `handleInterrupt`, `writeInterruptOutcome` (used for both entering and leaving an interrupt)              |
| `pkg/k8splan/plan_progress.go`                          | new — checkpoint parse/marshal, `resolveResume`                                                                                                                                |
| `pkg/k8splan/reconcile.go`                              | interrupt entry path, effective-state resolution, resume commit, interrupted outcome path                                                                                      |
| `pkg/k8splan/secret_outcome.go`                         | clear `plan-progress` on terminal outcomes                                                                                                                                     |
| `go.mod`                                                | `golang.org/x/sys` indirect → direct                                                                                                                                           |
| `test/framework/secret.go`                              | annotation helpers                                                                                                                                                             |
| tests + `CLAUDE.md`                                     | as above                                                                                                                                                                       |

`pkg/k8splan/plan_decision.go` is deliberately **not** in this list. Neither are
`pkg/config/config.go`, `main.go` or `k8splan.Watch`'s signature: the feature adds no configuration
surface, which is the whole point of "On not shipping a kill switch" above.

## Verification

```bash
make test                              # unit tests, -tags=test -cover
make validate                          # lint + fmt-check + vet
make verify                            # go.mod tidy after the x/sys promotion
GOOS=windows GOARCH=amd64 make build   # confirms the process_windows.go split compiles
make test-e2e                          # Kind + Ginkgo, short label — covers cancel and pause end to end
```

Notes: skip executing the 'make test-e2e' as the environment may not be properly configured. It will be ran in a different environment. 

Targeted while iterating:

```bash
go test -tags=test ./pkg/applyinator/ -run 'Interrupt|Cancel|Pause|Resume' -v
go test -tags=test ./pkg/k8splan/ -run 'Interrupt|Progress|ReconcileSecret' -v
```

Manual smoke against a live cluster (optional, mirrors the e2e specs):

```bash
kubectl -n <ns> annotate secret <machine>-machine-plan plan.cattle.io/paused=true
journalctl -u rancher-system-agent -f      # expect "[k8splan] pause requested"
kubectl -n <ns> get secret <machine>-machine-plan -o jsonpath='{.data.plan-state}' | base64 -d
kubectl -n <ns> get secret <machine>-machine-plan -o jsonpath='{.data.plan-progress}' | base64 -d

# on the node: restart across the pause, then confirm the hold and the checkpoint both survived
systemctl restart rancher-system-agent
journalctl -u rancher-system-agent -f      # expect the pause to be re-observed, no apply

# release the hold — either form works and both should be tried
kubectl -n <ns> annotate --overwrite secret <machine>-machine-plan plan.cattle.io/paused=false
kubectl -n <ns> annotate secret <machine>-machine-plan plan.cattle.io/paused-
```

Confirm the state reports `paused`; that the restart re-enters the hold rather than re-running the
plan, with `plan-progress` reporting the same `completedInstructions` before and after it; that the
plan reaches `succeeded` after the annotation is cleared without re-running completed instructions;
and — the write-amplification check — that the Secret's resourceVersion is stable for the duration
of the pause rather than incrementing every few seconds. Repeating the unpause with the agent
stopped (annotate, then `systemctl start`) exercises the resume-on-startup path.

The invalid-value path is worth one manual pass too, because its whole purpose is operator-facing:

```bash
kubectl -n <ns> annotate --overwrite secret <machine>-machine-plan plan.cattle.io/paused=True
journalctl -u rancher-system-agent -f      # expect an error naming the key, the value and the two accepted ones
kubectl -n <ns> get secret <machine>-machine-plan -o jsonpath='{.metadata.resourceVersion}'
```

Confirm the plan does not advance, the message says what to write instead, and the resourceVersion
does not move while the bad value sits there — then correct it to `true` and confirm the hold is
recorded on the very next reconcile with no manual intervention.

For cancel, additionally confirm on the node that no descendant of the killed instruction survives:

```bash
pgrep -af <installer-or-script-name>
```

## Out of scope / follow-ups

- Everything under "Proposed changes in rancher/rancher" — upstreaming the constants, teaching the
  planner the two states, reporting them to the operator, and the recovery path out of a cancel.
  None of it blocks this change, but two items there should land before either control is exposed
  to operators: item 5 (a way back from cancel; see "Remaining risks" item 1) and item 8 (do not
  offer the controls to an agent too old to honor them; see "Mixed-version behavior").
- `pkg/localplan`: no annotation channel exists for file-based plans; unchanged.
- The `logrus.Fatalf` on `updateSecret` failure in the normal (non-interrupted) outcome path.
  `writeInterruptOutcome` avoids it on the new paths; making the existing path non-fatal is a
  behavioral change to plan reporting that deserves its own discussion.
- Closing the Windows job-object assignment race, which needs `CREATE_SUSPENDED` support that
  `os/exec` does not expose.
