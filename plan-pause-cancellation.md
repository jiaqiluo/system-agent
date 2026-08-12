# Plan cancellation and pause support in rancher-system-agent

## Context

`github.com/rancher/rancher/pkg/plan` now models an explicit plan lifecycle
(`pending` → `in-progress` → `succeeded` | `failed` | `cancelled`), and the agent already drives
those transitions in `pkg/k8splan/reconcile.go`. Two lifecycle controls are still missing on the
node side:

- **Cancellation** — an operator needs to abort a Day 2 operation that is hanging, was triggered by
  accident, or has left the cluster in a partial state. `PlanStateCancelled` exists in the API but
  nothing ever writes it.
- **Pause** — an operator needs to hold execution at an instruction boundary (maintenance window,
  incident) and later resume from where it stopped rather than re-running the whole plan.

Both are signaled by annotations on the plan Secret (`plan.cattle.io/cancelled: "true"`,
`plan.cattle.io/paused: "true"`). The hard part is that `Applyinator.Apply` runs synchronously
inside the `OnChange` handler with `DefaultWorkers: 1`, so while an apply is in flight the
controller cannot deliver the annotation change. Detecting an interruption mid-apply needs a
separate observer, and stopping cleanly needs the applyinator to grow interruption points.

**Outcome:** an operator can cancel or pause a plan at any point in its lifecycle; the agent stops,
reports the resulting state in the Secret, and resumes a paused plan at the first instruction that
has not yet completed.

The two controls stop at different granularity, and the difference is load-bearing:

- **Cancel is prompt.** It signals the in-flight instruction's process tree (SIGTERM, then SIGKILL
  after a grace period) and abandons the plan. This is the tool for a hung instruction.
- **Pause is a boundary.** It lets the running instruction finish and stops before the next one.
  Pause latency is therefore the remaining runtime of the current instruction, and pause will
  **never** take effect on an instruction that never returns. That is deliberate: a checkpoint is
  only trustworthy if every instruction it claims is complete actually ran to completion.

## Settled decisions

| Decision | Choice |
| --- | --- |
| Where new wire constants live | Locally in `pkg/k8splan` for now; follow-up to upstream them into `rancher/pkg/plan` |
| Paused representation | New non-terminal plan-state value `paused` |
| Resume semantics | Checkpoint the instruction index in the Secret; completed instructions are not re-run |
| Checkpoint durability | Scoped to the agent process — a restart resets `Completed` to 0, preserving the crash-recovery contract |
| Stopping a running instruction | Cancel only. SIGTERM → SIGKILL after 10s, to the whole **process group** (Windows: Job Object terminate) |
| Pause granularity | Instruction boundary only; never interrupts a running instruction |
| Probes during an interrupt | Keep running. An interrupt suppresses *execution*, never *observation* |
| Writing the interrupted outcome | Fresh read-modify-write against the API server, never a merge from the stale in-hand copy |
| Dependence on the `paused` wire state | None. The checkpoint is self-describing, so `paused` can be suppressed by config without losing pause or resume |
| Scope | Remote mode (`pkg/k8splan`) only — annotations are a Secret concept; `pkg/localplan` is unaffected |

## Wire format additions

New constants in `pkg/k8splan/watcher.go`:

```go
// TODO: upstream these into github.com/rancher/rancher/pkg/plan alongside PlanStateCancelled,
// then drop the local definitions and bump the dependency.

// PlanCancelledAnnotation, set to "true" on the plan Secret, requests that the agent abort the plan.
PlanCancelledAnnotation = "plan.cattle.io/cancelled"
// PlanPausedAnnotation, set to "true" on the plan Secret, requests that the agent hold execution.
PlanPausedAnnotation = "plan.cattle.io/paused"
// PlanProgressKey is the Secret data key holding the resume checkpoint (JSON, see planProgress).
PlanProgressKey = "plan-progress"
// PlanStatePaused is a non-terminal plan-state: execution is held at an instruction boundary and
// will resume into planProgress.ResumeState when the pause annotation is removed.
PlanStatePaused planapi.PlanState = "paused"
```

The `plan.cattle.io/` prefix is intentional and matches the doc comment on `PlanStateCancelled` in
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

New `pkg/k8splan/plan_progress.go`. The record is scoped to both the plan checksum and the agent
process, so a checkpoint recorded for a different plan — or by a previous agent lifetime — is not
blindly trusted:

```go
type planProgress struct {
    Checksum      string            `json:"checksum"`
    AgentInstance string            `json:"agentInstance"`
    Completed     int               `json:"completedInstructions"`
    Total         int               `json:"totalInstructions"`
    ResumeState   planapi.PlanState `json:"resumeState,omitempty"` // state restored when the pause lifts
    // Paused makes the record self-describing: the agent can recognise a suspended plan from the
    // checkpoint alone, without depending on plan-state carrying the new "paused" value. That is
    // what makes the wire state optional — see "Contingency: degrading the paused wire state".
    Paused bool `json:"paused,omitempty"`
}

// parsePlanProgress decodes the checkpoint stored under PlanProgressKey.
//   - malformed JSON, or a checksum that does not match the plan being reconciled: zero value.
//   - checksum matches but AgentInstance does not: Completed is forced to 0 and the mismatch is
//     logged at info. The agent restarted while the plan was paused, so it cannot vouch that the
//     effects of "completed" instructions survived — this is the same reasoning behind
//     re-executing an in-progress plan from the beginning on startup. ResumeState and Total are
//     retained, because they describe operator intent rather than node state.
//   - both match: returned verbatim.
func parsePlanProgress(data map[string][]byte, checksum, agentInstance string) planProgress
func marshalPlanProgress(p planProgress) []byte
```

`agentInstance` is a per-process identifier assigned once in `Watch` and stored on the `watcher`:
`fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())`. No new dependency; uniqueness only has
to hold against the agent's own previous lifetimes on the same node.

## Behavior rules

The governing rule: **an interrupt suppresses execution, never observation.** While either
annotation is `"true"` the agent skips `Apply` entirely — no file reconciliation, no one-time
instructions, no periodic instructions — but it still runs probes and still persists probe
statuses. Freezing probe statuses would feed stale health data to Rancher's MachineHealthCheck on
exactly the nodes most likely to be unhealthy (a plan cancelled mid-flight leaves the node in a
partial state). Reporting honestly is the safer default, and it costs nothing: `applyProbeResult`
saturates its counters at the success/failure threshold, so a steady-state probe produces identical
Secret data and the existing `reflect.DeepEqual` guard suppresses the write.

On top of that:

- **Cancel wins over pause.** If the effective state is non-terminal, write `plan-state: cancelled`
  plus a `plan-progress` record. If it is already terminal, log at debug and write nothing.
- **Pause writes `plan-state: paused`** and a `plan-progress` record. Only in the plan-state flow —
  see below.
- **State is written once.** If the interrupt is already recorded, the interrupt path writes nothing
  at all. Without this guard the periodic re-enqueue re-enters the interrupt path every few seconds
  for the entire duration of the pause, rewriting the Secret — and, worse, recomputing `Completed`
  and `ResumeState` from a reconcile where no apply is in flight, which would silently reset a
  checkpoint that had just recorded real progress.

  "Already recorded" is read from the **checkpoint**, not from `plan-state`: `plan-progress.Paused`
  for pause, `plan-state == cancelled` for cancel. Keying pause off `plan-state == paused` would be
  vacuous whenever `DisablePausedPlanState` is set — the state never changes, so the guard never
  fires and every re-enqueue rewrites the Secret and clobbers the checkpoint. Cancel has no such
  problem: it is not gated by any kill switch, so `cancelled` is always on the wire to test against
  (its `plan-progress` record has `Paused: false` and exists only to report how far the plan got).
- **Cancelled is terminal and needs no special-casing.** `decidePlanStateAction` already returns
  `NeedsApplied: false` for every terminal state, which is precisely "monitor only". Once the
  annotation is removed, a cancelled plan behaves exactly like a failed one: probes run, nothing
  executes, and the orchestrator must deliver new content with `pending` to move forward. No `Halt`
  flag and no change to terminal-state handling are required.
- **Checksum flow** (`plan-state` absent, legacy orchestrators): both annotations still suppress
  apply, but no `plan-state`/`plan-progress` is written — doing so would silently switch the Secret
  into the plan-state flow. Probes still run. Unpausing falls back to normal checksum
  reconciliation.

  This rule binds **both** interrupt paths, not just the reconcile-entry one. An interrupt that
  arrives mid-apply is the easier case to get wrong: the natural implementation of the interrupted
  outcome writes the state and the checkpoint unconditionally, which on a legacy orchestrator
  materialises `plan-state` for the first time and silently promotes that Secret into plan-state
  semantics for the rest of its life. Every `plan-state`/`plan-progress` write on either path is
  gated on `UsesPlanState`. Concretely, in the checksum flow an interrupted apply writes only
  `applied-output` and `probe-statuses`, leaves `applied-checksum` stale, and therefore re-runs the
  whole plan from instruction 0 once the annotation clears — which is the correct legacy semantic,
  since there is nowhere to record a checkpoint. `resumeFrom` is likewise always 0 there: it can
  only be non-zero via `resolveResume`, which requires a checkpoint that the checksum flow never
  writes.
- **The agent never edits the annotations**; the orchestrator owns them. A new plan delivered with
  the cancel annotation still set is cancelled again — logged at warn level.

### ResumeState

`ResumeState` is the plan-state the plan would carry had the pause not intervened. It is captured
**exactly once**, at the moment `plan-state` first becomes `paused`, and is never recomputed while
the plan stays paused (guaranteed by the write-once rule above).

| Where the pause was observed                        | ResumeState                                                                                                    | Completed                                                       |
|-----------------------------------------------------|----------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------|
| Reconcile entry, before Apply                       | `currentPlanState` — the state being suspended, which may be `pending`, `in-progress`, `succeeded` or `failed` | preserved from an existing checksum-matching checkpoint, else 0 |
| During Apply, one-time set running (`needsApplied`) | `in-progress` — the Secret was already transitioned to `in-progress` before Apply ran                          | `applyOutput.CompletedOneTimeInstructions`                      |
| During Apply, periodic-only (`!needsApplied`)       | `effectiveState` — the state the monitoring reconcile was in                                                   | 0, with `Total` 0: there is nothing to resume                   |

The third row is the one that makes the distinction worth drawing. A `succeeded` plan still runs
periodic instructions on every reconcile; a pause landing in that window must restore `succeeded`.
Defaulting it to `in-progress` would re-execute a completed Day 2 operation in full on unpause.

All three rows set `Paused: true`; the cancel paths write the same record with `Paused: false` and
no `ResumeState`, since there is nothing to resume into.

`succeeded` and `failed` restore as no-ops: `decidePlanStateAction` sends them down the terminal
branch.

### Resume path resolution

At the top of the reconcile, after `currentPlanState` is read:

```go
// resolveResume maps a stored plan-state onto the state the reconcile should act on. A plan is
// treated as suspended if plan-state says so *or* if a valid checkpoint claims it — the latter is
// what keeps resume working when the paused wire state is disabled. Every other state passes
// through unchanged with resumeFrom 0.
func resolveResume(state planapi.PlanState, data map[string][]byte, checksum, agentInstance string) (planapi.PlanState, int) {
    if state == "" { // checksum flow: no checkpoint is ever written, nothing to resolve
        return state, 0
    }
    p := parsePlanProgress(data, checksum, agentInstance)
    if state != PlanStatePaused && !p.Paused {
        return state, 0
    }
    return orDefault(p.ResumeState, planapi.PlanStateInProgress), p.Completed
}
```

`resumeFrom` is non-zero only on the unpause reconcile, and only within the agent lifetime that
recorded it. Plain `in-progress` on startup keeps today's documented crash-recovery contract
(re-execute from the beginning).

### Compatibility with today's Rancher — verified

Writing an unknown `plan-state` value must be inert against a server that predates it. This has
been checked directly against `rancher/rancher` at `73d6cd578`, not assumed.

`SecretToNode` in `pkg/capr/planner/store.go:297-308` switches on the state and routes anything it
does not recognise to a default branch:

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

1. **`cancelled` is in the same default branch.** `PlanStateCancelled` is declared in
   `pkg/plan/state.go:38` and has no `case` anywhere in the planner — it is a dead constant today.
   A cancelled plan is therefore also evaluated by checksum and also reports `InSync = false`.
2. **The planner will not fight a pause.** `UpdatePlan` — the only writer of `plan-state: pending`
   (`store.go:407-408`) — is called only when the desired plan actually differs
   (`assignAndCheckPlan`, `store.go:483-486`). A paused plan whose content is unchanged is never
   re-driven, so the pause holds. If the desired plan *does* change mid-pause, the planner writes new content plus
   `pending` while the annotation is still set; the agent then records `ResumeState: pending`
   against the new checksum and holds, which is the correct outcome.
3. **A suspended node is counted as unavailable.** `isUnavailable` (`planner.go:531-534`) is
   `!InSync || ...`, so a paused or cancelled node consumes the rolling-update availability budget
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

### Contingency: degrading the paused wire state

The verification above removes the unknown, but the agent's runtime behaviour still depends on a
property of a separately-released component, and a regression there would land on provisioned
nodes. The design therefore does not make the `paused` wire value load-bearing.

`plan-progress` is self-describing: `planProgress.Paused` records that the plan is suspended, and
`resolveResume` triggers on either that flag or `plan-state == paused`. Suppressing the wire value
is consequently a reporting change, not a semantic one — suppression, checkpointing and resume all
keep working with `plan-state` left at `in-progress`, which every version of the planner already
understands.

The kill switch is a new `AgentConfig` field with an environment override, both read once at
startup:

```go
// DisablePausedPlanState suppresses writing plan-state: paused. Pause suppression, checkpointing
// and resume are unaffected; the plan simply continues to report the state it was suspended from.
// Escape hatch for an orchestrator that mishandles the value.
DisablePausedPlanState bool `json:"disablePausedPlanState,omitempty"`
```

`CATTLE_AGENT_DISABLE_PAUSED_PLAN_STATE=true` overrides it, following the `CATTLE_AGENT_STRICT_VERIFY`
precedent at `main.go:114-119` exactly: read once in `main`, passed to `k8splan.Watch`, stored on the
`watcher`. Rather than adding a fifth positional bool to `Watch`, fold it and `strictVerify` into a
small `WatchOptions` struct — a two-line change at the only call site. The env var matters more than
the config field in an incident: the generated systemd unit already carries
`EnvironmentFile=-/etc/default/rancher-system-agent`, so an operator can flip this and restart
without Rancher re-rendering `config.yaml`.

Degradation ladder, in the order it would be applied:

| Failure discovered | Response | What still works |
| --- | --- | --- |
| Planner mishandles `paused` | `DisablePausedPlanState` | Everything except the distinct wire state; plan reports `in-progress` while held |
| Planner mishandles `cancelled` too | Do not ship cancel; pause alone is unaffected | Pause, checkpoint, resume |
| Checkpointing itself is unsound | Ship cancel only, or set `ResumeFromOneTimeInstruction` to 0 unconditionally | Pause degrades to "suppress and re-run from the start" — the checksum-flow semantic |

Each rung is independently reachable, and none requires reverting the applyinator work — the
interruption points, the process-tree kill and the fresh read-modify-write are orthogonal to how
the state is reported. A test asserts the disabled path: pause, unpause, and confirm the checkpoint
is still honoured with `plan-state` never leaving `in-progress`.

## Changes by package

### 1. `pkg/applyinator` — interruption points

`applyinator.go`:

- New `Interruption` string type: `InterruptionNone` (`""`), `InterruptionPaused`,
  `InterruptionCancelled`.
- `ApplyInput` gains `Cancel <-chan struct{}`, `Pause <-chan struct{}`, and
  `ResumeFromOneTimeInstruction int`. Zero values mean "never interrupted, start at 0", so
  `pkg/localplan/watcher.go` needs no change.
- `ApplyOutput` gains `Interruption Interruption` and `CompletedOneTimeInstructions int`.
- `checkInterruption(cancel, pause <-chan struct{}) Interruption` — pure, non-blocking select,
  cancel first. Nil channels are never ready.
- `Apply` short-circuits on a pre-existing interruption **before `a.mu.Lock()`**, not merely before
  touching files. An apply that is already cancelled must not queue behind an in-flight apply, and
  must not sit in `checkInterlock`'s `restart-pending` wait for up to five minutes only to return
  an error instead of a clean `InterruptionCancelled`.
- After the interlock, `Apply` derives `execCtx` from `ctx` and cancels it when `input.Cancel`
  closes, so exec'd process trees are signaled.
- `runOneTimeInstructions` takes a start index and the channels and returns a `oneTimeResult`
  struct (`Output`, `Succeeded`, `Completed`, `Interruption`) rather than growing to five return
  values — matching the existing `planStateResult`/`checksumFlowResult` style. It checks
  `checkInterruption` before each instruction, and re-checks after a failed instruction so a
  cancel-induced kill is reported as cancelled rather than failed. Output from a killed instruction
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
Reporting `plan-state: cancelled` while the operation the operator wanted stopped is still running
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

- `readInterrupt(annotations map[string]string) applyinator.Interruption` — pure; `"true"` only,
  cancel takes precedence.
- `interruptWatch` + `(w *watcher) startInterruptWatch(ctx, sc)` — a goroutine polling every
  `interruptPollInterval` (2s) that closes `Cancel`/`Pause` the first time the matching annotation
  is observed. It reads `sc.Cache().Get(ns, name)` (the informer's indexer is updated by its own
  goroutine and is not blocked by the busy workqueue worker, so it stays fresh during an apply),
  falling back to `sc.Get(...)` if the cache read errors. Cache objects are shared and must be
  treated as read-only — the watch only reads `Annotations` and never mutates or retains the
  object. Started around the `Apply` call and stopped via `defer`.
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
cancelled) that comparison is against the *previous* plan's checksum and fails. The predicate
returns false, `updateSecret` returns the error, and `reconcile.go` calls `logrus.Fatalf`.

And the conflict is not a rare race — it is the normal path. The operator's annotation write bumps
the Secret's resourceVersion while the agent holds a copy read before the apply started, so the
outcome `Update` is guaranteed to 409. Consequences under the original design:

- Cancel self-heals by crashing: the agent exits, restarts, and eventually writes `cancelled` from
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
2. `interrupt := readInterrupt(secret.Annotations)`. If set:
   - `handleInterrupt` computes the state updates (possibly none),
   - lifecycle-key writes (`plan-state`, `plan-progress`) are gated on `UsesPlanState`; in checksum flow this path writes probe status (and any non-lifecycle output keys) only,
   - `prober.DoProbes(cp.Plan.Probes, probeStatuses, false)` runs and the marshaled statuses join
     the same update map,
   - one `writeInterruptOutcome` call persists both,
   - `sc.EnqueueAfter(..., interruptedEnqueuePeriod)`; return.
3. `effectiveState, resumeFrom := resolveResume(currentPlanState, secret.Data, cp.Checksum, w.agentInstance)`.
   Every downstream use of `currentPlanState` becomes `effectiveState`, including `UsesPlanState`
   in `applyOutcomeInput`.
4. Existing `decidePlanStateAction` / checksum-flow branch, and the `pending` → `in-progress`
   pre-commit — unchanged apart from reading `effectiveState`.
5. Start the interrupt watch; pass `Cancel`, `Pause`, `ResumeFromOneTimeInstruction: resumeFrom`
   into `ApplyInput`.
6. If `applyOutput.Interruption != InterruptionNone`, take the interrupted outcome path instead of
   `buildSecretDataUpdates` (which would otherwise write `plan-state: failed`, since an interrupted
   apply has `OneTimeApplySucceeded == false`):
   - **only when `UsesPlanState`**, `plan-state` = `cancelled`/`paused` (the latter subject to
     `DisablePausedPlanState`) and the `plan-progress` checkpoint, per the ResumeState table. This
     guard is not optional: writing either key in the checksum flow promotes the Secret into
     plan-state semantics permanently, which the entry path is already careful to avoid,
   - `applied-output` with the accumulated one-time output — this is what `selectExistingOutput`
     feeds back as `ExistingOneTimeOutput`, so `SaveOutput` results from already-completed
     instructions survive the pause,
   - **not** `applied-checksum` (the plan is not applied),
   - failure-count / success-count untouched,
   - probes run and their statuses are persisted, as on every other path,
   - all of it written through `writeInterruptOutcome`,
   - on cancel with `0 < completed < total`, `logrus.Warnf` that the plan was cancelled after
     partial execution and the node may be in an inconsistent state.

   In the checksum flow this reduces to `applied-output` plus `probe-statuses`, which is the whole
   of what a legacy Secret can express about an interrupted apply.
7. Otherwise the existing `buildSecretDataUpdates` path, unchanged.

`interruptedEnqueuePeriod` (1 minute) replaces `probePeriod` for the interrupt path's re-enqueue.
Removing the annotation arrives as a watch event, so this is a slow-poll safety net rather than the
mechanism for noticing an unpause; at `probePeriod` (5s default) it would churn the workqueue on
every node for the whole duration of a pause to no purpose.

`plan_decision.go`: **no changes.** The original design added a `Halt` flag on `planStateResult` for
`PlanStateCancelled` and a defensive `PlanStatePaused` case. Neither is needed once interrupts
preserve observation: `cancelled` falls through the existing terminal default branch to
`NeedsApplied: false`, and `paused` is resolved to its `ResumeState` before the function is
reached.

`secret_outcome.go`: `buildSecretDataUpdates` clears `PlanProgressKey` on both the success and
failure paths so a stale checkpoint cannot leak into a later run.

## Remaining risks

The compatibility question that used to head this section is settled — see "Compatibility with
today's Rancher — verified" above. What is left is not verifiable by reading code:

1. **Cancel is a one-way door for CAPR-managed machines.** Confirmed by inspection:
   `assignAndCheckPlan` (`store.go:483-486`) re-drives a plan only when its content changes, so a
   cancelled plan is never returned to `pending` on its own, and `isUnavailable` keeps counting the
   node against the rolling-update budget while it sits there. This is a product decision, not a bug — the agent's
   rule that a terminal plan waits for new content is correct. But it means cancel is not an
   "undo" until the Rancher-side recovery path below lands, and shipping it earlier hands
   operators a control that can strand a machine. Recommend gating the cancel half of this work on
   that Rancher change; pause has no such dependency.

   **Release gate:** do not expose cancel through UI/API defaults until "Proposed changes in
   rancher/rancher" item 5 (re-drive from `cancelled` when the annotation is cleared) has landed.
2. **MachineHealthCheck interaction.** Keeping probes running on a cancelled plan is the right
   default, but a node left in a partial state will start reporting unhealthy and may be
   remediated. Confirm that is the desired behavior with whoever owns the MHC configuration.
3. **Pause has no expiry.** Nothing reaps a forgotten `plan.cattle.io/paused` annotation, and the
   node it is on silently blocks cluster convergence for as long as it stays. Out of scope here,
   but worth a Rancher-side alert or a max-pause-duration policy.

## Proposed changes in rancher/rancher

The agent side is complete without any of this — the states are inert against today's planner. What
these buy is (a) an operator-legible reason for a stalled rollout and (b) a way back from a cancel.
Line references are against `73d6cd578`.

### 1. Upstream the wire constants — `pkg/plan/state.go`

Add alongside `PlanStateCancelled`, then bump the dependency in system-agent and delete the local
copies:

```go
// PlanStatePaused means the agent is holding execution at an instruction boundary in response to
// the plan.cattle.io/paused annotation. Non-terminal: the agent resumes into the state recorded in
// plan-progress when the annotation is removed.
PlanStatePaused PlanState = "paused"

// PlanCancelledAnnotation / PlanPausedAnnotation request that the agent abort or hold the plan.
PlanCancelledAnnotation = "plan.cattle.io/cancelled"
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
case planapi.PlanStateCancelled:
    result.InSync = false
```

The computed `InSync` is unchanged from what the default branch produces today, which is the point:
this is a readability and intent change that unblocks the message work below, and it is safe to
merge independently of everything else. `plan.Node` already carries `PlanState`
(`pkg/apis/rke.cattle.io/v1/plan/plan.go:37`), so no API type change is required.

### 3. Say why the rollout is stalled — `getPlanStatusReasonMessage`, `store.go:150-168`

Today a paused or cancelled node reports `WaitingPlanStatusMessage` — `"waiting for plan to be
applied"` — which is indistinguishable from a slow node. Add ahead of the `InSync` case:

```go
case entry.Plan.PlanState == planapi.PlanStatePaused:
    return PausedPlanStatusMessage    // "plan execution paused by operator"
case entry.Plan.PlanState == planapi.PlanStateCancelled:
    return CancelledPlanStatusMessage // "plan execution cancelled by operator"
```

This is the highest-value item in the list and the cheapest. `Message` in `pkg/plan/store.go`
buckets nodes for the cluster-level summary and should grow matching buckets, so a stalled upgrade
names the paused machines instead of reporting a generic wait.

### 4. Surface it on the machine — `pkg/controllers/capr/plansecret/plansecret.go:153-186`

The handler currently branches on failure and otherwise clears `PlanApplied` to nil (line 185), so
a suspended plan looks condition-clean. Add a branch before that which sets `PlanApplied` to a
distinct non-error reason (`Paused` / `Cancelled`) carrying the same message as above. This is what
the UI and `kubectl describe machine` read, and it is where an operator will actually look.

### 5. A way back from cancel — `assignAndCheckPlan`, `store.go:483-486`

Without this, cancel strands the machine. The existing guard re-drives a plan only when
`!store.equalities.DeepEqual(entry.Plan.Plan, newPlan)`. Widen it: when the cancel annotation has
been removed and `plan-state` is still `cancelled`, call `UpdatePlan` to rewrite
`plan-state: pending` even though the plan bytes are unchanged, so the operator's act of clearing
the annotation is what re-arms the node:

```go
if entry.Plan.PlanState == planapi.PlanStateCancelled &&
    entry.Metadata.Annotations[planapi.PlanCancelledAnnotation] != "true" {
    // operator cleared the cancellation; re-drive the existing plan
}
```

Two properties worth stating explicitly in review, because they are the whole risk of this item:
re-arming must be driven by the *annotation transition* and not by the mere presence of
`cancelled`, or the planner will immediately undo every cancellation; and the agent re-runs the
plan from instruction 0, since a cancelled plan keeps no checkpoint. The alternative — require a
plan content change — is simpler and needs no code, but leaves the operator with no obvious action
to take.

### 6. Setting the annotations

Nothing in Rancher writes either annotation today. Whether that lands as a UI action, an API field
on the machine, or a documented `kubectl annotate` is a product question. The agent treats the
annotations as operator-owned and never edits them, so any of the three works, and a documented
`kubectl annotate` is enough to ship the pause half.

### 7. The successor store — `pkg/plan/store.go`

`Store.AssignPlan` (line ~370) is documented as superseding the CAPR path once its CAPI dependency
is unravelled, and it does not read `plan-state` at all — it infers `InProgress` from plan bytes
matching. Whenever it takes over it needs items 2-5 reapplied, or pause and cancel become invisible
again. Worth a tracking issue now rather than a rediscovery later.

### Suggested sequencing

Items 1-4 are additive, independently mergeable, and carry no behaviour change for existing plans;
they can land before or after the agent work in any order. Item 5 changes planner behaviour and
should land before cancel is exposed to operators. Item 6 gates any of this being reachable from
the UI.

## Tests

### Unit (`make test`, build tag `test`), table-driven and parallel per repo conventions

`pkg/applyinator/applyinator_test.go` (extend; POSIX-shell cases guarded by the existing
`runtime.GOOS == "windows"` skip):

- `checkInterruption` precedence table.
- Pre-closed `Cancel` runs nothing — and short-circuits without blocking on a held mutex.
- `Pause` closed after instruction 0 stops before instruction 1 with `Completed == 1`.
- `ResumeFromOneTimeInstruction: 1` leaves instruction 0's sentinel file absent.
- Closing `Cancel` during a `sleep 60` instruction returns promptly with `InterruptionCancelled`.
- A script trapping TERM proves SIGTERM precedes the kill.
- A script that backgrounds a child writing to a sentinel file proves the **grandchild** is also
  killed — the specific regression the process-group work exists to prevent.

`pkg/k8splan/plan_decision_test.go`: `readInterrupt` table. (No `Halt` case; the flag is gone.)

`pkg/k8splan/plan_progress_test.go`: round-trip; checksum mismatch → zero value; agent-instance
mismatch → `Completed` zeroed but `ResumeState` retained; malformed JSON.

`pkg/k8splan/reconcile_test.go` (extend `TestReconcileSecretScenarios`): cancel on pending, cancel
on in-progress, cancel on terminal (ignored), pause on pending, pause on in-progress, unpause
resumes at the checkpoint. Probes are asserted to **keep running** during an interrupt with a
counting `httptest` server.

Four cases exist specifically for the defects this revision fixes, and should be written first:

- **Checksum-flow interrupt writes no lifecycle keys — entry path.** No `plan-state` on the input
  Secret, pause annotation set. Assert the resulting Secret data contains neither `plan-state` nor
  `plan-progress`, and that `probe-statuses` is still written.
- **Checksum-flow interrupt writes no lifecycle keys — mid-apply path.** Same Secret, but the
  interrupt is delivered while `Apply` is in flight so the outcome runs through step 6 rather than
  the entry path. Assert the same two keys are absent, that `applied-checksum` is unchanged, and
  that the following reconcile re-runs from instruction 0. This is the case the `UsesPlanState`
  guard exists for; without a test it regresses invisibly, because the plan-state flow keeps
  passing.

- **Conflicting write during an interrupted apply.** The fake client returns `Conflict` on the
  first `Update` and a Secret bearing the operator's annotation on the subsequent `Get`. Assert
  `plan-state: paused` and the checkpoint land on the fresh object, that the reconcile returns
  without a fatal, and — the actual regression — that `Completed` is preserved rather than lost.
  A companion case where the `Get` returns a *different* plan asserts the write is abandoned.
- **Interrupt-path re-entry is a no-op.** Reconcile twice with the pause annotation set and
  `plan-state` already `paused`. Assert exactly one `Update` call across both reconciles and that
  `plan-progress` still reports the original `Completed`, not 0.

`pkg/k8splan/reconcile_test.go`, degraded mode: with `DisablePausedPlanState` set, pause an
`in-progress` plan, then unpause. Assert `plan-state` never leaves `in-progress`, that
`plan-progress` still records `Paused: true` and the completed count, and that the unpause
reconcile resumes at the checkpoint rather than at instruction 0 — i.e. the contingency costs only
the wire state.

`pkg/k8splan/interrupt_test.go`: `fake.MockCacheInterface[*corev1.Secret]` wired through
`MockControllerInterface.EXPECT().Cache()`, returning the annotation on the second poll; assert the
channel closes and that a cache error falls back to `sc.Get`.

### E2E (`test/e2e/suites/remote-plan/`, build tag `e2e`, `short` label)

- New `test/framework/secret.go` helpers `SetSecretAnnotation` / `RemoveSecretAnnotation`.
- `cancellation_test.go`: cancel a plan whose first instruction sleeps → `plan-state` reaches
  `cancelled`, the later instruction's file never appears, `plan-progress` shows partial execution;
  cancel a pending plan → no side effects at all.
- `pause_test.go`: pause between instructions → `plan-state: paused` and the next instruction's
  file is absent; remove the annotation → `plan-state: succeeded` and an append-once marker file
  proves the completed instruction was not re-executed. Probe statuses are asserted to **continue**
  advancing while paused.

### Docs

Add a "Cancellation and pause" paragraph to the `pkg/k8splan` section of `CLAUDE.md`, covering the
execution-vs-observation rule and the pause-is-a-boundary semantics.

## Files touched

| File | Change |
| --- | --- |
| `pkg/applyinator/applyinator.go` | `Interruption`, `checkInterruption`, input/output fields, start index and channel checks in both instruction loops, pre-lock short-circuit, `watchForTermination` in `execute` |
| `pkg/applyinator/process_unix.go`, `process_windows.go` | new — process-group / Job Object setup and tree termination |
| `pkg/k8splan/watcher.go` | new constants, `secretConflictMergeKeys` entry and absent-key fix, `agentInstance` on the watcher |
| `pkg/k8splan/interrupt.go` | new — `readInterrupt`, `interruptWatch`, `handleInterrupt`, `writeInterruptOutcome` |
| `pkg/k8splan/plan_progress.go` | new — checkpoint parse/marshal, `resolveResume` |
| `pkg/k8splan/reconcile.go` | interrupt entry path, effective-state resolution, interrupted outcome path |
| `pkg/k8splan/secret_outcome.go` | clear `plan-progress` on terminal outcomes |
| `pkg/config/config.go` | `DisablePausedPlanState` on `AgentConfig` |
| `main.go` | `CATTLE_AGENT_DISABLE_PAUSED_PLAN_STATE` override, plumbed into `k8splan.Watch` |
| `go.mod` | `golang.org/x/sys` indirect → direct |
| `test/framework/secret.go` | annotation helpers |
| tests + `CLAUDE.md` | as above |

`pkg/k8splan/plan_decision.go` is deliberately **not** in this list.

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
kubectl -n <ns> annotate --overwrite secret <machine>-machine-plan plan.cattle.io/paused-
```

Confirm the state reports `paused`, that it reaches `succeeded` after the annotation is removed
without re-running completed instructions, and — the write-amplification check — that the Secret's
resourceVersion is stable for the duration of the pause rather than incrementing every few seconds.

For cancel, additionally confirm on the node that no descendant of the killed instruction survives:

```bash
pgrep -af <installer-or-script-name>
```

## Out of scope / follow-ups

- Everything under "Proposed changes in rancher/rancher" — upstreaming the constants, teaching the
  planner the two states, reporting them to the operator, and the recovery path out of a cancel.
  None of it blocks this change, but item 5 there should land before cancel is exposed to
  operators; see "Remaining risks" item 1.
- `pkg/localplan`: no annotation channel exists for file-based plans; unchanged.
- The `logrus.Fatalf` on `updateSecret` failure in the normal (non-interrupted) outcome path.
  `writeInterruptOutcome` avoids it on the new paths; making the existing path non-fatal is a
  behavioral change to plan reporting that deserves its own discussion.
- Closing the Windows job-object assignment race, which needs `CREATE_SUSPENDED` support that
  `os/exec` does not expose.
