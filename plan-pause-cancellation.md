Plan cancellation and pause support in rancher-system-agent

Context

github.com/rancher/rancher/pkg/plan now models an explicit plan lifecycle
(pending → in-progress → succeeded | failed | cancelled), and the agent already drives those
transitions in pkg/k8splan/reconcile.go. Two lifecycle controls are still missing on the node side:

- Cancellation — an operator needs to abort a Day 2 operation that is hanging, was triggered by
  accident, or has left the cluster in a partial state. PlanStateCancelled exists in the API but
  nothing ever writes it.
- Pause — an operator needs to hold execution at a safe point (maintenance window, incident) and
  later resume from where it stopped rather than re-running the whole plan.

Both are signalled by annotations on the plan Secret (plan.cattle.io/cancelled: "true",
plan.cattle.io/paused: "true"). The hard part is that Applyinator.Apply runs synchronously
inside the OnChange handler with DefaultWorkers: 1, so while an apply is in flight the
controller cannot deliver the annotation change. Detecting an interruption mid-apply needs a
separate observer, and stopping cleanly needs the applyinator to grow interruption points.

Outcome: an operator can cancel or pause a plan at any point in its lifecycle; the agent stops
promptly and cleanly, reports the resulting state in the Secret, and resumes a paused plan at the
first instruction that has not yet completed.

Settled decisions

┌────────────────────────────────┬────────────────────────────────────────────────────────────────────────────────────────────────┐
│            Decision            │                                             Choice                                             │
├────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Where new wire constants live  │ Locally in pkg/k8splan for now; follow-up to upstream them into rancher/pkg/plan               │
├────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Paused representation          │ New non-terminal plan-state value paused                                                       │
├────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Resume semantics               │ Checkpoint the instruction index in the Secret; completed instructions are not re-run          │
├────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Stopping a running instruction │ SIGTERM, then SIGKILL after 10s (Windows: direct kill)                                         │
├────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Scope                          │ Remote mode (pkg/k8splan) only — annotations are a Secret concept; pkg/localplan is unaffected │
└────────────────────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────┘

Wire format additions (new constants in pkg/k8splan/watcher.go)

// TODO: upstream these into github.com/rancher/rancher/pkg/plan alongside PlanStateCancelled,
// then drop the local definitions and bump the dependency.

// PlanCancelledAnnotation, set to "true" on the plan Secret, requests that the agent abort the plan.
PlanCancelledAnnotation = "plan.cattle.io/cancelled"
// PlanPausedAnnotation, set to "true" on the plan Secret, requests that the agent hold execution.
PlanPausedAnnotation = "plan.cattle.io/paused"
// PlanProgressKey is the Secret data key holding the resume checkpoint (JSON, see planProgress).
PlanProgressKey = "plan-progress"
// PlanStatePaused is a non-terminal plan-state: execution is held at a safe point and will resume
// into planProgress.ResumeState when the pause annotation is removed.
PlanStatePaused planapi.PlanState = "paused"

PlanProgressKey is appended to secretConflictMergeKeys so a conflict retry carries the
checkpoint over.

planProgress (new pkg/k8splan/plan_progress.go) is checksum-scoped so a checkpoint recorded for
a different plan is discarded:

type planProgress struct {
Checksum    string            `json:"checksum"`
Completed   int               `json:"completedInstructions"`
Total       int               `json:"totalInstructions"`
ResumeState planapi.PlanState `json:"resumeState,omitempty"` // state restored when the pause lifts
}

func parsePlanProgress(data map[string][]byte, checksum string) planProgress // zero value on mismat
func marshalPlanProgress(p planProgress) []byte

Rancher's pkg/capr/planner/store.go sends unknown plan-state values down its
"fall back to checksum comparison" default branch, so writing paused is safe against today's
server without a coordinated release.

Behavior rules

While either annotation is "true", the agent skips Apply and probes entirely for that
reconcile. On top of that:

- Cancel wins over pause. If the effective state is non-terminal, write plan-state: cancelled
  plus a plan-progress record. If it is already terminal, log at debug and write nothing.
- Pause writes plan-state: paused and a plan-progress record whose ResumeState is the
  state being suspended (including succeeded/failed, which restore as no-ops). Only in the
  plan-state flow — see below.
- After cancellation plan-state: cancelled persists, and the agent halts instructions and
  probes until the orchestrator delivers new content with pending. This is a deliberate change to
  terminal-state handling and applies to cancelled only; succeeded/failed keep monitoring.
- Checksum flow (plan-state absent, legacy orchestrators): both annotations still suppress
  apply and probes, but no plan-state/plan-progress is written — doing so would silently switch
  the Secret into the plan-state flow. Unpausing falls back to normal checksum reconciliation.
- The agent never edits the annotations; the orchestrator owns them. A new plan delivered with the
  cancel annotation still set is cancelled again — logged at warn level.

Resume path resolution at the top of the reconcile:

effectiveState, resumeFrom := currentPlanState, 0
if currentPlanState == PlanStatePaused {
p := parsePlanProgress(secret.Data, cp.Checksum)
effectiveState = orDefault(p.ResumeState, planapi.PlanStateInProgress)
resumeFrom = p.Completed
}

resumeFrom is non-zero only on the unpause reconcile. Plain in-progress on startup keeps
today's documented crash-recovery contract (re-execute from the beginning).

Changes by package

1. pkg/applyinator — interruption points

applyinator.go:

- New Interruption string type: InterruptionNone (""), InterruptionPaused,
  InterruptionCancelled.
- ApplyInput gains Cancel <-chan struct{}, Pause <-chan struct{}, and
  ResumeFromOneTimeInstruction int. Zero values mean "never interrupted, start at 0", so
  pkg/localplan/watcher.go needs no change.
- ApplyOutput gains Interruption Interruption and CompletedOneTimeInstructions int.
- checkInterruption(cancel, pause <-chan struct{}) Interruption — pure, non-blocking select,
  cancel first. Nil channels are never ready.
- Apply short-circuits on a pre-existing interruption before touching files, then derives
  execCtx from ctx and cancels it when input.Cancel closes, so exec'd processes are signalled.
- runOneTimeInstructions takes a start index and the channels and returns a oneTimeResult
  struct (Output, Succeeded, Completed, Interruption) rather than growing to five return
  values — matching the existing planStateResult/checksumFlowResult style. It checks
  checkInterruption before each instruction, and re-checks after a failed instruction so a
  cancel-induced kill is reported as cancelled rather than failed. Output from a killed instruction
  is still saved when SaveOutput is set.
- runPeriodicInstructions gets the same per-iteration check.

execute() graceful termination — replace exec.CommandContext with exec.Command plus an
explicit watchdog, so both operator cancellation and agent shutdown take the same path:

const instructionTerminationGrace = 10 * time.Second

// watchForTermination signals cmd's process once ctx is done: terminateProcess first, escalating
// to Kill after instructionTerminationGrace. The pipes are closed alongside the kill so streamLogs
// cannot block forever on a grandchild that inherited them. The returned func stops the watchdog.
func watchForTermination(ctx context.Context, cmd *exec.Cmd, pipes ...io.Closer) func()

New pkg/applyinator/signal_unix.go (//go:build !windows) and signal_windows.go, matching the
existing file_unix.go/file_windows.go split:
terminateProcess(p *os.Process) error → p.Signal(syscall.SIGTERM) on Unix, p.Kill() on Windows.

2. pkg/k8splan — detection and state writes

New pkg/k8splan/interrupt.go:

- readInterrupt(annotations map[string]string) applyinator.Interruption — pure; "true" only,
  cancel takes precedence.
- interruptWatch + (w *watcher) startInterruptWatch(ctx, sc) — a goroutine polling every
  interruptPollInterval (2s) that closes Cancel/Pause the first time the matching annotation
  is observed. It reads sc.Cache().Get(ns, name) (the informer's indexer is updated by its own
  goroutine and is not blocked by the busy workqueue worker, so it stays fresh during an apply),
  falling back to sc.Get(...) if the cache read errors. Started around the Apply call and
  stopped via defer.
- handleInterrupt(...) — the reconcile-entry path: emits the decision logs, writes the
  plan-state/plan-progress updates per the rules above, EnqueueAfter(w.probePeriod), and
  returns without applying or probing.

plan_decision.go:

- decidePlanStateAction gains a Halt bool on planStateResult, set for PlanStateCancelled, and
  a PlanStatePaused case is unreachable by construction (the caller resolves it to
  ResumeState first) but returns NeedsApplied: true defensively.

reconcile.go — insert into the existing flow:

1. After CalculatePlan and reading currentPlanState: readInterrupt(secret.Annotations);
   if set, delegate to handleInterrupt and return.
2. Resolve effectiveState/resumeFrom (snippet above); every downstream use of
   currentPlanState becomes effectiveState, including UsesPlanState in applyOutcomeInput.
3. If decidePlanStateAction(...).Halt, skip Apply and probes, enqueue, return.
4. Start the interrupt watch; pass Cancel, Pause, ResumeFromOneTimeInstruction: resumeFrom
   into ApplyInput.
5. After Apply, if applyOutput.Interruption != InterruptionNone, take a dedicated outcome path
   instead of buildSecretDataUpdates (which would otherwise write plan-state: failed, since an
   interrupted apply has OneTimeApplySucceeded == false):
- write plan-state = cancelled/paused and the plan-progress checkpoint,
- write applied-output with the accumulated one-time output — this is what
  selectExistingOutput feeds back as ExistingOneTimeOutput, so SaveOutput results from
  already-completed instructions survive the pause,
- not write applied-checksum (the plan is not applied),
- leave failure-count/success-count untouched,
- skip prober.DoProbes,
- on cancel with 0 < completed < total, logrus.Warnf that the plan was cancelled after
  partial execution and the node may be in an inconsistent state.

secret_outcome.go: buildSecretDataUpdates clears PlanProgressKey on both the success and
failure paths so a stale checkpoint cannot leak into a later run.

3. Tests

Unit (make test, build tag test), table-driven and parallel per the repo conventions:

- pkg/applyinator/applyinator_test.go (extend; POSIX-shell cases guarded by the existing
  runtime.GOOS == "windows" skip): checkInterruption precedence table; pre-closed Cancel runs
  nothing; Pause closed after instruction 0 stops before instruction 1 with Completed == 1;
  ResumeFromOneTimeInstruction: 1 leaves instruction 0's sentinel file absent; closing Cancel
  during a sleep 60 instruction returns promptly with InterruptionCancelled; a script trapping
  TERM proves SIGTERM precedes the kill.
- pkg/k8splan/plan_decision_test.go: readInterrupt table, Halt for cancelled.
- pkg/k8splan/plan_progress_test.go: round-trip, checksum mismatch → zero value, malformed JSON.
- pkg/k8splan/reconcile_test.go (extend the existing TestReconcileSecretScenarios table):
  cancel on pending, cancel on in-progress, cancel on terminal (ignored), pause on pending, pause on
  in-progress, unpause resumes at the checkpoint, cancelled halts probes, checksum flow writes no
  plan-state. Probe suppression is asserted with a counting httptest server.
- pkg/k8splan/interrupt_test.go: fake.MockCacheInterface[*corev1.Secret] wired through
  MockControllerInterface.EXPECT().Cache(), returning the annotation on the second poll; assert
  the channel closes and that a cache error falls back to sc.Get.

E2E (test/e2e/suites/remote-plan/, build tag e2e, short label):

- New test/framework/secret.go helpers SetSecretAnnotation / RemoveSecretAnnotation.
- cancellation_test.go: cancel a plan whose first instruction sleeps → plan-state reaches
  cancelled, the later instruction's file never appears, plan-progress shows partial execution;
  cancel a pending plan → no side effects at all.
- pause_test.go: pause between instructions → plan-state: paused and the next instruction's file
  is absent; remove the annotation → plan-state: succeeded and an append-once marker file proves
  the completed instruction was not re-executed; probe statuses do not advance while paused.

Docs: add a "Cancellation and pause" paragraph to the pkg/k8splan section of CLAUDE.md.

Files touched

┌───────────────────────────────────────────────────┬─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                       File                        │                                                                                                           │
├───────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ pkg/applyinator/applyinator.go                    │ Interruption, checkInterruption, input/output s in both instruction loops, watchForTermination in execute │
├───────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ pkg/applyinator/signal_unix.go, signal_windows.go │ new — terminateProcess                                                                                                              │
├───────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ pkg/k8splan/watcher.go                            │ new constants, secretConflictMergeKeys entry                                                                                        │
├───────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ pkg/k8splan/interrupt.go                          │ new — readInterrupt, interruptWatch, handleInterrupt                                                                                │
├───────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ pkg/k8splan/plan_progress.go                      │ new — checkpoint parse/marshal                                                                                                      │
├───────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ pkg/k8splan/plan_decision.go                      │ Halt on planStateResult                                                                                   │
├───────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ pkg/k8splan/reconcile.go                          │ interrupt entry check, effective-state resolutcome path                                                   │
├───────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ pkg/k8splan/secret_outcome.go                     │ clear plan-progress on terminal outcomes                                                                                            │
├───────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ test/framework/secret.go                          │ annotation helpers                                                                                                                  │
├───────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ tests + CLAUDE.md                                 │ as above                                                                                                                            │
└───────────────────────────────────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

Verification

make test                     # unit tests, -tags=test -cover
make validate                 # lint + fmt-check + vet
GOOS=windows GOARCH=amd64 make build   # confirms the signal_windows.go split compiles
make test-e2e                 # Kind + Ginkgo, short label — covers cancel and pause end to end

Targeted while iterating:

go test -tags=test ./pkg/applyinator/ -run 'Interrupt|Cancel|Pause|Resume' -v
go test -tags=test ./pkg/k8splan/ -run 'Interrupt|Progress|ReconcileSecret' -v

Manual smoke against a live cluster (optional, mirrors the e2e specs):
kubectl -n <ns> annotate secret <machine>-machine-plan plan.cattle.io/paused=true, watch
journalctl -u rancher-system-agent for [k8splan] pause requested, confirm
kubectl -n <ns> get secret <machine>-machine-plan -o jsonpath='{.data.plan-state}' | base64 -d
reports paused, then kubectl annotate --overwrite ... plan.cattle.io/paused- and confirm it
reaches succeeded without re-running completed instructions.

Out of scope / follow-ups

- Upstreaming PlanCancelledAnnotation, PlanPausedAnnotation, PlanStatePaused, and
  PlanProgressKey into github.com/rancher/rancher/pkg/plan, then bumping the dependency here and
  deleting the local definitions.
- Rancher-side UI/controller work to set the annotations and surface the paused/cancelled
  states — this change only makes the agent honor them.
- pkg/localplan: no annotation channel exists for file-based plans; unchanged.
- Killing the whole process group. SIGTERM/SIGKILL target the direct child only; a grandchild that
  inherited the pipes is handled by the pipe close in watchForTermination rather than by a
  setpgid process-group kill, which is a larger and riskier change to instruction execution.

