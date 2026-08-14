//go:build e2e

package remoteplan_test

import (
	"context"
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	planapi "github.com/rancher/rancher/pkg/plan"
	"github.com/rancher/system-agent/test/framework"
)

// Helpers shared by the interrupt specs (cancellation_test.go and pause_test.go).
//
// They live in the spec package rather than in test/framework because they are only meaningful
// inside the agent container, and because "did this file appear" is the load-bearing observation
// for both files. Sharing across spec files matches the suite's existing shape: cl, kubeconfigPath
// and bootstrapClusterProxy are declared in suite_test.go and used everywhere.

// instructionWedgeCapSeconds bounds how long any instruction in these specs can block.
//
// Applyinator.Apply holds the applyinator mutex and runs synchronously inside the controller's
// single OnChange worker, so an instruction that outlives its spec does not merely leak a shell: it
// wedges the agent. Every other spec waits on framework.WaitTimeout (120s), so an instruction that
// can block longer than that turns one failure into a cascade of them. This cap is the last of
// three lines of defence — the interrupt under test, then releaseGateOnCleanup, then this — and it
// is the only one that holds when the other two have failed.
const instructionWedgeCapSeconds = 60

// blockingScript returns a shell snippet that waits for gate to appear and gives up after
// instructionWedgeCapSeconds regardless.
//
// The gate half is what makes the pause specs deterministic: the spec can prove the agent has
// observed an annotation before it lets the running instruction return, so there is no timing
// window to lose. The cap half is the wedge bound described above. Both halves are load-bearing.
func blockingScript(gate string) string {
	return fmt.Sprintf("i=0; while [ ! -e %s ] && [ $i -lt %d ]; do sleep 1; i=$((i+1)); done",
		gate, instructionWedgeCapSeconds)
}

// releaseGateOnCleanup opens gate however the spec ends, so an instruction blocked on it is
// released as soon as the spec is over rather than at the cap.
//
// Exec-based rather than Secret-based on purpose. Ginkgo runs AfterEach nodes before DeferCleanup
// nodes: internal/group.go:246-331 makes one pass with includeDeferCleanups false, running the
// AfterEach/JustAfterEach/AfterAll nodes, and only then sets it true and repeats. So by the time
// this runs, the suite's AfterEach has already deleted the plan Secret, and any cleanup that tried
// to release an instruction by writing to that Secret — setting the cancel annotation, say — would
// be a silent no-op. That is also why blockingScript's cap exists rather than being redundant.
//
// The body makes no assertion at all, deliberately: a Gomega failure inside it would abort the
// cleanup before it did its job, which is the exact outcome it exists to prevent. That is also why
// the pod name is fetched through agentPodName rather than framework.KubectlGetPodName.
func releaseGateOnCleanup(gate string) {
	DeferCleanup(func() {
		ctx := context.Background()
		podName, ok := agentPodName(ctx)
		if !ok {
			return
		}
		_, _, _ = framework.KubectlExec(ctx, kubeconfigPath,
			framework.E2ENamespace, podName, framework.AgentContainerName,
			[]string{"/bin/sh", "-c", "touch " + gate})
	})
}

// agentPodName returns the agent pod's name, reporting failure rather than asserting it. For use
// on cleanup paths, where framework.KubectlGetPodName's internal Expect would do more harm than
// returning nothing.
func agentPodName(ctx context.Context) (string, bool) {
	result := &framework.RunCommandResult{}
	framework.RunCommand(ctx, framework.RunCommandInput{
		Command: "kubectl",
		Args: []string{
			"--kubeconfig", kubeconfigPath, "get", "pods",
			"-l", framework.AgentLabel, "-n", framework.E2ENamespace,
			"-o", "jsonpath={.items[0].metadata.name}",
		},
	}, result)
	name := strings.TrimSpace(string(result.Stdout))
	return name, result.Error == nil && name != ""
}

// nodeFileExists reports whether path exists inside the agent container.
func nodeFileExists(ctx context.Context, podName, path string) bool {
	stdout := execInAgent(ctx, podName, fmt.Sprintf("if [ -e %s ]; then echo yes; else echo no; fi", path))
	return strings.TrimSpace(stdout) == "yes"
}

// nodeFileLineCount returns the number of lines in path inside the agent container, or 0 when the
// file does not exist. Counted here rather than with wc so the only node-side commands these specs
// need are /bin/sh and cat, both of which the existing specs already rely on.
func nodeFileLineCount(ctx context.Context, podName, path string) int {
	content := nodeFileContent(ctx, podName, path)
	if content == "" {
		return 0
	}
	return strings.Count(content, "\n") + 1
}

// nodeFileContent returns path's contents inside the agent container, or "" when it does not
// exist. Surrounding whitespace is trimmed so callers can compare against a plain literal.
func nodeFileContent(ctx context.Context, podName, path string) string {
	return strings.TrimSpace(execInAgent(ctx, podName, fmt.Sprintf("cat %s 2>/dev/null || true", path)))
}

// execInAgent runs a shell snippet inside the agent container and returns its stdout.
//
// A kubectl failure fails the spec rather than being folded into the return value, and that is
// deliberate. Most callers feed a "the instruction never ran" assertion, and one that cannot tell
// "the file is absent" from "kubectl could not ask" passes vacuously exactly when something is
// wrong. Gomega propagates a failed Expect out of an Eventually or Consistently poller rather than
// retrying it (internal/async_assertion.go:329-335 re-panics unless the poller takes a Gomega
// argument), so a broken exec surfaces immediately and loudly.
//
// Cleanup paths must not use this — see releaseGateOnCleanup.
func execInAgent(ctx context.Context, podName, script string) string {
	stdout, stderr, err := framework.KubectlExec(ctx, kubeconfigPath,
		framework.E2ENamespace, podName, framework.AgentContainerName,
		[]string{"/bin/sh", "-c", script})
	Expect(err).NotTo(HaveOccurred(), "kubectl exec failed running %q: %s", script, stderr)
	return stdout
}

// currentPlanState reads plan-state off the plan Secret.
func currentPlanState(ctx context.Context) planapi.PlanState {
	data := framework.GetSecretData(ctx, cl, framework.E2ENamespace, framework.PlanSecretName)
	return planapi.PlanState(data[planapi.PlanStateKey])
}
