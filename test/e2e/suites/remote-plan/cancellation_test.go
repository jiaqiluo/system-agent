//go:build e2e

package remoteplan_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	planapi "github.com/rancher/rancher/pkg/plan"
	"github.com/rancher/system-agent/pkg/k8splan"
	"github.com/rancher/system-agent/test/framework"
)

// Node paths used by the cancellation specs. One prefix per spec: the suite's AfterEach deletes
// the plan Secret but nothing cleans the agent container's filesystem, and a "this file never
// appeared" assertion that inherits a file from an earlier spec is worse than no assertion at all.
const (
	cancelRunningStepTwo = "/tmp/e2e-cancel-running-step-two.txt"
	cancelPendingRan     = "/tmp/e2e-cancel-pending-ran.txt"
	cancelTreeChildLog   = "/tmp/e2e-cancel-tree-child.log"
)

var _ = Describe("Remote Plan - Cancellation", Label(framework.ShortTestLabel), func() {
	It("should cancel an in-flight plan and never run the instructions that follow", func() {
		ctx := context.Background()
		podName := framework.KubectlGetPodName(ctx, kubeconfigPath,
			framework.E2ENamespace, framework.AgentLabel)

		By("Creating a plan whose first instruction runs long enough to be caught mid-flight")
		plan := framework.NewPlan().
			WithInstruction("long-running", "/bin/sh",
				[]string{"-c", "sleep 60"}, true).
			WithInstruction("should-not-run", "/bin/sh",
				[]string{"-c", "touch " + cancelRunningStepTwo}, true).
			Build()

		Expect(framework.CreatePlanSecretWithData(ctx, cl,
			framework.E2ENamespace, framework.PlanSecretName, plan,
			map[string][]byte{
				planapi.PlanStateKey: []byte(planapi.PlanStatePending),
			})).To(Succeed())

		By("Waiting for plan-state to become in-progress")
		framework.WaitForSecretFieldCondition(ctx, cl,
			framework.E2ENamespace, framework.PlanSecretName,
			planapi.PlanStateKey,
			func(val []byte) bool { return planapi.PlanState(val) == planapi.PlanStateInProgress },
			framework.WaitTimeout, time.Second)

		By("Setting " + k8splan.PlanCancelledAnnotation + " while the first instruction is still running")
		Expect(framework.SetSecretAnnotation(ctx, cl,
			framework.E2ENamespace, framework.PlanSecretName,
			k8splan.PlanCancelledAnnotation, "true")).To(Succeed())

		By("Waiting for plan-state to become cancelled")
		// Cancel is prompt: the in-flight instruction's context is cancelled rather than being
		// allowed to finish, so this must not take anything like the sixty seconds it would sleep.
		framework.WaitForSecretFieldCondition(ctx, cl,
			framework.E2ENamespace, framework.PlanSecretName,
			planapi.PlanStateKey,
			func(val []byte) bool { return planapi.PlanState(val) == planapi.PlanStateCancelled },
			framework.WaitTimeout, 2*time.Second)

		By("Verifying the instruction after the cancelled one never runs")
		Consistently(func() bool { return nodeFileExists(ctx, podName, cancelRunningStepTwo) },
			20*time.Second, 4*time.Second).Should(BeFalse(),
			"a cancelled plan must start nothing further, including on the re-enqueues that follow")

		By("Verifying plan-progress reports partial execution rather than a suspension")
		progress := framework.GetPlanProgress(ctx, cl,
			framework.E2ENamespace, framework.PlanSecretName)
		Expect(progress).NotTo(BeNil(), "a cancellation must leave a plan-progress report behind")
		Expect(progress["paused"]).NotTo(Equal(true),
			"a cancellation is a report, not a suspension: only a suspended checkpoint may grant a resume")
		Expect(progress["completedInstructions"]).To(BeNumerically("<", progress["totalInstructions"]),
			"the plan was stopped mid-flight, so fewer instructions completed than the plan contains")

		By("Verifying applied-checksum was not written for a plan that never finished")
		// Writing it would tell Rancher the node is in sync with a plan that was abandoned partway.
		Expect(framework.GetAppliedChecksum(ctx, cl,
			framework.E2ENamespace, framework.PlanSecretName)).To(BeEmpty())
	})

	It("should cancel a pending plan without executing anything at all", func() {
		ctx := context.Background()
		podName := framework.KubectlGetPodName(ctx, kubeconfigPath,
			framework.E2ENamespace, framework.AgentLabel)

		By("Creating a pending plan that already carries " + k8splan.PlanCancelledAnnotation)
		plan := framework.NewPlan().
			WithInstruction("should-not-run", "/bin/sh",
				[]string{"-c", "touch " + cancelPendingRan}, true).
			Build()

		Expect(framework.CreatePlanSecretWithAnnotations(ctx, cl,
			framework.E2ENamespace, framework.PlanSecretName, plan,
			map[string][]byte{planapi.PlanStateKey: []byte(planapi.PlanStatePending)},
			map[string]string{k8splan.PlanCancelledAnnotation: "true"})).To(Succeed())

		By("Waiting for plan-state to become cancelled")
		framework.WaitForSecretFieldCondition(ctx, cl,
			framework.E2ENamespace, framework.PlanSecretName,
			planapi.PlanStateKey,
			func(val []byte) bool { return planapi.PlanState(val) == planapi.PlanStateCancelled },
			framework.WaitTimeout, 2*time.Second)

		By("Verifying the plan's only instruction never ran")
		Consistently(func() bool { return nodeFileExists(ctx, podName, cancelPendingRan) },
			20*time.Second, 4*time.Second).Should(BeFalse(),
			"a plan cancelled before it started must have no side effects on the node whatsoever")

		By("Verifying the checkpoint records that nothing was executed")
		progress := framework.GetPlanProgress(ctx, cl,
			framework.E2ENamespace, framework.PlanSecretName)
		Expect(progress).NotTo(BeNil())
		Expect(progress["completedInstructions"]).To(BeEquivalentTo(0))
		Expect(progress["totalInstructions"]).To(BeEquivalentTo(1))
		Expect(progress["paused"]).NotTo(Equal(true))

		By("Verifying applied-checksum was not written")
		Expect(framework.GetAppliedChecksum(ctx, cl,
			framework.E2ENamespace, framework.PlanSecretName)).To(BeEmpty())
	})

	It("should kill the instruction's whole process tree, not merely its shell", func() {
		ctx := context.Background()
		podName := framework.KubectlGetPodName(ctx, kubeconfigPath,
			framework.E2ENamespace, framework.AgentLabel)

		By("Creating a plan whose instruction backgrounds a child that keeps writing")
		// The backgrounded loop is what the process-group work exists to reach. Plan instructions
		// are near-universally a run.sh that shells out to an installer or a package manager, so
		// signalling the direct child alone would leave the real work running on a node whose
		// operator believes they stopped it.
		script := fmt.Sprintf("(while true; do echo tick >> %s; sleep 1; done) & sleep 300", cancelTreeChildLog)
		plan := framework.NewPlan().
			WithInstruction("spawns-a-child", "/bin/sh", []string{"-c", script}, true).
			Build()

		Expect(framework.CreatePlanSecretWithData(ctx, cl,
			framework.E2ENamespace, framework.PlanSecretName, plan,
			map[string][]byte{
				planapi.PlanStateKey: []byte(planapi.PlanStatePending),
			})).To(Succeed())

		By("Waiting until the child has actually written, so the assertion below cannot pass vacuously")
		Eventually(func() int { return nodeFileLineCount(ctx, podName, cancelTreeChildLog) },
			framework.WaitTimeout, 2*time.Second).Should(BeNumerically(">=", 2),
			"the backgrounded child should be looping before the cancel is issued")

		By("Setting " + k8splan.PlanCancelledAnnotation)
		Expect(framework.SetSecretAnnotation(ctx, cl,
			framework.E2ENamespace, framework.PlanSecretName,
			k8splan.PlanCancelledAnnotation, "true")).To(Succeed())

		By("Waiting for plan-state to become cancelled")
		framework.WaitForSecretFieldCondition(ctx, cl,
			framework.E2ENamespace, framework.PlanSecretName,
			planapi.PlanStateKey,
			func(val []byte) bool { return planapi.PlanState(val) == planapi.PlanStateCancelled },
			framework.WaitTimeout, 2*time.Second)

		By("Waiting for the child to stop writing")
		// The process group is asked to stop with SIGTERM and only killed outright ten seconds
		// later, so allow for that escalation rather than asserting on the instant plan-state
		// lands. lastCount starts at -1 so the very first sample can never be mistaken for a
		// stable one.
		lastCount := -1
		Eventually(func() bool {
			current := nodeFileLineCount(ctx, podName, cancelTreeChildLog)
			stopped := current == lastCount
			lastCount = current
			return stopped
		}, 60*time.Second, 5*time.Second).Should(BeTrue(),
			"the grandchild is still appending well after the cancel: the signal did not reach the process group")

		By("Verifying the child stays dead")
		Consistently(func() int { return nodeFileLineCount(ctx, podName, cancelTreeChildLog) },
			20*time.Second, 4*time.Second).Should(Equal(lastCount),
			"the file grew again, so a descendant of the cancelled instruction is still alive")
	})
})

// --- Helpers shared by the interrupt specs (this file and pause_test.go) ---
//
// They live in the spec package rather than in test/framework because they are only meaningful
// inside the agent container, and because "did this file appear" is the load-bearing observation
// for both files. Package-level sharing across spec files matches the suite's existing shape:
// cl, kubeconfigPath and bootstrapClusterProxy are declared in suite_test.go and used everywhere.

// nodeFileExists reports whether path exists inside the agent container.
func nodeFileExists(ctx context.Context, podName, path string) bool {
	stdout := execInAgent(ctx, podName, fmt.Sprintf("if [ -e %s ]; then echo yes; else echo no; fi", path))
	return strings.TrimSpace(stdout) == "yes"
}

// nodeFileLineCount returns the number of lines in path inside the agent container, or 0 when the
// file does not exist. Counted here rather than with wc so the only node-side commands this file
// needs are /bin/sh and cat, both of which the existing specs already rely on.
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
// deliberate. Every caller feeds a "the instruction never ran" assertion, and one that cannot tell
// "the file is absent" from "kubectl could not ask" passes vacuously exactly when something is
// wrong. Gomega propagates a failed Expect out of an Eventually or Consistently poller rather than
// retrying it, so a broken exec surfaces immediately and loudly.
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
