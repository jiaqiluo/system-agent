//go:build e2e

package remoteplan_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	planapi "github.com/rancher/rancher/pkg/plan"
	"github.com/rancher/system-agent/pkg/k8splan"
	"github.com/rancher/system-agent/test/framework"
)

// Node paths used by the cancellation specs. Each spec gets its own path because AfterEach deletes
// the plan Secret but does not clean the agent container's filesystem. Without isolated paths, a
// "this file never appeared" assertion could pass or fail based on artifacts left by an earlier spec.
const (
	cancelRunningGate    = "/tmp/e2e-cancel-running-gate"
	cancelRunningStepTwo = "/tmp/e2e-cancel-running-step-two.txt"
	cancelPendingRan     = "/tmp/e2e-cancel-pending-ran.txt"
	cancelTreeGate       = "/tmp/e2e-cancel-tree-gate"
	cancelTreeChildLog   = "/tmp/e2e-cancel-tree-child.log"
)

var _ = Describe("Remote Plan - Cancellation", Label(framework.ShortTestLabel), func() {
	It("should cancel an in-flight plan and never run the instructions that follow", func() {
		ctx := context.Background()
		podName := framework.KubectlGetPodName(ctx, kubeconfigPath,
			framework.E2ENamespace, framework.AgentLabel)
		releaseGateOnCleanup(cancelRunningGate)

		By("Creating a plan whose first instruction blocks long enough to be caught mid-flight")
		plan := framework.NewPlan().
			WithInstruction("long-running", "/bin/sh",
				[]string{"-c", blockingScript(cancelRunningGate)}, true).
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
		// Cancel is prompt: the in-flight instruction's context is canceled rather than being
		// allowed to finish, so this must not take anything like the instruction's own cap.
		framework.WaitForSecretFieldCondition(ctx, cl,
			framework.E2ENamespace, framework.PlanSecretName,
			planapi.PlanStateKey,
			func(val []byte) bool { return planapi.PlanState(val) == planapi.PlanStateCancelled },
			framework.WaitTimeout, 2*time.Second)

		By("Verifying the instruction after the cancelled one never runs")
		Consistently(func() bool { return nodeFileExists(ctx, podName, cancelRunningStepTwo) },
			20*time.Second, 4*time.Second).Should(BeFalse(),
			"a cancelled plan must start nothing further")

		By("Verifying plan-progress reports partial execution rather than a suspension")
		progress := framework.GetPlanProgress(ctx, cl,
			framework.E2ENamespace, framework.PlanSecretName)
		Expect(progress).NotTo(BeNil(), "a cancellation must leave a plan-progress report behind")
		Expect(progress).NotTo(HaveKey("paused"),
			"a cancellation is a report, not a suspension: Paused is omitempty and must be left zero, "+
				"since only a suspended checkpoint may grant a resume")
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
		// No gate and no cleanup are needed here: the annotation is present before the agent's
		// first reconcile, so recordInterruptAtEntry returns before Apply is ever called and no
		// instruction can be left running.
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

		By("Verifying the plan's only instruction never runs, across a full re-enqueue cycle")
		// Sized to outlast interruptedEnqueuePeriod (60s, reconcile.go:24), so this spans at least one
		// re-enqueue of the canceled plan. This is the only cancellation coverage for reconciles that
		// follow the one that recorded the cancellation, which is where a missing terminal-state guard
		// would surface.
		Consistently(func() bool { return nodeFileExists(ctx, podName, cancelPendingRan) },
			70*time.Second, 5*time.Second).Should(BeFalse(),
			"a plan canceled before it started must have no side effects on the node whatsoever, "+
				"including on the re-enqueues that follow")

		By("Verifying the checkpoint records that nothing was executed")
		progress := framework.GetPlanProgress(ctx, cl,
			framework.E2ENamespace, framework.PlanSecretName)
		Expect(progress).NotTo(BeNil())
		Expect(progress["completedInstructions"]).To(BeEquivalentTo(0))
		Expect(progress["totalInstructions"]).To(BeEquivalentTo(1))
		Expect(progress).NotTo(HaveKey("paused"))

		By("Verifying applied-checksum was not written")
		Expect(framework.GetAppliedChecksum(ctx, cl,
			framework.E2ENamespace, framework.PlanSecretName)).To(BeEmpty())
	})

	It("should kill the instruction's whole process tree, not merely its shell", func() {
		ctx := context.Background()
		podName := framework.KubectlGetPodName(ctx, kubeconfigPath,
			framework.E2ENamespace, framework.AgentLabel)
		releaseGateOnCleanup(cancelTreeGate)

		By("Creating a plan whose instruction backgrounds a child that keeps writing")
		// The backgrounded loop is the process-tree case this test is meant to cover. Plan instructions
		// almost always run a run.sh that shells out to an installer or package manager, so signaling
		// only the direct child could leave the actual work running on a node whose operator believes
		// they stopped it.
		//
		// Redirect the child's stdout to /dev/null so it does not inherit the agent's pipes. execute()
		// waits for eg.Wait() before cmd.Wait(), and eg.Wait() does not return until both pipes reach EOF.
		// A child that keeps those pipes open would therefore block Apply and the agent's single worker for
		// the child's entire lifetime instead of the parent's. That is a separate failure mode from the
		// one under test; the watchdog's pipe-closing behavior is covered by unit tests in
		// pkg/applyinator.
		//
		// The child also watches the gate, so cleanup can release the entire tree. Its 300-iteration cap
		// is a backstop against a stray writer and far exceeds the ~90s of assertion windows below, so it
		// cannot mask a cancellation that failed to reach the child.
		child := fmt.Sprintf("i=0; while [ ! -e %s ] && [ $i -lt 300 ]; do echo tick >> %s; sleep 1; i=$((i+1)); done",
			cancelTreeGate, cancelTreeChildLog)
		script := fmt.Sprintf("(%s) >/dev/null 2>&1 & %s", child, blockingScript(cancelTreeGate))
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
