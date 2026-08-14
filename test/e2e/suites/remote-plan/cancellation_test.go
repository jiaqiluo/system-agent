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

// Node paths used by the cancellation specs. One prefix per spec: the suite's AfterEach deletes
// the plan Secret but nothing cleans the agent container's filesystem, and a "this file never
// appeared" assertion that inherits a file from an earlier spec is worse than no assertion at all.
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
		// Cancel is prompt: the in-flight instruction's context is cancelled rather than being
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
		// Sized to outlast interruptedEnqueuePeriod (60s, reconcile.go:24) so this spans at least
		// one re-enqueue of the cancelled plan. It is the only cancellation coverage of what the
		// agent does on the reconciles that follow the one that recorded the cancellation, which
		// is where a missing terminal-state guard would show up.
		Consistently(func() bool { return nodeFileExists(ctx, podName, cancelPendingRan) },
			70*time.Second, 5*time.Second).Should(BeFalse(),
			"a plan cancelled before it started must have no side effects on the node whatsoever, "+
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
		// The backgrounded loop is what the process-group work exists to reach. Plan instructions
		// are near-universally a run.sh that shells out to an installer or a package manager, so
		// signalling the direct child alone would leave the real work running on a node whose
		// operator believes they stopped it.
		//
		// The child's own stdout is redirected to /dev/null so it does not inherit the agent's
		// pipes. execute() calls eg.Wait() before cmd.Wait(), and eg.Wait() only returns once both
		// pipes reach EOF, so a child holding them open would keep Apply — and therefore the
		// agent's single worker — blocked for the child's whole lifetime rather than the parent's.
		// That is a different failure than the one under test, and it is the watchdog's
		// pipe-closing logic that covers it, in pkg/applyinator's unit tests.
		//
		// The child watches the gate too, so a cleanup releases the whole tree; its 300-iteration
		// cap is a backstop against a stray writer and is far beyond the ~90s of assertion windows
		// below, so it cannot mask a cancel that failed to reach it.
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
