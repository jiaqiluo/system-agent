package k8splan

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	planapi "github.com/rancher/rancher/pkg/plan"
	"github.com/rancher/system-agent/pkg/applyinator"
	"github.com/rancher/system-agent/pkg/config"
	"github.com/rancher/wrangler/v3/pkg/generic/fake"
	"github.com/sirupsen/logrus"
	"go.uber.org/mock/gomock"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	testNamespace = "test-ns"
	testSecret    = "test-secret"
)

func newTestWatcher(t *testing.T, hasRunOnce bool, lastAppliedRV string) *watcher {
	t.Helper()
	return &watcher{
		connInfo:                   config.ConnectionInfo{Namespace: testNamespace, SecretName: testSecret},
		applyinator:                *applyinator.NewApplyinator(t.TempDir(), false, "", "", nil),
		hasRunOnce:                 hasRunOnce,
		probePeriod:                5 * time.Second,
		lastAppliedResourceVersion: lastAppliedRV,
	}
}

func newMockSecretController(t *testing.T) *fake.MockControllerInterface[*corev1.Secret, *corev1.SecretList] {
	t.Helper()
	ctrl := gomock.NewController(t)
	sc := fake.NewMockControllerInterface[*corev1.Secret, *corev1.SecretList](ctrl)
	sc.EXPECT().Update(gomock.Any()).DoAndReturn(func(s *corev1.Secret) (*corev1.Secret, error) {
		return s, nil
	}).AnyTimes()
	return sc
}

func marshalPlan(t *testing.T, plan planapi.Plan) (raw []byte, checksum string) {
	t.Helper()
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("failed to marshal plan: %v", err)
	}
	return raw, planapi.Checksum(raw)
}

func TestReconcileSecretScenarios(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}
	t.Parallel()

	successPlanBytes, successChecksum := marshalPlan(t, planapi.Plan{
		OneTimeInstructions: []planapi.OneTimeInstruction{
			{CommonInstruction: planapi.CommonInstruction{Name: "ok", Command: "sh", Args: []string{"-c", "true"}}, SaveOutput: true},
		},
	})

	tests := []struct {
		name                string
		planBytes           []byte
		initialData         map[string][]byte
		hasRunOnce          bool
		lastAppliedRV       string
		wantEnqueueAfter    bool
		wantAppliedChecksum string // "" to skip this assertion
	}{
		{
			name:                "first start force-applies",
			planBytes:           successPlanBytes,
			initialData:         map[string][]byte{},
			hasRunOnce:          false,
			wantEnqueueAfter:    true,
			wantAppliedChecksum: successChecksum,
		},
		{
			name:      "checksum flow, checksum already applied and RV unchanged: no-op",
			planBytes: successPlanBytes,
			initialData: map[string][]byte{
				AppliedChecksumKey: []byte(successChecksum),
			},
			hasRunOnce:          true,
			lastAppliedRV:       "42",
			wantEnqueueAfter:    true,
			wantAppliedChecksum: successChecksum,
		},
		{
			name:      "checksum flow, checksum changed: re-applies",
			planBytes: successPlanBytes,
			initialData: map[string][]byte{
				AppliedChecksumKey: []byte("stale-checksum"),
			},
			hasRunOnce:          true,
			lastAppliedRV:       "42",
			wantEnqueueAfter:    true,
			wantAppliedChecksum: successChecksum,
		},
		{
			name:      "plan-state terminal (succeeded): monitors only",
			planBytes: successPlanBytes,
			initialData: map[string][]byte{
				planapi.PlanStateKey: []byte(planapi.PlanStateSucceeded),
			},
			hasRunOnce:       true,
			lastAppliedRV:    "42",
			wantEnqueueAfter: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sc := newMockSecretController(t)
			if tt.wantEnqueueAfter {
				sc.EXPECT().EnqueueAfter(testNamespace, testSecret, gomock.Any())
			}

			w := newTestWatcher(t, tt.hasRunOnce, tt.lastAppliedRV)
			data := map[string][]byte{PlanKey: tt.planBytes}
			for k, v := range tt.initialData {
				data[k] = v
			}
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testSecret, ResourceVersion: tt.lastAppliedRV},
				Data:       data,
			}

			result, err := w.reconcileSecret(context.Background(), sc, secret, 30*time.Second)
			if err != nil {
				t.Fatalf("reconcileSecret returned error: %v", err)
			}
			if tt.wantAppliedChecksum != "" && string(result.Data[AppliedChecksumKey]) != tt.wantAppliedChecksum {
				t.Errorf("expected applied checksum %q, got %q", tt.wantAppliedChecksum, result.Data[AppliedChecksumKey])
			}
		})
	}
}

func TestReconcileSecretNilSecretIsANoOp(t *testing.T) {
	t.Parallel()

	// No EXPECT() calls configured: a nil secret must return before touching the Kubernetes API.
	ctrl := gomock.NewController(t)
	sc := fake.NewMockControllerInterface[*corev1.Secret, *corev1.SecretList](ctrl)

	w := newTestWatcher(t, true, "")
	result, err := w.reconcileSecret(context.Background(), sc, nil, 30*time.Second)
	if err != nil {
		t.Fatalf("reconcileSecret returned error: %v", err)
	}
	if result != nil {
		t.Errorf("expected a nil result for a nil secret, got %+v", result)
	}
}

func TestReconcileSecretNoPlanDataEnqueuesAndReturns(t *testing.T) {
	t.Parallel()

	sc := newMockSecretController(t)
	sc.EXPECT().EnqueueAfter(testNamespace, testSecret, gomock.Any())

	w := newTestWatcher(t, true, "42")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testSecret, ResourceVersion: "42"},
		Data:       map[string][]byte{},
	}

	result, err := w.reconcileSecret(context.Background(), sc, secret, 30*time.Second)
	if err != nil {
		t.Fatalf("reconcileSecret returned error: %v", err)
	}
	if result == nil {
		t.Fatal("expected the secret to be returned unchanged, got nil")
	}
}

func TestReconcileSecretInvalidPlanJSONReturnsError(t *testing.T) {
	t.Parallel()

	// No EXPECT() calls configured: a CalculatePlan failure must return before any API call.
	ctrl := gomock.NewController(t)
	sc := fake.NewMockControllerInterface[*corev1.Secret, *corev1.SecretList](ctrl)

	w := newTestWatcher(t, true, "42")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testSecret, ResourceVersion: "42"},
		Data:       map[string][]byte{PlanKey: []byte("not valid json")},
	}

	if _, err := w.reconcileSecret(context.Background(), sc, secret, 30*time.Second); err == nil {
		t.Fatal("expected an error for unparsable plan JSON, got nil")
	}
}

func TestReconcileSecretPlanStateFirstObservationSetsHasRunOnce(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}
	t.Parallel()

	planBytes, _ := marshalPlan(t, planapi.Plan{
		OneTimeInstructions: []planapi.OneTimeInstruction{
			{CommonInstruction: planapi.CommonInstruction{Name: "ok", Command: "sh", Args: []string{"-c", "true"}}, SaveOutput: true},
		},
	})

	sc := newMockSecretController(t)
	sc.EXPECT().EnqueueAfter(testNamespace, testSecret, gomock.Any())

	w := newTestWatcher(t, false, "42") // hasRunOnce starts false, unlike the other plan-state cases
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testSecret, ResourceVersion: "42"},
		Data: map[string][]byte{
			PlanKey:              planBytes,
			planapi.PlanStateKey: []byte(planapi.PlanStateSucceeded),
		},
	}

	if _, err := w.reconcileSecret(context.Background(), sc, secret, 30*time.Second); err != nil {
		t.Fatalf("reconcileSecret returned error: %v", err)
	}
	if !w.hasRunOnce {
		t.Error("expected hasRunOnce to become true after observing any plan-state, even a terminal one")
	}
}

func TestReconcileSecretPendingCommitFailurePropagatesError(t *testing.T) {
	t.Parallel()

	planBytes, _ := marshalPlan(t, planapi.Plan{
		OneTimeInstructions: []planapi.OneTimeInstruction{
			{CommonInstruction: planapi.CommonInstruction{Name: "ok", Command: "sh", Args: []string{"-c", "true"}}, SaveOutput: true},
		},
	})

	ctrl := gomock.NewController(t)
	sc := fake.NewMockControllerInterface[*corev1.Secret, *corev1.SecretList](ctrl)
	// A non-conflict Update error on the pending -> in-progress commit propagates immediately;
	// the retry/merge machinery in updateSecret only special-cases conflicts, and
	// retry.DefaultBackoff also retries plain errors, so this mock keeps returning the same
	// error until the backoff is exhausted and the original error surfaces.
	sc.EXPECT().Update(gomock.Any()).Return(nil, errors.New("etcd is unavailable")).AnyTimes()

	w := newTestWatcher(t, true, "42")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testSecret, ResourceVersion: "42"},
		Data: map[string][]byte{
			PlanKey:              planBytes,
			planapi.PlanStateKey: []byte(planapi.PlanStatePending),
		},
	}

	if _, err := w.reconcileSecret(context.Background(), sc, secret, 30*time.Second); err == nil {
		t.Fatal("expected an error when the pending -> in-progress commit fails, got nil")
	}
}

func TestReconcileSecretSteadyStateSkipsUpdate(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}
	t.Parallel()

	planBytes, _ := marshalPlan(t, planapi.Plan{
		OneTimeInstructions: []planapi.OneTimeInstruction{
			{CommonInstruction: planapi.CommonInstruction{Name: "ok", Command: "sh", Args: []string{"-c", "true"}}, SaveOutput: true},
		},
	})

	w := newTestWatcher(t, false, "")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testSecret, ResourceVersion: "42"},
		Data:       map[string][]byte{PlanKey: planBytes},
	}

	// First reconcile force-applies (first start) and establishes steady-state secret data —
	// exact gzip-encoded byte values (periodic output, one-time output) are an implementation
	// detail of Applyinator, not hand-computed here.
	sc1 := newMockSecretController(t)
	sc1.EXPECT().EnqueueAfter(testNamespace, testSecret, gomock.Any())
	settled, err := w.reconcileSecret(context.Background(), sc1, secret, 30*time.Second)
	if err != nil {
		t.Fatalf("first reconcileSecret returned error: %v", err)
	}

	// Second reconcile against the exact same (now steady-state) data and an unchanged resource
	// version: nothing should differ, so reconcileSecret must skip the Update call entirely. No
	// Update EXPECT() is configured on this mock, so an unexpected call fails the test.
	ctrl := gomock.NewController(t)
	sc2 := fake.NewMockControllerInterface[*corev1.Secret, *corev1.SecretList](ctrl)
	sc2.EXPECT().EnqueueAfter(testNamespace, testSecret, gomock.Any())

	result, err := w.reconcileSecret(context.Background(), sc2, settled.DeepCopy(), 30*time.Second)
	if err != nil {
		t.Fatalf("second reconcileSecret returned error: %v", err)
	}
	if result.ResourceVersion != settled.ResourceVersion {
		t.Errorf("expected the unchanged secret to be returned as-is, got resource version %q", result.ResourceVersion)
	}
}

func TestReconcileSecretFailureCooldownActive(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}
	t.Parallel()

	planBytes, checksum := marshalPlan(t, planapi.Plan{
		OneTimeInstructions: []planapi.OneTimeInstruction{
			{CommonInstruction: planapi.CommonInstruction{Name: "bad", Command: "sh", Args: []string{"-c", "false"}}, SaveOutput: true},
		},
	})

	sc := newMockSecretController(t)
	sc.EXPECT().EnqueueAfter(testNamespace, testSecret, gomock.Any())

	w := newTestWatcher(t, true, "42")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testSecret, ResourceVersion: "42"},
		Data: map[string][]byte{
			PlanKey:            planBytes,
			AppliedChecksumKey: []byte(""),
			FailedChecksumKey:  []byte(checksum),
			FailureCountKey:    []byte("1"),
			LastApplyTimeKey:   []byte(time.Now().Format(time.UnixDate)),
		},
	}

	result, err := w.reconcileSecret(context.Background(), sc, secret, 30*time.Second)
	if err != nil {
		t.Fatalf("reconcileSecret returned error: %v", err)
	}
	if string(result.Data[FailedChecksumKey]) != checksum {
		t.Errorf("expected failed checksum to remain %q, got %q", checksum, result.Data[FailedChecksumKey])
	}
	if string(result.Data[FailureCountKey]) != "1" {
		t.Errorf("expected failure count to stay at 1 (no re-apply attempted), got %q", result.Data[FailureCountKey])
	}
}

func TestReconcileSecretFailureCooldownElapsedRetries(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}
	t.Parallel()

	planBytes, checksum := marshalPlan(t, planapi.Plan{
		OneTimeInstructions: []planapi.OneTimeInstruction{
			{CommonInstruction: planapi.CommonInstruction{Name: "bad", Command: "sh", Args: []string{"-c", "false"}}, SaveOutput: true},
		},
	})

	sc := newMockSecretController(t)
	sc.EXPECT().EnqueueAfter(testNamespace, testSecret, gomock.Any()).AnyTimes()

	w := newTestWatcher(t, true, "42")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testSecret, ResourceVersion: "42"},
		Data: map[string][]byte{
			PlanKey:            planBytes,
			AppliedChecksumKey: []byte(""),
			FailedChecksumKey:  []byte(checksum),
			FailureCountKey:    []byte("1"),
			LastApplyTimeKey:   []byte(time.Now().Add(-time.Hour).Format(time.UnixDate)),
		},
	}

	result, err := w.reconcileSecret(context.Background(), sc, secret, 30*time.Second)
	if err != nil {
		t.Fatalf("reconcileSecret returned error: %v", err)
	}
	if string(result.Data[FailureCountKey]) != "2" {
		t.Errorf("expected failure count to increment to 2 (retry attempted and failed again), got %q", result.Data[FailureCountKey])
	}
}

func TestReconcileSecretMaxFailureThresholdExceeded(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}
	t.Parallel()

	planBytes, checksum := marshalPlan(t, planapi.Plan{
		OneTimeInstructions: []planapi.OneTimeInstruction{
			{CommonInstruction: planapi.CommonInstruction{Name: "bad", Command: "sh", Args: []string{"-c", "false"}}, SaveOutput: true},
		},
	})

	sc := newMockSecretController(t)
	sc.EXPECT().EnqueueAfter(testNamespace, testSecret, gomock.Any())

	w := newTestWatcher(t, true, "42")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testSecret, ResourceVersion: "42"},
		Data: map[string][]byte{
			PlanKey:            planBytes,
			AppliedChecksumKey: []byte(""),
			FailedChecksumKey:  []byte(checksum),
			FailureCountKey:    []byte("3"),
			MaxFailuresKey:     []byte("3"),
			LastApplyTimeKey:   []byte(time.Now().Add(-time.Hour).Format(time.UnixDate)),
		},
	}

	result, err := w.reconcileSecret(context.Background(), sc, secret, 30*time.Second)
	if err != nil {
		t.Fatalf("reconcileSecret returned error: %v", err)
	}
	if string(result.Data[FailureCountKey]) != "3" {
		t.Errorf("expected failure count to stay at 3 (threshold exceeded, no retry attempted), got %q", result.Data[FailureCountKey])
	}
}

func TestReconcileSecretUIDChangeResetsState(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}
	t.Parallel()

	planBytes, checksum := marshalPlan(t, planapi.Plan{
		OneTimeInstructions: []planapi.OneTimeInstruction{
			{CommonInstruction: planapi.CommonInstruction{Name: "ok", Command: "sh", Args: []string{"-c", "true"}}, SaveOutput: true},
		},
	})

	sc := newMockSecretController(t)
	sc.EXPECT().EnqueueAfter(testNamespace, testSecret, gomock.Any())

	w := newTestWatcher(t, true, "100")
	w.secretUID = "old-uid"
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testSecret, ResourceVersion: "1", UID: "new-uid"},
		Data: map[string][]byte{
			PlanKey:            planBytes,
			AppliedChecksumKey: []byte(checksum), // would be a no-op if the UID reset didn't force a re-apply
		},
	}

	result, err := w.reconcileSecret(context.Background(), sc, secret, 30*time.Second)
	if err != nil {
		t.Fatalf("reconcileSecret returned error: %v", err)
	}
	// secretUID is reset to "" the moment the UID change is detected, but updateSecret's success
	// path re-populates it from the freshly-updated secret (see watcher.go's `if w.secretUID ==
	// "" { w.secretUID = string(resultingSecret.UID) }`) — so by the time reconcileSecret returns,
	// it has already been re-learned as the new UID, not left empty.
	if w.secretUID != "new-uid" {
		t.Errorf("expected secretUID to be re-learned as %q after the successful update, got %q", "new-uid", w.secretUID)
	}
	if string(result.Data[AppliedChecksumKey]) != checksum {
		t.Errorf("expected the plan to be force-re-applied despite matching checksum, got applied checksum %q", result.Data[AppliedChecksumKey])
	}
}

func TestReconcileSecretStaleResourceVersionRejected(t *testing.T) {
	t.Parallel()

	// No EXPECT() calls are configured: the mock fails the test if reconcileSecret makes any
	// Kubernetes API call at all, proving the stale-RV path returns before doing any work.
	ctrl := gomock.NewController(t)
	sc := fake.NewMockControllerInterface[*corev1.Secret, *corev1.SecretList](ctrl)

	w := newTestWatcher(t, true, "100")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testSecret, ResourceVersion: "1"},
		Data:       map[string][]byte{},
	}

	if _, err := w.reconcileSecret(context.Background(), sc, secret, 30*time.Second); err == nil {
		t.Fatal("expected an error for a stale resource version, got nil")
	}
}

func TestReconcileSecretPendingTransitionsThroughInProgress(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}
	t.Parallel()

	planBytes, _ := marshalPlan(t, planapi.Plan{
		OneTimeInstructions: []planapi.OneTimeInstruction{
			{CommonInstruction: planapi.CommonInstruction{Name: "ok", Command: "sh", Args: []string{"-c", "true"}}, SaveOutput: true},
		},
	})

	sc := newMockSecretController(t)
	sc.EXPECT().EnqueueAfter(testNamespace, testSecret, gomock.Any())

	w := newTestWatcher(t, true, "42")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testSecret, ResourceVersion: "42"},
		Data: map[string][]byte{
			PlanKey:              planBytes,
			planapi.PlanStateKey: []byte(planapi.PlanStatePending),
		},
	}

	result, err := w.reconcileSecret(context.Background(), sc, secret, 30*time.Second)
	if err != nil {
		t.Fatalf("reconcileSecret returned error: %v", err)
	}
	if planapi.PlanState(result.Data[planapi.PlanStateKey]) != planapi.PlanStateSucceeded {
		t.Errorf("expected final plan-state %q, got %q", planapi.PlanStateSucceeded, result.Data[planapi.PlanStateKey])
	}
	// The exact Update call count and the in-progress-before-Apply ordering are asserted in
	// TestReconcileSecretCommitsInProgressBeforeApply.
}

func TestReconcileSecretUpdateConflictRetry(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}
	t.Parallel()

	planBytes, checksum := marshalPlan(t, planapi.Plan{
		OneTimeInstructions: []planapi.OneTimeInstruction{
			{CommonInstruction: planapi.CommonInstruction{Name: "ok", Command: "sh", Args: []string{"-c", "true"}}, SaveOutput: true},
		},
	})

	ctrl := gomock.NewController(t)
	sc := fake.NewMockControllerInterface[*corev1.Secret, *corev1.SecretList](ctrl)
	sc.EXPECT().EnqueueAfter(testNamespace, testSecret, gomock.Any())

	firstAttempt := true
	sc.EXPECT().Update(gomock.Any()).DoAndReturn(func(s *corev1.Secret) (*corev1.Secret, error) {
		if firstAttempt {
			firstAttempt = false
			return nil, apierrors.NewConflict(corev1.Resource("secrets"), s.Name, errors.New("conflict"))
		}
		return s, nil
	}).Times(2)
	sc.EXPECT().Get(testNamespace, testSecret, gomock.Any()).Return(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testSecret, ResourceVersion: "43"},
		Data: map[string][]byte{
			PlanKey:            planBytes,
			AppliedChecksumKey: []byte(checksum),
		},
	}, nil)

	w := newTestWatcher(t, false, "")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testSecret, ResourceVersion: "42"},
		Data:       map[string][]byte{PlanKey: planBytes},
	}

	result, err := w.reconcileSecret(context.Background(), sc, secret, 30*time.Second)
	if err != nil {
		t.Fatalf("reconcileSecret returned error: %v", err)
	}
	if result.ResourceVersion != "43" {
		t.Errorf("expected the retried update to return the latest secret (resource version 43), got %q", result.ResourceVersion)
	}
}

// TestReconcileSecretCommitsInProgressBeforeApply verifies in-progress commit ordering.
// Ensure pending -> in-progress and plan-revision reach the API server before Apply runs.
// This write lets a crashed agent re-execute the plan on restart.
func TestReconcileSecretCommitsInProgressBeforeApply(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}
	t.Parallel()

	// The instruction records when Apply ran by touching a marker file, so ordering can be
	// asserted against the observed Update calls rather than inferred.
	markerDir := t.TempDir()
	marker := filepath.Join(markerDir, "apply-ran")
	planBytes, _ := marshalPlan(t, planapi.Plan{
		OneTimeInstructions: []planapi.OneTimeInstruction{
			{CommonInstruction: planapi.CommonInstruction{Name: "marker", Command: "sh", Args: []string{"-c", "touch " + marker}}},
		},
	})

	ctrl := gomock.NewController(t)
	sc := fake.NewMockControllerInterface[*corev1.Secret, *corev1.SecretList](ctrl)
	sc.EXPECT().EnqueueAfter(testNamespace, testSecret, gomock.Any()).AnyTimes()

	type observedUpdate struct {
		planState    string
		planRevision string
		applyHadRun  bool
	}
	var observed []observedUpdate
	sc.EXPECT().Update(gomock.Any()).DoAndReturn(func(s *corev1.Secret) (*corev1.Secret, error) {
		_, statErr := os.Stat(marker)
		observed = append(observed, observedUpdate{
			planState:    string(s.Data[planapi.PlanStateKey]),
			planRevision: string(s.Data[planapi.PlanRevisionKey]),
			applyHadRun:  statErr == nil,
		})
		return s, nil
	}).AnyTimes()

	w := newTestWatcher(t, false, "")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testSecret, ResourceVersion: "42"},
		Data: map[string][]byte{
			PlanKey:                 planBytes,
			planapi.PlanStateKey:    []byte(planapi.PlanStatePending),
			planapi.PlanRevisionKey: []byte("7"),
		},
	}

	if _, err := w.reconcileSecret(context.Background(), sc, secret, 30*time.Second); err != nil {
		t.Fatalf("reconcileSecret returned error: %v", err)
	}

	if len(observed) != 2 {
		t.Fatalf("expected exactly 2 Update calls (in-progress, then the outcome), got %d: %+v", len(observed), observed)
	}
	first := observed[0]
	if first.planState != string(planapi.PlanStateInProgress) {
		t.Errorf("expected the first Update to commit plan-state %q, got %q", planapi.PlanStateInProgress, first.planState)
	}
	if first.applyHadRun {
		t.Error("expected the in-progress write to reach the API server BEFORE Apply ran; crash recovery depends on this ordering")
	}
	if first.planRevision != "8" {
		t.Errorf("expected plan-revision to be incremented to 8 in the same write as in-progress, got %q", first.planRevision)
	}
	if observed[1].planState != string(planapi.PlanStateSucceeded) {
		t.Errorf("expected the second Update to commit the outcome %q, got %q", planapi.PlanStateSucceeded, observed[1].planState)
	}
	if !observed[1].applyHadRun {
		t.Error("expected the outcome write to happen after Apply ran")
	}
}

// TestReconcileSecretInProgressOnStartupReExecutes covers the crash-recovery entry point: an agent
// that restarts and finds plan-state already in-progress must re-execute the plan rather than
// treating it as terminal.
func TestReconcileSecretInProgressOnStartupReExecutes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}
	t.Parallel()

	marker := filepath.Join(t.TempDir(), "re-executed")
	planBytes, _ := marshalPlan(t, planapi.Plan{
		OneTimeInstructions: []planapi.OneTimeInstruction{
			{CommonInstruction: planapi.CommonInstruction{Name: "marker", Command: "sh", Args: []string{"-c", "touch " + marker}}},
		},
	})

	sc := newMockSecretController(t)
	sc.EXPECT().EnqueueAfter(testNamespace, testSecret, gomock.Any()).AnyTimes()

	w := newTestWatcher(t, false, "")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testSecret, ResourceVersion: "42"},
		Data: map[string][]byte{
			PlanKey:              planBytes,
			planapi.PlanStateKey: []byte(planapi.PlanStateInProgress),
		},
	}

	result, err := w.reconcileSecret(context.Background(), sc, secret, 30*time.Second)
	if err != nil {
		t.Fatalf("reconcileSecret returned error: %v", err)
	}
	if _, statErr := os.Stat(marker); statErr != nil {
		t.Errorf("expected the plan to be re-executed on an in-progress startup, marker missing: %v", statErr)
	}
	if planapi.PlanState(result.Data[planapi.PlanStateKey]) != planapi.PlanStateSucceeded {
		t.Errorf("expected final plan-state %q, got %q", planapi.PlanStateSucceeded, result.Data[planapi.PlanStateKey])
	}
}

// --- interrupt wiring helpers -------------------------------------------------------------------

// interruptRecorder models the API server for the interrupt paths. writeInterruptOutcome does a
// fresh Get before every write and verifies the Secret still carries the plan, so a mock that
// served a fixed Secret would hide the read half of that read-modify-write; this one serves back
// whatever was last written.
type interruptRecorder struct {
	mu       sync.Mutex
	server   *corev1.Secret
	updates  []*corev1.Secret
	enqueued []time.Duration
}

func newInterruptRecorder(secret *corev1.Secret) *interruptRecorder {
	return &interruptRecorder{server: secret.DeepCopy()}
}

func (r *interruptRecorder) get() *corev1.Secret {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.server.DeepCopy()
}

func (r *interruptRecorder) update(s *corev1.Secret) *corev1.Secret {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.updates = append(r.updates, s.DeepCopy())
	r.server = s.DeepCopy()
	return s
}

func (r *interruptRecorder) enqueue(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.enqueued = append(r.enqueued, d)
}

// writes returns the Secrets handed to each Update call, in order.
func (r *interruptRecorder) writes() []*corev1.Secret {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*corev1.Secret(nil), r.updates...)
}

func (r *interruptRecorder) enqueuePeriods() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Duration(nil), r.enqueued...)
}

// newInterruptTestController wires a SecretController backed by r.
//
// Cache() is deliberately left unstubbed: reconcileSecret reaches the informer cache only through
// startInterruptWatch, so any test that does not explicitly opt in fails outright if an interrupt
// watch is started. That is what makes "the checksum flow starts no interrupt channels" an
// assertion rather than a claim.
func newInterruptTestController(t *testing.T, r *interruptRecorder) *fake.MockControllerInterface[*corev1.Secret, *corev1.SecretList] {
	t.Helper()
	return newInterruptTestControllerWithHook(t, r, nil)
}

// newInterruptTestControllerWithHook is newInterruptTestController with a hook invoked on every
// Update, so a test can record what had already happened by the time each write reached the API
// server.
func newInterruptTestControllerWithHook(t *testing.T, r *interruptRecorder, onUpdate func(*corev1.Secret),
) *fake.MockControllerInterface[*corev1.Secret, *corev1.SecretList] {
	t.Helper()

	ctrl := gomock.NewController(t)
	sc := fake.NewMockControllerInterface[*corev1.Secret, *corev1.SecretList](ctrl)
	sc.EXPECT().Get(testNamespace, testSecret, gomock.Any()).DoAndReturn(
		func(string, string, metav1.GetOptions) (*corev1.Secret, error) { return r.get(), nil },
	).AnyTimes()
	sc.EXPECT().Update(gomock.Any()).DoAndReturn(func(s *corev1.Secret) (*corev1.Secret, error) {
		if onUpdate != nil {
			onUpdate(s)
		}
		return r.update(s), nil
	}).AnyTimes()
	sc.EXPECT().EnqueueAfter(testNamespace, testSecret, gomock.Any()).Do(
		func(_, _ string, d time.Duration) { r.enqueue(d) },
	).AnyTimes()
	return sc
}

// newInterruptTestSecret builds the plan Secret handed to reconcileSecret.
func newInterruptTestSecret(planBytes []byte, annotations map[string]string, data map[string][]byte) *corev1.Secret {
	full := map[string][]byte{PlanKey: planBytes}
	maps.Copy(full, data)
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       testNamespace,
			Name:            testSecret,
			ResourceVersion: "42",
			Annotations:     annotations,
		},
		Data: full,
	}
}

// checkpointIn decodes the resume checkpoint out of a Secret data map.
func checkpointIn(t *testing.T, data map[string][]byte) planProgress {
	t.Helper()
	raw, ok := data[PlanProgressKey]
	if !ok {
		t.Fatalf("expected a %q checkpoint, got only the keys %v", PlanProgressKey, keysOf(data))
	}
	var p planProgress
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("failed to decode the %q checkpoint %q: %v", PlanProgressKey, raw, err)
	}
	return p
}

// touchInstruction returns a one-time instruction that creates sentinel when it runs, so a test
// can assert whether the plan executed at all.
func touchInstruction(name, sentinel string) planapi.OneTimeInstruction {
	return planapi.OneTimeInstruction{
		CommonInstruction: planapi.CommonInstruction{Name: name, Command: "sh", Args: []string{"-c", "touch " + sentinel}},
		SaveOutput:        true,
	}
}

// gatedTouchInstruction is touchInstruction that then blocks until gate exists. It is how a test
// holds an apply open at a known point, long enough for the interrupt watch to observe an
// annotation, without depending on any timing.
func gatedTouchInstruction(name, sentinel, gate string) planapi.OneTimeInstruction {
	return planapi.OneTimeInstruction{
		CommonInstruction: planapi.CommonInstruction{
			Name:    name,
			Command: "sh",
			Args:    []string{"-c", "touch " + sentinel + "; while [ ! -f " + gate + " ]; do sleep 0.02; done"},
		},
		SaveOutput: true,
	}
}

func assertPathAbsent(t *testing.T, path, why string) {
	t.Helper()
	if _, err := os.Stat(path); err == nil {
		t.Errorf("expected %s not to exist: %s", path, why)
	}
}

// syncBuffer is an io.Writer safe to read while logrus is writing to it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureLogs redirects logrus's standard logger into a buffer for the duration of the test and
// returns a func reading everything written so far.
//
// The t.Setenv call sets nothing anybody reads: it is a tripwire, exactly as in
// withInterruptPollInterval. t.Setenv panics if the test, or any ancestor of it, has called
// t.Parallel() — which is precisely the condition under which swapping the process-wide logger
// output would interleave with, and steal output from, every other test in the package.
func captureLogs(t *testing.T) func() string {
	t.Helper()
	t.Setenv("K8SPLAN_LOG_CAPTURE_GUARD", "1")

	buf := &syncBuffer{}
	original := logrus.StandardLogger().Out
	logrus.SetOutput(buf)
	t.Cleanup(func() { logrus.SetOutput(original) })
	return buf.String
}

// --- interrupt wiring tests ---------------------------------------------------------------------

// TestReconcileSecretInterruptAtEntrySuppressesTheApply is the wiring half of the suppression
// invariant: in the plan-state flow an interrupt annotation is read before any decision to execute
// is taken, so the plan is recorded as held and nothing runs. The sentinel file is the assertion
// that matters — a plan-state of "paused" written by an agent that ran the plan anyway would be
// worse than no feature at all. Task 6 owns the exhaustive matrix.
func TestReconcileSecretInterruptAtEntrySuppressesTheApply(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}
	t.Parallel()

	tests := []struct {
		name            string
		annotation      string
		planState       planapi.PlanState
		wantState       planapi.PlanState
		wantPaused      bool
		wantResumeState planapi.PlanState
	}{
		{
			name:       "cancel on in-progress records a terminal cancellation and a report, not a suspension",
			annotation: PlanCancelledAnnotation,
			planState:  planapi.PlanStateInProgress,
			wantState:  planapi.PlanStateCancelled,
			wantPaused: false,
		},
		{
			name:            "pause on pending records a suspension that resumes into pending",
			annotation:      PlanPausedAnnotation,
			planState:       planapi.PlanStatePending,
			wantState:       PlanStatePaused,
			wantPaused:      true,
			wantResumeState: planapi.PlanStatePending,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sentinel := filepath.Join(t.TempDir(), "plan-ran")
			planBytes, checksum := marshalPlan(t, planapi.Plan{
				OneTimeInstructions: []planapi.OneTimeInstruction{touchInstruction("ran", sentinel)},
			})

			secret := newInterruptTestSecret(planBytes,
				map[string]string{tt.annotation: "true"},
				map[string][]byte{planapi.PlanStateKey: []byte(tt.planState)})
			rec := newInterruptRecorder(secret)
			sc := newInterruptTestController(t, rec)

			w := newTestWatcher(t, false, "42")
			result, err := w.reconcileSecret(context.Background(), sc, secret, 30*time.Second)
			if err != nil {
				t.Fatalf("reconcileSecret returned error: %v", err)
			}

			assertPathAbsent(t, sentinel, "an interrupt observed at reconcile entry must suppress the apply entirely")
			if w.hasRunOnce {
				t.Error("expected the interrupt entry path not to set hasRunOnce; it returns above the decision that owns that flag, " +
					"and mutable state must not live on the far side of the safety returns")
			}
			if planapi.PlanState(result.Data[planapi.PlanStateKey]) != tt.wantState {
				t.Errorf("expected plan-state %q, got %q", tt.wantState, result.Data[planapi.PlanStateKey])
			}
			if len(result.Data[AppliedChecksumKey]) != 0 {
				t.Errorf("expected applied-checksum not to be written for a plan that never ran, got %q", result.Data[AppliedChecksumKey])
			}

			got := checkpointIn(t, result.Data)
			if got.Paused != tt.wantPaused {
				t.Errorf("expected checkpoint Paused=%v, got %+v", tt.wantPaused, got)
			}
			if got.ResumeState != tt.wantResumeState {
				t.Errorf("expected checkpoint ResumeState %q, got %+v", tt.wantResumeState, got)
			}
			if got.Checksum != checksum || got.Total != 1 {
				t.Errorf("expected the checkpoint to be scoped to checksum %q with Total 1, got %+v", checksum, got)
			}

			if len(rec.writes()) != 1 {
				t.Errorf("expected exactly one Update (the interrupt outcome), got %d", len(rec.writes()))
			}
			if periods := rec.enqueuePeriods(); len(periods) != 1 || periods[0] != interruptedEnqueuePeriod {
				t.Errorf("expected a single re-enqueue after %v, got %v", interruptedEnqueuePeriod, periods)
			}
		})
	}
}

// TestReconcileSecretInvalidAnnotationValueWritesNothing pins the narrowest branch in the
// reconcile. An unreadable annotation executes nothing, interrupts nothing and writes nothing, so
// resourceVersion is stable for as long as the error persists and the error cannot amplify into a
// write loop. It deliberately does not record plan-state: failed — "failed" is a claim about the
// node, and nothing ran.
//
// The resourceVersion assertion is what makes "writes nothing" checked rather than inferred from a
// call count.
func TestReconcileSecretInvalidAnnotationValueWritesNothing(t *testing.T) {
	t.Parallel()

	planBytes, _ := marshalPlan(t, planapi.Plan{
		OneTimeInstructions: []planapi.OneTimeInstruction{
			{CommonInstruction: planapi.CommonInstruction{Name: "ok", Command: "sh", Args: []string{"-c", "true"}}},
		},
	})

	secret := newInterruptTestSecret(planBytes,
		map[string]string{PlanPausedAnnotation: "yes"},
		map[string][]byte{planapi.PlanStateKey: []byte(planapi.PlanStateInProgress)})
	rec := newInterruptRecorder(secret)
	sc := newInterruptTestController(t, rec)

	w := newTestWatcher(t, true, "42")
	result, err := w.reconcileSecret(context.Background(), sc, secret, 30*time.Second)
	if err == nil {
		t.Fatal("expected an error for an uninterpretable annotation value, got nil")
	}
	if len(rec.writes()) != 0 {
		t.Errorf("expected zero Update calls, got %d", len(rec.writes()))
	}
	if periods := rec.enqueuePeriods(); len(periods) != 0 {
		t.Errorf("expected no re-enqueue (the workqueue's rate limiter owns the retry), got %v", periods)
	}
	if result.ResourceVersion != "42" {
		t.Errorf("expected the returned secret's resource version to be byte-identical to the input's (%q), got %q", "42", result.ResourceVersion)
	}
}

// TestReconcileSecretInterruptStillRunsProbes pins that an interrupt suppresses execution but
// never observation. Freezing probe statuses would feed stale health data to Rancher's
// MachineHealthCheck on exactly the nodes most likely to be unhealthy: a plan stopped mid-flight
// leaves the node in a partial state.
func TestReconcileSecretInterruptStillRunsProbes(t *testing.T) {
	t.Parallel()

	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	planBytes, _ := marshalPlan(t, planapi.Plan{
		Probes: map[string]planapi.Probe{
			"health": {HTTPGetAction: planapi.HTTPGetAction{URL: server.URL, Insecure: true}},
		},
	})

	secret := newInterruptTestSecret(planBytes,
		map[string]string{PlanPausedAnnotation: "true"},
		map[string][]byte{planapi.PlanStateKey: []byte(planapi.PlanStateInProgress)})
	rec := newInterruptRecorder(secret)
	sc := newInterruptTestController(t, rec)

	w := newTestWatcher(t, true, "42")
	result, err := w.reconcileSecret(context.Background(), sc, secret, 30*time.Second)
	if err != nil {
		t.Fatalf("reconcileSecret returned error: %v", err)
	}

	if hits.Load() == 0 {
		t.Error("expected the probe to be executed while the plan was held")
	}
	writes := rec.writes()
	if len(writes) != 1 {
		t.Fatalf("expected exactly one Update, got %d", len(writes))
	}
	raw, ok := writes[0].Data[ProbeStatusesKey]
	if !ok {
		t.Fatalf("expected %q in the interrupt outcome write, got the keys %v", ProbeStatusesKey, keysOf(writes[0].Data))
	}
	var statuses map[string]planapi.ProbeStatus
	if err := json.Unmarshal(raw, &statuses); err != nil {
		t.Fatalf("failed to decode probe statuses %q: %v", raw, err)
	}
	if !statuses["health"].Healthy {
		t.Errorf("expected the probe status to be persisted as healthy, got %+v", statuses)
	}
	if planapi.PlanState(result.Data[planapi.PlanStateKey]) != PlanStatePaused {
		t.Errorf("expected the plan to still be recorded as %q, got %q", PlanStatePaused, result.Data[planapi.PlanStateKey])
	}
}

// TestReconcileSecretChecksumFlowIgnoresAnnotations pins the compatibility rule: legacy
// orchestrators that never write plan-state get ordinary checksum reconciliation, and the
// interrupt annotations have no effect there at all. Silently honouring them would give a
// half-working feature against a server that has no way to clear the resulting state.
//
// Not parallel: captureLogs swaps the process-wide logrus output.
func TestReconcileSecretChecksumFlowIgnoresAnnotations(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}
	logs := captureLogs(t)

	sentinel := filepath.Join(t.TempDir(), "plan-ran")
	planBytes, checksum := marshalPlan(t, planapi.Plan{
		OneTimeInstructions: []planapi.OneTimeInstruction{touchInstruction("ran", sentinel)},
	})

	// No plan-state key: the checksum flow. The annotation is set, and must change nothing.
	secret := newInterruptTestSecret(planBytes, map[string]string{PlanCancelledAnnotation: "true"}, nil)
	rec := newInterruptRecorder(secret)
	sc := newInterruptTestController(t, rec)

	w := newTestWatcher(t, false, "")
	result, err := w.reconcileSecret(context.Background(), sc, secret, 30*time.Second)
	if err != nil {
		t.Fatalf("reconcileSecret returned error: %v", err)
	}

	if _, statErr := os.Stat(sentinel); statErr != nil {
		t.Errorf("expected the plan to be applied under ordinary checksum semantics, sentinel missing: %v", statErr)
	}
	if string(result.Data[AppliedChecksumKey]) != checksum {
		t.Errorf("expected applied-checksum %q, got %q", checksum, result.Data[AppliedChecksumKey])
	}
	if len(result.Data[planapi.PlanStateKey]) != 0 {
		t.Errorf("expected the checksum flow never to write plan-state, got %q", result.Data[planapi.PlanStateKey])
	}
	// buildSecretDataUpdates clears the checkpoint unconditionally, so the key may be present as
	// an empty value; what must not exist is a checkpoint.
	if len(result.Data[PlanProgressKey]) != 0 {
		t.Errorf("expected the checksum flow never to write a resume checkpoint, got %q", result.Data[PlanProgressKey])
	}

	want := "ignoring unsupported annotation in checksum flow key=" + PlanCancelledAnnotation + " value=true"
	if !strings.Contains(logs(), want) {
		t.Errorf("expected a warning containing %q, got:\n%s", want, logs())
	}
}

// TestReconcileSecretChecksumFlowStartsNoInterruptWatch is the structural half of the rule above.
// The controller returned by newInterruptTestController does not stub Cache(), and
// startInterruptWatch is the only thing in reconcileSecret that reaches for it — so starting a
// watch in the checksum flow fails this test outright. That is what makes the interrupted-outcome
// path unreachable in the checksum flow rather than merely unlikely.
func TestReconcileSecretChecksumFlowStartsNoInterruptWatch(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}
	t.Parallel()

	sentinel := filepath.Join(t.TempDir(), "plan-ran")
	planBytes, checksum := marshalPlan(t, planapi.Plan{
		OneTimeInstructions: []planapi.OneTimeInstruction{touchInstruction("ran", sentinel)},
	})

	secret := newInterruptTestSecret(planBytes,
		map[string]string{PlanPausedAnnotation: "true", PlanCancelledAnnotation: "true"}, nil)
	rec := newInterruptRecorder(secret)
	sc := newInterruptTestController(t, rec)

	w := newTestWatcher(t, false, "")
	result, err := w.reconcileSecret(context.Background(), sc, secret, 30*time.Second)
	if err != nil {
		t.Fatalf("reconcileSecret returned error: %v", err)
	}
	if _, statErr := os.Stat(sentinel); statErr != nil {
		t.Errorf("expected the plan to be applied with both annotations present throughout, sentinel missing: %v", statErr)
	}
	if string(result.Data[AppliedChecksumKey]) != checksum {
		t.Errorf("expected applied-checksum %q, got %q", checksum, result.Data[AppliedChecksumKey])
	}
}

// TestReconcileSecretPauseDuringApplyIsNotRecordedAsAFailure pins the ordering Task 1 established:
// an interrupted apply reports OneTimeApplySucceeded: false alongside its Interruption, so the
// caller must test Interruption first. Routing this outcome through buildSecretDataUpdates would
// record plan-state: failed — and a failure count, and a failed-checksum — for a plan the operator
// stopped on purpose.
//
// Not parallel: it shortens the package-level interruptPollInterval.
func TestReconcileSecretPauseDuringApplyIsNotRecordedAsAFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}
	withInterruptPollInterval(t, 2*time.Millisecond)

	dir := t.TempDir()
	firstSentinel := filepath.Join(dir, "first-ran")
	secondSentinel := filepath.Join(dir, "second-ran")
	gate := filepath.Join(dir, "gate")

	planBytes, checksum := marshalPlan(t, planapi.Plan{
		OneTimeInstructions: []planapi.OneTimeInstruction{
			gatedTouchInstruction("first", firstSentinel, gate),
			touchInstruction("second", secondSentinel),
		},
	})

	// The input Secret carries no annotation: the pause must arrive mid-apply, through the
	// interrupt watch, not at reconcile entry.
	secret := newInterruptTestSecret(planBytes, nil, map[string][]byte{
		planapi.PlanStateKey: []byte(planapi.PlanStatePending),
	})
	rec := newInterruptRecorder(secret)
	sc := newInterruptTestController(t, rec)

	// The interrupt watch's view of the Secret. The annotation appears only once instruction 0 has
	// started, and the gate is released only on the poll *after* the one that served it — by which
	// point pollInterrupts has certainly closed the pause channel, since both polls run in the
	// same goroutine. So the pause lands strictly between the two instructions, with no timing
	// assumption and no race against Apply's pre-lock interruption check.
	var pollMu sync.Mutex
	var pollsServingThePause int
	cache := fake.NewMockCacheInterface[*corev1.Secret](gomock.NewController(t))
	sc.EXPECT().Cache().Return(cache).AnyTimes()
	cache.EXPECT().Get(testNamespace, testSecret).DoAndReturn(func(string, string) (*corev1.Secret, error) {
		pollMu.Lock()
		defer pollMu.Unlock()
		observed := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testSecret}}
		if _, err := os.Stat(firstSentinel); err != nil {
			return observed, nil // instruction 0 has not started yet
		}
		pollsServingThePause++
		if pollsServingThePause > 1 {
			if writeErr := os.WriteFile(gate, nil, 0600); writeErr != nil {
				t.Errorf("failed to release the gate: %v", writeErr)
			}
		}
		observed.Annotations = map[string]string{PlanPausedAnnotation: "true"}
		return observed, nil
	}).AnyTimes()

	w := newTestWatcher(t, true, "42")
	result, err := w.reconcileSecret(context.Background(), sc, secret, 30*time.Second)
	if err != nil {
		t.Fatalf("reconcileSecret returned error: %v", err)
	}

	if planapi.PlanState(result.Data[planapi.PlanStateKey]) != PlanStatePaused {
		t.Errorf("expected plan-state %q, got %q; an interrupted apply must not be reported as a plan failure",
			PlanStatePaused, result.Data[planapi.PlanStateKey])
	}
	if len(result.Data[AppliedChecksumKey]) != 0 {
		t.Errorf("expected applied-checksum NOT to be written for an unapplied plan, got %q", result.Data[AppliedChecksumKey])
	}
	if len(result.Data[AppliedOutputKey]) == 0 {
		t.Error("expected applied-output to be written so SaveOutput results from completed instructions survive the hold")
	}
	if len(result.Data[FailedChecksumKey]) != 0 || len(result.Data[FailureCountKey]) != 0 {
		t.Errorf("expected the failure bookkeeping to be untouched, got failed-checksum %q and failure-count %q",
			result.Data[FailedChecksumKey], result.Data[FailureCountKey])
	}
	assertPathAbsent(t, secondSentinel, "a pause stops the apply at the next instruction boundary")

	got := checkpointIn(t, result.Data)
	want := planProgress{Checksum: checksum, Completed: 1, Total: 2, ResumeState: planapi.PlanStateInProgress, Paused: true}
	if got != want {
		t.Errorf("expected checkpoint %+v, got %+v", want, got)
	}
	if periods := rec.enqueuePeriods(); len(periods) != 1 || periods[0] != interruptedEnqueuePeriod {
		t.Errorf("expected a single re-enqueue after %v, got %v", interruptedEnqueuePeriod, periods)
	}
}

// TestReconcileSecretResumeCommitLandsBeforeTheApply follows
// TestReconcileSecretCommitsInProgressBeforeApply's pattern for the same reason: ordering is the
// whole point. A plan that is executing must not report "paused" on the wire, so the write that
// clears the suspension has to reach the API server before Apply runs, not after it.
func TestReconcileSecretResumeCommitLandsBeforeTheApply(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}
	t.Parallel()

	marker := filepath.Join(t.TempDir(), "apply-ran")
	planBytes, checksum := marshalPlan(t, planapi.Plan{
		OneTimeInstructions: []planapi.OneTimeInstruction{touchInstruction("marker", marker)},
	})

	// A plan held at instruction 0, whose annotation the operator has just removed.
	secret := newInterruptTestSecret(planBytes, nil, map[string][]byte{
		planapi.PlanStateKey: []byte(PlanStatePaused),
		PlanProgressKey: marshalPlanProgress(planProgress{
			Checksum: checksum, Completed: 0, Total: 1, ResumeState: planapi.PlanStateInProgress, Paused: true,
		}),
	})

	type observedUpdate struct {
		planState   string
		paused      bool
		applyHadRun bool
	}
	var observed []observedUpdate
	rec := newInterruptRecorder(secret)
	sc := newInterruptTestControllerWithHook(t, rec, func(s *corev1.Secret) {
		_, statErr := os.Stat(marker)
		var p planProgress
		_ = json.Unmarshal(s.Data[PlanProgressKey], &p)
		observed = append(observed, observedUpdate{
			planState:   string(s.Data[planapi.PlanStateKey]),
			paused:      p.Paused,
			applyHadRun: statErr == nil,
		})
	})

	w := newTestWatcher(t, true, "42")
	result, err := w.reconcileSecret(context.Background(), sc, secret, 30*time.Second)
	if err != nil {
		t.Fatalf("reconcileSecret returned error: %v", err)
	}

	if len(observed) < 1 {
		t.Fatal("expected at least the resume commit to be written")
	}
	first := observed[0]
	if first.planState != string(planapi.PlanStateInProgress) {
		t.Errorf("expected the resume commit to write the resolved plan-state %q, got %q", planapi.PlanStateInProgress, first.planState)
	}
	if first.paused {
		t.Error("expected the resume commit to clear the checkpoint's Paused flag; a lingering flag would make a second pause a no-op")
	}
	if first.applyHadRun {
		t.Error("expected the resume commit to reach the API server BEFORE Apply ran; a plan that is executing must not report paused")
	}
	if _, statErr := os.Stat(marker); statErr != nil {
		t.Errorf("expected the resumed plan to actually be applied, marker missing: %v", statErr)
	}
	if planapi.PlanState(result.Data[planapi.PlanStateKey]) != planapi.PlanStateSucceeded {
		t.Errorf("expected the resumed plan to finish as %q, got %q", planapi.PlanStateSucceeded, result.Data[planapi.PlanStateKey])
	}
}

// TestReconcileSecretResumeCommitDoesNotBumpPlanRevision pins that leaving a suspension is not a
// new revision of the plan: the content has not changed, only the agent's permission to act on it.
// A bump here would look to Rancher like the orchestrator had delivered something new.
//
// The fixture resumes into a terminal state on purpose — that is the case where the resume commit
// is the only lifecycle write the reconcile makes, so a bump could not be blamed on anything else.
func TestReconcileSecretResumeCommitDoesNotBumpPlanRevision(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}
	t.Parallel()

	sentinel := filepath.Join(t.TempDir(), "plan-ran")
	planBytes, checksum := marshalPlan(t, planapi.Plan{
		OneTimeInstructions: []planapi.OneTimeInstruction{touchInstruction("ran", sentinel)},
	})

	// The likeliest operator action of all: pausing a node whose plan already succeeded and which
	// is only running periodic instructions, then unpausing it.
	secret := newInterruptTestSecret(planBytes, nil, map[string][]byte{
		planapi.PlanStateKey:    []byte(PlanStatePaused),
		planapi.PlanRevisionKey: []byte("7"),
		AppliedChecksumKey:      []byte(checksum),
		PlanProgressKey: marshalPlanProgress(planProgress{
			Checksum: checksum, Completed: 1, Total: 1, ResumeState: planapi.PlanStateSucceeded, Paused: true,
		}),
	})
	rec := newInterruptRecorder(secret)
	sc := newInterruptTestController(t, rec)

	w := newTestWatcher(t, true, "42")
	result, err := w.reconcileSecret(context.Background(), sc, secret, 30*time.Second)
	if err != nil {
		t.Fatalf("reconcileSecret returned error: %v", err)
	}

	writes := rec.writes()
	if len(writes) == 0 {
		t.Fatal("expected the resume commit to be written")
	}
	if got := string(writes[0].Data[planapi.PlanStateKey]); got != string(planapi.PlanStateSucceeded) {
		t.Errorf("expected the resume commit to restore plan-state %q, got %q", planapi.PlanStateSucceeded, got)
	}
	for i, write := range writes {
		if got := string(write.Data[planapi.PlanRevisionKey]); got != "7" {
			t.Errorf("expected plan-revision to stay at 7 in write %d, got %q", i, got)
		}
	}
	if got := string(result.Data[planapi.PlanRevisionKey]); got != "7" {
		t.Errorf("expected the resulting plan-revision to stay at 7, got %q", got)
	}
	assertPathAbsent(t, sentinel, "resuming into a terminal state monitors only; it must not re-execute the plan")
	if got := checkpointIn(t, writes[0].Data); got.Paused {
		t.Errorf("expected the resume commit to clear the checkpoint's Paused flag, got %+v", got)
	}
	// The terminal outcome write that follows clears the checkpoint outright — it has served its
	// purpose, and a stale one must not leak into a later run.
	if len(result.Data[PlanProgressKey]) != 0 {
		t.Errorf("expected the checkpoint to be cleared by the outcome write, got %q", result.Data[PlanProgressKey])
	}
}
