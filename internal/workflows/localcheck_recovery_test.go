package workflows_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
	k8s "github.com/ancyloce/anvilkit-agent-workflow/internal/adapters/kubernetes"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/bootstrap"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/workflows"
)

const testNamespace = "anvilkit-components"

// controlDouble is an in-memory stand-in for anvilkit.control.v1.ExecutionService
// with the reentry semantics the real one gives the LocalCheck commands: the
// same command identity returns the original record, a Pod is registered
// once, closes are recorded in order.
type controlDouble struct {
	mu        sync.Mutex
	instances map[string]activities.InstanceRef // by pod UID
	closes    []activities.CloseAttemptInput
}

func newControlDouble() *controlDouble {
	return &controlDouble{instances: map[string]activities.InstanceRef{}}
}

func (c *controlDouble) OpenAttempt(context.Context, activities.OpenAttemptInput) (activities.AttemptRef, error) {
	return attempt, nil
}

func (c *controlDouble) PrepareLaunch(context.Context, activities.PrepareLaunchInput) (activities.LaunchRef, error) {
	return launch, nil
}

func (c *controlDouble) RegisterInstance(_ context.Context, in activities.RegisterInstanceInput) (activities.InstanceRef, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ref, ok := c.instances[in.PodUID]; ok {
		return ref, nil
	}
	ref := activities.InstanceRef{InstanceID: "inst-" + in.PodUID, Current: len(c.instances) == 0}
	c.instances[in.PodUID] = ref
	return ref, nil
}

func (c *controlDouble) ObserveInstance(context.Context, string, string, activities.PodObservation) error {
	return nil
}

func (c *controlDouble) AcceptResult(context.Context, activities.AcceptResultInput) (activities.StageRef, error) {
	return activities.StageRef{StageID: "stg-1"}, nil
}

func (c *controlDouble) CloseAttempt(_ context.Context, in activities.CloseAttemptInput) (activities.CloseResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closes = append(c.closes, in)
	if in.Cleanup == "unknown" {
		return activities.CloseResult{Lifecycle: "reconciling"}, nil
	}
	return activities.CloseResult{Lifecycle: in.Outcome}, nil
}

func (c *controlDouble) closed() []activities.CloseAttemptInput {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]activities.CloseAttemptInput(nil), c.closes...)
}

func (c *controlDouble) registered() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.instances))
	for pod := range c.instances {
		out = append(out, pod)
	}
	return out
}

// inFlightCreateLauncher is the real fixed-template launcher on a fake API
// server whose create request never returns to the caller: the Job (and the
// Pod its controller runs) land on the backend when land is called, while
// the Activity only sees the canceled request. Everything else (observation,
// deletion, waiting for the Pods) is the real launcher.
type inFlightCreateLauncher struct {
	*k8s.Launcher
	cs       *fake.Clientset
	canceled <-chan struct{}
	creates  atomic.Int32
	landOnce sync.Once
	// landInRequest commits the create while the request is in flight;
	// otherwise the test commits it later through land.
	landInRequest bool
	t             *testing.T
}

func (l *inFlightCreateLauncher) CreateJob(ctx context.Context, in activities.CreateJobInput) (activities.JobRef, error) {
	l.creates.Add(1)
	if l.landInRequest {
		l.land(l.t)
	}
	<-l.canceled // the request is still in flight when the cancel arrives
	activity.RecordHeartbeat(ctx)
	<-ctx.Done()
	return activities.JobRef{}, ctx.Err()
}

// land commits the in-flight create (request 1) on the backend: the Job
// under the original launch key and the running Pod its controller creates.
func (l *inFlightCreateLauncher) land(t *testing.T) {
	t.Helper()
	l.landOnce.Do(func() { commitCreate(t, l.Launcher, l.cs, 1, "pod-uid-1") })
}

// commitCreate plays the API server committing create request n and the
// Job controller running its Pod.
func commitCreate(t *testing.T, l *k8s.Launcher, cs *fake.Clientset, request int, podUID string) {
	t.Helper()
	ref, err := l.CreateJob(context.Background(), activities.CreateJobInput{Launch: launch, ProfileID: "local-check-v1", Deadline: time.Now().Add(2 * time.Minute), Request: request})
	require.NoError(t, err)
	require.Equal(t, request, ref.Request, "the Job carries the marker of the request that committed it")
	runPod(t, cs, ref.JobUID, request, podUID)
}

// runPod plays the Job controller: the Pod of the Job carries the labels and
// the request marker of the Job's template.
func runPod(t *testing.T, cs *fake.Clientset, jobUID string, request int, podUID string) {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: launch.LaunchKey + "-" + podUID, Namespace: testNamespace, UID: types.UID(podUID),
			Labels:          map[string]string{"anvilkit.io/launch-key": launch.LaunchKey, "anvilkit.io/attempt-id": launch.AttemptID, "anvilkit.io/profile-id": "local-check-v1"},
			Annotations:     map[string]string{"anvilkit.io/create-request": strconv.Itoa(request)},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: launch.LaunchKey, UID: types.UID(jobUID), Controller: ptr.To(true)}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	_, err := cs.CoreV1().Pods(testNamespace).Create(context.Background(), pod, metav1.CreateOptions{})
	require.NoError(t, err)
}

// retriedCreateLauncher is the real launcher on a fake API server whose
// first create request (A) is written and never answered while the API
// server still holds it: the Activity sees only the unanswered request. The
// second request (B) runs for real and its Pod is played by the test; A
// lands when the test calls land, after B's Job may already be gone.
type retriedCreateLauncher struct {
	*k8s.Launcher
	cs       *fake.Clientset
	creates  atomic.Int32
	landOnce sync.Once
	t        *testing.T
}

func (l *retriedCreateLauncher) CreateJob(ctx context.Context, in activities.CreateJobInput) (activities.JobRef, error) {
	l.creates.Add(1)
	if in.Request == 1 {
		return activities.JobRef{}, errors.New("job lc-abc create request 1 unanswered: Post \"/apis/batch/v1/namespaces/anvilkit-components/jobs\": context deadline exceeded")
	}
	ref, err := l.Launcher.CreateJob(ctx, in)
	if err == nil {
		runPod(l.t, l.cs, ref.JobUID, in.Request, "pod-uid-b")
	}
	return ref, err
}

// land commits request A on the backend under the released launch key.
func (l *retriedCreateLauncher) land(t *testing.T) {
	t.Helper()
	l.landOnce.Do(func() { commitCreate(t, l.Launcher, l.cs, 1, "pod-uid-a") })
}

// garbageCollect plays the Job controller's foreground deletion on the fake
// API server: once the Job is gone its Pods disappear shortly after, as on
// a real cluster where the Pod lingers until it has stopped.
func garbageCollect(ctx context.Context, cs *fake.Clientset) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
		_, err := cs.BatchV1().Jobs(testNamespace).Get(ctx, launch.LaunchKey, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			continue
		}
		pods, err := cs.CoreV1().Pods(testNamespace).List(ctx, metav1.ListOptions{LabelSelector: "anvilkit.io/launch-key=" + launch.LaunchKey})
		if err != nil {
			continue
		}
		for _, p := range pods.Items {
			time.Sleep(200 * time.Millisecond)
			_ = cs.CoreV1().Pods(testNamespace).Delete(ctx, p.Name, metav1.DeleteOptions{})
		}
	}
}

func backendEmpty(t *testing.T, cs *fake.Clientset) bool {
	t.Helper()
	_, err := cs.BatchV1().Jobs(testNamespace).Get(context.Background(), launch.LaunchKey, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		return false
	}
	pods, err := cs.CoreV1().Pods(testNamespace).List(context.Background(), metav1.ListOptions{LabelSelector: "anvilkit.io/launch-key=" + launch.LaunchKey})
	require.NoError(t, err)
	return len(pods.Items) == 0
}

// recoveryEnv wires the real Workflow, the real Activities, the real
// launcher on a fake API server and the Control double into the Temporal
// test environment, with the DEVELOPMENT_ONLY lost-receipt interceptor of
// the worker dropping the first completion of every named Activity.
func recoveryEnv(t *testing.T, b workflows.Bounds, lose []string) (*testsuite.TestWorkflowEnvironment, *inFlightCreateLauncher, *controlDouble, *bytes.Buffer) {
	t.Helper()
	cs := fake.NewClientset()
	launcher := &inFlightCreateLauncher{Launcher: k8s.NewWithClient(cs, testNamespace), cs: cs, t: t}
	env, control, log := wireEnv(t, b, lose, cs, launcher)
	canceled := make(chan struct{})
	var once sync.Once
	env.SetOnActivityCanceledListener(func(*activity.Info) { once.Do(func() { close(canceled) }) })
	launcher.canceled = canceled
	return env, launcher, control, log
}

// wireEnv wires the real Workflow and Activities, the given launcher over
// the fake API server and the Control double into the Temporal test
// environment, with the lost-receipt interceptor and the Job controller's
// garbage collection played by the test.
func wireEnv(t *testing.T, b workflows.Bounds, lose []string, cs *fake.Clientset, launcher activities.Launcher) (*testsuite.TestWorkflowEnvironment, *controlDouble, *bytes.Buffer) {
	t.Helper()
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	control := newControlDouble()
	acts := &activities.Activities{Control: control, Launcher: launcher, Observer: "test-observer"}
	log := &bytes.Buffer{}
	env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{
		bootstrap.NewLoseReceiptOnce(lose, slog.New(slog.NewJSONHandler(log, nil))),
	}})
	env.RegisterWorkflowWithOptions(workflows.LocalCheck(queues, b), workflow.RegisterOptions{Name: workflows.LocalCheckWorkflowName})
	for name, fn := range map[string]any{
		activities.NameOpenAttempt: acts.OpenAttempt, activities.NamePrepareLaunch: acts.PrepareLaunch, activities.NameCreateJob: acts.CreateJob,
		activities.NameObserveJob: acts.ObserveJob, activities.NameRegisterInstance: acts.RegisterInstance, activities.NameObserveInstance: acts.ObserveInstance,
		activities.NameVerifyResult: acts.VerifyResult, activities.NameAcceptResult: acts.AcceptResult,
		activities.NameObserveLaunch: acts.ObserveLaunch, activities.NameDeleteJob: acts.DeleteJob, activities.NameCloseAttempt: acts.CloseAttempt,
	} {
		env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go garbageCollect(ctx, cs)
	return env, control, log
}

// recoveryBounds narrow the cleanup and reconciliation pacing so a run that
// never converges reaches its bound within the test budget; the settle
// window is real time inside the launcher.
var recoveryBounds = func() workflows.Bounds {
	b := bounds
	b.ControlActivityTimeout, b.ControlRetryInitial = 10*time.Second, 100*time.Millisecond
	b.CleanupTimeout, b.CleanupMaxAttempts, b.UnresolvedSettleWindow = 10*time.Second, 2, 1500*time.Millisecond
	b.ReconcileInitialInterval, b.ReconcileMaxInterval, b.ReconcileMaxDuration = 5*time.Second, 30*time.Second, 2*time.Minute
	return b
}()

// Regression (P05 fourth pass follow-up): a cancel arrives while the create
// request is in flight, the create has landed on the backend, the cleanup
// really stops the Job and its Pod, but the completion receipt of that
// cleanup is lost (worker gone, connection dropped). The retried cleanup
// starts with no memory of what it saw; the objects are already gone, so
// absence alone could never confirm anything. The run must still converge:
// the backend evidence acquired before the delete survives the lost
// receipt (Activity retry, Worker replacement, later reconciliation
// rounds), the pending cancel settles under the original launch, no Job is
// launched again, and elapsed time never stands in for evidence.
func TestLocalCheckUnresolvedCreateCleanupSurvivesALostCompletionReceipt(t *testing.T) {
	for _, tc := range []struct {
		name string
		// landAfterUnknownClose commits the create only after the run has
		// closed the attempt with cleanup unknown: the Job arrives after the
		// first observation window and is handled by a reconciliation round.
		landAfterUnknownClose bool
	}{
		{name: "create landed before the cleanup"},
		{name: "create lands after the observation window", landAfterUnknownClose: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The Activity names are literals on purpose: the first completion
			// of each is dropped, whichever Activities the cleanup consists of.
			env, launcher, control, log := recoveryEnv(t, recoveryBounds, []string{"ObserveLaunch", "RegisterInstance", "DeleteJob"})
			env.RegisterDelayedCallback(func() { env.CancelWorkflow() }, 2*time.Second)
			if !tc.landAfterUnknownClose {
				launcher.landInRequest = true
			} else {
				env.SetOnActivityCompletedListener(func(info *activity.Info, _ converter.EncodedValue, err error) {
					if info.ActivityType.Name != activities.NameCloseAttempt || err != nil {
						return
					}
					if closes := control.closed(); len(closes) > 0 && closes[len(closes)-1].Cleanup == "unknown" {
						launcher.land(t)
					}
				})
			}

			env.ExecuteWorkflow(workflows.LocalCheckWorkflowName, input)
			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError(), "the run converges instead of reaching the reconciliation bound")

			closes := control.closed()
			require.NotEmpty(t, closes)
			last := closes[len(closes)-1]
			require.Equal(t, "canceled", last.Outcome)
			require.Equal(t, "complete", last.Cleanup, "the pending cancel settles only with confirmed cleanup")
			require.Equal(t, "att_1", last.Attempt.AttemptID, "settled under the original attempt")
			if tc.landAfterUnknownClose {
				require.Equal(t, "unknown", closes[0].Cleanup, "absence during the window closed the attempt unknown first")
				require.Equal(t, "att_1:close:settled", last.CommandID, "the evidence settles through the recovery owner's settlement close")
			}
			require.Equal(t, int32(1), launcher.creates.Load(), "no replacement launch")
			require.True(t, backendEmpty(t, launcher.cs), "the Job and its Pod are gone under the original launch key")
			require.Equal(t, 1, strings.Count(log.String(), `"activity":"DeleteJob"`), "the cleanup's first completion receipt really was dropped")
			require.Contains(t, control.registered(), "pod-uid-1", "the observed Pod is registered as trusted execution evidence under the original launch")
		})
	}
}

// Regression (review of the fifth pass): create request A is written and
// never answered while the API server still holds it; the retry B creates
// the Job under the same launch key first, the run proceeds on B's Pod and
// is canceled while it runs. The cleanup stops B's Job and its Pod, but
// B's success and B's object being gone are not evidence for A: A can
// still commit under the released name. So the attempt closes with cleanup
// unknown (the cancel stays pending), this run keeps reconciling the
// original launch key, and when A's Job appears the round registers A's
// Pod, stops it and only then settles the cancel with complete cleanup.
// The lost completion receipt of the first delete changes nothing; no
// replacement launch identity is prepared.
func TestLocalCheckRetriedCreateCleanupWaitsForTheUnansweredRequestsObject(t *testing.T) {
	cs := fake.NewClientset()
	launcher := &retriedCreateLauncher{Launcher: k8s.NewWithClient(cs, testNamespace), cs: cs, t: t}
	env, control, log := wireEnv(t, recoveryBounds, []string{"DeleteJob"}, cs, launcher)
	env.RegisterDelayedCallback(func() { env.CancelWorkflow() }, 3*time.Second) // B's Pod is running
	env.SetOnActivityCompletedListener(func(info *activity.Info, _ converter.EncodedValue, err error) {
		if info.ActivityType.Name != activities.NameCloseAttempt || err != nil {
			return
		}
		if closes := control.closed(); len(closes) > 0 && closes[len(closes)-1].Cleanup == "unknown" {
			require.True(t, backendEmpty(t, cs), "B's Job and Pod were stopped before the unknown close")
			launcher.land(t) // A commits after B's Job released the name
		}
	})

	env.ExecuteWorkflow(workflows.LocalCheckWorkflowName, input)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError(), "the run converges once A's object is seen")

	closes := control.closed()
	require.Len(t, closes, 2)
	require.Equal(t, "att_1:close", closes[0].CommandID)
	require.Equal(t, "canceled", closes[0].Outcome)
	require.Equal(t, "unknown", closes[0].Cleanup, "B's Job being gone did not complete the cleanup while A was unaccounted for")
	require.Equal(t, "att_1:close:settled", closes[1].CommandID)
	require.Equal(t, "canceled", closes[1].Outcome)
	require.Equal(t, "complete", closes[1].Cleanup, "the pending cancel settles only after A's object was seen and stopped")
	require.Equal(t, int32(2), launcher.creates.Load(), "two requests under the one launch key, no replacement launch")
	require.True(t, backendEmpty(t, cs), "A's Job and Pod are gone as well")
	require.ElementsMatch(t, []string{"pod-uid-b", "pod-uid-a"}, control.registered(), "both Pods are registered under the original launch")
	require.Equal(t, 1, strings.Count(log.String(), `"activity":"DeleteJob"`), "the first delete's completion receipt really was dropped")
}

// Regression (review of the sixth pass): as above, but A commits while B's
// Job is being deleted. The delete finds A's object under the released
// launch key and, because A is unaccounted for, returns it instead of
// deleting it; the completion receipt of that very delete is lost, so the
// retried delete finds A still there and reports it again; the run records
// A (ledger, Control registration of A's Pod) and only then deletes again,
// which stops A. The cleanup completes in the first round with one close:
// the delete never destroyed the evidence the ledger needed.
func TestLocalCheckDeleteKeepsTheObjectOfAnUnaccountedRequestUntilItIsRecorded(t *testing.T) {
	cs := fake.NewClientset()
	launcher := &retriedCreateLauncher{Launcher: k8s.NewWithClient(cs, testNamespace), cs: cs, t: t}
	env, control, log := wireEnv(t, recoveryBounds, []string{"DeleteJob"}, cs, launcher)
	env.RegisterDelayedCallback(func() { env.CancelWorkflow() }, 3*time.Second) // B's Pod is running
	// The API server commits A right after B's Job is deleted: the fake
	// removes B on the first delete, A lands behind it.
	var once sync.Once
	cs.PrependReactor("delete", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		once.Do(func() {
			go func() {
				time.Sleep(50 * time.Millisecond)
				launcher.land(t)
			}()
		})
		return false, nil, nil
	})

	env.ExecuteWorkflow(workflows.LocalCheckWorkflowName, input)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	closes := control.closed()
	require.Len(t, closes, 1, "the cleanup completed in the first round: no unknown close, no reconciliation")
	require.Equal(t, "att_1:close", closes[0].CommandID)
	require.Equal(t, "canceled", closes[0].Outcome)
	require.Equal(t, "complete", closes[0].Cleanup)
	require.Equal(t, int32(2), launcher.creates.Load(), "two requests under the one launch key")
	require.True(t, backendEmpty(t, cs), "A's Job and Pod are gone after they were recorded")
	require.ElementsMatch(t, []string{"pod-uid-b", "pod-uid-a"}, control.registered(), "A's Pod was registered before A was stopped")
	require.Equal(t, 1, strings.Count(log.String(), `"activity":"DeleteJob"`), "the receipt of the delete that found A really was dropped")
}
