package workflows_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/workflows"
)

var (
	queues = workflows.Queues{Business: "anvilkit-workflow", Control: "anvilkit-workflow-control"}
	// bounds mirror the reviewed defaults of config.yaml; the reconciliation
	// tests below narrow them explicitly.
	bounds = workflows.Bounds{
		ControlActivityTimeout: 30 * time.Second, ControlRetryInitial: time.Second, ControlRetryMaxInterval: 30 * time.Second, ControlRetryMaxAttempts: 6,
		LaunchWindow: 2 * time.Minute, ObserveHeartbeatTimeout: 30 * time.Second, ObserveMaxAttempts: 3,
		CleanupTimeout: 3 * time.Minute, CleanupMaxAttempts: 2, UnresolvedSettleWindow: 60 * time.Second,
		ReconcileInitialInterval: 5 * time.Second, ReconcileMaxInterval: time.Minute, ReconcileMaxDuration: 24 * time.Hour,
	}
)

type stub struct{ activities.Activities }

func register(env *testsuite.TestWorkflowEnvironment) {
	registerWith(env, bounds)
}

func registerWith(env *testsuite.TestWorkflowEnvironment, b workflows.Bounds) {
	var a stub
	env.RegisterWorkflowWithOptions(workflows.LocalCheck(queues, b), workflow.RegisterOptions{Name: workflows.LocalCheckWorkflowName})
	for name, fn := range map[string]any{
		activities.NameOpenAttempt: a.OpenAttempt, activities.NamePrepareLaunch: a.PrepareLaunch, activities.NameCreateJob: a.CreateJob,
		activities.NameObserveJob: a.ObserveJob, activities.NameRegisterInstance: a.RegisterInstance, activities.NameObserveInstance: a.ObserveInstance,
		activities.NameVerifyResult: a.VerifyResult,
		activities.NameAcceptResult: a.AcceptResult, activities.NameObserveLaunch: a.ObserveLaunch, activities.NameDeleteJob: a.DeleteJob, activities.NameCloseAttempt: a.CloseAttempt,
	} {
		env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
}

var (
	attempt = activities.AttemptRef{AttemptID: "att_1", TenantID: "tenant_a", ProfileID: "local-check-v1", Deadline: time.Now().Add(time.Hour)}
	launch  = activities.LaunchRef{LaunchID: "lch_1", AttemptID: "att_1", LaunchKey: "lc-abc", ImageDigest: "sha256:9db7b59979c38555a39def84a31fb98b5296952f9e3afd4f6f11f05b07adfab0"}
	input   = workflows.Input{OperationID: "op_ABC", TenantID: "tenant_a"}
)

func TestLocalCheckCertifiedPath(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	register(env)
	exit := int32(0)
	env.OnActivity(activities.NameOpenAttempt, mock.Anything, mock.MatchedBy(func(in activities.OpenAttemptInput) bool {
		return in.OperationID == "op_ABC" && in.TenantID == "tenant_a" && in.CommandID == "op_ABC:open"
	})).Return(attempt, nil)
	env.OnActivity(activities.NamePrepareLaunch, mock.Anything, mock.MatchedBy(func(in activities.PrepareLaunchInput) bool {
		return in.LaunchKey == "lc-abc" && !in.Deadline.After(attempt.Deadline)
	})).Return(launch, nil)
	env.OnActivity(activities.NameCreateJob, mock.Anything, mock.Anything).Return(activities.JobRef{JobUID: "job-1"}, nil)
	env.OnActivity(activities.NameObserveJob, mock.Anything, mock.Anything).Return(activities.JobObservation{
		JobUID: "job-1", Pods: []activities.PodObservation{{PodUID: "pod-1", Phase: "succeeded", ExitCode: &exit, TerminationMessage: "{}"}},
	}, nil)
	env.OnActivity(activities.NameRegisterInstance, mock.Anything, mock.MatchedBy(func(in activities.RegisterInstanceInput) bool {
		return in.PodUID == "pod-1" && in.CommandID == "att_1:register:pod-1"
	})).Return(activities.InstanceRef{InstanceID: "inst-1", Current: true}, nil)
	env.OnActivity(activities.NameObserveInstance, mock.Anything, mock.MatchedBy(func(in activities.ObserveInstanceInput) bool {
		return in.Instance.InstanceID == "inst-1" && in.Observation.Phase == "succeeded"
	})).Return(nil)
	env.OnActivity(activities.NameVerifyResult, mock.Anything, mock.Anything).Return(activities.Verdict{Verdict: "certified", Manifest: []byte("{}"), ResultDigest: "sha256:aa"}, nil)
	env.OnActivity(activities.NameAcceptResult, mock.Anything, mock.MatchedBy(func(in activities.AcceptResultInput) bool {
		return in.Instance.InstanceID == "inst-1" && in.Verdict.Verdict == "certified"
	})).Return(activities.StageRef{StageID: "stg-1"}, nil)
	env.OnActivity(activities.NameDeleteJob, mock.Anything, mock.Anything).Return(activities.LaunchObservation{}, nil)
	env.OnActivity(activities.NameCloseAttempt, mock.Anything, mock.MatchedBy(func(in activities.CloseAttemptInput) bool {
		return in.Outcome == "completed" && in.Cleanup == "complete" && in.CommandID == "att_1:close"
	})).Return(activities.CloseResult{Lifecycle: "succeeded"}, nil)

	env.ExecuteWorkflow(workflows.LocalCheckWorkflowName, input)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestLocalCheckCancelStopsJobAndClosesCanceled(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	register(env)
	env.OnActivity(activities.NameOpenAttempt, mock.Anything, mock.Anything).Return(attempt, nil)
	env.OnActivity(activities.NamePrepareLaunch, mock.Anything, mock.Anything).Return(launch, nil)
	env.OnActivity(activities.NameCreateJob, mock.Anything, mock.Anything).Return(activities.JobRef{JobUID: "job-1"}, nil)
	env.OnActivity(activities.NameObserveJob, mock.Anything, mock.Anything).Return(func(ctx context.Context, in activities.ObserveJobInput) (activities.JobObservation, error) {
		<-ctx.Done() // the Job is still running when the cancel arrives
		return activities.JobObservation{}, ctx.Err()
	})
	env.OnActivity(activities.NameDeleteJob, mock.Anything, mock.MatchedBy(func(in activities.DeleteJobInput) bool {
		return in.Launch.LaunchKey == "lc-abc"
	})).Return(activities.LaunchObservation{}, nil)
	env.OnActivity(activities.NameCloseAttempt, mock.Anything, mock.MatchedBy(func(in activities.CloseAttemptInput) bool {
		return in.Outcome == "canceled" && in.Cleanup == "complete" && in.FailureCode == "CANCELED"
	})).Return(activities.CloseResult{Lifecycle: "canceled"}, nil)
	env.RegisterDelayedCallback(func() { env.CancelWorkflow() }, 2*time.Second)

	env.ExecuteWorkflow(workflows.LocalCheckWorkflowName, input)
	require.True(t, env.IsWorkflowCompleted())
	env.AssertExpectations(t)
	env.AssertNotCalled(t, activities.NameAcceptResult, mock.Anything, mock.Anything)
	env.AssertNotCalled(t, activities.NameObserveLaunch, mock.Anything, mock.Anything)
}

func TestLocalCheckFencedOperationLaunchesNothing(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	register(env)
	env.OnActivity(activities.NameOpenAttempt, mock.Anything, mock.Anything).Return(activities.AttemptRef{}, activities.Refused("STALE_EXECUTION", errors.New("operation is fenced")))
	env.ExecuteWorkflow(workflows.LocalCheckWorkflowName, input)
	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	env.AssertNotCalled(t, activities.NamePrepareLaunch, mock.Anything, mock.Anything)
	env.AssertNotCalled(t, activities.NameCreateJob, mock.Anything, mock.Anything)
	env.AssertNotCalled(t, activities.NameCloseAttempt, mock.Anything, mock.Anything)
}

func TestLocalCheckInvalidVerdictFailsTheAttempt(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	register(env)
	exit := int32(1)
	env.OnActivity(activities.NameOpenAttempt, mock.Anything, mock.Anything).Return(attempt, nil)
	env.OnActivity(activities.NamePrepareLaunch, mock.Anything, mock.Anything).Return(launch, nil)
	env.OnActivity(activities.NameCreateJob, mock.Anything, mock.Anything).Return(activities.JobRef{JobUID: "job-1"}, nil)
	env.OnActivity(activities.NameObserveJob, mock.Anything, mock.Anything).Return(activities.JobObservation{JobUID: "job-1", Pods: []activities.PodObservation{{PodUID: "pod-1", Phase: "failed", ExitCode: &exit}}}, nil)
	env.OnActivity(activities.NameRegisterInstance, mock.Anything, mock.Anything).Return(activities.InstanceRef{InstanceID: "inst-1", Current: true}, nil)
	env.OnActivity(activities.NameObserveInstance, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(activities.NameVerifyResult, mock.Anything, mock.Anything).Return(activities.Verdict{Verdict: "invalid", FailureCode: "CANDIDATE_TEST_FAILED", Manifest: []byte("{}"), ResultDigest: "sha256:bb"}, nil)
	env.OnActivity(activities.NameAcceptResult, mock.Anything, mock.Anything).Return(activities.StageRef{StageID: "stg-1"}, nil)
	env.OnActivity(activities.NameDeleteJob, mock.Anything, mock.Anything).Return(activities.LaunchObservation{}, nil)
	env.OnActivity(activities.NameCloseAttempt, mock.Anything, mock.MatchedBy(func(in activities.CloseAttemptInput) bool {
		return in.Outcome == "failed" && in.FailureCode == "CANDIDATE_TEST_FAILED"
	})).Return(activities.CloseResult{Lifecycle: "failed"}, nil)
	env.ExecuteWorkflow(workflows.LocalCheckWorkflowName, input)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

// certifiedRun mocks the business Activities of a certified run so the
// cleanup/close variants below differ only in what they exercise.
func certifiedRun(env *testsuite.TestWorkflowEnvironment) {
	exit := int32(0)
	env.OnActivity(activities.NameOpenAttempt, mock.Anything, mock.Anything).Return(attempt, nil)
	env.OnActivity(activities.NamePrepareLaunch, mock.Anything, mock.Anything).Return(launch, nil)
	env.OnActivity(activities.NameCreateJob, mock.Anything, mock.Anything).Return(activities.JobRef{JobUID: "job-1"}, nil)
	env.OnActivity(activities.NameObserveJob, mock.Anything, mock.Anything).Return(activities.JobObservation{
		JobUID: "job-1", Pods: []activities.PodObservation{{PodUID: "pod-1", Phase: "succeeded", ExitCode: &exit, TerminationMessage: "{}"}},
	}, nil)
	env.OnActivity(activities.NameRegisterInstance, mock.Anything, mock.Anything).Return(activities.InstanceRef{InstanceID: "inst-1", Current: true}, nil)
	env.OnActivity(activities.NameObserveInstance, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(activities.NameVerifyResult, mock.Anything, mock.Anything).Return(activities.Verdict{Verdict: "certified", Manifest: []byte("{}"), ResultDigest: "sha256:aa"}, nil)
	env.OnActivity(activities.NameAcceptResult, mock.Anything, mock.Anything).Return(activities.StageRef{StageID: "stg-1"}, nil)
}

// A close refused by Control (conflicting identity, unknown attempt) fails
// the Workflow visibly instead of completing it with an unsettled operation.
func TestLocalCheckRefusedCloseFailsTheWorkflow(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	register(env)
	certifiedRun(env)
	env.OnActivity(activities.NameDeleteJob, mock.Anything, mock.Anything).Return(activities.LaunchObservation{}, nil)
	env.OnActivity(activities.NameCloseAttempt, mock.Anything, mock.Anything).Return(activities.CloseResult{}, activities.Refused("IDEMPOTENCY_CONFLICT", errors.New("attempt att_1 closed as canceled")))
	env.ExecuteWorkflow(workflows.LocalCheckWorkflowName, input)
	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "close attempt att_1")
	require.Equal(t, "IDEMPOTENCY_CONFLICT", activities.RefusalCode(err))
	env.AssertNumberOfCalls(t, activities.NameCloseAttempt, 1)
}

// A transient close failure is retried under the same command identity
// until Control answers; the Workflow does not complete before that.
func TestLocalCheckTransientCloseFailureIsRetriedUnderTheSameIdentity(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	register(env)
	certifiedRun(env)
	env.OnActivity(activities.NameDeleteJob, mock.Anything, mock.Anything).Return(activities.LaunchObservation{}, nil)
	calls := 0
	env.OnActivity(activities.NameCloseAttempt, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.CloseAttemptInput) (activities.CloseResult, error) {
		calls++
		require.Equal(t, "att_1:close", in.CommandID)
		if calls < 8 {
			return activities.CloseResult{}, errors.New("rpc error: code = Unavailable desc = control unavailable")
		}
		return activities.CloseResult{Lifecycle: "succeeded"}, nil
	})
	env.ExecuteWorkflow(workflows.LocalCheckWorkflowName, input)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, 8, calls, "the close was retried past the business retry cap of 6")
}

// Nothing is launched once the absolute deadline has passed, even though
// the attempt was opened; the attempt closes as DEADLINE_EXCEEDED.
func TestLocalCheckDeadlinePassedLaunchesNothing(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	register(env)
	expired := attempt
	expired.Deadline = env.Now().Add(-time.Second)
	env.OnActivity(activities.NameOpenAttempt, mock.Anything, mock.Anything).Return(expired, nil)
	env.OnActivity(activities.NameCloseAttempt, mock.Anything, mock.MatchedBy(func(in activities.CloseAttemptInput) bool {
		return in.Outcome == "infrastructure_failed" && in.FailureCode == "DEADLINE_EXCEEDED" && in.Cleanup == "not_required"
	})).Return(activities.CloseResult{Lifecycle: "failed"}, nil)
	env.ExecuteWorkflow(workflows.LocalCheckWorkflowName, input)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
	env.AssertNotCalled(t, activities.NamePrepareLaunch, mock.Anything, mock.Anything)
	env.AssertNotCalled(t, activities.NameCreateJob, mock.Anything, mock.Anything)
	env.AssertNotCalled(t, activities.NameDeleteJob, mock.Anything, mock.Anything)
}

// Control's refusal of a launch (deadline or fence) closes the attempt with
// Control's own code rather than a generic observer failure.
func TestLocalCheckRefusedLaunchCarriesControlCode(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	register(env)
	env.OnActivity(activities.NameOpenAttempt, mock.Anything, mock.Anything).Return(attempt, nil)
	env.OnActivity(activities.NamePrepareLaunch, mock.Anything, mock.Anything).Return(activities.LaunchRef{}, activities.Refused("STALE_EXECUTION", errors.New("attempt att_1 deadline passed")))
	env.OnActivity(activities.NameCloseAttempt, mock.Anything, mock.MatchedBy(func(in activities.CloseAttemptInput) bool {
		return in.Outcome == "infrastructure_failed" && in.FailureCode == "STALE_EXECUTION" && in.Cleanup == "not_required"
	})).Return(activities.CloseResult{Lifecycle: "failed"}, nil)
	env.ExecuteWorkflow(workflows.LocalCheckWorkflowName, input)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
	env.AssertNumberOfCalls(t, activities.NamePrepareLaunch, 1)
	env.AssertNotCalled(t, activities.NameCreateJob, mock.Anything, mock.Anything)
}

// When the Job ran past its deadline and Control refuses to record the
// failed stage, the observer's verdict still settles the attempt.
func TestLocalCheckRefusedStageAfterDeadlineSettlesWithVerdict(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	register(env)
	env.OnActivity(activities.NameOpenAttempt, mock.Anything, mock.Anything).Return(attempt, nil)
	env.OnActivity(activities.NamePrepareLaunch, mock.Anything, mock.Anything).Return(launch, nil)
	env.OnActivity(activities.NameCreateJob, mock.Anything, mock.Anything).Return(activities.JobRef{JobUID: "job-1"}, nil)
	env.OnActivity(activities.NameObserveJob, mock.Anything, mock.Anything).Return(activities.JobObservation{JobUID: "job-1", Reason: "DeadlineExceeded", Pods: []activities.PodObservation{{PodUID: "pod-1", Phase: "failed"}}}, nil)
	env.OnActivity(activities.NameRegisterInstance, mock.Anything, mock.Anything).Return(activities.InstanceRef{InstanceID: "inst-1", Current: true}, nil)
	env.OnActivity(activities.NameObserveInstance, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(activities.NameVerifyResult, mock.Anything, mock.Anything).Return(activities.Verdict{Verdict: "infrastructure_failed", FailureCode: "DEADLINE_EXCEEDED", Manifest: []byte("{}"), ResultDigest: "sha256:cc"}, nil)
	env.OnActivity(activities.NameAcceptResult, mock.Anything, mock.Anything).Return(activities.StageRef{}, activities.Refused("STALE_EXECUTION", errors.New("attempt att_1 deadline passed")))
	env.OnActivity(activities.NameDeleteJob, mock.Anything, mock.Anything).Return(activities.LaunchObservation{}, nil)
	env.OnActivity(activities.NameCloseAttempt, mock.Anything, mock.MatchedBy(func(in activities.CloseAttemptInput) bool {
		return in.Outcome == "infrastructure_failed" && in.FailureCode == "DEADLINE_EXCEEDED" && in.Cleanup == "complete"
	})).Return(activities.CloseResult{Lifecycle: "failed"}, nil)
	env.ExecuteWorkflow(workflows.LocalCheckWorkflowName, input)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

// A cancel that arrives while the create request is unresolved waits for
// the create's real result; when it ends canceled the cleanup is
// evidence-first under the original launch key: the launcher observes the
// key (the frozen settle window paces that observation) and reports the
// object of the unanswered request (its marker), every reported Pod is
// registered with Control under the running path's command identities
// before anything is deleted, and only then is the Job stopped. Nothing
// launches under another identity.
func TestLocalCheckCancelDuringUnresolvedCreateCleansUpUnderTheOriginalLaunch(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	register(env)
	env.OnActivity(activities.NameOpenAttempt, mock.Anything, mock.Anything).Return(attempt, nil)
	env.OnActivity(activities.NamePrepareLaunch, mock.Anything, mock.Anything).Return(launch, nil)
	env.OnActivity(activities.NameCreateJob, mock.Anything, mock.MatchedBy(func(in activities.CreateJobInput) bool {
		return in.Request == 1 && in.Launch.LaunchKey == "lc-abc"
	})).Return(func(ctx context.Context, in activities.CreateJobInput) (activities.JobRef, error) {
		<-ctx.Done() // the create request is still in flight when the cancel arrives
		return activities.JobRef{}, ctx.Err()
	})
	var sequence []string
	env.OnActivity(activities.NameObserveLaunch, mock.Anything, mock.MatchedBy(func(in activities.ObserveLaunchInput) bool {
		return in.Launch.LaunchKey == "lc-abc" && in.SettleWindow == bounds.UnresolvedSettleWindow
	})).Return(func(context.Context, activities.ObserveLaunchInput) (activities.LaunchObservation, error) {
		sequence = append(sequence, "observe")
		return activities.LaunchObservation{JobUID: "job-late", Pods: []activities.PodObservation{{PodUID: "pod-late", Phase: "running"}}, Requests: []int{1}}, nil
	})
	env.OnActivity(activities.NameRegisterInstance, mock.Anything, mock.MatchedBy(func(in activities.RegisterInstanceInput) bool {
		return in.CommandID == "att_1:register:pod-late" && in.JobUID == "job-late" && in.Launch.LaunchKey == "lc-abc" && in.Attempt.AttemptID == "att_1"
	})).Return(func(context.Context, activities.RegisterInstanceInput) (activities.InstanceRef, error) {
		sequence = append(sequence, "register")
		return activities.InstanceRef{InstanceID: "inst-late", Current: true}, nil
	})
	env.OnActivity(activities.NameObserveInstance, mock.Anything, mock.MatchedBy(func(in activities.ObserveInstanceInput) bool {
		return in.Instance.InstanceID == "inst-late" && in.Observation.PodUID == "pod-late"
	})).Return(nil)
	env.OnActivity(activities.NameDeleteJob, mock.Anything, mock.MatchedBy(func(in activities.DeleteJobInput) bool {
		return in.Launch.LaunchKey == "lc-abc"
	})).Return(func(context.Context, activities.DeleteJobInput) (activities.LaunchObservation, error) {
		sequence = append(sequence, "delete")
		return activities.LaunchObservation{}, nil
	})
	env.OnActivity(activities.NameCloseAttempt, mock.Anything, mock.MatchedBy(func(in activities.CloseAttemptInput) bool {
		return in.Outcome == "canceled" && in.Cleanup == "complete" && in.CommandID == "att_1:close"
	})).Return(activities.CloseResult{Lifecycle: "canceled"}, nil)
	env.RegisterDelayedCallback(func() { env.CancelWorkflow() }, 2*time.Second)
	env.ExecuteWorkflow(workflows.LocalCheckWorkflowName, input)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
	require.Equal(t, []string{"observe", "register", "delete"}, sequence, "evidence is observed and registered before the delete")
	env.AssertNumberOfCalls(t, activities.NamePrepareLaunch, 1)
	env.AssertNumberOfCalls(t, activities.NameCreateJob, 1)
	env.AssertNotCalled(t, activities.NameObserveJob, mock.Anything, mock.Anything)
}

// A cancel during an unresolved create whose creation commits only after
// the launcher's observation window: absence during the window is not
// evidence, so no delete runs, the attempt closes as canceled with cleanup
// unknown (Control keeps the cancel pending) and no cancel-applied close
// happens before the evidence. The same run keeps reconciling the original
// launch key; the round that observes the late Job registers its Pod,
// deletes it, confirms it gone and settles the cancel.
func TestLocalCheckUnresolvedCreateCommittingAfterTheWindowSettlesOnlyWithEvidence(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	register(env)
	env.OnActivity(activities.NameOpenAttempt, mock.Anything, mock.Anything).Return(attempt, nil)
	env.OnActivity(activities.NamePrepareLaunch, mock.Anything, mock.Anything).Return(launch, nil)
	// The create request is still in flight when the cancel arrives; the
	// worker learns of the cancel through its heartbeat (the test environment
	// delivers it only that way) and returns the canceled request.
	canceled := make(chan struct{})
	var once sync.Once
	env.SetOnActivityCanceledListener(func(*activity.Info) { once.Do(func() { close(canceled) }) })
	env.OnActivity(activities.NameCreateJob, mock.Anything, mock.Anything).Return(func(ctx context.Context, in activities.CreateJobInput) (activities.JobRef, error) {
		<-canceled
		activity.RecordHeartbeat(ctx)
		<-ctx.Done()
		return activities.JobRef{}, ctx.Err()
	})
	var sequence []string
	observations := 0
	env.OnActivity(activities.NameObserveLaunch, mock.Anything, mock.MatchedBy(func(in activities.ObserveLaunchInput) bool {
		return in.Launch.LaunchKey == "lc-abc" && in.SettleWindow == bounds.UnresolvedSettleWindow
	})).Return(func(context.Context, activities.ObserveLaunchInput) (activities.LaunchObservation, error) {
		observations++
		sequence = append(sequence, "observe")
		if observations < 4 { // the first round (2 attempts) and one reconciliation attempt see nothing
			return activities.LaunchObservation{}, errors.New("job lc-abc create unresolved: neither the Job nor a Pod was observed within 1m0s, so the create is not known to have finished; cleanup stays unknown")
		}
		return activities.LaunchObservation{JobUID: "job-late", Pods: []activities.PodObservation{{PodUID: "pod-late", Phase: "succeeded"}}, Requests: []int{1}}, nil
	})
	env.OnActivity(activities.NameRegisterInstance, mock.Anything, mock.MatchedBy(func(in activities.RegisterInstanceInput) bool {
		return in.CommandID == "att_1:register:pod-late" && in.JobUID == "job-late"
	})).Return(func(context.Context, activities.RegisterInstanceInput) (activities.InstanceRef, error) {
		sequence = append(sequence, "register")
		return activities.InstanceRef{InstanceID: "inst-late", Current: true}, nil
	})
	env.OnActivity(activities.NameObserveInstance, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(activities.NameDeleteJob, mock.Anything, mock.MatchedBy(func(in activities.DeleteJobInput) bool {
		return in.Launch.LaunchKey == "lc-abc"
	})).Return(func(context.Context, activities.DeleteJobInput) (activities.LaunchObservation, error) {
		sequence = append(sequence, "delete")
		return activities.LaunchObservation{}, nil // the late create is stopped and its Pod is gone
	})
	closes := []activities.CloseAttemptInput{}
	env.OnActivity(activities.NameCloseAttempt, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.CloseAttemptInput) (activities.CloseResult, error) {
		closes = append(closes, in)
		sequence = append(sequence, "close:"+in.Cleanup)
		require.Equal(t, "att_1", in.Attempt.AttemptID)
		require.Equal(t, "canceled", in.Outcome)
		if in.Cleanup == "unknown" {
			return activities.CloseResult{Lifecycle: "reconciling"}, nil
		}
		return activities.CloseResult{Lifecycle: "canceled"}, nil
	})
	env.RegisterDelayedCallback(func() { env.CancelWorkflow() }, 2*time.Second)
	env.ExecuteWorkflow(workflows.LocalCheckWorkflowName, input)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []string{"observe", "observe", "close:unknown", "observe", "observe", "register", "delete", "close:complete"}, sequence,
		"nothing is deleted and no cleanup-complete close happens before the round that observed the late launch; its Pod is registered before the delete")
	require.Len(t, closes, 2)
	require.Equal(t, "att_1:close", closes[0].CommandID)
	require.Equal(t, "unknown", closes[0].Cleanup, "elapsed time alone did not apply the cancel")
	require.Equal(t, "att_1:close:settled", closes[1].CommandID)
	require.Equal(t, "complete", closes[1].Cleanup)
	env.AssertNumberOfCalls(t, activities.NamePrepareLaunch, 1)
	env.AssertNumberOfCalls(t, activities.NameCreateJob, 1) // no replacement launch identity
	env.AssertNotCalled(t, activities.NameObserveJob, mock.Anything, mock.Anything)
}

// An unconfirmed cleanup closes the attempt as unknown (Control keeps the
// operation reconciling) and the same run stays the recovery owner: it
// keeps reconciling the original launch key until the backend confirms,
// then settles the close with the evidence. Nothing is launched again.
func TestLocalCheckUnknownCleanupIsReconciledUntilEvidenceThenSettled(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	register(env)
	certifiedRun(env)
	// sequence records the protected control-queue calls in order.
	var sequence []string
	deletes := 0
	env.OnActivity(activities.NameDeleteJob, mock.Anything, mock.MatchedBy(func(in activities.DeleteJobInput) bool {
		return in.Launch.LaunchKey == "lc-abc"
	})).Return(func(context.Context, activities.DeleteJobInput) (activities.LaunchObservation, error) {
		deletes++
		sequence = append(sequence, "delete")
		if deletes < 6 { // the first round (2 attempts) and two reconciliation rounds fail
			return activities.LaunchObservation{}, errors.New("job lc-abc not confirmed stopped (job present: true, pods: 1): context deadline exceeded")
		}
		return activities.LaunchObservation{}, nil
	})
	closes := []activities.CloseAttemptInput{}
	env.OnActivity(activities.NameCloseAttempt, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.CloseAttemptInput) (activities.CloseResult, error) {
		closes = append(closes, in)
		sequence = append(sequence, "close:"+in.Cleanup)
		require.Equal(t, "att_1", in.Attempt.AttemptID)
		if in.Cleanup == "unknown" {
			return activities.CloseResult{Lifecycle: "reconciling"}, nil
		}
		return activities.CloseResult{Lifecycle: "succeeded"}, nil
	})
	env.ExecuteWorkflow(workflows.LocalCheckWorkflowName, input)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, 6, deletes)
	require.Len(t, closes, 2)
	require.Equal(t, []string{"delete", "delete", "close:unknown", "delete", "delete", "delete", "delete", "close:complete"}, sequence,
		"the unknown close is durable before any reconciliation round; the settlement follows the confirming round")
	require.Equal(t, "att_1:close", closes[0].CommandID)
	require.Equal(t, "completed", closes[0].Outcome)
	require.Equal(t, "att_1:close:settled", closes[1].CommandID)
	require.Equal(t, "completed", closes[1].Outcome, "the evidence settles the same attempt and outcome")
	require.Equal(t, "complete", closes[1].Cleanup)
	env.AssertNumberOfCalls(t, activities.NamePrepareLaunch, 1)
	env.AssertNumberOfCalls(t, activities.NameCreateJob, 1)
	env.AssertNumberOfCalls(t, activities.NameOpenAttempt, 1)
	env.AssertNotCalled(t, activities.NameObserveLaunch, mock.Anything, mock.Anything)
}

// Reconciliation is bounded: when the backend never confirms, the run fails
// visibly with the launch identity, after the unknown close and without a
// settlement close, so the operation is not reported settled.
func TestLocalCheckReconciliationBoundFailsVisibly(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	narrow := bounds
	narrow.ReconcileInitialInterval, narrow.ReconcileMaxInterval, narrow.ReconcileMaxDuration = 5*time.Second, 30*time.Second, 2*time.Minute
	registerWith(env, narrow)
	certifiedRun(env)
	deletes := 0
	env.OnActivity(activities.NameDeleteJob, mock.Anything, mock.Anything).Return(func(context.Context, activities.DeleteJobInput) (activities.LaunchObservation, error) {
		deletes++
		return activities.LaunchObservation{}, errors.New("job lc-abc not confirmed stopped (job present: true, pods: 1): context deadline exceeded")
	})
	env.OnActivity(activities.NameCloseAttempt, mock.Anything, mock.MatchedBy(func(in activities.CloseAttemptInput) bool {
		return in.Cleanup == "unknown" && in.CommandID == "att_1:close"
	})).Return(activities.CloseResult{Lifecycle: "reconciling"}, nil)
	env.ExecuteWorkflow(workflows.LocalCheckWorkflowName, input)
	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Equal(t, "CLEANUP_UNCONFIRMED", activities.RefusalCode(err))
	require.Contains(t, err.Error(), "lc-abc")
	require.GreaterOrEqual(t, deletes, 6, "several reconciliation rounds ran before the bound")
	env.AssertNumberOfCalls(t, activities.NameCloseAttempt, 1)
	env.AssertNotCalled(t, activities.NameCloseAttempt, mock.Anything, mock.MatchedBy(func(in activities.CloseAttemptInput) bool { return in.Cleanup == "complete" }))
}

// unansweredCreate is the launcher's answer for a create request that was
// written to the API server and never answered within the Activity's bound:
// not a refusal and not NotCreated, so the request stays in the ledger.
var unansweredCreate = errors.New("job lc-abc create request 1 unanswered: Post \"https://10.96.0.1/apis/batch/v1/namespaces/anvilkit-components/jobs\": context deadline exceeded")

// Regression for the interleaving: the original create request A is written
// and never answered while the API server still holds it, the retry B
// creates the Job under the same launch key first and the run completes
// on B's Pod. Cleanup deletes B's Job, but B's success is not evidence for
// A: A can still commit once the name is released. The cleanup therefore
// ends unknown after B is gone (the attempt closes unknown, Control keeps
// the cancel pending) and this run keeps reconciling the original launch
// key; the round that observes A's object (its marker) registers A's Pod,
// deletes it and only then settles the close as complete. No replacement
// launch is prepared; the second request is the same launch key.
func TestLocalCheckUnansweredCreateRetriedUnderTheSameKeyKeepsCleanupUnknownUntilItsObjectIsSeen(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	register(env)
	exit := int32(0)
	env.OnActivity(activities.NameOpenAttempt, mock.Anything, mock.Anything).Return(attempt, nil)
	env.OnActivity(activities.NamePrepareLaunch, mock.Anything, mock.Anything).Return(launch, nil)
	var requests []int
	env.OnActivity(activities.NameCreateJob, mock.Anything, mock.MatchedBy(func(in activities.CreateJobInput) bool {
		return in.Launch.LaunchKey == "lc-abc" && in.Launch.LaunchID == "lch_1"
	})).Return(func(_ context.Context, in activities.CreateJobInput) (activities.JobRef, error) {
		requests = append(requests, in.Request)
		if in.Request == 1 {
			return activities.JobRef{}, unansweredCreate // A: written, pending on the server
		}
		return activities.JobRef{JobUID: "job-b", Request: 2}, nil // B created the Job first
	})
	env.OnActivity(activities.NameObserveJob, mock.Anything, mock.Anything).Return(activities.JobObservation{
		JobUID: "job-b", Pods: []activities.PodObservation{{PodUID: "pod-b", Phase: "succeeded", ExitCode: &exit, TerminationMessage: "{}"}},
	}, nil)
	var sequence []string
	env.OnActivity(activities.NameRegisterInstance, mock.Anything, mock.MatchedBy(func(in activities.RegisterInstanceInput) bool {
		return in.Launch.LaunchKey == "lc-abc" && in.CommandID == "att_1:register:"+in.PodUID
	})).Return(func(_ context.Context, in activities.RegisterInstanceInput) (activities.InstanceRef, error) {
		sequence = append(sequence, "register:"+in.PodUID)
		return activities.InstanceRef{InstanceID: "inst-" + in.PodUID, Current: in.PodUID == "pod-b"}, nil
	})
	env.OnActivity(activities.NameObserveInstance, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(activities.NameVerifyResult, mock.Anything, mock.Anything).Return(activities.Verdict{Verdict: "certified", Manifest: []byte("{}"), ResultDigest: "sha256:aa"}, nil)
	env.OnActivity(activities.NameAcceptResult, mock.Anything, mock.Anything).Return(activities.StageRef{StageID: "stg-1"}, nil)
	observations := 0
	env.OnActivity(activities.NameObserveLaunch, mock.Anything, mock.MatchedBy(func(in activities.ObserveLaunchInput) bool {
		return in.Launch.LaunchKey == "lc-abc" && in.SettleWindow == bounds.UnresolvedSettleWindow
	})).Return(func(context.Context, activities.ObserveLaunchInput) (activities.LaunchObservation, error) {
		observations++
		sequence = append(sequence, "observe")
		switch {
		case observations == 1: // B's Job is still there: evidence for B, none for A
			return activities.LaunchObservation{JobUID: "job-b", Pods: []activities.PodObservation{{PodUID: "pod-b", Phase: "succeeded"}}, Requests: []int{2}}, nil
		case observations <= 3: // B is gone and A has not landed within the window (one round, two attempts)
			return activities.LaunchObservation{}, errors.New("job lc-abc create unresolved: neither the Job nor a Pod was observed within 1m0s, so the create is not known to have finished; cleanup stays unknown")
		}
		// A committed after B's Job was deleted: the Job reappeared under the launch key.
		return activities.LaunchObservation{JobUID: "job-a", Pods: []activities.PodObservation{{PodUID: "pod-a", Phase: "running"}}, Requests: []int{1}}, nil
	})
	var deletesUnaccounted [][]int
	env.OnActivity(activities.NameDeleteJob, mock.Anything, mock.MatchedBy(func(in activities.DeleteJobInput) bool {
		return in.Launch.LaunchKey == "lc-abc"
	})).Return(func(_ context.Context, in activities.DeleteJobInput) (activities.LaunchObservation, error) {
		sequence = append(sequence, "delete")
		deletesUnaccounted = append(deletesUnaccounted, in.Unaccounted)
		return activities.LaunchObservation{}, nil // whatever is under the key is stopped and gone
	})
	var closes []activities.CloseAttemptInput
	env.OnActivity(activities.NameCloseAttempt, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.CloseAttemptInput) (activities.CloseResult, error) {
		closes = append(closes, in)
		sequence = append(sequence, "close:"+in.Cleanup)
		require.Equal(t, "att_1", in.Attempt.AttemptID)
		require.Equal(t, "completed", in.Outcome)
		if in.Cleanup == "unknown" {
			return activities.CloseResult{Lifecycle: "reconciling"}, nil
		}
		return activities.CloseResult{Lifecycle: "succeeded"}, nil
	})
	env.ExecuteWorkflow(workflows.LocalCheckWorkflowName, input)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []int{1, 2}, requests, "the second request reuses the launch key; nothing else is created")
	require.Equal(t, []string{
		"register:pod-b",                      // the running path registers B's Pod
		"observe", "register:pod-b", "delete", // cleanup: B is seen and stopped, but A is unaccounted for
		"observe", "observe", "close:unknown", // A has not landed: unknown, never complete
		"observe", "register:pod-a", "delete", "close:complete", // A's object is seen, registered, stopped: settled
	}, sequence, "B's Job being gone does not complete the cleanup while A is unaccounted for")
	require.Len(t, deletesUnaccounted, 2)
	require.Equal(t, []int{1}, deletesUnaccounted[0], "the first delete is told that A is unaccounted for")
	require.Empty(t, deletesUnaccounted[1], "the settling delete has nothing left to protect")
	require.Len(t, closes, 2)
	require.Equal(t, "att_1:close", closes[0].CommandID)
	require.Equal(t, "unknown", closes[0].Cleanup, "one successful create and B's object appearing and disappearing are not evidence for A")
	require.Equal(t, "att_1:close:settled", closes[1].CommandID)
	require.Equal(t, "complete", closes[1].Cleanup)
	env.AssertNumberOfCalls(t, activities.NamePrepareLaunch, 1)
	env.AssertNumberOfCalls(t, activities.NameCreateJob, 2)
}

// A commits while B's Job is being deleted. The delete stops at A's object
// (an unaccounted request) and returns it instead of deleting it; the run
// records A (ledger and Control registration) and only then deletes again,
// which stops A and confirms the key empty. The cleanup completes in the
// first round: no unknown close, no reconciliation, and A's evidence was
// never destroyed by the delete.
func TestLocalCheckDeleteStopsAtTheObjectOfAnUnaccountedRequest(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	register(env)
	exit := int32(0)
	env.OnActivity(activities.NameOpenAttempt, mock.Anything, mock.Anything).Return(attempt, nil)
	env.OnActivity(activities.NamePrepareLaunch, mock.Anything, mock.Anything).Return(launch, nil)
	env.OnActivity(activities.NameCreateJob, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.CreateJobInput) (activities.JobRef, error) {
		if in.Request == 1 {
			return activities.JobRef{}, unansweredCreate
		}
		return activities.JobRef{JobUID: "job-b", Request: 2}, nil
	})
	env.OnActivity(activities.NameObserveJob, mock.Anything, mock.Anything).Return(activities.JobObservation{
		JobUID: "job-b", Pods: []activities.PodObservation{{PodUID: "pod-b", Phase: "succeeded", ExitCode: &exit, TerminationMessage: "{}"}},
	}, nil)
	var sequence []string
	env.OnActivity(activities.NameRegisterInstance, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.RegisterInstanceInput) (activities.InstanceRef, error) {
		sequence = append(sequence, "register:"+in.PodUID)
		return activities.InstanceRef{InstanceID: "inst-" + in.PodUID, Current: in.PodUID == "pod-b"}, nil
	})
	env.OnActivity(activities.NameObserveInstance, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(activities.NameVerifyResult, mock.Anything, mock.Anything).Return(activities.Verdict{Verdict: "certified", Manifest: []byte("{}"), ResultDigest: "sha256:aa"}, nil)
	env.OnActivity(activities.NameAcceptResult, mock.Anything, mock.Anything).Return(activities.StageRef{StageID: "stg-1"}, nil)
	env.OnActivity(activities.NameObserveLaunch, mock.Anything, mock.Anything).Return(func(context.Context, activities.ObserveLaunchInput) (activities.LaunchObservation, error) {
		sequence = append(sequence, "observe")
		return activities.LaunchObservation{JobUID: "job-b", Pods: []activities.PodObservation{{PodUID: "pod-b", Phase: "succeeded"}}, Requests: []int{2}}, nil
	})
	deletes := 0
	env.OnActivity(activities.NameDeleteJob, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.DeleteJobInput) (activities.LaunchObservation, error) {
		deletes++
		sequence = append(sequence, "delete")
		if deletes == 1 {
			require.Equal(t, []int{1}, in.Unaccounted)
			// B is going; A landed under the released name: reported, not deleted.
			return activities.LaunchObservation{JobUID: "job-a", Pods: []activities.PodObservation{{PodUID: "pod-a", Phase: "running"}}, Requests: []int{1}}, nil
		}
		require.Empty(t, in.Unaccounted, "A is accounted for before it is stopped")
		return activities.LaunchObservation{}, nil
	})
	env.OnActivity(activities.NameCloseAttempt, mock.Anything, mock.MatchedBy(func(in activities.CloseAttemptInput) bool {
		return in.Outcome == "completed" && in.Cleanup == "complete" && in.CommandID == "att_1:close"
	})).Return(activities.CloseResult{Lifecycle: "succeeded"}, nil)
	env.ExecuteWorkflow(workflows.LocalCheckWorkflowName, input)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
	require.Equal(t, []string{"register:pod-b", "observe", "register:pod-b", "delete", "register:pod-a", "delete"}, sequence,
		"A's object is registered between the delete that found it and the delete that stops it")
	require.Equal(t, 2, deletes)
	env.AssertNumberOfCalls(t, activities.NameCloseAttempt, 1)
	env.AssertNotCalled(t, activities.NameCloseAttempt, mock.Anything, mock.MatchedBy(func(in activities.CloseAttemptInput) bool { return in.Cleanup == "unknown" }))
}

// The same interleaving when A never commits: without evidence for A the
// run never reports the cleanup complete, reaches the reconciliation bound
// and fails visibly under the original launch identity; the attempt stays
// closed with cleanup unknown and no settlement close is sent.
func TestLocalCheckUnansweredCreateNeverSeenFailsVisiblyAtTheBound(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	narrow := bounds
	narrow.ReconcileInitialInterval, narrow.ReconcileMaxInterval, narrow.ReconcileMaxDuration = 5*time.Second, 30*time.Second, 2*time.Minute
	registerWith(env, narrow)
	exit := int32(0)
	env.OnActivity(activities.NameOpenAttempt, mock.Anything, mock.Anything).Return(attempt, nil)
	env.OnActivity(activities.NamePrepareLaunch, mock.Anything, mock.Anything).Return(launch, nil)
	env.OnActivity(activities.NameCreateJob, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.CreateJobInput) (activities.JobRef, error) {
		if in.Request == 1 {
			return activities.JobRef{}, unansweredCreate
		}
		return activities.JobRef{JobUID: "job-b", Request: 2}, nil
	})
	env.OnActivity(activities.NameObserveJob, mock.Anything, mock.Anything).Return(activities.JobObservation{
		JobUID: "job-b", Pods: []activities.PodObservation{{PodUID: "pod-b", Phase: "succeeded", ExitCode: &exit, TerminationMessage: "{}"}},
	}, nil)
	env.OnActivity(activities.NameRegisterInstance, mock.Anything, mock.Anything).Return(activities.InstanceRef{InstanceID: "inst-b", Current: true}, nil)
	env.OnActivity(activities.NameObserveInstance, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(activities.NameVerifyResult, mock.Anything, mock.Anything).Return(activities.Verdict{Verdict: "certified", Manifest: []byte("{}"), ResultDigest: "sha256:aa"}, nil)
	env.OnActivity(activities.NameAcceptResult, mock.Anything, mock.Anything).Return(activities.StageRef{StageID: "stg-1"}, nil)
	observations := 0
	env.OnActivity(activities.NameObserveLaunch, mock.Anything, mock.Anything).Return(func(context.Context, activities.ObserveLaunchInput) (activities.LaunchObservation, error) {
		observations++
		if observations == 1 {
			return activities.LaunchObservation{JobUID: "job-b", Pods: []activities.PodObservation{{PodUID: "pod-b", Phase: "succeeded"}}, Requests: []int{2}}, nil
		}
		return activities.LaunchObservation{}, errors.New("job lc-abc create unresolved: neither the Job nor a Pod was observed within 1m0s, so the create is not known to have finished; cleanup stays unknown")
	})
	deletes := 0
	env.OnActivity(activities.NameDeleteJob, mock.Anything, mock.Anything).Return(func(context.Context, activities.DeleteJobInput) (activities.LaunchObservation, error) {
		deletes++
		return activities.LaunchObservation{}, nil
	})
	env.OnActivity(activities.NameCloseAttempt, mock.Anything, mock.MatchedBy(func(in activities.CloseAttemptInput) bool {
		return in.Cleanup == "unknown" && in.CommandID == "att_1:close" && in.Outcome == "completed"
	})).Return(activities.CloseResult{Lifecycle: "reconciling"}, nil)
	env.ExecuteWorkflow(workflows.LocalCheckWorkflowName, input)
	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Equal(t, "CLEANUP_UNCONFIRMED", activities.RefusalCode(err))
	require.Contains(t, err.Error(), "lc-abc")
	require.Equal(t, 1, deletes, "B was stopped once; absence after that is never re-read as completion")
	require.GreaterOrEqual(t, observations, 6, "every reconciliation round looked for A's object")
	env.AssertNumberOfCalls(t, activities.NameCloseAttempt, 1)
	env.AssertNotCalled(t, activities.NameCloseAttempt, mock.Anything, mock.MatchedBy(func(in activities.CloseAttemptInput) bool { return in.Cleanup == "complete" }))
	env.AssertNumberOfCalls(t, activities.NameCreateJob, 2)
	env.AssertNumberOfCalls(t, activities.NamePrepareLaunch, 1)
}

// A create request the launcher reports as not created (the API server
// rejected it, or it never reached the wire) needs no evidence: the retry
// creates the Job under the same launch key, and cleanup completes with the
// delete alone, without observing the key.
func TestLocalCheckNotCreatedRequestNeedsNoCleanupEvidence(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	register(env)
	exit := int32(0)
	env.OnActivity(activities.NameOpenAttempt, mock.Anything, mock.Anything).Return(attempt, nil)
	env.OnActivity(activities.NamePrepareLaunch, mock.Anything, mock.Anything).Return(launch, nil)
	env.OnActivity(activities.NameCreateJob, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.CreateJobInput) (activities.JobRef, error) {
		if in.Request == 1 {
			return activities.JobRef{}, activities.NotCreated(errors.New("dial tcp 10.96.0.1:443: connect: connection refused"))
		}
		return activities.JobRef{JobUID: "job-1", Request: 2}, nil
	})
	env.OnActivity(activities.NameObserveJob, mock.Anything, mock.Anything).Return(activities.JobObservation{
		JobUID: "job-1", Pods: []activities.PodObservation{{PodUID: "pod-1", Phase: "succeeded", ExitCode: &exit, TerminationMessage: "{}"}},
	}, nil)
	env.OnActivity(activities.NameRegisterInstance, mock.Anything, mock.Anything).Return(activities.InstanceRef{InstanceID: "inst-1", Current: true}, nil)
	env.OnActivity(activities.NameObserveInstance, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(activities.NameVerifyResult, mock.Anything, mock.Anything).Return(activities.Verdict{Verdict: "certified", Manifest: []byte("{}"), ResultDigest: "sha256:aa"}, nil)
	env.OnActivity(activities.NameAcceptResult, mock.Anything, mock.Anything).Return(activities.StageRef{StageID: "stg-1"}, nil)
	env.OnActivity(activities.NameDeleteJob, mock.Anything, mock.Anything).Return(activities.LaunchObservation{}, nil)
	env.OnActivity(activities.NameCloseAttempt, mock.Anything, mock.MatchedBy(func(in activities.CloseAttemptInput) bool {
		return in.Outcome == "completed" && in.Cleanup == "complete" && in.CommandID == "att_1:close"
	})).Return(activities.CloseResult{Lifecycle: "succeeded"}, nil)
	env.ExecuteWorkflow(workflows.LocalCheckWorkflowName, input)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
	env.AssertNumberOfCalls(t, activities.NameCreateJob, 2)
	env.AssertNumberOfCalls(t, activities.NameDeleteJob, 1)
	env.AssertNotCalled(t, activities.NameObserveLaunch, mock.Anything, mock.Anything)
}

// The create requests are bounded by the frozen retry cap and end in a
// visible outcome: when every request goes unanswered, the attempt fails
// and the cleanup, with every request unaccounted for, needs the launch
// key observed before anything completes.
func TestLocalCheckCreateRequestsAreBoundedByTheRetryCap(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	register(env)
	env.OnActivity(activities.NameOpenAttempt, mock.Anything, mock.Anything).Return(attempt, nil)
	env.OnActivity(activities.NamePrepareLaunch, mock.Anything, mock.Anything).Return(launch, nil)
	var requests []int
	env.OnActivity(activities.NameCreateJob, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.CreateJobInput) (activities.JobRef, error) {
		requests = append(requests, in.Request)
		return activities.JobRef{}, unansweredCreate
	})
	env.OnActivity(activities.NameObserveLaunch, mock.Anything, mock.Anything).Return(activities.LaunchObservation{}, errors.New("job lc-abc create unresolved: cleanup stays unknown"))
	env.OnActivity(activities.NameCloseAttempt, mock.Anything, mock.MatchedBy(func(in activities.CloseAttemptInput) bool {
		return in.Outcome == "infrastructure_failed" && in.FailureCode == "OBSERVER_FAILED" && in.Cleanup == "unknown"
	})).Return(activities.CloseResult{Lifecycle: "reconciling"}, nil)
	env.ExecuteWorkflow(workflows.LocalCheckWorkflowName, input)
	require.True(t, env.IsWorkflowCompleted())
	require.Equal(t, "CLEANUP_UNCONFIRMED", activities.RefusalCode(env.GetWorkflowError()))
	require.Equal(t, []int{1, 2, 3, 4, 5, 6}, requests, "one request per retry, numbered, up to the frozen cap")
	env.AssertNotCalled(t, activities.NameDeleteJob, mock.Anything, mock.Anything)
}

// The bounds are frozen with the run: the Activities receive the values the
// run recorded, not the worker's live configuration.
func TestLocalCheckBoundsAreFrozenWithTheRun(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	custom := bounds
	custom.UnresolvedSettleWindow = 90 * time.Second
	custom.LaunchWindow = 45 * time.Second
	registerWith(env, custom)
	env.OnActivity(activities.NameOpenAttempt, mock.Anything, mock.Anything).Return(attempt, nil)
	env.OnActivity(activities.NamePrepareLaunch, mock.Anything, mock.Anything).Return(launch, nil)
	env.OnActivity(activities.NameCreateJob, mock.Anything, mock.Anything).Return(func(ctx context.Context, in activities.CreateJobInput) (activities.JobRef, error) {
		<-ctx.Done()
		return activities.JobRef{}, ctx.Err()
	})
	env.OnActivity(activities.NameObserveLaunch, mock.Anything, mock.MatchedBy(func(in activities.ObserveLaunchInput) bool {
		return in.SettleWindow == 90*time.Second
	})).Return(activities.LaunchObservation{JobUID: "job-1", Requests: []int{1}}, nil)
	env.OnActivity(activities.NameDeleteJob, mock.Anything, mock.Anything).Return(activities.LaunchObservation{}, nil)
	env.OnActivity(activities.NameCloseAttempt, mock.Anything, mock.Anything).Return(activities.CloseResult{Lifecycle: "canceled"}, nil)
	env.RegisterDelayedCallback(func() { env.CancelWorkflow() }, 2*time.Second)
	env.ExecuteWorkflow(workflows.LocalCheckWorkflowName, input)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
	env.AssertCalled(t, activities.NamePrepareLaunch, mock.Anything, mock.MatchedBy(func(in activities.PrepareLaunchInput) bool {
		return in.Deadline.Sub(env.Now()) <= 45*time.Second
	}))
}
