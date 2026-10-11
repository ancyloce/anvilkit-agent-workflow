package modelproxy_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/adapters/modelproxy"
)

// P1.3 AC4 (MP-04): the Proxy answers an admission under Control capacity
// exhaustion with a retryable envelope; the CallModel Activity, under its
// retry policy, retries the same call id and succeeds once capacity returns.
// One admission chain, one call id, the Proxy counts three POSTs: two
// refused before anything was asked of Control, one that sends.
func TestCallModelRetriesRetryableAnswersUnderTheActivityPolicy(t *testing.T) {
	f := newFakeProxy(t)
	f.frames = []string{
		`{"callId":"call_01J9ABCDEF","sequence":"0","type":"admitted"}`,
		`{"callId":"call_01J9ABCDEF","sequence":"1","type":"done","outcome":"succeeded","usage":{"inputUnits":"12","outputUnits":"5","reasoningUnits":"0","cachedInputUnits":"0"}}`,
	}
	f.retryableUntil.Store(2)
	c, err := modelproxy.New(modelproxy.Options{BaseURL: f.srv.URL, Token: "t", Timeout: 10 * time.Second})
	require.NoError(t, err)
	acts := &activities.Activities{Model: c}

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(acts.CallModel, activity.RegisterOptions{Name: activities.NameCallModel})
	callUnderPolicy := func(ctx workflow.Context, in activities.ModelCallInput) (activities.ModelCallResult, error) {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
			RetryPolicy:         &temporal.RetryPolicy{InitialInterval: 50 * time.Millisecond, BackoffCoefficient: 1, MaximumAttempts: 5},
		})
		var out activities.ModelCallResult
		err := workflow.ExecuteActivity(ctx, activities.NameCallModel, in).Get(ctx, &out)
		return out, err
	}
	env.RegisterWorkflow(callUnderPolicy)
	in := input("call_01J9ABCDEF")
	env.ExecuteWorkflow(callUnderPolicy, in)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var res activities.ModelCallResult
	require.NoError(t, env.GetWorkflowResult(&res))
	require.Equal(t, "succeeded", res.State)
	require.Equal(t, int32(3), f.posts.Load(), "two retryable refusals, then the one admitted send; the same call id throughout")

	// A non-retryable refusal is not retried: one POST, the Activity fails with the code.
	f2 := newFakeProxy(t)
	f2.status, f2.envelope = 403, `{"error":{"code":"STALE_EXECUTION","message":"the call deadline has passed","requestId":"req_1","retryable":false}}`
	c2, err := modelproxy.New(modelproxy.Options{BaseURL: f2.srv.URL, Token: "t", Timeout: 10 * time.Second})
	require.NoError(t, err)
	acts2 := &activities.Activities{Model: c2}
	env2 := suite.NewTestWorkflowEnvironment()
	env2.RegisterActivityWithOptions(acts2.CallModel, activity.RegisterOptions{Name: activities.NameCallModel})
	env2.RegisterWorkflow(callUnderPolicy)
	env2.ExecuteWorkflow(callUnderPolicy, in)
	require.True(t, env2.IsWorkflowCompleted())
	require.Error(t, env2.GetWorkflowError())
	require.Equal(t, int32(1), f2.posts.Load(), "a refusal of the call is final under the policy")
}
