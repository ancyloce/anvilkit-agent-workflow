package workflows

import (
	"context"
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestCloseWaitsForResolvedObligationsUnderOriginalIdentity(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var calls []activities.CloseAttemptInput
	env.RegisterActivityWithOptions(func(_ context.Context, in activities.CloseAttemptInput) (activities.CloseResult, error) {
		calls = append(calls, in)
		if len(calls) == 1 {
			return activities.CloseResult{Lifecycle: "reconciling"}, nil
		}
		return activities.CloseResult{Lifecycle: "canceled"}, nil
	}, activity.RegisterOptions{Name: activities.NameCloseAttempt})
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		return closeAttempt(ctx, Queues{Control: "control"}, Bounds{ControlActivityTimeout: time.Second, ControlRetryInitial: time.Second, ControlRetryMaxInterval: time.Second, ReconcileInitialInterval: time.Second}, activities.AttemptRef{AttemptID: "att_original"}, "cmd_original", "canceled", "complete", "")
	})
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, calls, 2)
	require.Equal(t, calls[0], calls[1])
}
