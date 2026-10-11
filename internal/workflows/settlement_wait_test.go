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

// TestFinalCloseGoesStraightToTheSettlement (P1.6): a multi-step run's
// close that ends its work does not wait while Control reconciles, so its
// settlement stores the intent at once; a close after a completed step
// still waits for the obligations the next step depends on.
func TestFinalCloseGoesStraightToTheSettlement(t *testing.T) {
	for _, c := range []struct {
		outcome string
		closes  int
	}{{"canceled", 1}, {"failed", 1}, {"completed", 2}} {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		var calls int
		env.RegisterActivityWithOptions(func(_ context.Context, in activities.CloseAttemptInput) (activities.CloseResult, error) {
			calls++
			if calls == 1 {
				return activities.CloseResult{Lifecycle: "reconciling"}, nil
			}
			return activities.CloseResult{Lifecycle: "running"}, nil
		}, activity.RegisterOptions{Name: activities.NameCloseAttempt})
		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			return closeForSettlement(ctx, Queues{Control: "control"}, Bounds{ControlActivityTimeout: time.Second, ControlRetryInitial: time.Second, ControlRetryMaxInterval: time.Second, ReconcileInitialInterval: time.Second}, activities.AttemptRef{AttemptID: "att_1"}, "att_1:close", c.outcome, "not_required", "")
		})
		require.NoError(t, env.GetWorkflowError(), c.outcome)
		require.Equal(t, c.closes, calls, c.outcome)
	}
}
