package workflows

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// eventsPerPoll bounds the history events of one settle poll: the
// Activity (scheduled, started, completed), the timer (started, fired) and
// the Workflow Task each of them wakes (scheduled, started, completed).
const eventsPerPoll = 3 + 2 + 3 + 3

// TestSettlementRollsOverAcrossThirtyDays is P1.6 AC1: Control keeps the
// operation reconciling for 30 simulated days. Every run re-polls the same
// settle command with capped backoff and continues as new at the poll
// bound, so no run's history approaches the event limit; when Control
// resolves, the last run completes. Each run is executed from the carry the
// previous one continued with (time skipping in the test environment).
func TestSettlementRollsOverAcrossThirtyDays(t *testing.T) {
	b := Bounds{
		ControlActivityTimeout: 30 * time.Second, ControlRetryInitial: time.Second, ControlRetryMaxInterval: 30 * time.Second,
		ReconcileInitialInterval: 5 * time.Second, SettleMaxInterval: 5 * time.Minute, SettlePollBound: 500,
	}
	const resolveAfter = 30 * 24 * time.Hour
	const eventLimit = 10_000
	carry := SettleCarry{CommandID: "op_S:settle", Outcome: "succeeded", Phase: "candidate_ready"}
	in := Input{OperationID: "op_S", TenantID: "tenant_a", Settle: &carry}
	var elapsed time.Duration
	runs, totalPolls, maxEvents := 0, 0, 0
	for {
		runs++
		require.Less(t, runs, 100, "the chain ends")
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		start := env.Now()
		polls := 0
		env.RegisterActivityWithOptions(func(_ context.Context, got activities.SettleOperationInput) (activities.OperationRef, error) {
			polls++
			require.Equal(t, activities.SettleOperationInput{OperationID: "op_S", TenantID: "tenant_a", CommandID: "op_S:settle", Outcome: "succeeded", Phase: "candidate_ready"}, got, "every poll is the original command")
			if elapsed+env.Now().Sub(start) >= resolveAfter {
				return activities.OperationRef{Lifecycle: "succeeded", Phase: "candidate_ready"}, nil
			}
			return activities.OperationRef{Lifecycle: "reconciling"}, nil
		}, activity.RegisterOptions{Name: activities.NameSettleOperation})
		env.RegisterWorkflowWithOptions(func(ctx workflow.Context, in Input) error {
			return settleOnly(ctx, Queues{Business: "business", Control: "control"}, b, in)
		}, workflow.RegisterOptions{Name: GenerationWorkflowName})
		env.ExecuteWorkflow(GenerationWorkflowName, in)
		require.True(t, env.IsWorkflowCompleted())
		elapsed += env.Now().Sub(start)
		totalPolls += polls
		events := eventsPerPoll*polls + 10
		maxEvents = max(maxEvents, events)
		require.Less(t, events, eventLimit, "run %d stays under the event limit", runs)
		err := env.GetWorkflowError()
		if err == nil {
			break
		}
		var can *workflow.ContinueAsNewError
		require.ErrorAs(t, err, &can, "run %d ends only by continuing as new", runs)
		require.Equal(t, GenerationWorkflowName, can.WorkflowType.Name, "the settlement continues as the same workflow type")
		require.LessOrEqual(t, polls, int(b.SettlePollBound)+1)
		var next Input
		require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(can.Input, &next))
		require.NotNil(t, next.Settle)
		require.Equal(t, b.SettleMaxInterval, next.Settle.Interval, "the backoff resumes at its cap")
		in = next
	}
	require.GreaterOrEqual(t, elapsed, resolveAfter)
	t.Logf("P1.6 AC1: %d runs over %s simulated, %d settle polls, at most %d events in one run (bound %d per poll + 10; limit %d)",
		runs, elapsed.Round(time.Minute), totalPolls, maxEvents, eventsPerPoll, eventLimit)
	require.Greater(t, runs, 1, "the settlement continued as new")
}

// TestSettlementRollsOverWhenTheServerSuggests: the server's
// continue-as-new suggestion ends the run before the poll bound.
func TestSettlementRollsOverWhenTheServerSuggests(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	polls := 0
	env.RegisterActivityWithOptions(func(context.Context, activities.SettleOperationInput) (activities.OperationRef, error) {
		polls++
		if polls == 3 {
			env.SetContinueAsNewSuggested(true)
		}
		return activities.OperationRef{Lifecycle: "reconciling"}, nil
	}, activity.RegisterOptions{Name: activities.NameSettleOperation})
	b := Bounds{ControlActivityTimeout: 30 * time.Second, ControlRetryInitial: time.Second, ControlRetryMaxInterval: 30 * time.Second, ReconcileInitialInterval: 5 * time.Second, SettleMaxInterval: time.Minute, SettlePollBound: 500}
	env.RegisterWorkflowWithOptions(func(ctx workflow.Context, in Input) error {
		return settleOnly(ctx, Queues{Control: "control"}, b, in)
	}, workflow.RegisterOptions{Name: PreparationWorkflowName})
	env.ExecuteWorkflow(PreparationWorkflowName, Input{OperationID: "op_P", TenantID: "tenant_a", Settle: &SettleCarry{CommandID: "op_P:settle", Outcome: "failed"}})
	var can *workflow.ContinueAsNewError
	require.ErrorAs(t, env.GetWorkflowError(), &can)
	require.Equal(t, 3, polls)
}
