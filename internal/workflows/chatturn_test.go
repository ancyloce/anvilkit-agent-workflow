package workflows_test

import (
	"context"
	"errors"
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
	chatInput   = workflows.Input{OperationID: "op_CHAT", TenantID: "tenant_a"}
	chatAttempt = activities.AttemptRef{AttemptID: "att_c1", TenantID: "tenant_a", ProfileID: "chat-turn-v1", ExecutionEpoch: "1"}
)

// chatEnv wires the chat turn with its funding, attempt and the scripted
// reply (answered after delay, in test time); it answers the recorder of
// settles and closes and the reply inputs.
func chatEnv(t *testing.T, fundingErr error, delay time.Duration, reply func(in activities.ChatReplyInput) (activities.ChatReplyResult, error)) (*testsuite.TestWorkflowEnvironment, *settled, *[]activities.ChatReplyInput, *[]activities.OpenAttemptInput) {
	t.Helper()
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	lb := lifecycleBounds
	lb.ChatRouteID, lb.ChatMaxOutputTokens, lb.ChatCallTimeout = "controlled-chat-v1", 4096, 110*time.Second
	lb.ChatMaxExposure = activities.Money{Currency: "USD", Amount: "100000"}
	registerLifecycle(env, lb)
	var a lifecycleStub
	env.RegisterWorkflowWithOptions(workflows.ChatTurn(queues, bounds, lb), workflow.RegisterOptions{Name: workflows.ChatTurnWorkflowName})
	env.RegisterActivityWithOptions(a.ChatReply, activity.RegisterOptions{Name: activities.NameChatReply})
	s := &settled{}
	s.record(env)
	env.OnActivity(activities.NameRecordFunding, mock.Anything, mock.MatchedBy(func(in activities.RecordFundingInput) bool { return in.CommandID == "op_CHAT:funding" })).Return(activities.FundingRef{}, fundingErr)
	opens := &[]activities.OpenAttemptInput{}
	env.OnActivity(activities.NameOpenAttempt, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.OpenAttemptInput) (activities.AttemptRef, error) {
		*opens = append(*opens, in)
		at := chatAttempt
		at.Deadline = env.Now().Add(120 * time.Second)
		return at, nil
	})
	replies := &[]activities.ChatReplyInput{}
	env.OnActivity(activities.NameChatReply, mock.Anything, mock.Anything).After(delay).Return(func(_ context.Context, in activities.ChatReplyInput) (activities.ChatReplyResult, error) {
		*replies = append(*replies, in)
		return reply(in)
	})
	return env, s, replies, opens
}

// TestChatTurnWorkflow (DEC-068, design 0001 §9.2): the turn is funded once,
// its Job-less reply attempt opened under the chat profile, the chat
// Activity run with the chat route and a deadline inside the attempt, and
// the operation settled from the turn's state; the attempt closes before the
// settlement.
func TestChatTurnWorkflow(t *testing.T) {
	for name, tc := range map[string]struct {
		reply        activities.ChatReplyResult
		outcome      string
		failureCode  string
		closeOutcome string
	}{
		"a whole reply": {activities.ChatReplyResult{TurnID: "turn_1", TurnState: "completed", SenderStatus: "stopped", CallState: "succeeded", ReplyBytes: 38},
			"succeeded", "", "completed"},
		"a partial failure": {activities.ChatReplyResult{TurnID: "turn_1", TurnState: "failed", FailureCode: "CONTENT_UNRECOVERABLE", SenderStatus: "stopped", CallState: "unknown"},
			"failed", "CONTENT_UNRECOVERABLE", "failed"},
		"a stop the Activity recorded": {activities.ChatReplyResult{TurnID: "turn_1", TurnState: "canceled", FailureCode: "CANCELED", SenderStatus: "active"},
			"canceled", "CANCELED", "canceled"},
		"an unknown sender": {activities.ChatReplyResult{TurnID: "turn_1", TurnState: "reconciling", FailureCode: "SENDER_LOST", SenderStatus: "unknown", CallState: "unknown"},
			"failed", "SENDER_LOST", "infrastructure_failed"},
	} {
		t.Run(name, func(t *testing.T) {
			env, s, replies, opens := chatEnv(t, nil, 0, func(activities.ChatReplyInput) (activities.ChatReplyResult, error) { return tc.reply, nil })
			start := env.Now()
			env.ExecuteWorkflow(workflows.ChatTurnWorkflowName, chatInput)
			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())

			require.Equal(t, []activities.OpenAttemptInput{{OperationID: "op_CHAT", TenantID: "tenant_a", CommandID: "op_CHAT:reply:open", StepID: "reply", Visit: 0, ProfileID: "chat-turn-v1"}}, *opens)
			require.Len(t, *replies, 1)
			in := (*replies)[0]
			require.Equal(t, activities.ChatReplyInput{OperationID: "op_CHAT", TenantID: "tenant_a", AttemptID: "att_c1", ExecutionEpoch: 1, RouteID: "controlled-chat-v1",
				MaxOutputTokens: 4096, MaxExposure: activities.Money{Currency: "USD", Amount: "100000"}, Deadline: in.Deadline}, in)
			require.WithinDuration(t, start.Add(110*time.Second), in.Deadline, time.Second, "the call timeout, inside the 120 s attempt")

			require.Len(t, s.closes, 1)
			require.Equal(t, "att_c1:close", s.closes[0].CommandID)
			require.Equal(t, tc.closeOutcome, s.closes[0].Outcome)
			require.Equal(t, tc.failureCode, s.closes[0].FailureCode)
			last := s.last()
			require.Equal(t, "op_CHAT:settle", last.CommandID)
			require.Equal(t, tc.outcome, last.Outcome)
			require.Equal(t, tc.failureCode, last.FailureCode)
		})
	}
}

// TestChatTurnWorkflowStop: a stop is the Workflow's cancellation; the run
// settles canceled after closing the attempt.
func TestChatTurnWorkflowStop(t *testing.T) {
	env, s, _, _ := chatEnv(t, nil, time.Minute, func(activities.ChatReplyInput) (activities.ChatReplyResult, error) {
		return activities.ChatReplyResult{TurnID: "turn_1", TurnState: "completed"}, nil
	})
	env.RegisterDelayedCallback(func() { env.CancelWorkflow() }, 5*time.Second)
	env.ExecuteWorkflow(workflows.ChatTurnWorkflowName, chatInput)
	require.True(t, env.IsWorkflowCompleted())
	require.Len(t, s.closes, 1)
	require.Equal(t, "canceled", s.closes[0].Outcome)
	require.Equal(t, "canceled", s.last().Outcome)
	require.Equal(t, "CANCELED", s.last().FailureCode)
}

// TestChatTurnWorkflowRefusals: refused funding settles without an attempt
// or a call; a refused reply Activity closes the attempt and settles failed
// with its code.
func TestChatTurnWorkflowRefusals(t *testing.T) {
	env, s, replies, opens := chatEnv(t, activities.Refused("BUDGET_EXHAUSTED", errors.New("the tenant cap")), 0, nil)
	env.ExecuteWorkflow(workflows.ChatTurnWorkflowName, chatInput)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Empty(t, *opens)
	require.Empty(t, *replies)
	require.Empty(t, s.closes)
	require.Equal(t, "failed", s.last().Outcome)
	require.Equal(t, "BUDGET_EXHAUSTED", s.last().FailureCode)

	env, s, _, _ = chatEnv(t, nil, 0, func(activities.ChatReplyInput) (activities.ChatReplyResult, error) {
		return activities.ChatReplyResult{}, activities.Refused("STALE_EXECUTION", errors.New("another epoch"))
	})
	env.ExecuteWorkflow(workflows.ChatTurnWorkflowName, chatInput)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, s.closes, 1)
	require.Equal(t, "failed", s.closes[0].Outcome)
	require.Equal(t, "STALE_EXECUTION", s.closes[0].FailureCode)
	require.Equal(t, "failed", s.last().Outcome)
	require.Equal(t, "STALE_EXECUTION", s.last().FailureCode)
}
