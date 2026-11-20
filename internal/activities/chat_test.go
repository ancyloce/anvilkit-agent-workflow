package activities_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// TestReplyOutcomeRules (DEC-068, design 0001 §7.4) pins the record each
// ending of the reply call produces: a final frame proves the Proxy's sender
// stopped; a reclaim without a fence leaves it unknown; a fenced reclaim or
// an unknown settlement seals the shown prefix as CONTENT_UNRECOVERABLE.
func TestReplyOutcomeRules(t *testing.T) {
	text := "Black, with a search box."
	for name, tc := range map[string]struct {
		res  activities.ModelCallResult
		want activities.ReplyRecord
	}{
		"a whole reply": {activities.ModelCallResult{State: "succeeded", Text: text},
			activities.ReplyRecord{Outcome: "completed", HasText: true, Text: text, Sender: "stopped"}},
		"an empty reply": {activities.ModelCallResult{State: "succeeded"},
			activities.ReplyRecord{Outcome: "failed", Sender: "stopped", FailureCode: "EMPTY_REPLY"}},
		"a tool call on the tool-less route": {activities.ModelCallResult{State: "succeeded", Text: text, ToolCalls: []activities.ModelToolCall{{Name: "read_brief"}}},
			activities.ReplyRecord{Outcome: "partial_failed", HasText: true, Text: text, Sender: "stopped", FailureCode: "UNEXPECTED_TOOL_CALL"}},
		"a failed call keeps its prefix": {activities.ModelCallResult{State: "failed", Text: "Black", ErrorCode: "OUTPUT_LIMIT"},
			activities.ReplyRecord{Outcome: "partial_failed", HasText: true, Text: "Black", Sender: "stopped", FailureCode: "OUTPUT_LIMIT"}},
		"a failed call without text": {activities.ModelCallResult{State: "failed"},
			activities.ReplyRecord{Outcome: "failed", Sender: "stopped", FailureCode: "MODEL_CALL_FAILED"}},
		"a canceled call": {activities.ModelCallResult{State: "canceled", Text: "Bl"},
			activities.ReplyRecord{Outcome: "canceled", HasText: true, Text: "Bl", Sender: "stopped", FailureCode: "CANCELED"}},
		"a fenced reclaim": {activities.ModelCallResult{State: "unknown", ErrorCode: "SENDER_LOST", Fenced: true, Text: "Bl"},
			activities.ReplyRecord{Outcome: "partial_failed", HasText: true, Text: "Bl", Sender: "stopped", FailureCode: "CONTENT_UNRECOVERABLE"}},
		"an unfenced reclaim": {activities.ModelCallResult{State: "unknown", ErrorCode: "SENDER_LOST"},
			activities.ReplyRecord{Outcome: "failed", Sender: "unknown", FailureCode: "SENDER_LOST"}},
		"an unknown settlement of the sender itself": {activities.ModelCallResult{State: "unknown", ErrorCode: "UPSTREAM_STREAM_ABORTED"},
			activities.ReplyRecord{Outcome: "failed", Sender: "stopped", FailureCode: "CONTENT_UNRECOVERABLE"}},
		"text with NUL is not stored": {activities.ModelCallResult{State: "succeeded", Text: "a\x00b"},
			activities.ReplyRecord{Outcome: "failed", Sender: "stopped", FailureCode: "CONTENT_INVALID"}},
	} {
		require.Equal(t, tc.want, activities.ReplyOf(tc.res), name)
	}

	long := strings.Repeat("a", activities.MaxReplyBytes-1) + "é" // the rune straddles the bound
	got := activities.ReplyOf(activities.ModelCallResult{State: "succeeded", Text: long})
	require.Equal(t, "partial_failed", got.Outcome)
	require.Equal(t, "REPLY_TOO_LONG", got.FailureCode)
	require.Equal(t, strings.Repeat("a", activities.MaxReplyBytes-1), got.Text, "cut at the rune boundary under the bound")

	require.Equal(t, activities.ReplyRecord{Outcome: "failed", Sender: "active", FailureCode: "INVALID_ARGUMENT"},
		activities.RefusedReply(activities.ModelCallResult{State: "sending"}, "INVALID_ARGUMENT"), "a refused stream of a send still running keeps the sender active")
	require.Equal(t, activities.ReplyRecord{Outcome: "partial_failed", HasText: true, Text: "Bl", Sender: "stopped", FailureCode: "INVALID_ARGUMENT"},
		activities.RefusedReply(activities.ModelCallResult{State: "succeeded", Text: "Bl"}, "INVALID_ARGUMENT"))
	require.Equal(t, activities.ReplyRecord{Outcome: "canceled", Sender: "active", FailureCode: "CANCELED"}, activities.Canceled(""))
}

type fakeTurns struct {
	mu       sync.Mutex
	claim    activities.TurnClaim
	claimErr error
	claims   []activities.ClaimReplyWriterInput
	context  activities.TurnContext
	records  []activities.RecordReplyOutcomeInput
	// recordCtxErr is the record's context error, to prove a stop records
	// under a live context.
	recordCtxErr error
}

func (f *fakeTurns) ClaimReplyWriter(_ context.Context, in activities.ClaimReplyWriterInput) (activities.TurnClaim, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims = append(f.claims, in)
	return f.claim, f.claimErr
}

func (f *fakeTurns) GetTurnContext(context.Context, activities.GetTurnContextInput) (activities.TurnContext, error) {
	return f.context, nil
}

func (f *fakeTurns) RecordReplyOutcome(ctx context.Context, in activities.RecordReplyOutcomeInput) (activities.TurnRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = append(f.records, in)
	f.recordCtxErr = ctx.Err()
	state := map[string]string{"completed": "completed", "partial_failed": "failed", "failed": "failed", "canceled": "canceled"}[in.Reply.Outcome]
	if in.Reply.Sender == "unknown" {
		state = "reconciling"
	}
	return activities.TurnRef{TurnID: in.TurnID, State: state, SenderStatus: in.Reply.Sender, FailureCode: in.Reply.FailureCode}, nil
}

type fakeModel struct {
	call  func(ctx context.Context, in activities.ModelCallInput) (activities.ModelCallResult, error)
	query func(callID string) (activities.ModelCallResult, error)
	calls []activities.ModelCallInput
}

func (m *fakeModel) CallModel(ctx context.Context, in activities.ModelCallInput, heartbeat func()) (activities.ModelCallResult, error) {
	m.calls = append(m.calls, in)
	heartbeat()
	return m.call(ctx, in)
}

func (m *fakeModel) GetModelCall(_ context.Context, callID string) (activities.ModelCallResult, error) {
	return m.query(callID)
}

func (m *fakeModel) CancelModelCall(context.Context, string) (activities.ModelCallResult, error) {
	panic("a stopped reply detaches; it never cancels the call (DEC-068 Q3)")
}

var (
	replyIn = activities.ChatReplyInput{
		OperationID: "op_T", TenantID: "tenant_a", AttemptID: "att_1", ExecutionEpoch: 1, RouteID: "controlled-chat-v1",
		MaxOutputTokens: 4096, MaxExposure: activities.Money{Currency: "USD", Amount: "100000"}, Deadline: time.Now().Add(time.Minute),
	}
	firstInvocation = activities.ReplyInvocation{ActivityKey: "op_T/run_1/chat-reply", Attempt: 1, InvocationID: "inv_1"}
	openClaim       = activities.TurnClaim{Turn: activities.TurnRef{TurnID: "turn_1", State: "running", SenderStatus: "active"}, WriterEpoch: 1}
	turnContext     = activities.TurnContext{Digest: "sha256:ctx", Messages: []activities.TurnMessage{
		{Role: "user", State: "completed", Text: "Make it black"},
		{Role: "assistant", State: "completed", Text: "Done in the next version."},
		{Role: "user", State: "completed", Text: "And a search box"},
	}}
)

func chatActivities(turns *fakeTurns, model *fakeModel) *activities.LifecycleActivities {
	return &activities.LifecycleActivities{Conversations: turns, Model: model}
}

// TestChatReplyRecordsTheReply: the writer is claimed under the trusted
// invocation, the context goes to the single reply call after the fixed
// instruction, without tools, and the whole text is recorded under the
// writer epoch; only the state and the byte count come back.
func TestChatReplyRecordsTheReply(t *testing.T) {
	turns := &fakeTurns{claim: openClaim, context: turnContext}
	model := &fakeModel{call: func(context.Context, activities.ModelCallInput) (activities.ModelCallResult, error) {
		return activities.ModelCallResult{CallID: "op_T:reply:1", State: "succeeded", Text: "Black, with a search box on the right."}, nil
	}}
	beats := 0
	got, err := chatActivities(turns, model).ChatReplyAs(t.Context(), replyIn, firstInvocation, func() { beats++ })
	require.NoError(t, err)
	require.Equal(t, activities.ChatReplyResult{TurnID: "turn_1", TurnState: "completed", SenderStatus: "stopped", CallState: "succeeded", ReplyBytes: 38}, got)
	require.Equal(t, 1, beats)

	require.Equal(t, []activities.ClaimReplyWriterInput{{
		Binding:   activities.ReplyBinding{OperationID: "op_T", TenantID: "tenant_a", AttemptID: "att_1", ExecutionEpoch: 1},
		CommandID: "op_T:reply:claim:1", ActivityKey: "op_T/run_1/chat-reply", TemporalAttempt: 1, InvocationID: "inv_1",
	}}, turns.claims)
	require.Len(t, model.calls, 1)
	call := model.calls[0]
	require.Equal(t, "op_T:reply:1", call.CallID)
	require.Equal(t, "controlled-chat-v1", call.RouteID)
	require.Empty(t, call.Tools, "the chat route is tool-less")
	require.Equal(t, replyIn.Deadline, call.Deadline, "the original deadline")
	require.Equal(t, "system", call.Messages[0].Role)
	require.Equal(t, []activities.ModelMessage{{Role: "user", Content: "Make it black"}, {Role: "assistant", Content: "Done in the next version."}, {Role: "user", Content: "And a search box"}}, call.Messages[1:])

	require.Len(t, turns.records, 1)
	rec := turns.records[0]
	require.Equal(t, "op_T:reply:outcome:1", rec.CommandID)
	require.Equal(t, "turn_1", rec.TurnID)
	require.Equal(t, uint64(1), rec.WriterEpoch)
	require.Equal(t, "op_T:reply:1", rec.CallID)
	require.Equal(t, activities.ReplyRecord{Outcome: "completed", HasText: true, Text: "Black, with a search box on the right.", Sender: "stopped"}, rec.Reply)
}

// TestChatReplySealedTurnAnswersItself: a retry after a lost answer finds the
// turn sealed at the claim and calls nothing.
func TestChatReplySealedTurnAnswersItself(t *testing.T) {
	turns := &fakeTurns{claim: activities.TurnClaim{Turn: activities.TurnRef{TurnID: "turn_1", State: "completed", SenderStatus: "stopped"}, WriterEpoch: 1, Existing: true}}
	model := &fakeModel{}
	got, err := chatActivities(turns, model).ChatReplyAs(t.Context(), replyIn, activities.ReplyInvocation{ActivityKey: "k", Attempt: 2, InvocationID: "inv_2"}, func() {})
	require.NoError(t, err)
	require.Equal(t, activities.ChatReplyResult{TurnID: "turn_1", TurnState: "completed", SenderStatus: "stopped", Existing: true}, got)
	require.Empty(t, model.calls)
	require.Empty(t, turns.records)
}

// TestChatReplyStop (DEC-068 Q3): a stop during the call detaches without a
// cancel of the call (the fake panics on one) and seals the shown prefix as
// canceled with the sender still metering; the record is made under a live
// context. A stop before the first attempt's call records a sender that
// never started; a later attempt asks the call's record first.
func TestChatReplyStop(t *testing.T) {
	turns := &fakeTurns{claim: openClaim, context: turnContext}
	ctx, cancel := context.WithCancelCause(t.Context())
	model := &fakeModel{call: func(context.Context, activities.ModelCallInput) (activities.ModelCallResult, error) {
		cancel(temporal.NewCanceledError())
		return activities.ModelCallResult{Text: "Black, with"}, context.Canceled
	}}
	got, err := chatActivities(turns, model).ChatReplyAs(ctx, replyIn, firstInvocation, func() {})
	require.NoError(t, err)
	require.Equal(t, "canceled", got.TurnState)
	require.Equal(t, activities.ReplyRecord{Outcome: "canceled", HasText: true, Text: "Black, with", Sender: "active", FailureCode: "CANCELED"}, turns.records[0].Reply)
	require.NoError(t, turns.recordCtxErr, "the outcome is recorded although the Activity was canceled")

	before, cancelBefore := context.WithCancelCause(t.Context())
	cancelBefore(temporal.NewCanceledError())
	turns = &fakeTurns{claim: openClaim, context: turnContext}
	model = &fakeModel{}
	_, err = chatActivities(turns, model).ChatReplyAs(before, replyIn, firstInvocation, func() {})
	require.NoError(t, err)
	require.Empty(t, model.calls)
	require.Equal(t, activities.ReplyRecord{Outcome: "canceled", Sender: "not_started", FailureCode: "CANCELED"}, turns.records[0].Reply)

	for name, tc := range map[string]struct {
		query  func(string) (activities.ModelCallResult, error)
		sender string
	}{
		"the call was never made":   {func(string) (activities.ModelCallResult, error) { return activities.ModelCallResult{}, activities.Refused("NOT_FOUND", errors.New("no call")) }, "not_started"},
		"the call settled":          {func(string) (activities.ModelCallResult, error) { return activities.ModelCallResult{State: "succeeded"}, nil }, "stopped"},
		"the call is still sending": {func(string) (activities.ModelCallResult, error) { return activities.ModelCallResult{State: "sending"}, nil }, "active"},
	} {
		turns = &fakeTurns{claim: openClaim, context: turnContext}
		model = &fakeModel{query: tc.query}
		_, err = chatActivities(turns, model).ChatReplyAs(before, replyIn, activities.ReplyInvocation{ActivityKey: "k", Attempt: 2, InvocationID: "inv_2"}, func() {})
		require.NoError(t, err, name)
		require.Equal(t, tc.sender, turns.records[0].Reply.Sender, name)
		require.Equal(t, "canceled", turns.records[0].Reply.Outcome, name)
	}
}

// TestChatReplyFailures: a transport failure or a timeout is returned for the
// retry, which re-enters the same call, and records nothing; a worker's own
// shutdown is no stop; a refusal before anything was sent records a sender
// that never started.
func TestChatReplyFailures(t *testing.T) {
	turns := &fakeTurns{claim: openClaim, context: turnContext}
	model := &fakeModel{call: func(context.Context, activities.ModelCallInput) (activities.ModelCallResult, error) {
		return activities.ModelCallResult{Text: "Bl"}, errors.New("model proxy stream interrupted after 2 frames")
	}}
	_, err := chatActivities(turns, model).ChatReplyAs(t.Context(), replyIn, firstInvocation, func() {})
	require.ErrorContains(t, err, "interrupted")
	require.Empty(t, turns.records)

	shutdown, stop := context.WithCancelCause(t.Context())
	model.call = func(context.Context, activities.ModelCallInput) (activities.ModelCallResult, error) {
		stop(errors.New("worker drain bound elapsed"))
		return activities.ModelCallResult{}, context.Canceled
	}
	_, err = chatActivities(turns, model).ChatReplyAs(shutdown, replyIn, firstInvocation, func() {})
	require.Error(t, err)
	require.Empty(t, turns.records, "an abandoned Activity is retried, not sealed as a stop")

	model = &fakeModel{
		call: func(context.Context, activities.ModelCallInput) (activities.ModelCallResult, error) {
			return activities.ModelCallResult{}, activities.Refused("BUDGET_EXHAUSTED", errors.New("denied"))
		},
		query: func(string) (activities.ModelCallResult, error) {
			return activities.ModelCallResult{}, activities.Refused("NOT_FOUND", errors.New("no call"))
		},
	}
	got, err := chatActivities(turns, model).ChatReplyAs(t.Context(), replyIn, firstInvocation, func() {})
	require.NoError(t, err)
	require.Equal(t, "failed", got.TurnState)
	require.Equal(t, activities.ReplyRecord{Outcome: "failed", Sender: "not_started", FailureCode: "BUDGET_EXHAUSTED"}, turns.records[0].Reply)

	turns.claimErr = activities.Refused("STALE_EXECUTION", errors.New("another epoch"))
	_, err = chatActivities(turns, model).ChatReplyAs(t.Context(), replyIn, firstInvocation, func() {})
	require.Equal(t, "STALE_EXECUTION", activities.RefusalCode(err))
	require.Len(t, turns.records, 1, "a stale writer records nothing")

	_, err = (&activities.LifecycleActivities{Model: model}).ChatReplyAs(t.Context(), replyIn, firstInvocation, func() {})
	require.Equal(t, "PROFILE_UNQUALIFIED", activities.RefusalCode(err), "a worker without the conversation port replies nothing")
}
