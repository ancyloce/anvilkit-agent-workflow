package control

import (
	"context"
	"fmt"
	"strconv"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// The chat turn's client path (DEC-068): ConversationExecutionService, for
// the Workflow's chat Activity only.
func (c *Client) conversations() controlv1.ConversationExecutionServiceClient {
	return controlv1.NewConversationExecutionServiceClient(c.conn)
}

// replyBinding is the Job-less binding of a chat attempt: no instance.
func replyBinding(b activities.ReplyBinding) *controlv1.ExecutionBinding {
	return &controlv1.ExecutionBinding{OperationId: b.OperationID, AttemptId: b.AttemptID, ExecutionEpoch: strconv.FormatUint(b.ExecutionEpoch, 10)}
}

func fromTurn(t *controlv1.Turn) activities.TurnRef {
	return activities.TurnRef{
		TurnID: t.GetTurnId(), State: enumWord(t.GetState().String(), "TURN_STATE_"),
		ContentStatus: enumWord(t.GetContentStatus().String(), "CONTENT_STATUS_"), SenderStatus: enumWord(t.GetSenderStatus().String(), "SENDER_STATUS_"),
		FailureCode: t.GetFailureCode(),
	}
}

var (
	replyOutcomes = map[string]controlv1.ReplyOutcome{
		"completed": controlv1.ReplyOutcome_REPLY_OUTCOME_COMPLETED, "partial_failed": controlv1.ReplyOutcome_REPLY_OUTCOME_PARTIAL_FAILED,
		"canceled": controlv1.ReplyOutcome_REPLY_OUTCOME_CANCELED, "failed": controlv1.ReplyOutcome_REPLY_OUTCOME_FAILED,
	}
	senderStatuses = map[string]controlv1.SenderStatus{
		"not_started": controlv1.SenderStatus_SENDER_STATUS_NOT_STARTED, "active": controlv1.SenderStatus_SENDER_STATUS_ACTIVE,
		"stopped": controlv1.SenderStatus_SENDER_STATUS_STOPPED, "unknown": controlv1.SenderStatus_SENDER_STATUS_UNKNOWN,
	}
)

func (c *Client) ClaimReplyWriter(ctx context.Context, in activities.ClaimReplyWriterInput) (activities.TurnClaim, error) {
	resp, err := c.conversations().ClaimReplyWriter(ctx, &controlv1.ClaimReplyWriterRequest{
		Command: c.command(in.Binding.TenantID, in.CommandID, in), Binding: replyBinding(in.Binding),
		ActivityKey: in.ActivityKey, TemporalAttempt: strconv.FormatInt(int64(in.TemporalAttempt), 10), InvocationId: in.InvocationID,
	})
	if err != nil {
		return activities.TurnClaim{}, nonRetryable(err)
	}
	epoch, err := strconv.ParseUint(resp.GetWriterEpoch(), 10, 64)
	if err != nil {
		return activities.TurnClaim{}, fmt.Errorf("control answered writer epoch %q: %w", resp.GetWriterEpoch(), err)
	}
	return activities.TurnClaim{Turn: fromTurn(resp.GetTurn()), WriterEpoch: epoch, Existing: resp.GetExisting()}, nil
}

func (c *Client) GetTurnContext(ctx context.Context, in activities.GetTurnContextInput) (activities.TurnContext, error) {
	resp, err := c.conversations().GetTurnContext(ctx, &controlv1.GetTurnContextRequest{
		TenantId: in.Binding.TenantID, Binding: replyBinding(in.Binding), TurnId: in.TurnID, WriterEpoch: strconv.FormatUint(in.WriterEpoch, 10),
	})
	if err != nil {
		return activities.TurnContext{}, nonRetryable(err)
	}
	out := activities.TurnContext{Digest: resp.GetContextDigest(), Messages: make([]activities.TurnMessage, 0, len(resp.GetMessages()))}
	for _, m := range resp.GetMessages() {
		out.Messages = append(out.Messages, activities.TurnMessage{Role: enumWord(m.GetRole().String(), "MESSAGE_ROLE_"), State: enumWord(m.GetState().String(), "MESSAGE_STATE_"), Text: m.GetText()})
	}
	return out, nil
}

func (c *Client) RecordReplyOutcome(ctx context.Context, in activities.RecordReplyOutcomeInput) (activities.TurnRef, error) {
	outcome, ok := replyOutcomes[in.Reply.Outcome]
	if !ok {
		return activities.TurnRef{}, activities.Refused("INVALID_ARGUMENT", fmt.Errorf("reply outcome %q", in.Reply.Outcome))
	}
	sender, ok := senderStatuses[in.Reply.Sender]
	if !ok {
		return activities.TurnRef{}, activities.Refused("INVALID_ARGUMENT", fmt.Errorf("sender status %q", in.Reply.Sender))
	}
	req := &controlv1.RecordReplyOutcomeRequest{
		Command: c.command(in.Binding.TenantID, in.CommandID, in), Binding: replyBinding(in.Binding), TurnId: in.TurnID,
		WriterEpoch: strconv.FormatUint(in.WriterEpoch, 10), CallId: in.CallID, Outcome: outcome, SenderStatus: sender,
	}
	if in.Reply.HasText {
		text, digest := in.Reply.Text, activities.TextDigest(in.Reply.Text)
		req.Text, req.ContentDigest = &text, &digest
	}
	if in.Reply.FailureCode != "" {
		code := in.Reply.FailureCode
		req.FailureCode = &code
	}
	resp, err := c.conversations().RecordReplyOutcome(ctx, req)
	if err != nil {
		return activities.TurnRef{}, nonRetryable(err)
	}
	return fromTurn(resp.GetTurn()), nil
}
