package control

import (
	"context"
	"fmt"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
		FailureCode: t.GetFailureCode(), AssistantMessageID: t.GetAssistantMessageId(),
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
	out := activities.TurnClaim{Turn: fromTurn(resp.GetTurn()), WriterEpoch: epoch, Existing: resp.GetExisting(), OriginalStreamID: resp.GetOriginalStreamId()}
	if resp.LastProviderSeq != nil {
		seq, err := strconv.ParseUint(resp.GetLastProviderSeq(), 10, 64)
		if err != nil {
			return activities.TurnClaim{}, fmt.Errorf("control answered last provider sequence %q: %w", resp.GetLastProviderSeq(), err)
		}
		out.LastProviderSeq = &seq
	}
	return out, nil
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
	switch {
	case in.FromBatches:
		// D4a: Control seals the committed batches; the text is not sent.
		req.FromBatches = true
		if in.BatchesDigest != "" {
			digest := in.BatchesDigest
			req.ContentDigest = &digest
		}
	case in.Reply.HasText:
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

// AppendReplyBatch commits one batch of the reply's original frames (D4a,
// DEC-070). A Control before D4a answers Unimplemented: the port answers
// ErrBatchesUnavailable, and the Activity records the reply whole.
func (c *Client) AppendReplyBatch(ctx context.Context, in activities.AppendReplyBatchInput) (activities.BatchReceipt, error) {
	req := &controlv1.AppendReplyBatchRequest{
		Command: c.command(in.Binding.TenantID, in.CommandID, in), Binding: replyBinding(in.Binding), TurnId: in.TurnID,
		WriterEpoch: strconv.FormatUint(in.WriterEpoch, 10), CallId: in.CallID, OriginalStreamId: in.OriginalStreamID,
		FromProviderSeq: strconv.FormatUint(in.From, 10), ThroughProviderSeq: strconv.FormatUint(in.Through, 10),
		SourceFramesDigest: in.SourceFramesDigest,
	}
	if in.HasText {
		text, digest := in.Text, activities.TextDigest(in.Text)
		req.Text, req.TextDigest = &text, &digest
	}
	resp, err := c.conversations().AppendReplyBatch(ctx, req)
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			return activities.BatchReceipt{}, activities.Refused("UNIMPLEMENTED", fmt.Errorf("%w: %w", activities.ErrBatchesUnavailable, err))
		}
		return activities.BatchReceipt{}, nonRetryable(err)
	}
	out := activities.BatchReceipt{Turn: fromTurn(resp.GetTurn()), Existing: resp.GetExisting(), Sealed: resp.GetSealed()}
	if resp.ChunkSeq != nil {
		chunk, err := strconv.ParseUint(resp.GetChunkSeq(), 10, 64)
		if err != nil {
			return activities.BatchReceipt{}, fmt.Errorf("control answered chunk sequence %q: %w", resp.GetChunkSeq(), err)
		}
		out.ChunkSeq = chunk
	}
	return out, nil
}
