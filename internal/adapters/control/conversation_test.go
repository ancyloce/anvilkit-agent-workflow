package control

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// conversationStub records the chat Activity's requests and answers them.
type conversationStub struct {
	controlv1.UnimplementedConversationExecutionServiceServer
	claim   *controlv1.ClaimReplyWriterRequest
	context *controlv1.GetTurnContextRequest
	record  *controlv1.RecordReplyOutcomeRequest
	refuse  error
}

func (s *conversationStub) ClaimReplyWriter(_ context.Context, in *controlv1.ClaimReplyWriterRequest) (*controlv1.ClaimReplyWriterResponse, error) {
	s.claim = in
	if s.refuse != nil {
		return nil, s.refuse
	}
	return &controlv1.ClaimReplyWriterResponse{WriterEpoch: "2", Turn: &controlv1.Turn{TurnId: "turn_1", OperationId: "op_T", State: controlv1.TurnState_TURN_STATE_RUNNING,
		ContentStatus: controlv1.ContentStatus_CONTENT_STATUS_UNKNOWN, SenderStatus: controlv1.SenderStatus_SENDER_STATUS_ACTIVE}}, nil
}

func (s *conversationStub) GetTurnContext(_ context.Context, in *controlv1.GetTurnContextRequest) (*controlv1.GetTurnContextResponse, error) {
	s.context = in
	return &controlv1.GetTurnContextResponse{ContextDigest: "sha256:ctx", Messages: []*controlv1.Message{
		{MessageId: "msg_1", Role: controlv1.MessageRole_MESSAGE_ROLE_USER, State: controlv1.MessageState_MESSAGE_STATE_COMPLETED, Text: "Make it black"},
		{MessageId: "msg_2", Role: controlv1.MessageRole_MESSAGE_ROLE_ASSISTANT, State: controlv1.MessageState_MESSAGE_STATE_PARTIAL_FAILED, Text: "Bl"},
	}}, nil
}

func (s *conversationStub) RecordReplyOutcome(_ context.Context, in *controlv1.RecordReplyOutcomeRequest) (*controlv1.RecordReplyOutcomeResponse, error) {
	s.record = in
	code := "CONTENT_UNRECOVERABLE"
	return &controlv1.RecordReplyOutcomeResponse{Turn: &controlv1.Turn{TurnId: "turn_1", State: controlv1.TurnState_TURN_STATE_FAILED,
		ContentStatus: controlv1.ContentStatus_CONTENT_STATUS_PARTIAL, SenderStatus: controlv1.SenderStatus_SENDER_STATUS_STOPPED, FailureCode: &code}}, nil
}

// TestConversationExecutionClient (DEC-068): the chat Activity's three calls
// carry the Job-less binding (no instance), the trusted invocation and the
// writer epoch as canonical decimals, the text with its digest, and map the
// turn's enums back; Control's refusal becomes the Activity's refusal.
func TestConversationExecutionClient(t *testing.T) {
	stub := &conversationStub{}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	controlv1.RegisterConversationExecutionServiceServer(server, stub)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///control", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	c := &Client{conn: conn, worker: "anvilkit-agent-workflow"}
	b := activities.ReplyBinding{OperationID: "op_T", TenantID: "tenant_a", AttemptID: "att_1", ExecutionEpoch: 3}

	claim, err := c.ClaimReplyWriter(t.Context(), activities.ClaimReplyWriterInput{Binding: b, CommandID: "op_T:reply:claim:2", ActivityKey: "op_T/run/chat-reply", TemporalAttempt: 2, InvocationID: "inv_x"})
	require.NoError(t, err)
	require.Equal(t, activities.TurnClaim{Turn: activities.TurnRef{TurnID: "turn_1", State: "running", ContentStatus: "unknown", SenderStatus: "active"}, WriterEpoch: 2}, claim)
	require.Equal(t, &controlv1.ExecutionBinding{OperationId: "op_T", AttemptId: "att_1", ExecutionEpoch: "3"}, stub.claim.GetBinding(), "no instance: the chat attempt runs no Job")
	require.Equal(t, "2", stub.claim.GetTemporalAttempt())
	require.Equal(t, "inv_x", stub.claim.GetInvocationId())
	require.Equal(t, "op_T/run/chat-reply", stub.claim.GetActivityKey())
	require.Equal(t, "tenant_a", stub.claim.GetCommand().GetTenantId())
	require.Equal(t, "op_T:reply:claim:2", stub.claim.GetCommand().GetCommandId())

	tc, err := c.GetTurnContext(t.Context(), activities.GetTurnContextInput{Binding: b, TurnID: "turn_1", WriterEpoch: 2})
	require.NoError(t, err)
	require.Equal(t, activities.TurnContext{Digest: "sha256:ctx", Messages: []activities.TurnMessage{{Role: "user", State: "completed", Text: "Make it black"}, {Role: "assistant", State: "partial_failed", Text: "Bl"}}}, tc)
	require.Equal(t, "tenant_a", stub.context.GetTenantId())
	require.Equal(t, "2", stub.context.GetWriterEpoch())

	turn, err := c.RecordReplyOutcome(t.Context(), activities.RecordReplyOutcomeInput{Binding: b, CommandID: "op_T:reply:outcome:2", TurnID: "turn_1", WriterEpoch: 2, CallID: "op_T:reply:1",
		Reply: activities.ReplyRecord{Outcome: "partial_failed", HasText: true, Text: "Bl", Sender: "stopped", FailureCode: "CONTENT_UNRECOVERABLE"}})
	require.NoError(t, err)
	require.Equal(t, activities.TurnRef{TurnID: "turn_1", State: "failed", ContentStatus: "partial", SenderStatus: "stopped", FailureCode: "CONTENT_UNRECOVERABLE"}, turn)
	require.Equal(t, controlv1.ReplyOutcome_REPLY_OUTCOME_PARTIAL_FAILED, stub.record.GetOutcome())
	require.Equal(t, controlv1.SenderStatus_SENDER_STATUS_STOPPED, stub.record.GetSenderStatus())
	require.Equal(t, "Bl", stub.record.GetText())
	require.Equal(t, activities.TextDigest("Bl"), stub.record.GetContentDigest())
	require.Equal(t, "CONTENT_UNRECOVERABLE", stub.record.GetFailureCode())
	require.Equal(t, "2", stub.record.GetWriterEpoch())

	_, err = c.RecordReplyOutcome(t.Context(), activities.RecordReplyOutcomeInput{Binding: b, Reply: activities.ReplyRecord{Outcome: "failed", Sender: "not_started", FailureCode: "BUDGET_EXHAUSTED"}})
	require.NoError(t, err)
	require.Nil(t, stub.record.Text, "a failure stores no text")
	require.Nil(t, stub.record.ContentDigest)
	_, err = c.RecordReplyOutcome(t.Context(), activities.RecordReplyOutcomeInput{Binding: b, Reply: activities.ReplyRecord{Outcome: "maybe", Sender: "stopped"}})
	require.Equal(t, "INVALID_ARGUMENT", activities.RefusalCode(err))

	stub.refuse = status.Error(codes.FailedPrecondition, "STALE_EXECUTION: turn turn_1 is written by another activity")
	_, err = c.ClaimReplyWriter(t.Context(), activities.ClaimReplyWriterInput{Binding: b, CommandID: "x", ActivityKey: "k", TemporalAttempt: 1, InvocationID: "i"})
	require.Equal(t, "STALE_EXECUTION", activities.RefusalCode(err))
}

// batchStub is a Control with D4a's AppendReplyBatch: a claim that names the
// watermark and stream, and a batch answered as committed or sealed.
type batchStub struct {
	conversationStub
	batch  *controlv1.AppendReplyBatchRequest
	sealed bool
}

func (s *batchStub) ClaimReplyWriter(_ context.Context, in *controlv1.ClaimReplyWriterRequest) (*controlv1.ClaimReplyWriterResponse, error) {
	seq, stream, reply := "4", "str_a", "msg_reply"
	return &controlv1.ClaimReplyWriterResponse{WriterEpoch: "2", LastProviderSeq: &seq, OriginalStreamId: &stream, Turn: &controlv1.Turn{TurnId: "turn_1",
		State: controlv1.TurnState_TURN_STATE_RUNNING, AssistantMessageId: &reply}}, nil
}

func (s *batchStub) AppendReplyBatch(_ context.Context, in *controlv1.AppendReplyBatchRequest) (*controlv1.AppendReplyBatchResponse, error) {
	s.batch = in
	chunk := "3"
	out := &controlv1.AppendReplyBatchResponse{AcceptedThroughProviderSeq: in.GetThroughProviderSeq(), ChunkSeq: &chunk, Turn: &controlv1.Turn{TurnId: "turn_1", State: controlv1.TurnState_TURN_STATE_RUNNING}}
	if s.sealed {
		out = &controlv1.AppendReplyBatchResponse{Sealed: true, Turn: &controlv1.Turn{TurnId: "turn_1", State: controlv1.TurnState_TURN_STATE_CANCELED,
			SenderStatus: controlv1.SenderStatus_SENDER_STATUS_ACTIVE}}
	}
	return out, nil
}

func dialStub(t *testing.T, register func(*grpc.Server)) *Client {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///control", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return &Client{conn: conn, worker: "anvilkit-agent-workflow"}
}

// TestReplyBatchClient (D4a, DEC-070): the claim answers Control's watermark
// and stream; a batch carries its range as canonical decimals, the stream,
// the frames' digest and the text with its digest; a sealed turn comes back
// as sealed; a record from the batches sends no text, only the digest when
// it is known; a Control before D4a answers ErrBatchesUnavailable.
func TestReplyBatchClient(t *testing.T) {
	stub := &batchStub{}
	c := dialStub(t, func(s *grpc.Server) { controlv1.RegisterConversationExecutionServiceServer(s, stub) })
	b := activities.ReplyBinding{OperationID: "op_T", TenantID: "tenant_a", AttemptID: "att_1", ExecutionEpoch: 3}

	claim, err := c.ClaimReplyWriter(t.Context(), activities.ClaimReplyWriterInput{Binding: b, CommandID: "c", ActivityKey: "k", TemporalAttempt: 2, InvocationID: "i"})
	require.NoError(t, err)
	require.NotNil(t, claim.LastProviderSeq)
	require.Equal(t, uint64(4), *claim.LastProviderSeq)
	require.Equal(t, "str_a", claim.OriginalStreamID)
	require.Equal(t, "msg_reply", claim.Turn.AssistantMessageID)

	in := activities.AppendReplyBatchInput{Binding: b, CommandID: "op_T:reply:batch:2:5", TurnID: "turn_1", WriterEpoch: 2, CallID: "op_T:reply:1",
		OriginalStreamID: "str_a", From: 5, Through: 9, SourceFramesDigest: activities.TextDigest("frames"), HasText: true, Text: " world"}
	rec, err := c.AppendReplyBatch(t.Context(), in)
	require.NoError(t, err)
	require.Equal(t, activities.BatchReceipt{Turn: activities.TurnRef{TurnID: "turn_1", State: "running", ContentStatus: "unspecified", SenderStatus: "unspecified"}, ChunkSeq: 3}, rec)
	require.Equal(t, "5", stub.batch.GetFromProviderSeq())
	require.Equal(t, "9", stub.batch.GetThroughProviderSeq())
	require.Equal(t, "str_a", stub.batch.GetOriginalStreamId())
	require.Equal(t, "2", stub.batch.GetWriterEpoch())
	require.Equal(t, " world", stub.batch.GetText())
	require.Equal(t, activities.TextDigest(" world"), stub.batch.GetTextDigest())
	require.Equal(t, activities.TextDigest("frames"), stub.batch.GetSourceFramesDigest())
	require.Equal(t, "op_T:reply:batch:2:5", stub.batch.GetCommand().GetCommandId())

	in.HasText, in.Text = false, ""
	_, err = c.AppendReplyBatch(t.Context(), in)
	require.NoError(t, err)
	require.Nil(t, stub.batch.Text, "a textless batch")
	require.Nil(t, stub.batch.TextDigest)

	stub.sealed = true
	rec, err = c.AppendReplyBatch(t.Context(), in)
	require.NoError(t, err)
	require.True(t, rec.Sealed)
	require.Equal(t, "canceled", rec.Turn.State)

	_, err = c.RecordReplyOutcome(t.Context(), activities.RecordReplyOutcomeInput{Binding: b, TurnID: "turn_1", WriterEpoch: 2, CallID: "op_T:reply:1",
		Reply: activities.ReplyRecord{Outcome: "completed", HasText: true, Text: "Hello world", Sender: "stopped"}, FromBatches: true, BatchesDigest: activities.TextDigest("Hello world")})
	require.NoError(t, err)
	require.True(t, stub.record.GetFromBatches())
	require.Nil(t, stub.record.Text, "the sealed text is Control's batches")
	require.Equal(t, activities.TextDigest("Hello world"), stub.record.GetContentDigest())
	_, err = c.RecordReplyOutcome(t.Context(), activities.RecordReplyOutcomeInput{Binding: b, Reply: activities.ReplyRecord{Outcome: "partial_failed", Sender: "stopped", FailureCode: "STREAM_INCONSISTENT"}, FromBatches: true})
	require.NoError(t, err)
	require.Nil(t, stub.record.ContentDigest, "an unknown committed text sends no digest")

	old := dialStub(t, func(s *grpc.Server) { controlv1.RegisterConversationExecutionServiceServer(s, &conversationStub{}) })
	_, err = old.AppendReplyBatch(t.Context(), in)
	require.ErrorIs(t, err, activities.ErrBatchesUnavailable, "a Control before D4a")
}
