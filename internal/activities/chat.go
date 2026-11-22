package activities

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// The chat reply of a conversation turn (DEC-068, design 0001 D3 §7.2 to
// §7.4 and §11.3): one fixed Activity claims the turn's reply writer with
// Control, reads the turn's context, makes the turn's single reply call
// through the controlled Model Proxy and records the reply's outcome. Only
// identities, states and counts cross into the Workflow's history; the text
// goes from the Proxy's frames to Control inside the Activity.
const NameChatReply = "ChatReply"

// Chat bounds shared with Control (DEC-068): the stored message bound and the
// failure codes the Activity records.
const (
	MaxReplyBytes = 32768

	FailureCanceled             = "CANCELED"
	FailureEmptyReply           = "EMPTY_REPLY"
	FailureUnexpectedToolCall   = "UNEXPECTED_TOOL_CALL"
	FailureReplyTooLong         = "REPLY_TOO_LONG"
	FailureContentInvalid       = "CONTENT_INVALID"
	FailureContentUnrecoverable = "CONTENT_UNRECOVERABLE"
	FailureModelCallFailed      = "MODEL_CALL_FAILED"
	FailureSenderLost           = "SENDER_LOST"

	// recordAllowance bounds the outcome record after a stop, when the
	// Activity's own context is already canceled.
	recordAllowance = 30 * time.Second
)

// chatSystemPrompt is the turn's fixed instruction (design 0001 §9.3 order
// 1): the reply is text only, and the assistant acts on nothing.
const chatSystemPrompt = "You are the AnvilKit assistant in a conversation about the user's page components. Answer in plain text. You cannot run tools, change components or start tasks yourself."

// ReplyBinding is the chat attempt's execution binding: Job-less, so it names
// no instance (design 0001 §7.2).
type ReplyBinding struct {
	OperationID    string
	TenantID       string
	AttemptID      string
	ExecutionEpoch uint64
}

type ClaimReplyWriterInput struct {
	Binding         ReplyBinding
	CommandID       string
	ActivityKey     string
	TemporalAttempt int32
	InvocationID    string
}

// TurnRef is the turn as Control answered it, without text.
type TurnRef struct {
	TurnID        string
	State         string // queued | running | completed | failed | cancel_pending | canceled | reconciling
	ContentStatus string
	SenderStatus  string
	FailureCode   string
	// AssistantMessageID names the reply's message once any text is stored
	// or committed (D4a).
	AssistantMessageID string
}

// Open reports whether the turn still holds its conversation's reply slot.
func (t TurnRef) Open() bool {
	switch t.State {
	case "completed", "failed", "canceled":
		return false
	}
	return true
}

type TurnClaim struct {
	Turn        TurnRef
	WriterEpoch uint64
	Existing    bool
	// D4a: the last committed provider sequence of the reply's original
	// stream (nil before any batch) and that stream; a reentry resumes after
	// it.
	LastProviderSeq  *uint64
	OriginalStreamID string
}

type GetTurnContextInput struct {
	Binding     ReplyBinding
	TurnID      string
	WriterEpoch uint64
}

// TurnMessage is one message of a turn's context.
type TurnMessage struct {
	Role  string // user | assistant
	State string
	Text  string
}

type TurnContext struct {
	Messages []TurnMessage
	Digest   string
}

type RecordReplyOutcomeInput struct {
	Binding     ReplyBinding
	CommandID   string
	TurnID      string
	WriterEpoch uint64
	CallID      string
	Reply       ReplyRecord
	// FromBatches (D4a): the sealed text is the committed batches; Reply's
	// text classifies the outcome and is not sent. BatchesDigest is that
	// text's digest when it is known and not empty.
	FromBatches   bool
	BatchesDigest string
}

// ReplyRecord is the outcome the chat Activity records (Control's
// RecordReplyOutcome rules): completed has the whole text and a stopped
// sender; partial_failed keeps a prefix; failed stores no text; every
// outcome but completed names a failure code.
type ReplyRecord struct {
	Outcome     string // completed | partial_failed | canceled | failed
	HasText     bool
	Text        string
	Sender      string // not_started | active | stopped | unknown
	FailureCode string
}

// ConversationControl is the Workflow's port to Control's
// ConversationExecutionService.
type ConversationControl interface {
	ClaimReplyWriter(ctx context.Context, in ClaimReplyWriterInput) (TurnClaim, error)
	GetTurnContext(ctx context.Context, in GetTurnContextInput) (TurnContext, error)
	RecordReplyOutcome(ctx context.Context, in RecordReplyOutcomeInput) (TurnRef, error)
	// AppendReplyBatch commits one batch of the reply's original frames
	// (D4a); ErrBatchesUnavailable when Control has no such method.
	AppendReplyBatch(ctx context.Context, in AppendReplyBatchInput) (BatchReceipt, error)
}

// ChatReplyInput carries the identities and the frozen bounds of the turn's
// reply call; the deadline is the original one, never extended by a retry.
type ChatReplyInput struct {
	OperationID     string
	TenantID        string
	AttemptID       string
	ExecutionEpoch  uint64
	RouteID         string
	MaxOutputTokens int
	MaxExposure     Money
	Deadline        time.Time
}

// ChatReplyResult is what the Workflow learns: the turn's state and the
// reply's classification, never its text.
type ChatReplyResult struct {
	TurnID       string
	TurnState    string
	FailureCode  string
	SenderStatus string
	CallState    string
	ReplyBytes   int
	Existing     bool
}

// ReplyCallID is the turn's single reply call (Control checks the same
// derivation).
func ReplyCallID(operationID string) string { return operationID + ":reply:1" }

// ChatReply claims the turn's reply writer, reads the context, makes the
// reply call and records its outcome. A stop (the Workflow's cancellation)
// detaches from the stream without canceling the call: the Model Proxy reads
// the provider's stream to its end and settles the actual usage (DEC-068
// Q3), and the shown prefix is sealed as canceled. A transport failure is
// returned for Temporal's retry, whose higher attempt takes the writer over
// and re-enters the same call id (the Proxy never sends twice).
func (a *LifecycleActivities) ChatReply(ctx context.Context, in ChatReplyInput) (ChatReplyResult, error) {
	info := activity.GetInfo(ctx)
	invocation, err := newInvocationID()
	if err != nil {
		return ChatReplyResult{}, err
	}
	return a.ChatReplyAs(ctx, in, ReplyInvocation{
		ActivityKey: info.WorkflowExecution.ID + "/" + info.WorkflowExecution.RunID + "/" + info.ActivityID,
		Attempt:     info.Attempt, InvocationID: invocation,
	}, func() { activity.RecordHeartbeat(ctx) })
}

// ReplyInvocation is the trusted identity of one execution of the chat
// Activity (design 0001 §7.2): the registered Activity (its Workflow run and
// Activity id), Temporal's attempt ordinal and this invocation.
type ReplyInvocation struct {
	ActivityKey  string
	Attempt      int32
	InvocationID string
}

// ChatReplyAs is ChatReply under an explicit invocation and heartbeat.
func (a *LifecycleActivities) ChatReplyAs(ctx context.Context, in ChatReplyInput, inv ReplyInvocation, heartbeat func()) (ChatReplyResult, error) {
	if a.Conversations == nil {
		return ChatReplyResult{}, Refused("PROFILE_UNQUALIFIED", errors.New("no conversation port is configured"))
	}
	b := ReplyBinding{OperationID: in.OperationID, TenantID: in.TenantID, AttemptID: in.AttemptID, ExecutionEpoch: in.ExecutionEpoch}
	claim, err := a.Conversations.ClaimReplyWriter(ctx, ClaimReplyWriterInput{
		Binding: b, CommandID: fmt.Sprintf("%s:reply:claim:%d", in.OperationID, inv.Attempt),
		ActivityKey: inv.ActivityKey, TemporalAttempt: inv.Attempt, InvocationID: inv.InvocationID,
	})
	if err != nil {
		return ChatReplyResult{}, err
	}
	if !claim.Turn.Open() {
		// Sealed by an earlier attempt whose answer was lost: the turn's
		// outcome stands.
		return ChatReplyResult{TurnID: claim.Turn.TurnID, TurnState: claim.Turn.State, FailureCode: claim.Turn.FailureCode, SenderStatus: claim.Turn.SenderStatus, Existing: true}, nil
	}
	// batches is what Control holds of the reply's text (D4a): from the claim
	// until the call streamed, then from the batch sink.
	batches := claimedBatches(claim)
	record := func(rec ReplyRecord, callState string) (ChatReplyResult, error) {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recordAllowance)
		defer cancel()
		ri := RecordReplyOutcomeInput{
			Binding: b, CommandID: fmt.Sprintf("%s:reply:outcome:%d", in.OperationID, claim.WriterEpoch),
			TurnID: claim.Turn.TurnID, WriterEpoch: claim.WriterEpoch, CallID: ReplyCallID(in.OperationID),
		}
		if batches.MayHaveBatches {
			rec = fromBatches(rec, batches)
			ri.FromBatches = true
			if batches.TextKnown && batches.Text != "" {
				ri.BatchesDigest = TextDigest(batches.Text)
			}
		}
		ri.Reply = rec
		turn, err := a.Conversations.RecordReplyOutcome(rctx, ri)
		if err != nil {
			return ChatReplyResult{}, err
		}
		return ChatReplyResult{TurnID: turn.TurnID, TurnState: turn.State, FailureCode: turn.FailureCode, SenderStatus: turn.SenderStatus, CallState: callState, ReplyBytes: len(rec.Text)}, nil
	}
	callID := ReplyCallID(in.OperationID)
	if stopped(ctx) {
		return record(a.stopBeforeCall(ctx, inv.Attempt, callID), "")
	}
	tc, err := a.Conversations.GetTurnContext(ctx, GetTurnContextInput{Binding: b, TurnID: claim.Turn.TurnID, WriterEpoch: claim.WriterEpoch})
	if err != nil {
		if stopped(ctx) {
			return record(a.stopBeforeCall(ctx, inv.Attempt, callID), "")
		}
		return ChatReplyResult{}, err
	}
	messages := make([]ModelMessage, 0, len(tc.Messages)+1)
	messages = append(messages, ModelMessage{Role: "system", Content: chatSystemPrompt})
	for _, m := range tc.Messages {
		messages = append(messages, ModelMessage{Role: m.Role, Content: m.Text})
	}
	// D4a: the frames are committed in batches while they stream; a seal
	// (a stop) detaches, and the send meters to its end.
	sink := newReplyBatcher(a.Conversations, AppendReplyBatchInput{Binding: b, TurnID: claim.Turn.TurnID, WriterEpoch: claim.WriterEpoch, CallID: callID}, in.OperationID, claim)
	go sink.run(ctx)
	res, callErr := a.Model.CallModel(ctx, ModelCallInput{
		CallID: callID, TenantID: in.TenantID, OperationID: in.OperationID, AttemptID: in.AttemptID, ExecutionEpoch: in.ExecutionEpoch,
		RouteID: in.RouteID, Messages: messages, MaxOutputTokens: in.MaxOutputTokens, MaxExposure: in.MaxExposure, Deadline: in.Deadline,
		Frames: sink.Frame,
	}, heartbeat)
	batches = sink.Close()
	switch {
	case batches.Fatal != nil:
		return ChatReplyResult{}, batches.Fatal
	case batches.Sealed:
		// Sealed by the stop with exactly the committed text (DEC-070 Q3).
		t := batches.Turn
		return ChatReplyResult{TurnID: t.TurnID, TurnState: t.State, FailureCode: t.FailureCode, SenderStatus: t.SenderStatus, CallState: res.State, Existing: true}, nil
	case batches.Lost != nil && !stopped(ctx):
		// A retry re-enters the call and resumes after Control's watermark.
		return ChatReplyResult{}, batches.Lost
	}
	if batches.MayHaveBatches && batches.TextKnown {
		res.Text = batches.Text
	}
	if callErr == nil {
		return record(ReplyOf(res), res.State)
	}
	if stopped(ctx) {
		// The stop: detach, seal the shown prefix; the send meters to the end.
		return record(Canceled(res.Text), res.State)
	}
	if ctx.Err() != nil || RefusalCode(callErr) == "" {
		// A timeout, a lost stream or the worker's own shutdown: a retry
		// re-enters the same call.
		return ChatReplyResult{}, callErr
	}
	// A refusal: the call's record tells whether anything was sent.
	q, qerr := a.Model.GetModelCall(ctx, callID)
	switch {
	case RefusalCode(qerr) == "NOT_FOUND":
		return record(ReplyRecord{Outcome: "failed", Sender: "not_started", FailureCode: RefusalCode(callErr)}, "")
	case qerr != nil:
		return ChatReplyResult{}, fmt.Errorf("query the refused reply call %s: %w", callID, qerr)
	}
	q.Text = res.Text
	return record(RefusedReply(q, RefusalCode(callErr)), q.State)
}

// stopBeforeCall is the record of a stop before this invocation called the
// model: the first attempt sent nothing; a later attempt asks the call's
// record, since the attempt it took over may have sent it.
func (a *LifecycleActivities) stopBeforeCall(ctx context.Context, attempt int32, callID string) ReplyRecord {
	notStarted := ReplyRecord{Outcome: "canceled", Sender: "not_started", FailureCode: FailureCanceled}
	if attempt <= 1 {
		return notStarted
	}
	qctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recordAllowance)
	defer cancel()
	q, err := a.Model.GetModelCall(qctx, callID)
	switch {
	case RefusalCode(err) == "NOT_FOUND":
		return notStarted
	case err == nil && (q.State == "succeeded" || q.State == "failed" || q.State == "canceled"):
		rec := Canceled("")
		rec.Sender = "stopped"
		return rec
	}
	return Canceled("")
}

// stopped reports a stop: a cancellation the Temporal server sent, whose
// cause is a CanceledError (the SDK's own rule). A worker's shutdown, a pause
// or the worker's drain abandonment carry other causes; their Activity is
// retried, and the retry takes the writer over.
func stopped(ctx context.Context) bool {
	return errors.Is(ctx.Err(), context.Canceled) && temporal.IsCanceledError(context.Cause(ctx))
}

// ReplyOf classifies a reply call that ended with its final frame (design
// 0001 §7.4, DEC-068 Q4). The Proxy's final frame proves its sending path
// ended, so the sender is stopped, except for a send the sweep reclaimed
// without a fence, whose original sender may still be running.
func ReplyOf(res ModelCallResult) ReplyRecord {
	switch res.State {
	case "succeeded":
		switch {
		case len(res.ToolCalls) > 0:
			return partialOrFailed(res.Text, "stopped", FailureUnexpectedToolCall)
		case res.Text == "":
			return ReplyRecord{Outcome: "failed", Sender: "stopped", FailureCode: FailureEmptyReply}
		}
		return bounded(ReplyRecord{Outcome: "completed", HasText: true, Text: res.Text, Sender: "stopped"})
	case "canceled":
		rec := Canceled(res.Text)
		rec.Sender = "stopped"
		return rec
	case "unknown":
		return unknownReply(res)
	default:
		return partialOrFailed(res.Text, "stopped", firstNonEmpty(res.ErrorCode, FailureModelCallFailed))
	}
}

// RefusedReply classifies a reply call the Proxy or its frames refused,
// given the call's record: a settled record's sender stopped; an unsettled
// one is still sending, which keeps the turn reconciling.
func RefusedReply(rec ModelCallResult, code string) ReplyRecord {
	switch rec.State {
	case "succeeded", "failed", "canceled":
		return partialOrFailed(rec.Text, "stopped", code)
	case "unknown":
		return unknownReply(rec)
	default:
		return partialOrFailed(rec.Text, "active", code)
	}
}

// Canceled is a stopped reply's record: the shown prefix, the send left
// metering to its end.
func Canceled(text string) ReplyRecord {
	rec := ReplyRecord{Outcome: "canceled", Sender: "active", FailureCode: FailureCanceled}
	if text != "" {
		rec.HasText, rec.Text = true, text
	}
	return bounded(rec)
}

// unknownReply is a call without a proven outcome: content beyond the shown
// prefix is unrecoverable once the sender is stopped (the Proxy settled it
// itself, or the sweep fenced its reclaim); a reclaim without a fence leaves
// the original sender unknown.
func unknownReply(res ModelCallResult) ReplyRecord {
	if res.ErrorCode == FailureSenderLost && !res.Fenced {
		return partialOrFailed(res.Text, "unknown", FailureSenderLost)
	}
	return partialOrFailed(res.Text, "stopped", FailureContentUnrecoverable)
}

func partialOrFailed(text, sender, code string) ReplyRecord {
	if text == "" {
		return ReplyRecord{Outcome: "failed", Sender: sender, FailureCode: code}
	}
	return bounded(ReplyRecord{Outcome: "partial_failed", HasText: true, Text: text, Sender: sender, FailureCode: code})
}

// bounded keeps a record's text within the stored message contract: text
// that is not UTF-8 or carries NUL is not stored, and text over the bound is
// cut at a rune boundary, which makes a completed reply a partial failure.
func bounded(rec ReplyRecord) ReplyRecord {
	if !rec.HasText {
		return rec
	}
	if !utf8.ValidString(rec.Text) || strings.ContainsRune(rec.Text, 0) {
		rec.HasText, rec.Text = false, ""
		if rec.Outcome != "canceled" {
			rec.Outcome, rec.FailureCode = "failed", FailureContentInvalid
		}
		return rec
	}
	if len(rec.Text) <= MaxReplyBytes {
		return rec
	}
	cut := MaxReplyBytes
	for cut > 0 && !utf8.RuneStart(rec.Text[cut]) {
		cut--
	}
	rec.Text = rec.Text[:cut]
	if rec.Outcome == "completed" {
		rec.Outcome, rec.FailureCode = "partial_failed", FailureReplyTooLong
	}
	return rec
}

// TextDigest is the stored message's content digest (Control's derivation).
func TextDigest(text string) string {
	return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(text)))
}

func newInvocationID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("invocation id: %w", err)
	}
	return "inv_" + hex.EncodeToString(b[:]), nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
