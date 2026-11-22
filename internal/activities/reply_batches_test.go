package activities_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// The D3 fakes predate batches: Control answers as a release before D4a.
func (f *fakeTurns) AppendReplyBatch(context.Context, activities.AppendReplyBatchInput) (activities.BatchReceipt, error) {
	return activities.BatchReceipt{}, activities.Refused("UNIMPLEMENTED", activities.ErrBatchesUnavailable)
}

// batchTurns is a Control that takes the reply's batches (D4a): by default it
// accepts each, numbering the chunks; answer overrides one call.
type batchTurns struct {
	*fakeTurns
	bmu     sync.Mutex
	batches []activities.AppendReplyBatchInput
	chunks  uint64
	answer  func(n int, in activities.AppendReplyBatchInput) (activities.BatchReceipt, error, bool)
}

func (b *batchTurns) AppendReplyBatch(_ context.Context, in activities.AppendReplyBatchInput) (activities.BatchReceipt, error) {
	b.bmu.Lock()
	defer b.bmu.Unlock()
	b.batches = append(b.batches, in)
	if b.answer != nil {
		if rec, err, ok := b.answer(len(b.batches), in); ok {
			return rec, err
		}
	}
	rec := activities.BatchReceipt{Turn: activities.TurnRef{TurnID: in.TurnID, State: "running"}}
	if in.HasText {
		b.chunks++
		rec.ChunkSeq = b.chunks
	}
	return rec, nil
}

func newBatchTurns(claim activities.TurnClaim) *batchTurns {
	return &batchTurns{fakeTurns: &fakeTurns{claim: claim, context: turnContext}}
}

// frame is one frame of the fake Proxy's original stream str_a.
func frame(seq uint64, typ, text string) activities.ReplyFrame {
	return activities.ReplyFrame{Sequence: seq, Origin: "original", StreamID: "str_a", Type: typ, Text: text,
		Payload: fmt.Sprintf(`{"callId":"op_T:reply:1","sequence":"%d","type":"%s"}`, seq, typ), Final: typ == "done" || typ == "error"}
}

// streaming is a fake model call that relays the frames to the sink as the
// client does (an ErrDetached ends it) and then answers the result.
func streaming(frames []activities.ReplyFrame, res activities.ModelCallResult) *fakeModel {
	return &fakeModel{call: func(_ context.Context, in activities.ModelCallInput) (activities.ModelCallResult, error) {
		var text strings.Builder
		for _, f := range frames {
			text.WriteString(f.Text)
			if err := in.Frames(f); err != nil {
				return activities.ModelCallResult{CallID: in.CallID, Text: text.String()}, err
			}
		}
		res.Text, res.FrameIdentity = text.String(), frames[0].Origin != ""
		return res, nil
	}}
}

func reply(texts ...string) []activities.ReplyFrame {
	out := []activities.ReplyFrame{frame(0, "admitted", "")}
	for i, t := range texts {
		out = append(out, frame(uint64(i+1), "text", t))
	}
	return append(out, frame(uint64(len(texts)+1), "done", ""))
}

func joined(batches []activities.AppendReplyBatchInput) (string, []string) {
	var text strings.Builder
	var ranges []string
	for _, b := range batches {
		text.WriteString(b.Text)
		ranges = append(ranges, fmt.Sprintf("%d-%d", b.From, b.Through))
	}
	return text.String(), ranges
}

// contiguous checks that the batches cover the provider sequences from
// `from` without a gap or an overlap, ending at `through`.
func contiguous(t *testing.T, batches []activities.AppendReplyBatchInput, from, through uint64) {
	t.Helper()
	require.NotEmpty(t, batches)
	next := from
	for _, b := range batches {
		require.Equal(t, next, b.From, "batches continue the watermark")
		require.GreaterOrEqual(t, b.Through, b.From)
		next = b.Through + 1
	}
	require.Equal(t, through+1, next)
}

// TestReplyBatchesStream (design 0001 §8.2, AT-10, AT-46): the frames are
// committed from provider sequence 0 in contiguous batches of whole frames
// under the writer epoch and the call's original stream, closed at 4 KiB of
// text; the reply is sealed from them with their digest.
func TestReplyBatchesStream(t *testing.T) {
	turns := newBatchTurns(openClaim)
	parts := []string{strings.Repeat("a", 3000), "日本語の", strings.Repeat("b", 3000), " 😀 end"}
	full := strings.Join(parts, "")
	acts := &activities.LifecycleActivities{Conversations: turns, Model: streaming(reply(parts...), activities.ModelCallResult{CallID: "op_T:reply:1", State: "succeeded"})}
	got, err := acts.ChatReplyAs(t.Context(), replyIn, firstInvocation, func() {})
	require.NoError(t, err)
	require.Equal(t, "completed", got.TurnState)

	text, ranges := joined(turns.batches)
	require.Equal(t, full, text, "every text frame is committed once, in order")
	contiguous(t, turns.batches, 0, uint64(len(parts)+1))
	require.GreaterOrEqual(t, len(ranges), 2, "a batch closes at 4 KiB: %v", ranges)
	for _, b := range turns.batches {
		require.Equal(t, "str_a", b.OriginalStreamID)
		require.Equal(t, uint64(1), b.WriterEpoch)
		require.Equal(t, "op_T:reply:1", b.CallID)
		require.Equal(t, fmt.Sprintf("op_T:reply:batch:1:%d", b.From), b.CommandID)
		require.Regexp(t, `^sha256:[0-9a-f]{64}$`, b.SourceFramesDigest)
		require.Equal(t, b.HasText, b.Text != "")
	}
	require.Len(t, turns.records, 1)
	rec := turns.records[0]
	require.True(t, rec.FromBatches, "a reply with batches is sealed from them")
	require.Equal(t, activities.TextDigest(full), rec.BatchesDigest)
	require.Equal(t, activities.ReplyRecord{Outcome: "completed", HasText: true, Text: full, Sender: "stopped"}, rec.Reply)
}

// TestReplyBatchesLostAnswer (AT-11): a batch whose answer was lost is resent
// unchanged — Control answers it with its first receipt — and the stream goes
// on after it.
func TestReplyBatchesLostAnswer(t *testing.T) {
	turns := newBatchTurns(openClaim)
	turns.answer = func(n int, in activities.AppendReplyBatchInput) (activities.BatchReceipt, error, bool) {
		switch n {
		case 1:
			return activities.BatchReceipt{}, errors.New("unavailable: the answer was lost"), true
		case 2:
			return activities.BatchReceipt{Turn: activities.TurnRef{TurnID: in.TurnID, State: "running"}, Existing: true}, nil, true
		}
		return activities.BatchReceipt{}, nil, false
	}
	acts := &activities.LifecycleActivities{Conversations: turns, Model: streaming(reply("Hello", " world"), activities.ModelCallResult{State: "succeeded"})}
	got, err := acts.ChatReplyAs(t.Context(), replyIn, firstInvocation, func() {})
	require.NoError(t, err)
	require.Equal(t, "completed", got.TurnState)
	require.GreaterOrEqual(t, len(turns.batches), 2)
	require.Equal(t, turns.batches[0], turns.batches[1], "the resend is the same batch")
	text, _ := joined(turns.batches[1:])
	require.Equal(t, "Hello world", text)
	contiguous(t, turns.batches[1:], 0, 3)
	require.Equal(t, activities.TextDigest("Hello world"), turns.records[0].BatchesDigest)
}

// TestReplyBatchesSealedDetaches (DEC-070 Q3 A): a batch that finds the turn
// sealed by its stop ends the relay — the stream is closed without a cancel
// of the call (the fake panics on one) — and no outcome is recorded: the
// seal stands with exactly the committed text.
func TestReplyBatchesSealedDetaches(t *testing.T) {
	turns := newBatchTurns(openClaim)
	sealed := activities.TurnRef{TurnID: "turn_1", State: "canceled", SenderStatus: "active", FailureCode: "CANCELED"}
	turns.answer = func(n int, _ activities.AppendReplyBatchInput) (activities.BatchReceipt, error, bool) {
		if n == 2 {
			return activities.BatchReceipt{Turn: sealed, Sealed: true}, nil, true
		}
		return activities.BatchReceipt{}, nil, false
	}
	frames := reply(strings.Repeat("x", 5000), strings.Repeat("y", 5000), strings.Repeat("z", 5000), "tail")
	acts := &activities.LifecycleActivities{Conversations: turns, Model: streaming(frames, activities.ModelCallResult{State: "succeeded"})}
	got, err := acts.ChatReplyAs(t.Context(), replyIn, firstInvocation, func() {})
	require.NoError(t, err)
	got.CallState = "" // the call may have ended before the seal reached the reader
	require.Equal(t, activities.ChatReplyResult{TurnID: "turn_1", TurnState: "canceled", SenderStatus: "active", FailureCode: "CANCELED", Existing: true}, got)
	require.Empty(t, turns.records, "the stop's seal is the outcome")
	require.Len(t, turns.batches, 2, "nothing is sent after the seal")
}

// TestReplyBatchesReentry: a later attempt resumes after Control's watermark
// on the same original stream: the replayed prefix is not sent again, its
// text joins the sealed text, and the digest covers both.
func TestReplyBatchesReentry(t *testing.T) {
	two := uint64(2)
	claim := openClaim
	claim.LastProviderSeq, claim.OriginalStreamID = &two, "str_a"
	claim.Turn.AssistantMessageID = "msg_reply"
	turns := newBatchTurns(claim)
	acts := &activities.LifecycleActivities{Conversations: turns, Model: streaming(reply("Hel", "lo", " wor", "ld"), activities.ModelCallResult{State: "succeeded"})}
	got, err := acts.ChatReplyAs(t.Context(), replyIn, activities.ReplyInvocation{ActivityKey: "k", Attempt: 2, InvocationID: "inv_2"}, func() {})
	require.NoError(t, err)
	require.Equal(t, "completed", got.TurnState)
	contiguous(t, turns.batches, 3, 5)
	text, _ := joined(turns.batches)
	require.Equal(t, " world", text)
	rec := turns.records[0]
	require.True(t, rec.FromBatches)
	require.Equal(t, activities.TextDigest("Hello world"), rec.BatchesDigest)
	require.Equal(t, "Hello world", rec.Reply.Text)
}

// TestReplyBatchesInconsistentStream (AT-45): a reentry that replays another
// stream appends nothing and seals the committed batches as they stand
// (no digest: the writer does not know them); a refusal of a batch keeps
// what was committed and appends nothing after it.
func TestReplyBatchesInconsistentStream(t *testing.T) {
	two := uint64(2)
	claim := openClaim
	claim.LastProviderSeq, claim.OriginalStreamID = &two, "str_other"
	claim.Turn.AssistantMessageID = "msg_reply"
	turns := newBatchTurns(claim)
	acts := &activities.LifecycleActivities{Conversations: turns, Model: streaming(reply("Hel", "lo", " wor", "ld"), activities.ModelCallResult{State: "succeeded"})}
	_, err := acts.ChatReplyAs(t.Context(), replyIn, activities.ReplyInvocation{ActivityKey: "k", Attempt: 2, InvocationID: "inv_2"}, func() {})
	require.NoError(t, err)
	require.Empty(t, turns.batches)
	rec := turns.records[0]
	require.True(t, rec.FromBatches)
	require.Empty(t, rec.BatchesDigest)
	require.Equal(t, activities.ReplyRecord{Outcome: "partial_failed", Sender: "stopped", FailureCode: "STREAM_INCONSISTENT"}, rec.Reply)

	refused := newBatchTurns(openClaim)
	refused.answer = func(n int, _ activities.AppendReplyBatchInput) (activities.BatchReceipt, error, bool) {
		if n == 2 {
			return activities.BatchReceipt{}, activities.Refused("RESULT_INTEGRITY", errors.New("STREAM_INCONSISTENT")), true
		}
		return activities.BatchReceipt{}, nil, false
	}
	frames := reply(strings.Repeat("x", 5000), strings.Repeat("y", 5000), "tail")
	acts = &activities.LifecycleActivities{Conversations: refused, Model: streaming(frames, activities.ModelCallResult{State: "succeeded"})}
	_, err = acts.ChatReplyAs(t.Context(), replyIn, firstInvocation, func() {})
	require.NoError(t, err)
	require.Len(t, refused.batches, 2, "nothing is appended after the refusal")
	rec = refused.records[0]
	require.Equal(t, activities.TextDigest(strings.Repeat("x", 5000)), rec.BatchesDigest)
	require.Equal(t, "partial_failed", rec.Reply.Outcome)
	require.Equal(t, "STREAM_INCONSISTENT", rec.Reply.FailureCode)
}

// TestReplyBatchesFallBack: without Control's batches (a release before D4a)
// or the Proxy's stream identity, the reply is recorded whole as in D3.
func TestReplyBatchesFallBack(t *testing.T) {
	turns := newBatchTurns(openClaim)
	turns.answer = func(int, activities.AppendReplyBatchInput) (activities.BatchReceipt, error, bool) {
		return activities.BatchReceipt{}, activities.Refused("UNIMPLEMENTED", activities.ErrBatchesUnavailable), true
	}
	acts := &activities.LifecycleActivities{Conversations: turns, Model: streaming(reply("Hello", " world"), activities.ModelCallResult{State: "succeeded"})}
	_, err := acts.ChatReplyAs(t.Context(), replyIn, firstInvocation, func() {})
	require.NoError(t, err)
	require.Len(t, turns.batches, 1)
	require.False(t, turns.records[0].FromBatches)
	require.Equal(t, "Hello world", turns.records[0].Reply.Text)

	plain := newBatchTurns(openClaim)
	frames := reply("Hello", " world")
	for i := range frames {
		frames[i].Origin, frames[i].StreamID = "", ""
	}
	acts = &activities.LifecycleActivities{Conversations: plain, Model: streaming(frames, activities.ModelCallResult{State: "succeeded"})}
	_, err = acts.ChatReplyAs(t.Context(), replyIn, firstInvocation, func() {})
	require.NoError(t, err)
	require.Empty(t, plain.batches, "frames without a stream identity are not appended")
	require.False(t, plain.records[0].FromBatches)
	require.Equal(t, activities.ReplyRecord{Outcome: "completed", HasText: true, Text: "Hello world", Sender: "stopped"}, plain.records[0].Reply)
}

// TestReplyBatchesBound: text beyond the stored reply bound is not committed;
// the reply is sealed from its batches as a partial failure, REPLY_TOO_LONG.
func TestReplyBatchesBound(t *testing.T) {
	turns := newBatchTurns(openClaim)
	parts := []string{strings.Repeat("a", 20000), strings.Repeat("b", 12000), strings.Repeat("c", 2000)}
	acts := &activities.LifecycleActivities{Conversations: turns, Model: streaming(reply(parts...), activities.ModelCallResult{State: "succeeded"})}
	_, err := acts.ChatReplyAs(t.Context(), replyIn, firstInvocation, func() {})
	require.NoError(t, err)
	text, _ := joined(turns.batches)
	require.Equal(t, parts[0]+parts[1], text)
	rec := turns.records[0]
	require.True(t, rec.FromBatches)
	require.Equal(t, activities.TextDigest(text), rec.BatchesDigest)
	require.Equal(t, "partial_failed", rec.Reply.Outcome)
	require.Equal(t, "REPLY_TOO_LONG", rec.Reply.FailureCode)
}

// TestReplyBatchesStop: a stop mid-stream commits nothing more and records
// the canceled reply from its batches under a live context.
func TestReplyBatchesStop(t *testing.T) {
	turns := newBatchTurns(openClaim)
	ctx, cancel := context.WithCancelCause(t.Context())
	model := &fakeModel{call: func(_ context.Context, in activities.ModelCallInput) (activities.ModelCallResult, error) {
		for _, f := range reply(strings.Repeat("x", 5000), "later")[:2] {
			require.NoError(t, in.Frames(f))
		}
		// The stop comes once the first batch is committed.
		require.Eventually(t, func() bool {
			turns.bmu.Lock()
			defer turns.bmu.Unlock()
			return len(turns.batches) == 1
		}, 2*time.Second, time.Millisecond)
		cancel(temporal.NewCanceledError())
		return activities.ModelCallResult{Text: strings.Repeat("x", 5000)}, context.Canceled
	}}
	acts := &activities.LifecycleActivities{Conversations: turns, Model: model}
	got, err := acts.ChatReplyAs(ctx, replyIn, firstInvocation, func() {})
	require.NoError(t, err)
	require.Equal(t, "canceled", got.TurnState)
	rec := turns.records[0]
	require.True(t, rec.FromBatches)
	require.Equal(t, activities.TextDigest(strings.Repeat("x", 5000)), rec.BatchesDigest, "exactly the committed text")
	require.Equal(t, "canceled", rec.Reply.Outcome)
	require.Equal(t, "active", rec.Reply.Sender, "the send meters to its end")
	require.NoError(t, turns.recordCtxErr, "the record is made under a live context")
}
