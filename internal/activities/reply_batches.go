package activities

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"strings"
	"time"
	"unicode/utf8"
)

// Streaming persistence of a chat reply (D4a, design 0001 §8.2, DEC-070):
// while the reply call streams, the chat Activity commits the Proxy's
// original frames to Control in batches under its writer epoch, so a reader
// sees the reply as it grows and a stop seals exactly the committed text.
// Bodies never enter the Workflow's history: the batches go from the frames
// to Control inside the Activity.

const (
	// A batch is closed at 4 KiB of text or 50 ms after its first frame,
	// whichever comes first; a frame is never split.
	batchFlushBytes = 4096
	batchFlushAfter = 50 * time.Millisecond
	// batchRetryAfter paces the identical resend of a batch whose answer
	// was lost; the stream's end tries it batchTailAttempts times at most.
	batchRetryAfter   = 200 * time.Millisecond
	batchTailAttempts = 3

	// FailureStreamInconsistent seals a reply whose committed batches and
	// stream disagree (Control's reason, design 0001 §8.2).
	FailureStreamInconsistent = "STREAM_INCONSISTENT"
)

// ErrBatchesUnavailable is the Control port's answer when Control has no
// AppendReplyBatch (a release before D4a): the reply is recorded whole.
var ErrBatchesUnavailable = errors.New("control accepts no reply batches")

// AppendReplyBatchInput is one batch of the reply's original frames: the
// provider sequences from From through Through, the digest of their
// payloads (each followed by a newline) and their text, if any.
type AppendReplyBatchInput struct {
	Binding            ReplyBinding
	CommandID          string
	TurnID             string
	WriterEpoch        uint64
	CallID             string
	OriginalStreamID   string
	From               uint64
	Through            uint64
	SourceFramesDigest string
	HasText            bool
	Text               string
}

// BatchReceipt is Control's answer to one batch: the turn as it stands, the
// committed chunk (0 for a textless batch), and whether the batch repeated a
// committed one or found the turn sealed (nothing stored).
type BatchReceipt struct {
	Turn     TurnRef
	ChunkSeq uint64
	Existing bool
	Sealed   bool
}

// batch is one range of original frames on its way to Control.
type batch struct {
	from, through uint64
	frames        hash.Hash
	text          strings.Builder
	opened        time.Time
}

func (b *batch) digest() string { return fmt.Sprintf("sha256:%x", b.frames.Sum(nil)) }

// due reports whether the batch is closed: by its text or its age.
func (b *batch) due() bool {
	return b.text.Len() >= batchFlushBytes || time.Since(b.opened) >= batchFlushAfter
}

// batchSummary is what the reply's record needs from its batches once the
// stream ended: the committed text (when it is known), whether Control may
// hold batches of this turn, why appending stopped, and a seal or a failure
// that ends the Activity instead of a record.
type batchSummary struct {
	// MayHaveBatches: a batch was committed (by this or an earlier
	// invocation) or one was sent whose answer is unknown; the record is then
	// sealed from the batches.
	MayHaveBatches bool
	// TextKnown: Text is exactly the committed text.
	TextKnown bool
	Text      string
	// HasChunks: some text is committed.
	HasChunks bool
	// Cut names why later text was not committed (REPLY_TOO_LONG,
	// CONTENT_INVALID, STREAM_INCONSISTENT), "" when nothing was cut.
	Cut string
	// Sealed: Control answered that the turn is sealed (a stop); Turn is it.
	Sealed bool
	Turn   TurnRef
	// Fatal is a refusal that ends this invocation (a fenced writer); Lost a
	// transient failure of the tail, which a retry re-enters.
	Fatal error
	Lost  error
}

// replyBatcher reads a reply call's frames (as its FrameSink) and commits
// them from one goroutine, so the stream is never held up by Control: the
// reader hands each frame over, the committer batches and sends them in
// order. A batch whose answer was lost is resent unchanged, which Control
// answers with its first receipt (AT-11).
type replyBatcher struct {
	ctl  ConversationControl
	base AppendReplyBatchInput
	opID string

	frames chan ReplyFrame
	// stop closes when the reader should detach: the turn is sealed, or a
	// refusal ends this invocation; done when the committer returned.
	stop chan struct{}
	done chan struct{}

	// The committer's state, read by Close after done.
	resume       *uint64 // Control's watermark at the claim; nil before any batch
	resumeStream string
	stream       string
	appending    bool
	pending      *batch // filling
	inflight     *batch // sent, its answer not yet known
	committed    strings.Builder
	lost         error
	sum          batchSummary
}

func newReplyBatcher(ctl ConversationControl, base AppendReplyBatchInput, opID string, claim TurnClaim) *replyBatcher {
	return &replyBatcher{
		ctl: ctl, base: base, opID: opID,
		frames: make(chan ReplyFrame, 1024), stop: make(chan struct{}), done: make(chan struct{}),
		resume: claim.LastProviderSeq, resumeStream: claim.OriginalStreamID, appending: true,
		sum: claimedBatches(claim),
	}
}

// claimedBatches is what the claim tells of the turn's batches: an earlier
// invocation committed some (a watermark), and some text (a reply message);
// that text is known only once a replay reached the watermark.
func claimedBatches(claim TurnClaim) batchSummary {
	return batchSummary{
		MayHaveBatches: claim.LastProviderSeq != nil,
		HasChunks:      claim.Turn.AssistantMessageID != "",
		TextKnown:      claim.LastProviderSeq == nil,
	}
}

// Frame is the reader's side: it hands the frame over, or answers
// ErrDetached once the committer asked the reader to stop or returned.
func (r *replyBatcher) Frame(f ReplyFrame) error {
	select {
	case <-r.stop:
		return ErrDetached
	case <-r.done:
		return ErrDetached
	case r.frames <- f:
		return nil
	}
}

// Close ends the frame stream and waits for the committer: it commits the
// tail unless the Activity's context is done (a stop seals what is
// committed; nothing is added after it).
func (r *replyBatcher) Close() batchSummary {
	close(r.frames)
	<-r.done
	s := r.sum
	if s.TextKnown {
		s.Text = r.committed.String()
	}
	return s
}

// run is the committer: it reads the handed-over frames until the stream
// ends, closes batches by size or age, and commits them in order.
func (r *replyBatcher) run(ctx context.Context) {
	defer close(r.done)
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	var due <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			r.abandon()
			return
		case <-due:
			due = nil
			r.flush(ctx, false)
		case f, ok := <-r.frames:
			if !ok {
				r.tail(ctx)
				return
			}
			r.take(f)
			if r.pending != nil && (f.Final || r.pending.text.Len() >= batchFlushBytes) {
				r.flush(ctx, f.Final)
			}
		}
		if due == nil {
			switch {
			case r.inflight != nil:
				timer.Reset(batchRetryAfter)
				due = timer.C
			case r.pending != nil:
				timer.Reset(max(0, batchFlushAfter-time.Since(r.pending.opened)))
				due = timer.C
			}
		}
	}
}

// tail commits what is left once the stream ended (its final frame, a lost
// connection, a refusal or a detach), resending a batch whose answer was
// lost a bounded number of times.
func (r *replyBatcher) tail(ctx context.Context) {
	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			r.abandon()
			return
		}
		r.flush(ctx, true)
		if r.inflight == nil && r.pending == nil {
			return
		}
		if attempt == batchTailAttempts {
			r.sum.Lost = fmt.Errorf("commit the reply's last batch: %w", r.lost)
			return
		}
		select {
		case <-ctx.Done():
		case <-time.After(batchRetryAfter):
		}
	}
}

// take places one frame: the replayed committed prefix is checked and its
// text kept; a later original frame joins the open batch while appending
// goes on; a recovery frame never advances the watermark (design 0001 §8.2).
func (r *replyBatcher) take(f ReplyFrame) {
	if r.resume != nil && f.Sequence <= *r.resume {
		// The committed prefix, replayed on a reentry: the same original
		// stream, whose frames are fixed, proves it is the committed text.
		switch {
		case !r.sum.TextKnown && r.appending && f.Origin == "original" && f.StreamID == r.resumeStream:
			r.committed.WriteString(f.Text)
			if f.Sequence == *r.resume {
				r.sum.TextKnown = true
			}
		case f.Origin == "recovery":
			// The send was reclaimed: Control holds the prefix, which the
			// stream no longer shows; the call's own outcome applies.
			r.appending = false
		default:
			r.cut(FailureStreamInconsistent)
		}
		return
	}
	if !r.sum.TextKnown && r.appending {
		// The replay never reached the watermark.
		r.cut(FailureStreamInconsistent)
	}
	if !r.appending || f.Origin == "recovery" {
		return
	}
	if f.Origin != "original" {
		// No stream identity (a Proxy or a record before D4a): nothing can be
		// appended; a reply with committed batches loses what follows.
		r.appending = false
		if r.sum.MayHaveBatches && f.Text != "" {
			r.cut(FailureStreamInconsistent)
		}
		return
	}
	if r.stream == "" {
		r.stream = f.StreamID
	}
	if f.Text != "" {
		if !utf8.ValidString(f.Text) || strings.ContainsRune(f.Text, 0) {
			r.cut(FailureContentInvalid)
			return
		}
		held := r.committed.Len() + len(f.Text)
		for _, b := range []*batch{r.inflight, r.pending} {
			if b != nil {
				held += b.text.Len()
			}
		}
		if held > MaxReplyBytes {
			r.cut(FailureReplyTooLong)
			return
		}
	}
	if r.pending == nil {
		r.pending = &batch{from: f.Sequence, frames: sha256.New(), opened: time.Now()}
	}
	r.pending.through = f.Sequence
	r.pending.frames.Write([]byte(f.Payload))
	r.pending.frames.Write([]byte{'\n'})
	r.pending.text.WriteString(f.Text)
}

// cut stops appending: later text stays uncommitted for the named reason.
// Before the committed prefix was replayed whole, that prefix is unknown.
func (r *replyBatcher) cut(reason string) {
	r.appending = false
	if r.sum.Cut == "" {
		r.sum.Cut = reason
	}
}

// abandon ends the committer at a stop or a timeout: a batch whose answer is
// unknown leaves the committed text unknown; the seal decides it.
func (r *replyBatcher) abandon() {
	if r.inflight != nil {
		r.sum.MayHaveBatches, r.sum.TextKnown = true, false
		if r.inflight.text.Len() > 0 {
			r.sum.HasChunks = true
		}
	}
}

// flush commits the batch whose answer is pending, then the open one when it
// is due (or the stream ended), in order. A lost answer keeps the batch for
// an identical resend.
func (r *replyBatcher) flush(ctx context.Context, final bool) {
	for {
		if r.inflight == nil {
			if r.pending == nil || (!final && !r.pending.due()) {
				return
			}
			r.inflight, r.pending = r.pending, nil
		}
		if !r.send(ctx) {
			return
		}
	}
}

// send commits the inflight batch and answers whether the next one may
// follow: false when its answer is unknown (it stays inflight) or when
// appending ended.
func (r *replyBatcher) send(ctx context.Context) bool {
	b := r.inflight
	in := r.base
	in.CommandID = fmt.Sprintf("%s:reply:batch:%d:%d", r.opID, r.base.WriterEpoch, b.from)
	in.OriginalStreamID, in.From, in.Through, in.SourceFramesDigest = r.stream, b.from, b.through, b.digest()
	if b.text.Len() > 0 {
		in.HasText, in.Text = true, b.text.String()
	}
	rec, err := r.ctl.AppendReplyBatch(ctx, in)
	switch {
	case err == nil:
	case errors.Is(err, ErrBatchesUnavailable) && !r.sum.MayHaveBatches:
		// Control before D4a: the reply is recorded whole.
		r.appending, r.inflight, r.pending = false, nil, nil
		return false
	case RefusalCode(err) == "RESULT_INTEGRITY":
		// Control refused the stream and keeps the turn reconciling: what
		// was committed stands; nothing more is appended.
		r.cut(FailureStreamInconsistent)
		r.inflight, r.pending = nil, nil
		return false
	case RefusalCode(err) != "":
		r.sum.Fatal = err
		r.appending, r.inflight, r.pending = false, nil, nil
		r.detach()
		return false
	default:
		r.sum.MayHaveBatches = true // it may have committed; the resend tells
		r.lost = err
		return false
	}
	r.sum.MayHaveBatches, r.inflight, r.lost = true, nil, nil
	if rec.Sealed {
		r.sum.Sealed, r.sum.Turn = true, rec.Turn
		r.appending, r.pending = false, nil
		r.detach()
		return false
	}
	r.committed.WriteString(b.text.String())
	if rec.ChunkSeq > 0 {
		r.sum.HasChunks = true
	}
	return true
}

func (r *replyBatcher) detach() {
	select {
	case <-r.stop:
	default:
		close(r.stop)
	}
}

// fromBatches fits a reply's record to its committed batches (D4a): the
// sealed text is what Control holds, so the outcome agrees with it. A cut
// makes a completed reply a partial failure (or a failure without text); an
// unknown committed text keeps the outcome's kind and lets Control seal its
// batches as they stand.
func fromBatches(rec ReplyRecord, s batchSummary) ReplyRecord {
	if s.TextKnown {
		if s.Cut != "" && (rec.Outcome == "completed" || rec.FailureCode == FailureEmptyReply) {
			return partialOrFailed(rec.Text, rec.Sender, s.Cut)
		}
		return rec
	}
	rec.HasText, rec.Text = false, ""
	switch rec.Outcome {
	case "canceled":
		return rec
	case "completed":
		rec.FailureCode = firstNonEmpty(s.Cut, FailureStreamInconsistent)
	}
	rec.Outcome = "failed"
	if s.HasChunks {
		rec.Outcome = "partial_failed"
	}
	return rec
}
