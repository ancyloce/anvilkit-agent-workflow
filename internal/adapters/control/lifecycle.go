package control

import (
	"context"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// The P13 client paths: PreparationService, GenerationService, the
// SettleOperation and scoped GetAcceptedStage of ExecutionService and the
// EffectService path of the candidate registration.

func (c *Client) preparation() controlv1.PreparationServiceClient {
	return controlv1.NewPreparationServiceClient(c.conn)
}

func (c *Client) generation() controlv1.GenerationServiceClient {
	return controlv1.NewGenerationServiceClient(c.conn)
}

func (c *Client) effects() controlv1.EffectServiceClient {
	return controlv1.NewEffectServiceClient(c.conn)
}

func fromBinding(b *controlv1.ArtifactBinding, handle string) activities.ArtifactBinding {
	return activities.ArtifactBinding{TransferID: b.GetTransferId(), Digest: b.GetDigest(), Handle: handle}
}

func fromSourceRefs(refs []*controlv1.SourceReference) []activities.SourceReference {
	out := make([]activities.SourceReference, 0, len(refs))
	for _, r := range refs {
		out = append(out, activities.SourceReference{SourceID: r.GetSourceId(), Revision: r.GetRevision()})
	}
	return out
}

func toSourceRefs(refs []activities.SourceReference) []*controlv1.SourceReference {
	out := make([]*controlv1.SourceReference, 0, len(refs))
	for _, r := range refs {
		out = append(out, &controlv1.SourceReference{SourceId: r.SourceID, Revision: r.Revision})
	}
	return out
}

func toContentDigests(ds []activities.ContentDigest) []*controlv1.ContentDigest {
	out := make([]*controlv1.ContentDigest, 0, len(ds))
	for _, d := range ds {
		out = append(out, &controlv1.ContentDigest{SourceId: d.SourceID, Revision: d.Revision, Digest: d.Digest})
	}
	return out
}

func fromQuestionSet(qs *controlv1.QuestionSet) activities.QuestionSetRef {
	round, _ := strconv.ParseUint(qs.GetRound(), 10, 64)
	out := activities.QuestionSetRef{QuestionSetID: qs.GetQuestionSetId(), Revision: qs.GetRevision(), Round: round, AskedAt: qs.GetAskedAt().AsTime(), ExpiresAt: qs.GetExpiresAt().AsTime(), State: enumWord(qs.GetState().String(), "QUESTION_SET_STATE_")}
	for _, q := range qs.GetQuestions() {
		out.Questions = append(out.Questions, activities.Question{QuestionID: q.GetQuestionId(), Text: q.GetText()})
	}
	return out
}

func fromBrief(b *controlv1.Brief) activities.BriefRef {
	return activities.BriefRef{BriefID: b.GetBriefId(), Brief: fromBinding(b.GetBrief(), b.GetHandle()), RequirementsDigest: b.GetRequirementsDigest(), Revision: b.GetRevision(), State: enumWord(b.GetState().String(), "BRIEF_STATE_")}
}

func enumWord(name, prefix string) string { return strings.ToLower(strings.TrimPrefix(name, prefix)) }

func (c *Client) GetPreparation(ctx context.Context, in activities.GetPreparationInput) (activities.PreparationView, error) {
	resp, err := c.preparation().GetPreparation(ctx, &controlv1.GetPreparationRequest{TenantId: in.TenantID, OperationId: in.OperationID})
	if err != nil {
		return activities.PreparationView{}, nonRetryable(err)
	}
	p := resp.GetPreparation()
	rounds, _ := strconv.ParseUint(p.GetMaxRounds(), 10, 64)
	questions, _ := strconv.ParseUint(p.GetMaxQuestions(), 10, 64)
	wait, _ := strconv.ParseUint(p.GetWaitSeconds(), 10, 64)
	view := activities.PreparationView{
		Prompt: fromBinding(p.GetPrompt(), p.GetPromptHandle()), BrandReferences: fromSourceRefs(p.GetBrandReferences()), AssetReferences: fromSourceRefs(p.GetAssetReferences()),
		MaxRounds: rounds, MaxQuestions: questions, Wait: time.Duration(wait) * time.Second,
	}
	for _, qs := range p.GetQuestionSets() {
		view.QuestionSets = append(view.QuestionSets, fromQuestionSet(qs))
	}
	if p.Brief != nil {
		b := fromBrief(p.GetBrief())
		view.Brief = &b
	}
	return view, nil
}

func (c *Client) RecordQuestionSet(ctx context.Context, in activities.RecordQuestionSetInput) (activities.QuestionSetRef, error) {
	req := &controlv1.RecordQuestionSetRequest{Command: c.command(in.TenantID, in.CommandID, in), OperationId: in.OperationID, Round: strconv.FormatUint(in.Round, 10)}
	for _, q := range in.Questions {
		req.Questions = append(req.Questions, &controlv1.Question{QuestionId: q.QuestionID, Text: q.Text})
	}
	resp, err := c.preparation().RecordQuestionSet(ctx, req)
	if err != nil {
		return activities.QuestionSetRef{}, nonRetryable(err)
	}
	return fromQuestionSet(resp.GetQuestionSet()), nil
}

func (c *Client) GetAnswer(ctx context.Context, in activities.GetAnswerInput) (activities.AnswerRef, error) {
	resp, err := c.preparation().GetAnswer(ctx, &controlv1.GetAnswerRequest{TenantId: in.TenantID, OperationId: in.OperationID, AnswerId: in.AnswerID, QuestionSetId: in.QuestionSetID})
	if err != nil {
		if RefusalCodeOf(err) == "NOT_FOUND" {
			return activities.AnswerRef{Found: false}, nil
		}
		return activities.AnswerRef{}, nonRetryable(err)
	}
	a := resp.GetAnswer()
	return activities.AnswerRef{Found: true, AnswerID: a.GetAnswerId(), QuestionSetID: a.GetQuestionSetId(), QuestionSetRevision: a.GetQuestionSetRevision(), Answer: fromBinding(a.GetAnswer(), a.GetHandle()), Relay: enumWord(a.GetRelay().String(), "RELAY_STATE_")}, nil
}

func (c *Client) RecordBrief(ctx context.Context, operationID, tenantID, commandID string, brief activities.ArtifactBinding, requirementsDigest string, sources []activities.SourceReference, frozen activities.FrozenReferences) (activities.BriefRef, error) {
	req := &controlv1.RecordBriefRequest{
		OperationId: operationID, Brief: &controlv1.ArtifactBinding{TransferId: brief.TransferID, Digest: brief.Digest}, RequirementsDigest: requirementsDigest,
		SourceRevisions: toSourceRefs(sources), BrandDigests: toContentDigests(frozen.BrandDigests), AssetDigests: toContentDigests(frozen.AssetDigests),
	}
	req.Command = c.command(tenantID, commandID, req)
	resp, err := c.preparation().RecordBrief(ctx, req)
	if err != nil {
		return activities.BriefRef{}, nonRetryable(err)
	}
	return fromBrief(resp.GetBrief()), nil
}

var operationOutcomes = map[string]controlv1.OperationOutcome{
	"succeeded": controlv1.OperationOutcome_OPERATION_OUTCOME_SUCCEEDED, "failed": controlv1.OperationOutcome_OPERATION_OUTCOME_FAILED, "canceled": controlv1.OperationOutcome_OPERATION_OUTCOME_CANCELED,
}

func (c *Client) SettleOperation(ctx context.Context, in activities.SettleOperationInput) (activities.OperationRef, error) {
	req := &controlv1.SettleOperationRequest{Command: c.command(in.TenantID, in.CommandID, in), OperationId: in.OperationID, Outcome: operationOutcomes[in.Outcome], Phase: in.Phase}
	if in.FailureCode != "" {
		req.FailureCode = &in.FailureCode
	}
	resp, err := c.exec.SettleOperation(ctx, req)
	if err != nil {
		return activities.OperationRef{}, nonRetryable(err)
	}
	op := resp.GetOperation()
	return activities.OperationRef{Lifecycle: enumWord(op.GetLifecycle().String(), "LIFECYCLE_"), Phase: op.GetPhase(), Revision: op.GetRevision()}, nil
}

// ---- Generation ----

func fromLease(l *controlv1.LeaseRecord) activities.LeaseRecord {
	occ, _ := strconv.ParseUint(l.GetOccurrence(), 10, 64)
	out := activities.LeaseRecord{State: enumWord(l.GetState().String(), "LEASE_STATE_"), LeaseID: l.GetLeaseId(), Fence: l.GetFence(), Occurrence: occ}
	if l.ExpiresAt != nil {
		t := l.GetExpiresAt().AsTime()
		out.ExpiresAt = &t
	}
	return out
}

func (c *Client) GetGeneration(ctx context.Context, in activities.GetGenerationInput) (activities.GenerationView, error) {
	resp, err := c.generation().GetGeneration(ctx, &controlv1.GetGenerationRequest{TenantId: in.TenantID, OperationId: in.OperationID})
	if err != nil {
		return activities.GenerationView{}, nonRetryable(err)
	}
	g := resp.GetGeneration()
	repairs, _ := strconv.ParseUint(g.GetMaxRepairs(), 10, 64)
	view := activities.GenerationView{
		QueueDeadline: g.GetQueueDeadline().AsTime(), Lease: fromLease(g.GetLease()), Funded: g.Funding != nil, DefinitionActivation: g.GetDefinitionActivation(),
		MaxRepairs: repairs, CodegenProfileID: g.GetCodegenProfileId(), ValidatorProfileID: g.GetValidatorProfileId(), CandidateEffectID: g.GetCandidateEffectId(),
		PermitActive: g.Permit != nil && g.GetPermit().GetState() == controlv1.PermitState_PERMIT_STATE_ACTIVE,
		ProfileID:    g.GetSubject().GetProfileId(), SubjectDigest: g.GetSubject().GetSubjectDigest(), SourceRevision: g.GetSubject().GetSourceRevision(), ActorID: g.GetActorId(),
	}
	if g.Brief != nil {
		view.Brief = fromBrief(g.GetBrief())
		view.SourceRevisions = fromSourceRefs(g.GetBrief().GetSourceRevisions())
	}
	if g.ActiveDeadline != nil {
		t := g.GetActiveDeadline().AsTime()
		view.ActiveDeadline = &t
	}
	return view, nil
}

func (c *Client) RequestExecutionPermit(ctx context.Context, in activities.RequestExecutionPermitInput) (activities.PermitAnswer, error) {
	resp, err := c.generation().RequestExecutionPermit(ctx, &controlv1.RequestExecutionPermitRequest{Command: c.command(in.TenantID, in.CommandID, in), OperationId: in.OperationID})
	if err != nil {
		return activities.PermitAnswer{}, nonRetryable(err)
	}
	ahead, _ := strconv.ParseUint(resp.GetQueuedAhead(), 10, 64)
	out := activities.PermitAnswer{Granted: resp.GetGranted(), PermitID: resp.GetPermit().GetPermitId(), QueueDeadline: resp.GetQueueDeadline().AsTime(), QueuedAhead: ahead}
	if resp.ActiveDeadline != nil {
		t := resp.GetActiveDeadline().AsTime()
		out.ActiveDeadline = &t
	}
	return out, nil
}

func (c *Client) RecordFunding(ctx context.Context, in activities.RecordFundingInput) (activities.FundingRef, error) {
	resp, err := c.generation().RecordFunding(ctx, &controlv1.RecordFundingRequest{Command: c.command(in.TenantID, in.CommandID, in), OperationId: in.OperationID})
	if err != nil {
		return activities.FundingRef{}, nonRetryable(err)
	}
	return activities.FundingRef{Currency: resp.GetAmount().GetCurrency(), Amount: resp.GetAmount().GetAmount(), Existing: resp.GetExisting()}, nil
}

var leaseStates = map[string]controlv1.LeaseState{"held": controlv1.LeaseState_LEASE_STATE_HELD, "lost": controlv1.LeaseState_LEASE_STATE_LOST, "released": controlv1.LeaseState_LEASE_STATE_RELEASED}

func (c *Client) RecordLease(ctx context.Context, in activities.RecordLeaseInput) (activities.LeaseRecord, error) {
	req := &controlv1.RecordLeaseRequest{Command: c.command(in.TenantID, in.CommandID, in), OperationId: in.OperationID, Occurrence: strconv.FormatUint(in.Occurrence, 10), State: leaseStates[in.State], LeaseId: in.LeaseID, Fence: in.Fence}
	if in.ExpiresAt != nil {
		req.ExpiresAt = timestamppb.New(*in.ExpiresAt)
	}
	resp, err := c.generation().RecordLease(ctx, req)
	if err != nil {
		return activities.LeaseRecord{}, nonRetryable(err)
	}
	return fromLease(resp.GetLease()), nil
}

func (c *Client) GetAcceptedStage(ctx context.Context, in activities.GetAcceptedStageInput) (activities.AcceptedStage, error) {
	resp, err := c.exec.GetAcceptedStage(ctx, &controlv1.GetAcceptedStageRequest{AttemptId: in.AttemptID, TenantId: in.TenantID, OperationId: in.OperationID})
	if err != nil {
		if RefusalCodeOf(err) == "NOT_FOUND" {
			return activities.AcceptedStage{Found: false}, nil
		}
		return activities.AcceptedStage{}, nonRetryable(err)
	}
	st := resp.GetStage()
	out := activities.AcceptedStage{Found: true, StageID: st.GetStageId(), AttemptID: st.GetAttemptId(), Verdict: enumWord(st.GetVerdict().String(), "VERDICT_"), FailureCode: st.GetFailureCode(), ResultDigest: st.GetResultDigest(), ExecutionEpoch: st.GetExecutionEpoch()}
	for _, a := range st.GetArtifacts() {
		out.Artifacts = append(out.Artifacts, activities.StageArtifact{Handle: a.GetHandle(), Class: a.GetClass(), Digest: a.GetDigest(), SizeBytes: a.GetSizeBytes(), TransferID: a.GetTransferId(), ObjectVersion: a.GetObjectVersion()})
	}
	return out, nil
}

// RefusalCodeOf is the public code in front of a gRPC status message.
func RefusalCodeOf(err error) string {
	if r := nonRetryable(err); r != nil {
		return activities.RefusalCode(r)
	}
	return ""
}

func (c *Client) PrepareEffect(ctx context.Context, in activities.RegisterCandidateInput) (activities.EffectPermit, error) {
	req := &controlv1.PrepareEffectRequest{
		Binding: &controlv1.ExecutionBinding{OperationId: in.OperationID, AttemptId: in.AttemptID, InstanceId: in.InstanceID, ExecutionEpoch: strconv.FormatUint(in.ExecutionEpoch, 10)},
		Owner:   c.worker, Kind: controlv1.EffectKind_EFFECT_KIND_BUSINESS_WRITE, Occurrence: strconv.FormatUint(in.Occurrence, 10), CanonicalSubject: in.Subject,
		Deadline: timestamppb.New(in.Deadline),
	}
	if in.SourceRevision != "" {
		req.ExpectedRevision = &in.SourceRevision
	}
	if in.Lease.State == "held" && in.Lease.LeaseID != "" && in.Lease.ExpiresAt != nil {
		req.Lease = &controlv1.Lease{LeaseId: in.Lease.LeaseID, Fence: in.Lease.Fence, ExpiresAt: timestamppb.New(*in.Lease.ExpiresAt)}
	}
	// Bind the accepted source and independent certification into the immutable effect identity.
	req.Command = c.command(in.TenantID, in.CommandID, in)
	resp, err := c.effects().PrepareEffect(ctx, req)
	if err != nil {
		return activities.EffectPermit{}, nonRetryable(err)
	}
	return fromEffect(resp.GetEffect(), resp.GetPermitted()), nil
}

func fromEffect(e *controlv1.Effect, permitted bool) activities.EffectPermit {
	return activities.EffectPermit{EffectID: e.GetEffectId(), Permitted: permitted, State: enumWord(e.GetState().String(), "EFFECT_STATE_"), Outcome: enumWord(e.GetOutcome().String(), "EFFECT_OUTCOME_"), DenialCode: e.GetDenialCode(), OutcomeRef: e.GetOutcomeRef()}
}

var effectOutcomes = map[string]controlv1.EffectOutcome{"succeeded": controlv1.EffectOutcome_EFFECT_OUTCOME_SUCCEEDED, "failed": controlv1.EffectOutcome_EFFECT_OUTCOME_FAILED, "unknown": controlv1.EffectOutcome_EFFECT_OUTCOME_UNKNOWN}

func (c *Client) ObserveEffect(ctx context.Context, tenantID, effectID, source string, sequence uint64, outcome, receiptDigest, nativeRef string, at time.Time) error {
	_, err := c.effects().ObserveEffect(ctx, &controlv1.ObserveEffectRequest{TenantId: tenantID, EffectId: effectID, Source: source, Sequence: strconv.FormatUint(sequence, 10), Outcome: effectOutcomes[outcome], ReceiptDigest: receiptDigest, NativeReference: nativeRef, ObservedAt: timestamppb.New(at)})
	return nonRetryable(err)
}

func (c *Client) GetEffect(ctx context.Context, tenantID, effectID string) (activities.EffectPermit, error) {
	resp, err := c.effects().GetEffect(ctx, &controlv1.GetEffectRequest{TenantId: tenantID, EffectId: effectID})
	if err != nil {
		return activities.EffectPermit{}, nonRetryable(err)
	}
	return fromEffect(resp.GetEffect(), false), nil
}
