package control

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// Releases (P21): the release operation's binding read from Control (the
// operation and the Workflow's own ReadArtifact of the source Control
// bound), the release projection recorded through ReleaseService, and the
// release mutations' permits through EffectService.

func (c *Client) GetReleaseBinding(ctx context.Context, tenantID, operationID string) (activities.ReleaseBinding, error) {
	op, err := controlv1.NewOperationServiceClient(c.conn).GetOperation(ctx, &controlv1.GetOperationRequest{
		Scope: &controlv1.Scope{TenantId: tenantID, ActorId: c.worker}, OperationId: operationID,
	})
	if err != nil {
		return activities.ReleaseBinding{}, nonRetryable(err)
	}
	view := op.GetOperation()
	subject := view.GetSubject()
	if view.GetKind() != controlv1.OperationKind_OPERATION_KIND_RELEASE || subject.GetSourceHandle() == "" || subject.GetPackageVersion() == "" {
		return activities.ReleaseBinding{}, activities.Refused("INVALID_ARGUMENT", fmt.Errorf("operation %s is not a release with its bound source", operationID))
	}
	read, err := controlv1.NewArtifactServiceClient(c.conn).ReadArtifact(ctx, &controlv1.ReadArtifactRequest{Handle: subject.GetSourceHandle(), OperationId: operationID})
	if err != nil {
		return activities.ReleaseBinding{}, nonRetryable(err)
	}
	t := read.GetTransfer()
	epoch, err := strconv.ParseUint(view.GetExecutionEpoch(), 10, 64)
	if err != nil {
		return activities.ReleaseBinding{}, fmt.Errorf("operation %s epoch %q", operationID, view.GetExecutionEpoch())
	}
	return activities.ReleaseBinding{
		ProfileID: subject.GetProfileId(), Lineage: subject.GetSubjectDigest(), SourceRevision: subject.GetSourceRevision(), PackageVersion: subject.GetPackageVersion(),
		Source: activities.StageArtifact{Handle: t.GetHandle(), Class: "source", Digest: t.GetExpectedDigest(), SizeBytes: t.GetExpectedSize(),
			TransferID: t.GetTransferId(), ObjectVersion: t.GetObjectVersion()},
		ExecutionEpoch: epoch, Deadline: view.GetDeadline().AsTime(),
	}, nil
}

var (
	releaseStates = map[string]controlv1.ReleaseState{
		"certifying": controlv1.ReleaseState_RELEASE_STATE_CERTIFYING, "awaiting_approval": controlv1.ReleaseState_RELEASE_STATE_AWAITING_APPROVAL,
		"publishing": controlv1.ReleaseState_RELEASE_STATE_PUBLISHING, "published": controlv1.ReleaseState_RELEASE_STATE_PUBLISHED,
		"activated": controlv1.ReleaseState_RELEASE_STATE_ACTIVATED, "partially_published": controlv1.ReleaseState_RELEASE_STATE_PARTIALLY_PUBLISHED,
		"reconciling": controlv1.ReleaseState_RELEASE_STATE_RECONCILING, "rejected": controlv1.ReleaseState_RELEASE_STATE_REJECTED,
		"failed": controlv1.ReleaseState_RELEASE_STATE_FAILED,
	}
	approvalStates = map[string]controlv1.ApprovalState{
		"pending": controlv1.ApprovalState_APPROVAL_STATE_PENDING, "approved": controlv1.ApprovalState_APPROVAL_STATE_APPROVED,
		"rejected": controlv1.ApprovalState_APPROVAL_STATE_REJECTED, "invalidated": controlv1.ApprovalState_APPROVAL_STATE_INVALIDATED,
	}
	targetStates = map[string]controlv1.TargetState{
		"pending": controlv1.TargetState_TARGET_STATE_PENDING, "succeeded": controlv1.TargetState_TARGET_STATE_SUCCEEDED,
		"failed": controlv1.TargetState_TARGET_STATE_FAILED, "unknown": controlv1.TargetState_TARGET_STATE_UNKNOWN,
	}
	effectKinds = map[string]controlv1.EffectKind{
		"review": controlv1.EffectKind_EFFECT_KIND_REVIEW, "publication": controlv1.EffectKind_EFFECT_KIND_PUBLICATION,
		"activation": controlv1.EffectKind_EFFECT_KIND_ACTIVATION,
	}
)

func toTarget(t activities.ReleaseTargetRecord) *controlv1.ReleaseTarget {
	state := t.State
	if state == "" {
		state = "pending"
	}
	return &controlv1.ReleaseTarget{
		State: targetStates[state], EffectId: optional(t.EffectID), ReceiptId: optional(t.ReceiptID), ReceiptDigest: optional(t.ReceiptDigest),
		SubjectDigest: optional(t.SubjectDigest), Destination: optional(t.Destination), Version: optional(t.Version),
		ManifestDigest: optional(t.ManifestDigest), FailureCode: optional(t.FailureCode),
	}
}

func toSubject(s *activities.ReleaseSubject) *controlv1.ReleaseSubject {
	if s == nil {
		return nil
	}
	ref := func(a activities.ArtifactDigest) *controlv1.ArtifactDigest {
		return &controlv1.ArtifactDigest{Digest: a.Digest, SizeBytes: a.SizeBytes}
	}
	out := &controlv1.ReleaseSubject{
		ComponentId: s.ComponentID, PuckType: s.PuckType, SourceRevision: s.SourceRevision, SourceDigest: s.SourceDigest, PackageName: s.PackageName,
		Version: s.Version, Npm: ref(s.Npm), Browser: ref(s.Browser), BuildProfileId: s.BuildProfileID, BuildProfileDigest: s.BuildProfileDigest,
		ValidatorProfileId: s.ValidatorProfileID, ValidatorProfileDigest: s.ValidatorProfileDigest, HostAbi: s.HostAbi, HostAbiDigest: s.HostAbiDigest,
		NpmRegistry: s.Destinations.NpmRegistry, BrowserOrigin: s.Destinations.BrowserOrigin, CertificationEvidenceDigest: s.CertificationEvidenceDigest,
		SubjectDigest: s.SubjectDigest,
	}
	for _, c := range s.CSS {
		out.Css = append(out.Css, ref(c))
	}
	return out
}

func optTimestamp(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

func (c *Client) RecordRelease(ctx context.Context, in activities.ReleaseRecordInput) (uint64, error) {
	req := &controlv1.RecordReleaseRequest{
		OperationId: in.OperationID, ExpectedRevision: strconv.FormatUint(in.ExpectedRevision, 10), State: releaseStates[in.State], Subject: toSubject(in.Subject),
		ReleaseId: optional(in.ReleaseID), ReviewEffectId: optional(in.ReviewEffectID), ApprovalDeadline: optTimestamp(in.ApprovalDeadline),
		Npm: toTarget(in.Npm), Browser: toTarget(in.Browser), Activation: toTarget(in.Activation), CatalogRevision: optional(in.CatalogRevision),
		FailureCode: optional(in.FailureCode),
	}
	if a := in.Approval; a != nil {
		req.Approval = &controlv1.Approval{State: approvalStates[a.State], SubjectDigest: a.SubjectDigest, ApproverId: optional(a.ApproverID),
			DecidedAt: optTimestamp(a.DecidedAt), ReasonCode: optional(a.ReasonCode)}
	}
	resp, err := controlv1.NewReleaseServiceClient(c.conn).RecordRelease(ctx, req)
	if err != nil {
		return 0, nonRetryable(err)
	}
	return strconv.ParseUint(resp.GetRelease().GetRevision(), 10, 64)
}

func (c *Client) PrepareReleaseEffect(ctx context.Context, in activities.ReleaseEffectInput) (activities.EffectPermit, error) {
	req := &controlv1.PrepareEffectRequest{
		Binding: &controlv1.ExecutionBinding{OperationId: in.OperationID, AttemptId: in.AttemptID, ExecutionEpoch: strconv.FormatUint(in.ExecutionEpoch, 10)},
		Owner:   c.worker, Kind: effectKinds[in.Kind], Occurrence: strconv.FormatUint(in.Occurrence, 10), CanonicalSubject: in.CanonicalSubject,
		Deadline: timestamppb.New(in.Deadline), Command: c.command(in.TenantID, in.CommandID, in),
	}
	resp, err := c.effects().PrepareEffect(ctx, req)
	if err != nil {
		return activities.EffectPermit{}, nonRetryable(err)
	}
	return fromEffect(resp.GetEffect(), resp.GetPermitted()), nil
}
