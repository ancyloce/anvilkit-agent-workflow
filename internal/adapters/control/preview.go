package control

import (
	"context"
	"fmt"
	"strconv"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// Preview builds (P20): the operation's binding read from Control
// (OperationService and the Workflow's own ReadArtifact of the edited
// source, which Control relates to the preview operation), and the preview
// projection recorded through PreviewService.

func (c *Client) GetPreviewSource(ctx context.Context, tenantID, operationID string) (activities.PreviewSource, error) {
	op, err := controlv1.NewOperationServiceClient(c.conn).GetOperation(ctx, &controlv1.GetOperationRequest{
		Scope: &controlv1.Scope{TenantId: tenantID, ActorId: c.worker}, OperationId: operationID,
	})
	if err != nil {
		return activities.PreviewSource{}, nonRetryable(err)
	}
	subject := op.GetOperation().GetSubject()
	if op.GetOperation().GetKind() != controlv1.OperationKind_OPERATION_KIND_PREVIEW_BUILD || subject.GetSourceHandle() == "" {
		return activities.PreviewSource{}, activities.Refused("INVALID_ARGUMENT", fmt.Errorf("operation %s is not a preview build with its source", operationID))
	}
	read, err := controlv1.NewArtifactServiceClient(c.conn).ReadArtifact(ctx, &controlv1.ReadArtifactRequest{Handle: subject.GetSourceHandle(), OperationId: operationID})
	if err != nil {
		return activities.PreviewSource{}, nonRetryable(err)
	}
	t := read.GetTransfer()
	return activities.PreviewSource{
		ProfileID: subject.GetProfileId(), Lineage: subject.GetSubjectDigest(), BaseRevision: subject.GetSourceRevision(),
		Source: activities.StageArtifact{Handle: t.GetHandle(), Class: "source", Digest: t.GetExpectedDigest(), SizeBytes: t.GetExpectedSize(),
			TransferID: t.GetTransferId(), ObjectVersion: t.GetObjectVersion()},
	}, nil
}

var previewStates = map[string]controlv1.PreviewState{
	"saving": controlv1.PreviewState_PREVIEW_STATE_SAVING, "conflict": controlv1.PreviewState_PREVIEW_STATE_CONFLICT,
	"building": controlv1.PreviewState_PREVIEW_STATE_BUILDING, "ready": controlv1.PreviewState_PREVIEW_STATE_READY,
	"stale": controlv1.PreviewState_PREVIEW_STATE_STALE, "failed": controlv1.PreviewState_PREVIEW_STATE_FAILED,
}

func artifactRef(a activities.StageArtifact) *controlv1.ArtifactReference {
	return &controlv1.ArtifactReference{Handle: a.Handle, Class: a.Class, Digest: a.Digest, SizeBytes: a.SizeBytes, TransferId: a.TransferID, ObjectVersion: a.ObjectVersion}
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func (c *Client) RecordPreview(ctx context.Context, in activities.PreviewRecordInput) (uint64, error) {
	req := &controlv1.RecordPreviewRequest{
		OperationId: in.OperationID, ExpectedRevision: strconv.FormatUint(in.ExpectedRevision, 10), State: previewStates[in.State],
		SourceRevision: optional(in.SourceRevision), CurrentRevision: optional(in.CurrentRevision), SourceDigest: in.SourceDigest,
		BuildProfileId: in.BuildProfileID, HostProfileId: in.HostProfileID, FailureCode: optional(in.FailureCode),
	}
	if in.Module != nil {
		req.Module = artifactRef(*in.Module)
	}
	for _, s := range in.Styles {
		req.Styles = append(req.Styles, artifactRef(s))
	}
	resp, err := controlv1.NewPreviewServiceClient(c.conn).RecordPreview(ctx, req)
	if err != nil {
		return 0, nonRetryable(err)
	}
	return strconv.ParseUint(resp.GetPreview().GetRevision(), 10, 64)
}
