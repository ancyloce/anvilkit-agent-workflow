package activities

import (
	"context"
	"fmt"
	"time"
)

// Preview build activities (P20, DD-04 §5): read the operation's edited
// source, save it conditionally through the source authority under an
// effect identity (a conflict names the current revision; a lost answer is
// asked about the original identity, never saved again), record the
// preview projection in Control, and read the lineage's current revision
// to mark a finished build stale.
const (
	NameGetPreviewSource      = "GetPreviewSource"
	NameRecordPreview         = "RecordPreview"
	NameSaveSource            = "SaveSource"
	NameCurrentSourceRevision = "CurrentSourceRevision"
)

// PreviewSource is the preview_build operation's binding.
type PreviewSource struct {
	ProfileID    string
	Lineage      string // the component source lineage (subject digest)
	BaseRevision string
	Source       StageArtifact
}

// PreviewRecordInput is one transition of the preview projection.
type PreviewRecordInput struct {
	OperationID      string
	TenantID         string
	ExpectedRevision uint64
	State            string // saving | conflict | building | ready | stale | failed
	SourceRevision   string
	CurrentRevision  string
	SourceDigest     string
	Module           *StageArtifact
	Styles           []StageArtifact
	BuildProfileID   string
	HostProfileID    string
	FailureCode      string
}

// PreviewControl is the Workflow's port to Control for preview builds.
type PreviewControl interface {
	GetPreviewSource(ctx context.Context, tenantID, operationID string) (PreviewSource, error)
	// RecordPreview answers the projection revision after the transition.
	RecordPreview(ctx context.Context, in PreviewRecordInput) (uint64, error)
}

// SaveSourceInput saves the edited source of a lineage under the expected
// revision.
type SaveSourceInput struct {
	OperationID    string
	TenantID       string
	AttemptID      string
	ExecutionEpoch uint64
	CommandID      string
	Occurrence     uint64
	Lineage        string
	BaseRevision   string
	Source         StageArtifact
	Deadline       time.Time
}

// SaveSourceResult is the save's outcome: saved (Revision is the new
// revision), conflict (CurrentRevision is newer than the base), denied by
// Control, or unknown (the save may have happened; never repeated).
type SaveSourceResult struct {
	EffectID        string
	State           string
	Revision        string
	CurrentRevision string
	DenialCode      string
}

func (a *LifecycleActivities) GetPreviewSource(ctx context.Context, tenantID, operationID string) (PreviewSource, error) {
	return a.Preview.GetPreviewSource(ctx, tenantID, operationID)
}

func (a *LifecycleActivities) RecordPreview(ctx context.Context, in PreviewRecordInput) (uint64, error) {
	return a.Preview.RecordPreview(ctx, in)
}

func (a *LifecycleActivities) CurrentSourceRevision(ctx context.Context, lineage string) (string, error) {
	return a.Source.CurrentRevision(ctx, "source:"+lineage)
}

// SaveSource saves the edited source under the effect identity: the permit
// is prepared under CommandID with the base revision as the expected one;
// one save is sent when this call consumed the permit and its outcome is
// observed; when the permit was consumed earlier (a lost receipt) the
// original effect is read and, if unresolved, the source authority is asked
// about the original identity — never saved again.
func (a *LifecycleActivities) SaveSource(ctx context.Context, in SaveSourceInput) (SaveSourceResult, error) {
	permit, err := a.Generation.PrepareEffect(ctx, RegisterCandidateInput{
		OperationID: in.OperationID, TenantID: in.TenantID, AttemptID: in.AttemptID, ExecutionEpoch: in.ExecutionEpoch, CommandID: in.CommandID,
		Occurrence: in.Occurrence, Subject: "source:" + in.Lineage, SourceRevision: in.BaseRevision, Source: in.Source, Deadline: in.Deadline,
	})
	if err != nil {
		return SaveSourceResult{}, err
	}
	observe := func(res SaveSourceResult, source string) (SaveSourceResult, error) {
		outcome, ref := "succeeded", "revision:"+res.Revision
		if res.State == "conflict" {
			outcome, ref = "failed", "conflict:"+res.CurrentRevision
		}
		if err := a.Generation.ObserveEffect(ctx, in.TenantID, permit.EffectID, source, 1, outcome, in.Source.Digest, ref, time.Now().UTC()); err != nil {
			return SaveSourceResult{}, err
		}
		res.EffectID = permit.EffectID
		return res, nil
	}
	if permit.Permitted {
		res, err := a.Source.SaveRevision(ctx, permit.EffectID, in)
		if err != nil {
			// The save may have happened: the effect stays permitted and
			// unresolved for the query path.
			return SaveSourceResult{EffectID: permit.EffectID, State: "unknown"}, fmt.Errorf("source save of effect %s did not answer: %w", permit.EffectID, err)
		}
		return observe(res, "workflow")
	}
	if permit.State == "denied" {
		return SaveSourceResult{EffectID: permit.EffectID, State: "denied", DenialCode: permit.DenialCode}, nil
	}
	// Permitted earlier, settled or unknown: the original identity answers.
	res, known, err := a.Source.QuerySave(ctx, permit.EffectID)
	if err != nil {
		return SaveSourceResult{EffectID: permit.EffectID, State: "unknown"}, err
	}
	if !known {
		return SaveSourceResult{EffectID: permit.EffectID, State: "unknown"}, nil
	}
	if permit.State == "succeeded" || permit.State == "failed" {
		res.EffectID = permit.EffectID
		return res, nil
	}
	return observe(res, "workflow-query")
}
