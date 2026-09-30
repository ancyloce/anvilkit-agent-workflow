package workflows

import (
	"strconv"
	"strings"

	"go.temporal.io/sdk/workflow"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// The profiles of the preview build: the DEVELOPMENT_ONLY fixed validator
// Job builds the saved revision under the build-support profile for the
// host ABI its fixture exercises (jobs/validator/profiles); a real Studio
// host profile is ENV-08.
const (
	previewStepID         = "preview-build"
	previewBuildProfileID = "build-support-dev-v1"
	previewHostProfileID  = "host-abi-dev-v1"
	previewSaveEffect     = 1
)

// PreviewBuild is the PreviewBuildWorkflow (P20, DD-04 §5): it saves the
// edited source conditionally under the base revision through the source
// authority (one effect identity; a conflict keeps the caller's edits and
// names the current revision; a lost answer is asked about, never saved
// again), then builds exactly the saved revision in the isolated build Job
// and records the preview's module and stylesheets. A build that finishes
// after a newer revision of the lineage was saved is stale: kept as a
// diagnostic, never the current preview. Compute only: no funding, no
// model call, no publication.
func PreviewBuild(q Queues, b Bounds, lb LifecycleBounds) func(ctx workflow.Context, in Input) error {
	return func(ctx workflow.Context, in Input) (err error) {
		logger := workflow.GetLogger(ctx)
		var bounds Bounds
		if err := workflow.SideEffect(ctx, func(workflow.Context) any { return b }).Get(&bounds); err != nil {
			return err
		}
		var buildProfile string
		if err := workflow.SideEffect(ctx, func(workflow.Context) any { return lb.PreviewBuildProfile }).Get(&buildProfile); err != nil {
			return err
		}
		operationID, tenantID := in.OperationID, in.TenantID
		business := activityOptions(ctx, q, bounds)
		state := &generationState{}

		outcome, failureCode, phase := "failed", "", ""
		defer func() {
			disconnected, _ := workflow.NewDisconnectedContext(ctx)
			if serr := settleOperation(disconnected, q, bounds, operationID, tenantID, operationID+":settle", outcome, failureCode, phase); serr != nil {
				logger.Error("preview settlement refused", "operationId", operationID, "error", serr)
				if err == nil {
					err = serr
				}
			}
		}()

		var src activities.PreviewSource
		if err := workflow.ExecuteActivity(business, activities.NameGetPreviewSource, tenantID, operationID).Get(ctx, &src); err != nil {
			outcome, failureCode = settleOutcome(ctx, err, "SOURCE_UNAVAILABLE")
			return nil
		}
		revision := uint64(0)
		record := func(r activities.PreviewRecordInput) bool {
			r.OperationID, r.TenantID, r.ExpectedRevision, r.SourceDigest = operationID, tenantID, revision, src.Source.Digest
			r.BuildProfileID, r.HostProfileID = previewBuildProfileID, previewHostProfileID
			disconnected, _ := workflow.NewDisconnectedContext(ctx)
			var next uint64
			if err := workflow.ExecuteActivity(controlOptions(disconnected, q, bounds), activities.NameRecordPreview, r).Get(disconnected, &next); err != nil {
				logger.Error("preview projection refused", "operationId", operationID, "state", r.State, "error", err)
				outcome, failureCode = settleOutcome(ctx, err, "PREVIEW_UNRECORDED")
				return false
			}
			revision = next
			return true
		}
		if !record(activities.PreviewRecordInput{State: "saving"}) {
			return nil
		}
		// savedRevision is the revision the save produced; a failure after the
		// save still names it (a preview's saved revision never changes).
		savedRevision := ""
		fail := func(code string) {
			outcome, failureCode, phase = "failed", code, "preview_failed"
			record(activities.PreviewRecordInput{State: "failed", FailureCode: code, SourceRevision: savedRevision})
		}

		// 1. The conditional save under one effect identity, owned by a
		// current attempt of the operation.
		var saveAttempt activities.AttemptRef
		if err := workflow.ExecuteActivity(business, activities.NameOpenAttempt, activities.OpenAttemptInput{
			OperationID: operationID, TenantID: tenantID, CommandID: operationID + ":save:1:open", StepID: "save_source", ProfileID: src.ProfileID,
		}).Get(ctx, &saveAttempt); err != nil {
			_, code := settleOutcome(ctx, err, "STALE_EXECUTION")
			fail(code)
			return nil
		}
		epoch, _ := strconv.ParseUint(saveAttempt.ExecutionEpoch, 10, 64)
		var saved activities.SaveSourceResult
		saveErr := workflow.ExecuteActivity(controlOptions(ctx, q, bounds), activities.NameSaveSource, activities.SaveSourceInput{
			OperationID: operationID, TenantID: tenantID, AttemptID: saveAttempt.AttemptID, ExecutionEpoch: epoch,
			CommandID: operationID + ":save:" + strconv.Itoa(previewSaveEffect), Occurrence: previewSaveEffect, Lineage: src.Lineage,
			BaseRevision: src.BaseRevision, Source: src.Source, Deadline: saveAttempt.Deadline,
		}).Get(ctx, &saved)
		closeOutcome := "completed"
		if saveErr != nil || saved.State != "saved" {
			closeOutcome = "failed"
		}
		disconnected, _ := workflow.NewDisconnectedContext(ctx)
		if cerr := closeAttempt(disconnected, q, bounds, saveAttempt, saveAttempt.AttemptID+":close", closeOutcome, "not_required", ""); cerr != nil {
			logger.Error("save attempt close refused", "attemptId", saveAttempt.AttemptID, "error", cerr)
		}
		switch {
		case saveErr != nil:
			_, code := settleOutcome(ctx, saveErr, "EFFECT_UNCERTAIN")
			fail(code)
			return nil
		case saved.State == "conflict":
			outcome, failureCode, phase = "failed", "REVISION_CONFLICT", "source_conflict"
			record(activities.PreviewRecordInput{State: "conflict", CurrentRevision: saved.CurrentRevision})
			return nil
		case saved.State == "denied":
			fail(firstNonEmpty(saved.DenialCode, "EFFECT_DENIED"))
			return nil
		case saved.State != "saved":
			fail("EFFECT_UNCERTAIN")
			return nil
		}
		savedRevision = saved.Revision
		if !record(activities.PreviewRecordInput{State: "building", SourceRevision: saved.Revision}) {
			return nil
		}

		// 2. The isolated build of exactly the saved bytes.
		stage, _, _, code := runJobStep(ctx, q, bounds, stepSpec{
			operationID: operationID, tenantID: tenantID, stepID: previewStepID, profileID: buildProfile, launchPrefix: "prv",
			inputs: []activities.LaunchInput{{Name: "source", Digest: src.Source.Digest, Handle: src.Source.Handle}},
		}, state)
		if code != "" {
			fail(code)
			return nil
		}
		if stage.Verdict != "certified" {
			fail(firstNonEmpty(stage.FailureCode, strings.ToUpper(stage.Verdict)))
			return nil
		}
		module := stageArtifact(&stage, "browser")
		if module == nil {
			fail("MODULE_MISSING")
			return nil
		}
		var styles []activities.StageArtifact
		for _, a := range stage.Artifacts {
			if a.Class == "css" {
				styles = append(styles, a)
			}
		}

		// 3. Ready, unless a newer revision of the lineage was saved while
		// this one built.
		var current string
		if err := workflow.ExecuteActivity(business, activities.NameCurrentSourceRevision, src.Lineage).Get(ctx, &current); err != nil {
			_, code := settleOutcome(ctx, err, "SOURCE_UNAVAILABLE")
			fail(code)
			return nil
		}
		final := activities.PreviewRecordInput{State: "ready", SourceRevision: saved.Revision, Module: module, Styles: styles}
		phase = "preview_ready"
		if current != saved.Revision {
			final.State, final.CurrentRevision, phase = "stale", current, "preview_stale"
		}
		if !record(final) {
			return nil
		}
		outcome = "succeeded"
		return nil
	}
}
