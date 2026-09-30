package workflows_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/workflows"
)

var (
	previewInput = workflows.Input{OperationID: "op_PRV", TenantID: "tenant_a"}
	editedSource = activities.StageArtifact{Handle: "h_edit", Class: "source", Digest: "sha256:edit", SizeBytes: "12", TransferID: "xfer_edit", ObjectVersion: "v1"}
	moduleA      = activities.StageArtifact{Handle: "h_mod", Class: "browser", Digest: "sha256:mod", SizeBytes: "20", TransferID: "xfer_mod", ObjectVersion: "v1"}
	cssA         = activities.StageArtifact{Handle: "h_css", Class: "css", Digest: "sha256:css", SizeBytes: "5", TransferID: "xfer_css", ObjectVersion: "v1"}
)

type previewScript struct {
	save    activities.SaveSourceResult
	saveErr error
	stage   activities.AcceptedStage
	current string
}

type previewRecord struct {
	settled
	mu2     sync.Mutex
	records []activities.PreviewRecordInput
	saves   []activities.SaveSourceInput
	creates []activities.CreateJobInput
}

func previewEnv(t *testing.T, script previewScript) (*testsuite.TestWorkflowEnvironment, *previewRecord) {
	t.Helper()
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	registerWith(env, bounds)
	var a lifecycleStub
	lb := workflows.LifecycleBounds{PreviewBuildProfile: "validator-fixed-dev-v1"}
	env.RegisterWorkflowWithOptions(workflows.PreviewBuild(queues, bounds, lb), workflow.RegisterOptions{Name: workflows.PreviewWorkflowName})
	for name, fn := range map[string]any{
		activities.NameSettleOperation: a.SettleOperation, activities.NameGetPreviewSource: a.GetPreviewSource, activities.NameRecordPreview: a.RecordPreview,
		activities.NameSaveSource: a.SaveSource, activities.NameCurrentSourceRevision: a.CurrentSourceRevision, activities.NameGetAcceptedStage: a.GetAcceptedStage,
	} {
		env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
	r := &previewRecord{}
	r.record(env)
	env.OnActivity(activities.NameGetPreviewSource, mock.Anything, "tenant_a", "op_PRV").Return(activities.PreviewSource{
		ProfileID: "preview-build-v1", Lineage: "sha256:lineage", BaseRevision: "3", Source: editedSource,
	}, nil)
	rev := uint64(0)
	env.OnActivity(activities.NameRecordPreview, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.PreviewRecordInput) (uint64, error) {
		r.mu2.Lock()
		defer r.mu2.Unlock()
		if in.ExpectedRevision != rev {
			return 0, errors.New("revision conflict")
		}
		r.records = append(r.records, in)
		rev++
		return rev, nil
	})
	env.OnActivity(activities.NameSaveSource, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.SaveSourceInput) (activities.SaveSourceResult, error) {
		r.mu2.Lock()
		r.saves = append(r.saves, in)
		r.mu2.Unlock()
		return script.save, script.saveErr
	})
	env.OnActivity(activities.NameCurrentSourceRevision, mock.Anything, "sha256:lineage").Return(script.current, nil)
	env.OnActivity(activities.NameOpenAttempt, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.OpenAttemptInput) (activities.AttemptRef, error) {
		return activities.AttemptRef{AttemptID: "att_" + in.CommandID, TenantID: in.TenantID, ProfileID: in.ProfileID, Deadline: env.Now().Add(time.Hour), ExecutionEpoch: "1"}, nil
	})
	env.OnActivity(activities.NamePrepareLaunch, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.PrepareLaunchInput) (activities.LaunchRef, error) {
		return activities.LaunchRef{LaunchID: "lch_" + in.Attempt.AttemptID, AttemptID: in.Attempt.AttemptID, LaunchKey: in.LaunchKey, ImageDigest: "sha256:img", LaunchEpoch: "1"}, nil
	})
	env.OnActivity(activities.NameCreateJob, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.CreateJobInput) (activities.JobRef, error) {
		r.mu2.Lock()
		r.creates = append(r.creates, in)
		r.mu2.Unlock()
		return activities.JobRef{JobUID: "job-" + in.Launch.AttemptID, Request: in.Request}, nil
	})
	exit := int32(0)
	env.OnActivity(activities.NameAwaitJobOwner, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.ObserveJobInput) (activities.JobObservation, error) {
		return activities.JobObservation{JobUID: "job-" + in.Launch.AttemptID, Pods: []activities.PodObservation{{PodUID: "pod-" + in.Launch.AttemptID, Phase: "running"}}}, nil
	})
	env.OnActivity(activities.NameObserveJob, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.ObserveJobInput) (activities.JobObservation, error) {
		return activities.JobObservation{JobUID: "job-" + in.Launch.AttemptID, Pods: []activities.PodObservation{{PodUID: "pod-" + in.Launch.AttemptID, Phase: "succeeded", ExitCode: &exit}}}, nil
	})
	env.OnActivity(activities.NameRegisterInstance, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.RegisterInstanceInput) (activities.InstanceRef, error) {
		return activities.InstanceRef{InstanceID: "inst-" + in.Attempt.AttemptID, Current: true}, nil
	})
	env.OnActivity(activities.NameObserveInstance, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(activities.NameDeleteJob, mock.Anything, mock.Anything).Return(activities.LaunchObservation{}, nil)
	env.OnActivity(activities.NameGetAcceptedStage, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.GetAcceptedStageInput) (activities.AcceptedStage, error) {
		st := script.stage
		st.AttemptID = in.AttemptID
		return st, nil
	})
	return env, r
}

func (r *previewRecord) states() []string {
	r.mu2.Lock()
	defer r.mu2.Unlock()
	out := make([]string, 0, len(r.records))
	for _, rec := range r.records {
		out = append(out, rec.State)
	}
	return out
}

func TestPreviewBuild(t *testing.T) {
	t.Run("the saved revision is built exactly and becomes the ready preview", func(t *testing.T) {
		env, r := previewEnv(t, previewScript{save: activities.SaveSourceResult{EffectID: "eff_1", State: "saved", Revision: "4"},
			stage: certified("", moduleA, cssA, evidence), current: "4"})
		env.ExecuteWorkflow(workflows.PreviewWorkflowName, previewInput)
		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError())
		require.Equal(t, []string{"saving", "building", "ready"}, r.states())
		ready := r.records[2]
		require.Equal(t, "4", ready.SourceRevision)
		require.Equal(t, moduleA, *ready.Module)
		require.Equal(t, []activities.StageArtifact{cssA}, ready.Styles)
		require.Equal(t, "sha256:edit", ready.SourceDigest)
		require.Len(t, r.saves, 1)
		require.Equal(t, "3", r.saves[0].BaseRevision, "the save expects the revision the edit was based on")
		require.Equal(t, "op_PRV:save:1", r.saves[0].CommandID)
		require.Len(t, r.creates, 1)
		require.Equal(t, []activities.LaunchInput{{Name: "source", Digest: "sha256:edit", Handle: "h_edit"}}, r.creates[0].Inputs, "the build gets exactly the saved bytes")
		require.Equal(t, "validator-fixed-dev-v1", r.creates[0].ProfileID)
		last := r.last()
		require.Equal(t, "succeeded", last.Outcome)
		require.Equal(t, "preview_ready", last.Phase)
	})

	t.Run("a conflicting save keeps the edits: no build, the current revision is named", func(t *testing.T) {
		env, r := previewEnv(t, previewScript{save: activities.SaveSourceResult{EffectID: "eff_1", State: "conflict", CurrentRevision: "5"}})
		env.ExecuteWorkflow(workflows.PreviewWorkflowName, previewInput)
		require.NoError(t, env.GetWorkflowError())
		require.Equal(t, []string{"saving", "conflict"}, r.states())
		require.Equal(t, "5", r.records[1].CurrentRevision)
		require.Empty(t, r.creates, "nothing is built for a conflicting save")
		require.Equal(t, "REVISION_CONFLICT", r.last().FailureCode)
	})

	t.Run("a build that finishes after a newer save is stale, never current", func(t *testing.T) {
		env, r := previewEnv(t, previewScript{save: activities.SaveSourceResult{EffectID: "eff_1", State: "saved", Revision: "4"},
			stage: certified("", moduleA), current: "6"})
		env.ExecuteWorkflow(workflows.PreviewWorkflowName, previewInput)
		require.NoError(t, env.GetWorkflowError())
		require.Equal(t, []string{"saving", "building", "stale"}, r.states())
		require.Equal(t, "6", r.records[2].CurrentRevision)
		require.NotNil(t, r.records[2].Module, "kept as a diagnostic")
		require.Equal(t, "preview_stale", r.last().Phase)
	})

	t.Run("a build without a certified module fails with the build's own code", func(t *testing.T) {
		stage := activities.AcceptedStage{Found: true, StageID: "stg", Verdict: "invalid", FailureCode: "BUILD_FAILED", ExecutionEpoch: "1", Artifacts: []activities.StageArtifact{evidence}}
		env, r := previewEnv(t, previewScript{save: activities.SaveSourceResult{EffectID: "eff_1", State: "saved", Revision: "4"}, stage: stage, current: "4"})
		env.ExecuteWorkflow(workflows.PreviewWorkflowName, previewInput)
		require.NoError(t, env.GetWorkflowError())
		require.Equal(t, []string{"saving", "building", "failed"}, r.states())
		require.Equal(t, "BUILD_FAILED", r.records[2].FailureCode)
		require.Equal(t, "4", r.records[2].SourceRevision, "the failure keeps naming the saved revision (Control refuses a changed one)")
		require.Equal(t, "failed", r.last().Outcome)
	})

	t.Run("a save whose answer was lost stays uncertain: never saved again, never built", func(t *testing.T) {
		env, r := previewEnv(t, previewScript{saveErr: errors.New("source save of effect eff_1 did not answer")})
		env.ExecuteWorkflow(workflows.PreviewWorkflowName, previewInput)
		require.NoError(t, env.GetWorkflowError())
		require.Equal(t, "failed", r.states()[len(r.states())-1])
		require.Empty(t, r.creates)
		require.Equal(t, "failed", r.last().Outcome)
	})
}
