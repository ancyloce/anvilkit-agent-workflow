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
	releaseInput = workflows.Input{OperationID: "op_REL", TenantID: "tenant_a"}
	npmA         = activities.StageArtifact{Handle: "h_npm", Class: "npm", Digest: "sha256:npm", SizeBytes: "30", TransferID: "xfer_npm", ObjectVersion: "v1"}
	subjectA     = activities.ReleaseSubject{SchemaVersion: 1, ComponentID: "cmp_hero", PuckType: "Hero", SourceRevision: "4", PackageName: "@anvilkit/hero", Version: "1.0.0", HostAbi: "host-abi-dev-v1", SubjectDigest: "sha256:subjectA"}
)

// releaseScript scripts the ports: the maintainer's decisions in poll
// order (the last one repeats), each target's answers in call order (the
// last repeats) and the activation answers.
type releaseScript struct {
	stage      activities.AcceptedStage
	decisions  []activities.ReviewDecision
	npm        []activities.ReleaseEffectResult
	browser    []activities.ReleaseEffectResult
	activation []activities.ReleaseEffectResult
	bindingErr error
	pollsRun   int
}

type releaseRecord struct {
	settled
	mu2         sync.Mutex
	records     []activities.ReleaseRecordInput
	publishes   map[string][]activities.ReleaseEffectCall
	activations []activities.ReleaseEffectCall
	creates     []activities.CreateJobInput
	polls       int
}

func approved(digest string) activities.ReviewDecision {
	return activities.ReviewDecision{State: "approved", SubjectDigest: digest, ApproverID: "maintainer_a"}
}

func published(target string) activities.ReleaseEffectResult {
	t := activities.ReleaseTargetRecord{State: "succeeded", EffectID: "eff_" + target, ReceiptID: "rcpt_" + target, ReceiptDigest: "sha256:r" + target,
		SubjectDigest: subjectA.SubjectDigest, Destination: "https://" + target + ".invalid/", Version: "1.0.0"}
	return activities.ReleaseEffectResult{State: "succeeded", EffectID: t.EffectID, Target: t}
}

func unknownTarget(target string) activities.ReleaseEffectResult {
	return activities.ReleaseEffectResult{State: "unknown", EffectID: "eff_" + target, Target: activities.ReleaseTargetRecord{State: "unknown", EffectID: "eff_" + target}}
}

func failedTarget(target, code string) activities.ReleaseEffectResult {
	return activities.ReleaseEffectResult{State: "failed", EffectID: "eff_" + target, Target: activities.ReleaseTargetRecord{State: "failed", EffectID: "eff_" + target, FailureCode: code}}
}

func nth[T any](list []T, i int) T {
	if i >= len(list) {
		return list[len(list)-1]
	}
	return list[i]
}

func releaseEnv(t *testing.T, script releaseScript) (*testsuite.TestWorkflowEnvironment, *releaseRecord) {
	t.Helper()
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	registerWith(env, bounds)
	var a lifecycleStub
	polls := script.pollsRun
	if polls == 0 {
		polls = 100
	}
	lb := workflows.LifecycleBounds{
		ReleaseValidatorProfile: "validator-fixed-dev-v1", ReleaseDestinations: activities.ReleaseDestinations{NpmRegistry: "https://npm.invalid/", BrowserOrigin: "https://browser.invalid/"},
		ReleaseApprovalWait: 48 * time.Hour, ReleaseApprovalPoll: time.Minute, ReleasePollsPerRun: polls, ReleaseReconcileRounds: 3, ReleaseReconcilePause: time.Minute,
	}
	env.RegisterWorkflowWithOptions(workflows.Release(queues, bounds, lb), workflow.RegisterOptions{Name: workflows.ReleaseWorkflowName})
	for name, fn := range map[string]any{
		activities.NameSettleOperation: a.SettleOperation, activities.NameGetReleaseBinding: a.GetReleaseBinding, activities.NameBuildReleaseSubj: a.BuildReleaseSubject,
		activities.NameRegisterReview: a.RegisterReview, activities.NameReviewDecision: a.ReviewDecision, activities.NamePublishTarget: a.PublishTarget,
		activities.NameCatalogRevision: a.CatalogRevision, activities.NameActivateRelease: a.ActivateRelease, activities.NameRecordRelease: a.RecordRelease,
		activities.NameGetAcceptedStage: a.GetAcceptedStage,
	} {
		env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
	r := &releaseRecord{publishes: map[string][]activities.ReleaseEffectCall{}}
	r.record(env)
	env.OnActivity(activities.NameGetReleaseBinding, mock.Anything, "tenant_a", "op_REL").Return(activities.ReleaseBinding{
		ProfileID: "release-v1", Lineage: "sha256:lineage", SourceRevision: "4", PackageVersion: "1.0.0", Source: editedSource, ExecutionEpoch: 1,
		Deadline: env.Now().Add(720 * time.Hour),
	}, script.bindingErr)
	env.OnActivity(activities.NameBuildReleaseSubj, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.BuildSubjectInput) (activities.BuiltSubject, error) {
		return activities.BuiltSubject{Subject: subjectA, Artifacts: activities.ReleaseArtifacts{Npm: npmA, Browser: moduleA, CSS: []activities.ReleaseStyle{{Path: "styles/hero.css", Artifact: cssA}}}}, nil
	})
	env.OnActivity(activities.NameRegisterReview, mock.Anything, mock.Anything).Return(activities.ReleaseEffectResult{State: "succeeded", EffectID: "eff_review", ReleaseID: "rel_1"}, nil)
	env.OnActivity(activities.NameReviewDecision, mock.Anything, "rel_1").Return(func(_ context.Context, _ string) (activities.ReviewDecision, error) {
		r.mu2.Lock()
		defer r.mu2.Unlock()
		d := nth(script.decisions, r.polls)
		r.polls++
		return d, nil
	})
	env.OnActivity(activities.NamePublishTarget, mock.Anything, mock.Anything).Return(func(_ context.Context, call activities.ReleaseEffectCall) (activities.ReleaseEffectResult, error) {
		r.mu2.Lock()
		defer r.mu2.Unlock()
		n := len(r.publishes[call.Target])
		r.publishes[call.Target] = append(r.publishes[call.Target], call)
		if call.Target == "npm" {
			return nth(script.npm, n), nil
		}
		return nth(script.browser, n), nil
	})
	env.OnActivity(activities.NameCatalogRevision, mock.Anything).Return("4", nil)
	env.OnActivity(activities.NameActivateRelease, mock.Anything, mock.Anything).Return(func(_ context.Context, call activities.ReleaseEffectCall) (activities.ReleaseEffectResult, error) {
		r.mu2.Lock()
		defer r.mu2.Unlock()
		n := len(r.activations)
		r.activations = append(r.activations, call)
		return nth(script.activation, n), nil
	})
	rev := uint64(0)
	env.OnActivity(activities.NameRecordRelease, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.ReleaseRecordInput) (uint64, error) {
		r.mu2.Lock()
		defer r.mu2.Unlock()
		if in.ExpectedRevision != rev {
			return 0, errors.New("revision conflict")
		}
		r.records = append(r.records, in)
		rev++
		return rev, nil
	})
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

func (r *releaseRecord) states() []string {
	r.mu2.Lock()
	defer r.mu2.Unlock()
	out := make([]string, 0, len(r.records))
	for _, rec := range r.records {
		out = append(out, rec.State)
	}
	return out
}

func (r *releaseRecord) final() activities.ReleaseRecordInput {
	r.mu2.Lock()
	defer r.mu2.Unlock()
	return r.records[len(r.records)-1]
}

func certifiedRelease() activities.AcceptedStage {
	return certified("", npmA, moduleA, cssA, evidence)
}

func TestReleaseWorkflow(t *testing.T) {
	t.Run("the exact certified subject is approved, both targets are published once and then activated", func(t *testing.T) {
		env, r := releaseEnv(t, releaseScript{stage: certifiedRelease(),
			decisions: []activities.ReviewDecision{{State: "pending"}, {State: "pending"}, approved(subjectA.SubjectDigest)},
			npm:       []activities.ReleaseEffectResult{published("npm")}, browser: []activities.ReleaseEffectResult{published("browser")},
			activation: []activities.ReleaseEffectResult{{State: "succeeded", EffectID: "eff_act", CatalogRevision: "5",
				Target: activities.ReleaseTargetRecord{State: "succeeded", EffectID: "eff_act", ReceiptID: "rcpt_act", ReceiptDigest: "sha256:ract", SubjectDigest: subjectA.SubjectDigest}}}})
		env.ExecuteWorkflow(workflows.ReleaseWorkflowName, releaseInput)
		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError())
		require.Equal(t, []string{"certifying", "awaiting_approval", "publishing", "publishing", "published", "activated"}, r.states())
		require.Len(t, r.creates, 1)
		require.Equal(t, []activities.LaunchInput{{Name: "source", Digest: "sha256:edit", Handle: "h_edit"}}, r.creates[0].Inputs, "the validator certifies exactly the bound bytes")
		require.Len(t, r.publishes["npm"], 1)
		require.Len(t, r.publishes["browser"], 1)
		npm, browser := r.publishes["npm"][0].Effect, r.publishes["browser"][0].Effect
		require.Equal(t, "release:sha256:subjectA:npm", npm.CanonicalSubject)
		require.Equal(t, "release:sha256:subjectA:browser", browser.CanonicalSubject)
		require.NotEqual(t, npm.CommandID, browser.CommandID, "each target has its own effect identity")
		require.Len(t, r.activations, 1)
		require.Equal(t, "4", r.activations[0].ExpectedCatalogRevision)
		final := r.final()
		require.Equal(t, "5", final.CatalogRevision)
		require.Equal(t, "approved", final.Approval.State)
		require.Equal(t, subjectA.SubjectDigest, final.Approval.SubjectDigest)
		require.Equal(t, "succeeded", r.last().Outcome)
		require.Equal(t, "activated", r.last().Phase)
		awaiting := r.records[1]
		require.NotNil(t, awaiting.ApprovalDeadline)
		require.Equal(t, "rel_1", awaiting.ReleaseID)
	})

	t.Run("a stale approval of another subject is rejected and publishes nothing", func(t *testing.T) {
		env, r := releaseEnv(t, releaseScript{stage: certifiedRelease(), decisions: []activities.ReviewDecision{approved("sha256:older")}})
		env.ExecuteWorkflow(workflows.ReleaseWorkflowName, releaseInput)
		require.NoError(t, env.GetWorkflowError())
		require.Equal(t, "rejected", r.final().State)
		require.Equal(t, "APPROVAL_SUBJECT_MISMATCH", r.final().FailureCode)
		require.Empty(t, r.publishes)
		require.Empty(t, r.activations)
		require.Equal(t, "failed", r.last().Outcome)
	})

	t.Run("npm-only success is partially published: never activated, never republished", func(t *testing.T) {
		env, r := releaseEnv(t, releaseScript{stage: certifiedRelease(), decisions: []activities.ReviewDecision{approved(subjectA.SubjectDigest)},
			npm: []activities.ReleaseEffectResult{published("npm")}, browser: []activities.ReleaseEffectResult{failedTarget("browser", "DESTINATION_MISMATCH")}})
		env.ExecuteWorkflow(workflows.ReleaseWorkflowName, releaseInput)
		require.NoError(t, env.GetWorkflowError())
		require.Equal(t, "partially_published", r.final().State)
		require.Len(t, r.publishes["npm"], 1, "the published target is not sent again")
		require.Len(t, r.publishes["browser"], 1)
		require.Empty(t, r.activations)
		require.Equal(t, "PARTIALLY_PUBLISHED", r.last().FailureCode)
	})

	t.Run("a browser target whose answer was lost is queried under its original identity, then activated", func(t *testing.T) {
		env, r := releaseEnv(t, releaseScript{stage: certifiedRelease(), decisions: []activities.ReviewDecision{approved(subjectA.SubjectDigest)},
			npm: []activities.ReleaseEffectResult{published("npm")}, browser: []activities.ReleaseEffectResult{unknownTarget("browser"), unknownTarget("browser"), published("browser")},
			activation: []activities.ReleaseEffectResult{{State: "succeeded", EffectID: "eff_act", CatalogRevision: "5", Target: activities.ReleaseTargetRecord{State: "succeeded", EffectID: "eff_act", ReceiptID: "r", ReceiptDigest: "sha256:r", SubjectDigest: subjectA.SubjectDigest}}}})
		env.ExecuteWorkflow(workflows.ReleaseWorkflowName, releaseInput)
		require.NoError(t, env.GetWorkflowError())
		require.Contains(t, r.states(), "reconciling")
		require.Equal(t, "activated", r.final().State)
		calls := r.publishes["browser"]
		require.Len(t, calls, 3)
		for _, c := range calls {
			require.Equal(t, calls[0].Effect.CommandID, c.Effect.CommandID, "the query reenters the original identity; no new effect")
		}
		browserCloses := 0
		for _, c := range r.closes {
			require.NotEqual(t, "unknown", c.Outcome, "effect uncertainty lives on the effect, never on the attempt")
			if c.CommandID == calls[0].Effect.AttemptID+":close" {
				browserCloses++
			}
		}
		require.GreaterOrEqual(t, browserCloses, 2, "closed once while unknown, then again under the same command to settle the obligations")
	})

	t.Run("a browser target that stays unknown keeps the release reconciling: no activation", func(t *testing.T) {
		env, r := releaseEnv(t, releaseScript{stage: certifiedRelease(), decisions: []activities.ReviewDecision{approved(subjectA.SubjectDigest)},
			npm: []activities.ReleaseEffectResult{published("npm")}, browser: []activities.ReleaseEffectResult{unknownTarget("browser")}})
		env.ExecuteWorkflow(workflows.ReleaseWorkflowName, releaseInput)
		require.NoError(t, env.GetWorkflowError())
		require.Equal(t, "reconciling", r.final().State)
		require.Empty(t, r.activations)
		require.Equal(t, "EFFECT_UNCERTAIN", r.last().FailureCode)
		require.Len(t, r.publishes["browser"], 4, "the first send and three bounded queries of the same identity")
	})

	t.Run("a definitely failed npm target stops before the browser target", func(t *testing.T) {
		env, r := releaseEnv(t, releaseScript{stage: certifiedRelease(), decisions: []activities.ReviewDecision{approved(subjectA.SubjectDigest)},
			npm: []activities.ReleaseEffectResult{failedTarget("npm", "VERSION_EXISTS")}})
		env.ExecuteWorkflow(workflows.ReleaseWorkflowName, releaseInput)
		require.NoError(t, env.GetWorkflowError())
		require.Equal(t, "failed", r.final().State)
		require.Equal(t, "VERSION_EXISTS", r.final().FailureCode)
		require.Empty(t, r.publishes["browser"])
	})

	t.Run("an approval invalidated between the targets stops further effects", func(t *testing.T) {
		env, r := releaseEnv(t, releaseScript{stage: certifiedRelease(),
			decisions: []activities.ReviewDecision{approved(subjectA.SubjectDigest), approved(subjectA.SubjectDigest), {State: "invalidated", SubjectDigest: subjectA.SubjectDigest}},
			npm:       []activities.ReleaseEffectResult{published("npm")}})
		env.ExecuteWorkflow(workflows.ReleaseWorkflowName, releaseInput)
		require.NoError(t, env.GetWorkflowError())
		require.Equal(t, "APPROVAL_INVALIDATED", r.final().FailureCode)
		require.Empty(t, r.publishes["browser"])
		require.Empty(t, r.activations)
	})

	t.Run("a conditional activation conflict is not overwritten", func(t *testing.T) {
		env, r := releaseEnv(t, releaseScript{stage: certifiedRelease(), decisions: []activities.ReviewDecision{approved(subjectA.SubjectDigest)},
			npm: []activities.ReleaseEffectResult{published("npm")}, browser: []activities.ReleaseEffectResult{published("browser")},
			activation: []activities.ReleaseEffectResult{{State: "conflict", EffectID: "eff_act", CatalogRevision: "9", Target: activities.ReleaseTargetRecord{State: "failed", EffectID: "eff_act", FailureCode: "ACTIVATION_CONFLICT"}}}})
		env.ExecuteWorkflow(workflows.ReleaseWorkflowName, releaseInput)
		require.NoError(t, env.GetWorkflowError())
		require.Equal(t, "failed", r.final().State)
		require.Equal(t, "ACTIVATION_CONFLICT", r.final().FailureCode)
		require.Len(t, r.activations, 1)
	})

	t.Run("no decision before the absolute approval deadline expires the release", func(t *testing.T) {
		env, r := releaseEnv(t, releaseScript{stage: certifiedRelease(), decisions: []activities.ReviewDecision{{State: "pending"}}, pollsRun: 10000})
		env.ExecuteWorkflow(workflows.ReleaseWorkflowName, releaseInput)
		require.NoError(t, env.GetWorkflowError())
		require.Equal(t, "APPROVAL_EXPIRED", r.final().FailureCode)
		require.Empty(t, r.publishes)
	})

	t.Run("the long approval wait continues as new carrying its facts", func(t *testing.T) {
		env, r := releaseEnv(t, releaseScript{stage: certifiedRelease(), decisions: []activities.ReviewDecision{{State: "pending"}}, pollsRun: 10})
		env.ExecuteWorkflow(workflows.ReleaseWorkflowName, releaseInput)
		err := env.GetWorkflowError()
		require.Error(t, err)
		var can *workflow.ContinueAsNewError
		require.True(t, errors.As(err, &can), "continue as new: %v", err)
		require.Empty(t, r.settles, "a rollover is not a settlement")
		require.Equal(t, []string{"certifying", "awaiting_approval"}, r.states())
	})

	t.Run("a continued run resumes the wait without certifying or registering again", func(t *testing.T) {
		env, r := releaseEnv(t, releaseScript{decisions: []activities.ReviewDecision{approved(subjectA.SubjectDigest)},
			npm: []activities.ReleaseEffectResult{published("npm")}, browser: []activities.ReleaseEffectResult{published("browser")},
			activation: []activities.ReleaseEffectResult{{State: "succeeded", EffectID: "eff_act", CatalogRevision: "5", Target: activities.ReleaseTargetRecord{State: "succeeded", EffectID: "eff_act", ReceiptID: "r", ReceiptDigest: "sha256:r", SubjectDigest: subjectA.SubjectDigest}}}})
		in := releaseInput
		in.Release = &workflows.ReleaseCarry{Revision: 0, Built: activities.BuiltSubject{Subject: subjectA}, ReleaseID: "rel_1", ReviewEffectID: "eff_review",
			ApprovalDeadline: env.Now().Add(time.Hour), Approval: activities.ReleaseApproval{State: "pending", SubjectDigest: subjectA.SubjectDigest},
			Binding: activities.ReleaseBinding{ProfileID: "release-v1"}}
		env.ExecuteWorkflow(workflows.ReleaseWorkflowName, in)
		require.NoError(t, env.GetWorkflowError())
		require.Empty(t, r.creates, "no second certification")
		require.Equal(t, "activated", r.final().State)
	})

	t.Run("a failed certification publishes nothing", func(t *testing.T) {
		stage := activities.AcceptedStage{Found: true, StageID: "stg", Verdict: "invalid", FailureCode: "MISSING_CSS", ExecutionEpoch: "1", Artifacts: []activities.StageArtifact{evidence}}
		env, r := releaseEnv(t, releaseScript{stage: stage})
		env.ExecuteWorkflow(workflows.ReleaseWorkflowName, releaseInput)
		require.NoError(t, env.GetWorkflowError())
		require.Equal(t, []string{"certifying", "failed"}, r.states())
		require.Equal(t, "MISSING_CSS", r.final().FailureCode)
		require.Empty(t, r.publishes)
	})

	t.Run("bytes that are not the named revision are never certified", func(t *testing.T) {
		env, r := releaseEnv(t, releaseScript{bindingErr: activities.Refused("SOURCE_REVISION_MISMATCH", errors.New("revision 4 holds other bytes"))})
		env.ExecuteWorkflow(workflows.ReleaseWorkflowName, releaseInput)
		require.NoError(t, env.GetWorkflowError())
		require.Equal(t, "SOURCE_REVISION_MISMATCH", r.final().FailureCode)
		require.Empty(t, r.creates)
	})
}
