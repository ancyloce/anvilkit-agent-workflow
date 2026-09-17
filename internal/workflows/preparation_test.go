package workflows_test

import (
	"context"
	"encoding/json"
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

// lifecycleBounds mirror config.yaml's lifecycle section.
var lifecycleBounds = workflows.LifecycleBounds{
	AnalysisRouteID: "controlled-openai-v1", AnalysisMaxOutputTokens: 2048, AnalysisMaxExposure: activities.Money{Currency: "USD", Amount: "100000"},
	AnalysisCallTimeout: 5 * time.Minute, ContentRepairAllowance: 1,
	PermitPollInterval: 30 * time.Second, LeaseTTL: 10 * time.Minute, LeaseRenewLead: 3 * time.Minute, LeaseCallTimeout: 20 * time.Second,
	Definitions: []workflows.DefinitionActivation{{ID: "generation-v1:def-1", MaxRepairs: -1}, {ID: "generation-v1:def-2", MaxRepairs: 0}},
}

type lifecycleStub struct{ activities.LifecycleActivities }

// registerLifecycle registers both P13 workflows and every Activity they
// use under the stable names; the tests mock the Activities.
func registerLifecycle(env *testsuite.TestWorkflowEnvironment, lb workflows.LifecycleBounds) {
	registerWith(env, bounds)
	var a lifecycleStub
	env.RegisterWorkflowWithOptions(workflows.Preparation(queues, bounds, lb), workflow.RegisterOptions{Name: workflows.PreparationWorkflowName})
	env.RegisterWorkflowWithOptions(workflows.Generation(queues, bounds, lb), workflow.RegisterOptions{Name: workflows.GenerationWorkflowName})
	for name, fn := range map[string]any{
		activities.NameGetPreparation: a.GetPreparation, activities.NameAnalyzeRequirements: a.AnalyzeRequirements, activities.NameRecordQuestionSet: a.RecordQuestionSet,
		activities.NameGetAnswer: a.GetAnswer, activities.NameFreezeReferences: a.FreezeReferences, activities.NameFreezeBrief: a.FreezeBrief,
		activities.NameSettleOperation: a.SettleOperation, activities.NameRecordFunding: a.RecordFunding,
		activities.NameGetGeneration: a.GetGeneration, activities.NameRequestExecutionPermit: a.RequestExecutionPermit, activities.NameCheckSourceScope: a.CheckSourceScope,
		activities.NameAcquireLease: a.AcquireLease, activities.NameRenewLease: a.RenewLease, activities.NameQueryLease: a.QueryLease, activities.NameReleaseLease: a.ReleaseLease,
		activities.NameRecordLease: a.RecordLease, activities.NameGetAcceptedStage: a.GetAcceptedStage, activities.NameRegisterCandidate: a.RegisterCandidate,
	} {
		env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
}

var (
	prepInput = workflows.Input{OperationID: "op_PREP", TenantID: "tenant_a"}
	prompt    = activities.ArtifactBinding{TransferID: "xfer_prompt", Digest: "sha256:aa", Handle: "h_prompt"}
	prepView  = activities.PreparationView{Prompt: prompt, BrandReferences: []activities.SourceReference{{SourceID: "brand_1", Revision: "3"}}, MaxRounds: 2, MaxQuestions: 3, Wait: 7 * 24 * time.Hour}
	reqs      = json.RawMessage(`{"componentName":"Hero","packageName":"@acme/hero","purpose":"p","content":"c","interaction":"","editableFields":[],"constraints":[],"acceptance":[],"unknowns":[]}`)
	analysis  = activities.AttemptRef{AttemptID: "att_a1", TenantID: "tenant_a", ProfileID: "preparation-v1", Deadline: time.Now().Add(15 * 24 * time.Hour), ExecutionEpoch: "1"}
)

// settled records the settle and close commands a run issued.
type settled struct {
	mu      sync.Mutex
	settles []activities.SettleOperationInput
	closes  []activities.CloseAttemptInput
	calls   []activities.AnalyzeRequirementsInput
}

func (s *settled) record(env *testsuite.TestWorkflowEnvironment) {
	env.OnActivity(activities.NameSettleOperation, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.SettleOperationInput) (activities.OperationRef, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.settles = append(s.settles, in)
		return activities.OperationRef{Lifecycle: in.Outcome, Phase: in.Phase}, nil
	})
	env.OnActivity(activities.NameCloseAttempt, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.CloseAttemptInput) (activities.CloseResult, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.closes = append(s.closes, in)
		return activities.CloseResult{Lifecycle: "running"}, nil
	})
}

func (s *settled) last() activities.SettleOperationInput {
	s.mu.Lock()
	defer s.mu.Unlock()
	require.NotEmpty(mockT{}, s.settles)
	return s.settles[len(s.settles)-1]
}

type mockT struct{}

func (mockT) Errorf(string, ...interface{}) {}
func (mockT) FailNow()                      { panic("no settle recorded") }

// preparationEnv wires the common preparation mocks: the view, the
// attempt, the funding, the freeze; the analysis is scripted per test.
func preparationEnv(t *testing.T, analyze func(in activities.AnalyzeRequirementsInput) (activities.Analysis, error)) (*testsuite.TestWorkflowEnvironment, *settled) {
	t.Helper()
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	registerLifecycle(env, lifecycleBounds)
	s := &settled{}
	s.record(env)
	env.OnActivity(activities.NameGetPreparation, mock.Anything, mock.Anything).Return(prepView, nil)
	env.OnActivity(activities.NameRecordFunding, mock.Anything, mock.MatchedBy(func(in activities.RecordFundingInput) bool { return in.CommandID == "op_PREP:funding" })).Return(activities.FundingRef{}, nil)
	env.OnActivity(activities.NameOpenAttempt, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.OpenAttemptInput) (activities.AttemptRef, error) {
		a := analysis
		a.AttemptID = "att_a" + in.CommandID
		return a, nil
	})
	env.OnActivity(activities.NameAnalyzeRequirements, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.AnalyzeRequirementsInput) (activities.Analysis, error) {
		s.mu.Lock()
		s.calls = append(s.calls, in)
		s.mu.Unlock()
		return analyze(in)
	})
	env.OnActivity(activities.NameFreezeReferences, mock.Anything, mock.Anything).Return(activities.FrozenReferences{BrandDigests: []activities.ContentDigest{{SourceID: "brand_1", Revision: "3", Digest: "sha256:bb"}}}, nil)
	env.OnActivity(activities.NameFreezeBrief, mock.Anything, mock.MatchedBy(func(in activities.FreezeBriefInput) bool {
		return in.CommandID == "op_PREP:brief" && string(in.Requirements) == string(reqs) && len(in.Frozen.BrandDigests) == 1
	})).Return(activities.BriefRef{BriefID: "brf_1"}, nil)
	return env, s
}

func TestPreparationSkipsQuestionsWhenSufficient(t *testing.T) {
	env, s := preparationEnv(t, func(in activities.AnalyzeRequirementsInput) (activities.Analysis, error) {
		return activities.Analysis{CallID: in.CallID, State: "succeeded", Requirements: reqs}, nil
	})
	env.ExecuteWorkflow(workflows.PreparationWorkflowName, prepInput)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, s.calls, 1)
	require.Equal(t, "op_PREP:analysis:r1:c1", s.calls[0].CallID)
	require.False(t, s.calls[0].MustConclude)
	require.Equal(t, "succeeded", s.last().Outcome)
	require.Equal(t, "brief_frozen", s.last().Phase)
	require.Len(t, s.closes, 1, "the analysis attempt is closed before the brief")
	require.Equal(t, "completed", s.closes[0].Outcome)
}

func TestPreparationRoundAnsweredByUpdate(t *testing.T) {
	asked := activities.QuestionSetRef{QuestionSetID: "qs_1", Revision: "1", Round: 1, Questions: []activities.Question{{QuestionID: "q1", Text: "?"}}}
	env, s := preparationEnv(t, func(in activities.AnalyzeRequirementsInput) (activities.Analysis, error) {
		if in.Round == 1 {
			return activities.Analysis{CallID: in.CallID, State: "succeeded", Questions: asked.Questions}, nil
		}
		return activities.Analysis{CallID: in.CallID, State: "succeeded", Requirements: reqs}, nil
	})
	env.OnActivity(activities.NameRecordQuestionSet, mock.Anything, mock.MatchedBy(func(in activities.RecordQuestionSetInput) bool {
		return in.CommandID == "op_PREP:questions:1" && in.Round == 1 && len(in.Questions) == 1
	})).Return(func(_ context.Context, in activities.RecordQuestionSetInput) (activities.QuestionSetRef, error) {
		qs := asked
		qs.AskedAt = env.Now()
		qs.ExpiresAt = env.Now().Add(7 * 24 * time.Hour)
		return qs, nil
	})
	answer := activities.AnswerRef{Found: true, AnswerID: "ans_1", QuestionSetID: "qs_1", QuestionSetRevision: "1", Answer: activities.ArtifactBinding{TransferID: "xfer_ans", Digest: "sha256:cc", Handle: "h_ans"}}
	env.OnActivity(activities.NameGetAnswer, mock.Anything, mock.MatchedBy(func(in activities.GetAnswerInput) bool { return in.AnswerID == "ans_1" })).Return(answer, nil)
	// The answer arrives as the tracked Update after a day; the same Update
	// re-issued (a lost receipt) answers applied again without a second
	// application; a stale question set is rejected by the validator.
	var outcomes []string
	callbacks := func(label string) *testsuite.TestUpdateCallback {
		return &testsuite.TestUpdateCallback{
			OnReject: func(err error) { outcomes = append(outcomes, label+":rejected:"+err.Error()) },
			OnAccept: func() {},
			OnComplete: func(result interface{}, err error) {
				r := result.(workflows.UpdateResult)
				outcomes = append(outcomes, label+":"+r.Outcome+":"+r.ReasonCode)
			},
		}
	}
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(workflows.AnswerUpdateName, "ans_stale", callbacks("stale"), workflows.AnswerUpdate{AnswerID: "ans_stale", QuestionSetID: "qs_other", QuestionSetRevision: "1"})
		env.UpdateWorkflow(workflows.AnswerUpdateName, "ans_1", callbacks("first"), workflows.AnswerUpdate{AnswerID: "ans_1", QuestionSetID: "qs_1", QuestionSetRevision: "1"})
		env.UpdateWorkflow(workflows.AnswerUpdateName, "ans_1", callbacks("again"), workflows.AnswerUpdate{AnswerID: "ans_1", QuestionSetID: "qs_1", QuestionSetRevision: "1"})
	}, 24*time.Hour)
	env.ExecuteWorkflow(workflows.PreparationWorkflowName, prepInput)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Contains(t, outcomes, "stale:rejected:STALE_QUESTION_SET")
	require.Contains(t, outcomes, "first:applied:")
	require.Contains(t, outcomes, "again:applied:")
	require.Len(t, s.calls, 2)
	require.Equal(t, "op_PREP:analysis:r2:c1", s.calls[1].CallID)
	require.Len(t, s.calls[1].Answers, 1)
	require.Equal(t, "xfer_ans", s.calls[1].Answers[0].Answer.TransferID)
	require.Equal(t, "succeeded", s.last().Outcome)
	require.Len(t, s.closes, 2, "each round is its own attempt; the wait holds none")
}

func TestPreparationExpiryEndsTheWait(t *testing.T) {
	env, s := preparationEnv(t, func(in activities.AnalyzeRequirementsInput) (activities.Analysis, error) {
		return activities.Analysis{CallID: in.CallID, State: "succeeded", Questions: []activities.Question{{QuestionID: "q1", Text: "?"}}}, nil
	})
	env.OnActivity(activities.NameRecordQuestionSet, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.RecordQuestionSetInput) (activities.QuestionSetRef, error) {
		return activities.QuestionSetRef{QuestionSetID: "qs_1", Revision: "1", Round: 1, AskedAt: env.Now(), ExpiresAt: env.Now().Add(7 * 24 * time.Hour)}, nil
	})
	var checkedAt time.Time
	env.OnActivity(activities.NameGetAnswer, mock.Anything, mock.MatchedBy(func(in activities.GetAnswerInput) bool { return in.QuestionSetID == "qs_1" })).Return(func(_ context.Context, in activities.GetAnswerInput) (activities.AnswerRef, error) {
		checkedAt = env.Now()
		return activities.AnswerRef{Found: false}, nil
	})
	start := env.Now()
	env.ExecuteWorkflow(workflows.PreparationWorkflowName, prepInput)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "failed", s.last().Outcome)
	require.Equal(t, "CLARIFICATION_EXPIRED", s.last().FailureCode)
	require.GreaterOrEqual(t, checkedAt.Sub(start), 7*24*time.Hour, "the wait is the absolute expiry Control recorded")
	require.Len(t, s.calls, 1, "no second analysis after the expiry")
}

func TestPreparationLateAcceptedAnswerCountsAtExpiry(t *testing.T) {
	env, s := preparationEnv(t, func(in activities.AnalyzeRequirementsInput) (activities.Analysis, error) {
		if in.Round == 1 {
			return activities.Analysis{CallID: in.CallID, State: "succeeded", Questions: []activities.Question{{QuestionID: "q1", Text: "?"}}}, nil
		}
		return activities.Analysis{CallID: in.CallID, State: "succeeded", Requirements: reqs}, nil
	})
	env.OnActivity(activities.NameRecordQuestionSet, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.RecordQuestionSetInput) (activities.QuestionSetRef, error) {
		return activities.QuestionSetRef{QuestionSetID: "qs_1", Revision: "1", Round: 1, AskedAt: env.Now(), ExpiresAt: env.Now().Add(7 * 24 * time.Hour)}, nil
	})
	// Control accepted the answer before the instant; its Update receipt never arrived.
	env.OnActivity(activities.NameGetAnswer, mock.Anything, mock.Anything).Return(activities.AnswerRef{Found: true, AnswerID: "ans_late", QuestionSetID: "qs_1", QuestionSetRevision: "1", Answer: activities.ArtifactBinding{TransferID: "xfer_late", Digest: "sha256:dd"}}, nil)
	env.ExecuteWorkflow(workflows.PreparationWorkflowName, prepInput)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "succeeded", s.last().Outcome)
	require.Len(t, s.calls, 2)
	require.Equal(t, "xfer_late", s.calls[1].Answers[0].Answer.TransferID)
}

func TestPreparationConcludesAfterTwoRounds(t *testing.T) {
	env, s := preparationEnv(t, func(in activities.AnalyzeRequirementsInput) (activities.Analysis, error) {
		if in.MustConclude {
			return activities.Analysis{CallID: in.CallID, State: "succeeded", Requirements: reqs}, nil
		}
		return activities.Analysis{CallID: in.CallID, State: "succeeded", Questions: []activities.Question{{QuestionID: "q1", Text: "?"}}}, nil
	})
	env.OnActivity(activities.NameRecordQuestionSet, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.RecordQuestionSetInput) (activities.QuestionSetRef, error) {
		return activities.QuestionSetRef{QuestionSetID: "qs_" + string(rune('0'+in.Round)), Revision: "1", Round: in.Round, AskedAt: env.Now(), ExpiresAt: env.Now().Add(7 * 24 * time.Hour)}, nil
	})
	env.OnActivity(activities.NameGetAnswer, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.GetAnswerInput) (activities.AnswerRef, error) {
		return activities.AnswerRef{Found: true, AnswerID: "ans_" + in.QuestionSetID, QuestionSetID: in.QuestionSetID, QuestionSetRevision: "1", Answer: activities.ArtifactBinding{TransferID: "xfer_" + in.QuestionSetID, Digest: "sha256:ee"}}, nil
	})
	env.ExecuteWorkflow(workflows.PreparationWorkflowName, prepInput)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, s.calls, 3, "two question rounds, then the concluding analysis")
	require.False(t, s.calls[0].MustConclude)
	require.False(t, s.calls[1].MustConclude)
	require.True(t, s.calls[2].MustConclude, "after the last round the analysis must conclude")
	require.Len(t, s.calls[2].Answers, 2)
	require.Equal(t, "succeeded", s.last().Outcome)
}

func TestPreparationUnknownCallNeverCallsAgain(t *testing.T) {
	env, s := preparationEnv(t, func(in activities.AnalyzeRequirementsInput) (activities.Analysis, error) {
		return activities.Analysis{CallID: in.CallID, State: "unknown"}, nil
	})
	env.ExecuteWorkflow(workflows.PreparationWorkflowName, prepInput)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, s.calls, 1, "an uncertain network result is not a content error authorizing a new call")
	require.Equal(t, "failed", s.last().Outcome)
	require.Equal(t, "EFFECT_UNCERTAIN", s.last().FailureCode)
	require.Equal(t, "infrastructure_failed", s.closes[0].Outcome)
}

func TestPreparationContentRepairAllowance(t *testing.T) {
	env, s := preparationEnv(t, func(in activities.AnalyzeRequirementsInput) (activities.Analysis, error) {
		return activities.Analysis{CallID: in.CallID, State: "succeeded", ErrorCode: "CONTENT_INVALID"}, nil
	})
	env.ExecuteWorkflow(workflows.PreparationWorkflowName, prepInput)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, s.calls, 2, "one repair under a new call id, then the frozen allowance is consumed")
	require.Equal(t, "op_PREP:analysis:r1:c1", s.calls[0].CallID)
	require.Equal(t, "op_PREP:analysis:r1:c2", s.calls[1].CallID)
	require.Equal(t, "CONTENT_INVALID", s.last().FailureCode)
}
