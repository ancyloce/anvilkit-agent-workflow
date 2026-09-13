package preparation

import (
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/contracts"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/logging"
	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

const clearPrompt = "A launch hero for the spring campaign: heading, one-paragraph description, a primary call to action and an optional product image. Keep it responsive."
const vaguePrompt = "A section announcing our spring campaign with a heading and a short description."

func testInput(t *testing.T) contracts.PreparationWorkflowInputV1 {
	t.Helper()
	input := contracts.PreparationWorkflowInputV1{SchemaVersion: 1, OperationID: "op-prep-test", RequestDigest: "sha256:abababababababababababababababababababababababababababababababab", ExecutionGeneration: "1", RecoveryGeneration: "1",
		InputRef: contracts.ArtifactRefV1{Kind: "preparation-input", RefID: "input-1", SubjectDigest: "sha256:3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855e", ContentDigest: "sha256:7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f", SizeBytes: "1536", ObjectVersion: "v1"},
		Limits:   contracts.PreparationWorkflowLimitsV1{MaxClarificationRounds: "2", MaxQuestionsPerRound: "3", AwaitingInputExpirySeconds: "604800"}}
	if err := contracts.ValidatePreparationWorkflowInput(input); err != nil {
		t.Fatal(err)
	}
	return input
}

// analyzer stands in for Control's records: it runs the real fixed rules over
// a prompt and folds the answers the Workflow reports, without a client.
type analyzer struct {
	prompt string
	answer map[string]string // questionId -> answer text
}

func (a analyzer) analyze(in AnalyzeInput) (AnalyzeResult, error) {
	answers := []Answer{}
	resolved := append([]ResolvedAnswer{}, in.PriorAnswers...)
	for _, prior := range in.PriorAnswers {
		answers = append(answers, Answer{QuestionID: prior.QuestionID, Key: prior.Key, Text: prior.Answer})
	}
	if in.DeliveredAnswerSet != nil {
		for id, text := range a.answer {
			key := "interactions.primaryCta"
			if id == "q-1-content-image" {
				key = "content.image"
			}
			answers = append(answers, Answer{QuestionID: id, Key: key, Text: text})
			resolved = append(resolved, ResolvedAnswer{QuestionID: id, Key: key, Answer: text})
		}
	}
	requirements := Analyze(a.prompt, 1, 1, answers)
	result := AnalyzeResult{ResolvedAnswers: resolved}
	if len(requirements.MaterialUnknowns) > 0 {
		if !in.PoseQuestions {
			result.Unresolved = true
			return result, nil
		}
		var err error
		result.QuestionSet, err = PoseQuestions(in.OperationID, in.Round, in.Round, requirements, in.MaxQuestionsPerRound)
		return result, err
	}
	inputRef, _ := json.Marshal(in.InputRef)
	var err error
	result.Brief, err = ComposeBrief(in.OperationID, 1, inputRef, requirements, resolved, nil, nil)
	return result, err
}

func recordRound(in RoundInput) (RoundResult, error) {
	if in.Brief != nil {
		if _, err := contracts.ValidatePreparationDocument(in.Brief, "BriefV1"); err != nil {
			return RoundResult{}, temporal.NewNonRetryableApplicationError("brief violates its contract", "PreparationBriefInvalid", nil)
		}
		return RoundResult{Decision: "ACCEPTED", Stage: "brief_ready", BriefRevision: 1}, nil
	}
	if _, err := contracts.ValidatePreparationDocument(in.QuestionSet, "QuestionSetV1"); err != nil {
		return RoundResult{}, temporal.NewNonRetryableApplicationError("question set violates its contract", "PreparationQuestionsInvalid", nil)
	}
	return RoundResult{Decision: "ACCEPTED", Stage: "awaiting_input", QuestionSetRevision: in.Round, ExpiresAt: time.Now().Add(7 * 24 * time.Hour)}, nil
}

func environment(t *testing.T, input contracts.PreparationWorkflowInputV1, a analyzer) (*testsuite.TestWorkflowEnvironment, contracts.PreparationProfile) {
	t.Helper()
	profile, err := contracts.Preparation()
	if err != nil {
		t.Fatal(err)
	}
	suite := testsuite.WorkflowTestSuite{}
	suite.SetLogger(logging.New(io.Discard, "test", "test", "local"))
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: profile.WorkflowIDPrefix + input.OperationID, TaskQueue: profile.TaskQueue, WorkflowExecutionTimeout: time.Duration(profile.WorkflowExecutionTimeoutSeconds) * time.Second})
	env.RegisterActivityWithOptions(a.analyze, activity.RegisterOptions{Name: profile.AnalyzeActivityType})
	env.RegisterActivityWithOptions(recordRound, activity.RegisterOptions{Name: profile.RecordRoundActivityType})
	env.RegisterWorkflowWithOptions(NewWorkflow(profile).PreparationWorkflow, workflow.RegisterOptions{Name: profile.WorkflowType})
	return env, profile
}

func delivery(revision int64, digest string) AnswersDeliveryV1 {
	return AnswersDeliveryV1{SchemaVersion: 1, OperationID: "op-prep-test", QuestionSetRevision: strconv.FormatInt(revision, 10), AnswerSetRef: contracts.ArtifactRefV1{Kind: "evidence", RefID: "answer-set-" + strconv.FormatInt(revision, 10), SubjectDigest: "sha256:3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855e", ContentDigest: digest, SizeBytes: "210", ObjectVersion: digest}}
}

type updateOutcome struct {
	rejected error
	result   AnswersDeliveryResultV1
	err      error
}

func sendUpdate(env *testsuite.TestWorkflowEnvironment, profile contracts.PreparationProfile, id string, d AnswersDeliveryV1, into *updateOutcome) {
	env.UpdateWorkflow(profile.AnswersUpdateName, id, &testsuite.TestUpdateCallback{
		OnReject: func(err error) { into.rejected = err },
		OnAccept: func() {},
		OnComplete: func(value interface{}, err error) {
			into.err = err
			if value != nil {
				into.result = value.(AnswersDeliveryResultV1)
			}
		},
	}, d)
}

func TestClearPromptProducesBrief(t *testing.T) {
	input := testInput(t)
	env, _ := environment(t, input, analyzer{prompt: clearPrompt})
	env.ExecuteWorkflow(NewWorkflow(mustProfile(t)).PreparationWorkflow, input)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result contracts.PreparationWorkflowResultV1
	if err := env.GetWorkflowResult(&result); err != nil || result.Outcome != "brief_ready" || result.Round != "1" || result.BriefRevision != "1" {
		t.Fatalf("result %+v %v", result, err)
	}
}

func mustProfile(t *testing.T) contracts.PreparationProfile {
	t.Helper()
	profile, err := contracts.Preparation()
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func TestClarificationRoundAppliesOneDelivery(t *testing.T) {
	input := testInput(t)
	a := analyzer{prompt: vaguePrompt, answer: map[string]string{"q-1-interactions-primaryCta": "Yes, label 'See the offer' leading to /spring", "q-1-content-image": "Show the selected image"}}
	env, profile := environment(t, input, a)
	var first, tooEarly, duplicate, stale updateOutcome
	digest := "sha256:7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d"
	env.RegisterDelayedCallback(func() {
		// Revision 2 was never posed: rejected by the validator, nothing applied.
		sendUpdate(env, profile, "intent-early", delivery(2, digest), &tooEarly)
		sendUpdate(env, profile, "intent-1", delivery(1, digest), &first)
		// A second delivery of the applied identity, and a different answer set
		// for the same revision, both re-apply nothing.
		sendUpdate(env, profile, "intent-1-again", delivery(1, digest), &duplicate)
		sendUpdate(env, profile, "intent-1-other", delivery(1, "sha256:9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d"), &stale)
	}, time.Hour)
	env.ExecuteWorkflow(NewWorkflow(profile).PreparationWorkflow, input)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result contracts.PreparationWorkflowResultV1
	if err := env.GetWorkflowResult(&result); err != nil || result.Outcome != "brief_ready" || result.Round != "2" || result.BriefRevision != "1" {
		t.Fatalf("result %+v %v", result, err)
	}
	if tooEarly.rejected == nil {
		t.Fatal("answers for an unposed revision must be rejected")
	}
	if first.err != nil || first.result.Outcome != "applied" {
		t.Fatalf("first delivery %+v", first)
	}
	if duplicate.err != nil || duplicate.result.Outcome != "duplicate" {
		t.Fatalf("second delivery of the same identity re-applies nothing %+v", duplicate)
	}
	if stale.err != nil || stale.result.Outcome != "stale" {
		t.Fatalf("different answer set for the posed revision %+v", stale)
	}
}

func TestExpiryEndsTheWait(t *testing.T) {
	input := testInput(t)
	env, profile := environment(t, input, analyzer{prompt: vaguePrompt})
	env.ExecuteWorkflow(NewWorkflow(profile).PreparationWorkflow, input)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result contracts.PreparationWorkflowResultV1
	if err := env.GetWorkflowResult(&result); err != nil || result.Outcome != "expired" || result.Round != "1" {
		t.Fatalf("result %+v %v", result, err)
	}
}

func TestRoundBoundEndsUnresolved(t *testing.T) {
	input := testInput(t)
	// Answers that settle nothing keep the unknowns open through both rounds.
	a := analyzer{prompt: vaguePrompt}
	env, profile := environment(t, input, a)
	env.RegisterDelayedCallback(func() {
		sendUpdate(env, profile, "intent-1", delivery(1, "sha256:1111111111111111111111111111111111111111111111111111111111111111"), &updateOutcome{})
	}, time.Hour)
	env.RegisterDelayedCallback(func() {
		sendUpdate(env, profile, "intent-2", delivery(2, "sha256:2222222222222222222222222222222222222222222222222222222222222222"), &updateOutcome{})
	}, 2*time.Hour)
	env.ExecuteWorkflow(NewWorkflow(profile).PreparationWorkflow, input)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result contracts.PreparationWorkflowResultV1
	if err := env.GetWorkflowResult(&result); err != nil || result.Outcome != "unresolved" || result.Round != "2" {
		t.Fatalf("result %+v %v", result, err)
	}
}

func TestRefusedRoundFailsWithoutRetry(t *testing.T) {
	input := testInput(t)
	env, profile := environment(t, input, analyzer{prompt: clearPrompt})
	calls := 0
	env.OnActivity(profile.RecordRoundActivityType, mock.Anything, mock.Anything).Return(func(in RoundInput) (RoundResult, error) {
		calls++
		return RoundResult{}, temporal.NewNonRetryableApplicationError("Control refused", "PreparationRefused", nil, "aborted")
	})
	env.ExecuteWorkflow(NewWorkflow(profile).PreparationWorkflow, input)
	var application *temporal.ApplicationError
	if err := env.GetWorkflowError(); err == nil || !errors.As(err, &application) || calls != 1 {
		t.Fatalf("a refused round is not retried: %v (%d calls)", err, calls)
	}
}

func TestWrongExecutionProfileIsRejected(t *testing.T) {
	input := testInput(t)
	profile := mustProfile(t)
	suite := testsuite.WorkflowTestSuite{}
	suite.SetLogger(logging.New(io.Discard, "test", "test", "local"))
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "other:" + input.OperationID, TaskQueue: profile.TaskQueue, WorkflowExecutionTimeout: time.Duration(profile.WorkflowExecutionTimeoutSeconds) * time.Second})
	env.ExecuteWorkflow(NewWorkflow(profile).PreparationWorkflow, input)
	var application *temporal.ApplicationError
	if err := env.GetWorkflowError(); err == nil || !errors.As(err, &application) || application.Type() != "InvalidPreparationExecution" {
		t.Fatalf("unexpected %v", err)
	}
}
