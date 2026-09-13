package preparation

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"connectrpc.com/connect"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/contracts"
	controlv1 "github.com/ancyloce/anvilkit-agent-workflow/internal/contracts/controlv1"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/contracts/controlv1/controlv1connect"
	values "github.com/ancyloce/anvilkit-agent-workflow/internal/contracts/valuesv1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
)

// Activities are the Workflow's only I/O: they read the operation's records
// through Control and record the rounds Control accepts. A physical attempt
// may repeat; Control deduplicates by round ordinal and content.
type Activities struct {
	control controlv1connect.ControlServiceClient
	profile contracts.PreparationProfile
}

func NewActivities(control controlv1connect.ControlServiceClient, profile contracts.PreparationProfile) *Activities {
	return &Activities{control: control, profile: profile}
}

// AnalyzeInput selects the round: for a round after a clarification it names
// the delivered answer-set identity, which the read must confirm.
type AnalyzeInput struct {
	OperationID          string                   `json:"operationId"`
	InputRef             contracts.ArtifactRefV1  `json:"inputRef"`
	Round                int64                    `json:"round"`
	MaxQuestionsPerRound int64                    `json:"maxQuestionsPerRound"`
	PoseQuestions        bool                     `json:"poseQuestions"`
	PriorAnswers         []ResolvedAnswer         `json:"priorAnswers,omitempty"`
	DeliveredAnswerSet   *contracts.ArtifactRefV1 `json:"deliveredAnswerSet,omitempty"`
	DeliveredRevision    int64                    `json:"deliveredRevision,omitempty"`
}

// AnalyzeResult carries exactly one of a question set to pose or a brief to
// freeze; Unresolved reports unknowns left when no further round may be posed.
type AnalyzeResult struct {
	QuestionSet     json.RawMessage  `json:"questionSet,omitempty"`
	Brief           json.RawMessage  `json:"brief,omitempty"`
	Unresolved      bool             `json:"unresolved,omitempty"`
	ResolvedAnswers []ResolvedAnswer `json:"resolvedAnswers,omitempty"`
}

// RoundInput is what RecordPreparationRound sends to Control.
type RoundInput struct {
	OperationID         string          `json:"operationId"`
	ExecutionGeneration uint64          `json:"executionGeneration"`
	Round               int64           `json:"round"`
	QuestionSet         json.RawMessage `json:"questionSet,omitempty"`
	Brief               json.RawMessage `json:"brief,omitempty"`
}

type RoundResult struct {
	Decision            string    `json:"decision"`
	Stage               string    `json:"stage"`
	QuestionSetRevision int64     `json:"questionSetRevision,omitempty"`
	ExpiresAt           time.Time `json:"expiresAt,omitempty"`
	BriefRevision       int64     `json:"briefRevision,omitempty"`
}

func (a *Activities) context(method string) *controlv1.AuthenticatedContext {
	return &controlv1.AuthenticatedContext{ServiceIdentity: proto.String(ServiceIdentity), DestinationMethod: proto.String(method)}
}

// classify keeps Control's definitive answers out of the retry loop: an
// invalid, conflicting, ended or refused request never becomes a retried
// physical call, while transport failures retry under the profile's policy.
func classify(err error) error {
	if err == nil {
		return nil
	}
	switch connect.CodeOf(err) {
	case connect.CodeInvalidArgument, connect.CodeAborted, connect.CodePermissionDenied, connect.CodeUnauthenticated, connect.CodeNotFound, connect.CodeFailedPrecondition:
		return temporal.NewNonRetryableApplicationError("Control refused the preparation request", "PreparationRefused", nil, connect.CodeOf(err).String())
	}
	return err
}

func refFromProto(ref *values.ArtifactRef) contracts.ArtifactRefV1 {
	kinds := map[values.ArtifactKind]string{values.ArtifactKind_EVIDENCE: "evidence", values.ArtifactKind_PREPARATION_INPUT: "preparation-input"}
	return contracts.ArtifactRefV1{Kind: kinds[ref.GetKind()], RefID: ref.GetRefId(), SubjectDigest: ref.GetSubjectDigest(), ContentDigest: ref.GetContentDigest(), SizeBytes: strconv.FormatUint(ref.GetSizeBytes(), 10), ObjectVersion: ref.GetObjectVersion()}
}

func (a *Activities) log(ctx context.Context, operationID string, started time.Time, err *error) {
	info := activity.GetInfo(ctx)
	outcome := "ok"
	if *err != nil {
		outcome = "error"
		if temporal.IsCanceledError(*err) || errors.Is(*err, context.Canceled) {
			outcome = "canceled"
		}
	}
	activity.GetLogger(ctx).Info("activity.completed", "operationId", operationID, "temporal.workflowId", info.WorkflowExecution.ID, "temporal.runId", info.WorkflowExecution.RunID,
		"temporal.activityType", info.ActivityType.Name, "activityAttempt", int(info.Attempt), "profileRef", a.profile.ProfileRef, "outcome", outcome, "durationMs", time.Since(started).Milliseconds())
}

// AnalyzeRequirements reads the input and, after a clarification, the accepted
// answers, then applies the local fixture route.
func (a *Activities) AnalyzeRequirements(ctx context.Context, in AnalyzeInput) (result AnalyzeResult, err error) {
	started := time.Now()
	defer a.log(ctx, in.OperationID, started, &err)
	response, err := a.control.ReadPreparation(ctx, connect.NewRequest(&controlv1.ReadPreparationRequest{Context: a.context("/anvilkit.control.v1.ControlService/ReadPreparation"), OperationId: proto.String(in.OperationID)}))
	if err != nil {
		return result, classify(err)
	}
	read := response.Msg
	input, err := contracts.ValidatePreparationDocument(read.GetInput(), "PreparationInputV1")
	if err != nil {
		return result, temporal.NewNonRetryableApplicationError("stored preparation input is invalid", "PreparationInputInvalid", nil)
	}
	prompt := input["prompt"].(map[string]any)["text"].(string)
	brandRefs, _ := input["brandRefs"].([]any)
	assetRefs, _ := input["assetRefs"].([]any)

	answers := make([]Answer, 0, len(in.PriorAnswers)+3)
	resolved := append([]ResolvedAnswer{}, in.PriorAnswers...)
	for _, prior := range in.PriorAnswers {
		answers = append(answers, Answer{QuestionID: prior.QuestionID, Key: prior.Key, Text: prior.Answer})
	}
	if in.DeliveredAnswerSet != nil {
		accepted := read.GetPreparation().GetAcceptedAnswerSetRef()
		if accepted == nil || refFromProto(accepted) != *in.DeliveredAnswerSet || read.GetAcceptedAnswerSet() == nil || read.GetQuestionSet() == nil {
			// Control has not (or no longer) accepted the delivered identity: the
			// records are the authority, so retry the read rather than guess.
			return result, errors.New("delivered answer set is not the accepted answer set")
		}
		answerSet, err := contracts.ValidatePreparationDocument(read.GetAcceptedAnswerSet(), "AnswerSetV1")
		if err != nil || answerSet["questionSetRevision"] != strconv.FormatInt(in.DeliveredRevision, 10) || answerSet["operationId"] != in.OperationID {
			return result, temporal.NewNonRetryableApplicationError("accepted answer set is invalid", "PreparationAnswersInvalid", nil)
		}
		questionSet, err := contracts.ValidatePreparationDocument(read.GetQuestionSet(), "QuestionSetV1")
		if err != nil || questionSet["questionSetRevision"] != strconv.FormatInt(in.DeliveredRevision, 10) {
			return result, temporal.NewNonRetryableApplicationError("current question set is invalid", "PreparationQuestionsInvalid", nil)
		}
		keys := map[string]string{}
		for _, q := range questionSet["questions"].([]any) {
			question := q.(map[string]any)
			key, _ := question["materialUnknown"].(string)
			keys[question["questionId"].(string)] = key
		}
		for _, item := range answerSet["answers"].([]any) {
			answer := item.(map[string]any)
			id := answer["questionId"].(string)
			key, known := keys[id]
			if !known {
				return result, temporal.NewNonRetryableApplicationError("answer names an unknown question", "PreparationAnswersInvalid", nil)
			}
			answers = append(answers, Answer{QuestionID: id, Key: key, Text: answer["answer"].(string)})
			resolved = append(resolved, ResolvedAnswer{QuestionID: id, Key: key, Answer: answer["answer"].(string)})
		}
	}

	requirements := Analyze(prompt, len(brandRefs), len(assetRefs), answers)
	result.ResolvedAnswers = resolved
	if len(requirements.MaterialUnknowns) > 0 {
		if !in.PoseQuestions {
			result.Unresolved = true
			return result, nil
		}
		// One question set per round; its revision is the count of sets posed so
		// far plus one, which for one set per round equals the round ordinal.
		result.QuestionSet, err = PoseQuestions(in.OperationID, in.Round, in.Round, requirements, in.MaxQuestionsPerRound)
		if err != nil {
			return result, temporal.NewNonRetryableApplicationError("question set cannot be composed", "PreparationQuestionsInvalid", nil)
		}
		if _, err := contracts.ValidatePreparationDocument(result.QuestionSet, "QuestionSetV1"); err != nil {
			return result, temporal.NewNonRetryableApplicationError("question set violates its contract", "PreparationQuestionsInvalid", nil)
		}
		return result, nil
	}
	// The brief names the stored input record by the reference the Workflow was
	// started with; Control verifies it against the operation's own record.
	storedInputRef, _ := json.Marshal(in.InputRef)
	var brands, assets []json.RawMessage
	for _, ref := range brandRefs {
		raw, _ := json.Marshal(ref)
		brands = append(brands, raw)
	}
	for _, ref := range assetRefs {
		raw, _ := json.Marshal(ref)
		assets = append(assets, raw)
	}
	result.Brief, err = ComposeBrief(in.OperationID, 1, storedInputRef, requirements, resolved, brands, assets)
	if err != nil {
		return result, temporal.NewNonRetryableApplicationError("brief cannot be composed", "PreparationBriefInvalid", nil)
	}
	if _, err := contracts.ValidatePreparationDocument(result.Brief, "BriefV1"); err != nil {
		return result, temporal.NewNonRetryableApplicationError("brief violates its contract", "PreparationBriefInvalid", nil)
	}
	return result, nil
}

// RecordPreparationRound commits the posed question set or the frozen brief
// through Control; a retried attempt returns the same round.
func (a *Activities) RecordPreparationRound(ctx context.Context, in RoundInput) (result RoundResult, err error) {
	started := time.Now()
	defer a.log(ctx, in.OperationID, started, &err)
	request := &controlv1.RecordPreparationRoundRequest{Context: a.context("/anvilkit.control.v1.ControlService/RecordPreparationRound"), OperationId: proto.String(in.OperationID), ExecutionGeneration: proto.Uint64(in.ExecutionGeneration), Round: proto.Uint64(uint64(in.Round))}
	if in.QuestionSet != nil {
		request.QuestionSet = in.QuestionSet
	} else {
		request.Brief = in.Brief
	}
	response, err := a.control.RecordPreparationRound(ctx, connect.NewRequest(request))
	if err != nil {
		return result, classify(err)
	}
	m := response.Msg
	stages := map[controlv1.BusinessStage]string{controlv1.BusinessStage_BUSINESS_STAGE_AWAITING_INPUT: "awaiting_input", controlv1.BusinessStage_BUSINESS_STAGE_BRIEF_READY: "brief_ready"}
	result = RoundResult{Decision: m.GetDecision().String(), Stage: stages[m.GetBusinessStage()], QuestionSetRevision: int64(m.GetQuestionSetRevision()), BriefRevision: int64(m.GetBriefRevision())}
	if m.QuestionSetExpiresAt != nil {
		result.ExpiresAt = m.GetQuestionSetExpiresAt().AsTime()
	}
	if (in.QuestionSet != nil && (result.Stage != "awaiting_input" || result.QuestionSetRevision < 1 || result.ExpiresAt.IsZero())) || (in.Brief != nil && (result.Stage != "brief_ready" || result.BriefRevision < 1)) {
		return RoundResult{}, temporal.NewNonRetryableApplicationError("Control recorded a different round", "PreparationRoundMismatch", nil)
	}
	return result, nil
}
