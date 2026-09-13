package preparation

import (
	"errors"
	"strconv"
	"time"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/contracts"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// AnswersDeliveryV1 is the argument of the tracked Update Control's relay
// sends once per accepted answer set: the identity, never the bytes.
type AnswersDeliveryV1 struct {
	SchemaVersion       int                     `json:"schemaVersion"`
	OperationID         string                  `json:"operationId"`
	QuestionSetRevision string                  `json:"questionSetRevision"`
	AnswerSetRef        contracts.ArtifactRefV1 `json:"answerSetRef"`
}

// AnswersDeliveryResultV1 is the tracked result of one delivery.
type AnswersDeliveryResultV1 struct {
	Outcome string `json:"outcome"` // applied | duplicate | stale
}

// Workflow binds the fixed profile to the Workflow function and its Activities.
type Workflow struct {
	profile contracts.PreparationProfile
}

func NewWorkflow(profile contracts.PreparationProfile) *Workflow { return &Workflow{profile: profile} }

// PreparationWorkflow runs the fixed preparation flow: analyze, pose at most
// the bounded clarification rounds while waiting on a durable clock for the
// tracked answer delivery, and end with the frozen brief, the expired wait or
// the unresolved bound. Every read and write is an Activity; the code here is
// deterministic and re-derives its state from history on any worker.
func (w *Workflow) PreparationWorkflow(ctx workflow.Context, input contracts.PreparationWorkflowInputV1) (result contracts.PreparationWorkflowResultV1, err error) {
	if contracts.ValidatePreparationWorkflowInput(input) != nil {
		return result, temporal.NewNonRetryableApplicationError("invalid preparation input", "InvalidPreparationInput", nil)
	}
	maxRounds, maxQuestions, _, err := input.Bounds()
	if err != nil {
		return result, temporal.NewNonRetryableApplicationError("invalid preparation bounds", "InvalidPreparationInput", nil)
	}
	generation, err := strconv.ParseUint(input.ExecutionGeneration, 10, 64)
	if err != nil {
		return result, temporal.NewNonRetryableApplicationError("invalid execution generation", "InvalidPreparationInput", nil)
	}
	info := workflow.GetInfo(ctx)
	logger := workflow.GetLogger(ctx)
	logger.Info("workflow.started", "operationId", input.OperationID, "trace.source", "new")
	defer func() {
		outcome := "ok"
		if err != nil {
			outcome = "error"
			if temporal.IsCanceledError(err) {
				outcome = "canceled"
			} else {
				logger.Error("workflow.completed", "operationId", input.OperationID, "outcome", outcome)
				return
			}
		}
		logger.Info("workflow.completed", "operationId", input.OperationID, "outcome", outcome)
	}()
	if info.WorkflowExecution.ID != w.profile.WorkflowIDPrefix+input.OperationID || info.TaskQueueName != w.profile.TaskQueue ||
		info.WorkflowExecutionTimeout != time.Duration(w.profile.WorkflowExecutionTimeoutSeconds)*time.Second ||
		info.RetryPolicy != nil || info.CronSchedule != "" || info.ContinuedExecutionRunID != "" || info.ParentWorkflowExecution != nil {
		return result, temporal.NewNonRetryableApplicationError("unsupported preparation execution profile", "InvalidPreparationExecution", nil)
	}

	// Clarification state: the revision currently posed (0 before any), the
	// delivery accepted for it, and the revisions already applied.
	var posedRevision int64
	var delivered *AnswersDeliveryV1
	applied := map[int64]contracts.ArtifactRefV1{}
	validator := func(ctx workflow.Context, d AnswersDeliveryV1) error {
		revision, err := strconv.ParseInt(d.QuestionSetRevision, 10, 64)
		if d.SchemaVersion != 1 || d.OperationID != input.OperationID || err != nil || revision < 1 || d.AnswerSetRef.Kind != "evidence" || d.AnswerSetRef.RefID == "" || d.AnswerSetRef.ContentDigest == "" {
			return errors.New("invalid answers delivery")
		}
		if revision > posedRevision {
			return errors.New("answers for a question set not yet posed")
		}
		return nil
	}
	handler := func(ctx workflow.Context, d AnswersDeliveryV1) (AnswersDeliveryResultV1, error) {
		revision, _ := strconv.ParseInt(d.QuestionSetRevision, 10, 64)
		if ref, done := applied[revision]; done {
			if ref == d.AnswerSetRef {
				return AnswersDeliveryResultV1{Outcome: "duplicate"}, nil
			}
			return AnswersDeliveryResultV1{Outcome: "stale"}, nil
		}
		if revision != posedRevision || delivered != nil {
			// An earlier revision, or a second delivery for the posed one: the
			// records already hold the accepted set; nothing is re-applied.
			return AnswersDeliveryResultV1{Outcome: "stale"}, nil
		}
		copy := d
		delivered = &copy
		return AnswersDeliveryResultV1{Outcome: "applied"}, nil
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, w.profile.AnswersUpdateName, handler, workflow.UpdateHandlerOptions{Validator: validator}); err != nil {
		return result, err
	}

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           w.profile.TaskQueue,
		StartToCloseTimeout: time.Duration(w.profile.ActivityStartToCloseSeconds) * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Duration(w.profile.ActivityInitialIntervalSeconds) * time.Second,
			BackoffCoefficient: w.profile.ActivityBackoffCoefficient,
			MaximumInterval:    time.Duration(w.profile.ActivityMaximumIntervalSeconds) * time.Second,
		},
	})

	var prior []ResolvedAnswer
	var last *AnswersDeliveryV1
	var lastRevision int64
	for round := int64(1); ; round++ {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		// A question set may still be posed while posed sets stay under the bound.
		analyze := AnalyzeInput{OperationID: input.OperationID, InputRef: input.InputRef, Round: round, MaxQuestionsPerRound: maxQuestions, PoseQuestions: round <= maxRounds, PriorAnswers: prior}
		if last != nil {
			analyze.DeliveredAnswerSet, analyze.DeliveredRevision = &last.AnswerSetRef, lastRevision
		}
		var analysis AnalyzeResult
		if err := workflow.ExecuteActivity(ctx, w.profile.AnalyzeActivityType, analyze).Get(ctx, &analysis); err != nil {
			return result, err
		}
		prior = analysis.ResolvedAnswers
		if analysis.Unresolved {
			return contracts.PreparationWorkflowResultV1{SchemaVersion: 1, OperationID: input.OperationID, Outcome: "unresolved", Round: strconv.FormatInt(round-1, 10)}, nil
		}
		var recorded RoundResult
		if analysis.Brief != nil {
			if err := workflow.ExecuteActivity(ctx, w.profile.RecordRoundActivityType, RoundInput{OperationID: input.OperationID, ExecutionGeneration: generation, Round: round, Brief: analysis.Brief}).Get(ctx, &recorded); err != nil {
				return result, err
			}
			return contracts.PreparationWorkflowResultV1{SchemaVersion: 1, OperationID: input.OperationID, Outcome: "brief_ready", Round: strconv.FormatInt(round, 10), BriefRevision: strconv.FormatInt(recorded.BriefRevision, 10)}, nil
		}
		if err := workflow.ExecuteActivity(ctx, w.profile.RecordRoundActivityType, RoundInput{OperationID: input.OperationID, ExecutionGeneration: generation, Round: round, QuestionSet: analysis.QuestionSet}).Get(ctx, &recorded); err != nil {
			return result, err
		}
		// awaiting_input: nothing is held; the clock is Control's, set once.
		posedRevision, delivered = recorded.QuestionSetRevision, nil
		wait := recorded.ExpiresAt.Sub(workflow.Now(ctx))
		if wait < 0 {
			wait = 0
		}
		answered, err := workflow.AwaitWithTimeout(ctx, wait, func() bool { return delivered != nil })
		if err != nil {
			return result, err
		}
		if !answered {
			return contracts.PreparationWorkflowResultV1{SchemaVersion: 1, OperationID: input.OperationID, Outcome: "expired", Round: strconv.FormatInt(round, 10)}, nil
		}
		applied[posedRevision] = delivered.AnswerSetRef
		last, lastRevision = delivered, posedRevision
	}
}
