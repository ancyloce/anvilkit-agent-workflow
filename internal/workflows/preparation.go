package workflows

import (
	"errors"
	"fmt"
	"strconv"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

const (
	preparationProfileID = "preparation-v1"
	analysisStepID       = "analysis"
)

// preparationState is the run's deterministic record of the clarification:
// the open question set, the accepted answers by round and the analysis
// results.
type preparationState struct {
	open     *activities.QuestionSetRef
	answers  map[uint64]activities.AnalysisAnswer // by round
	applied  map[string]bool                      // answer ids applied
	answered bool
}

// Preparation runs the Preparation lifecycle (DD-01 §3, P13-02): read the
// intake, open the analysis attempt and fund it, analyze through the
// controlled Model Proxy into structured requirements; when the analysis
// asks a grouped round, record the question set with Control (which fixes
// the immutable expiry), wait on a durable timer to that absolute instant
// for the answer's tracked Update (or the answer Control accepted before
// the instant), and analyze again with the answers; after the last round
// the analysis must conclude. The brief then freezes the requirements,
// the prompt, the answers, the source revisions and the brand/asset
// content digests as a brief artifact recorded with Control, and the
// operation settles succeeded. Waiting holds no Job, model permit, source
// lease or spendable reservation: the analysis attempt is closed before
// the wait and reopened (a new visit) after it; incurred model cost stays
// in the ledger. Expiry ends the preparation (CLARIFICATION_EXPIRED);
// cancellation closes the attempt and settles canceled. No Signal,
// polling loop or wake-up service exists: the answer arrives as an Update
// under its own identity, and a lost receipt re-issues that Update.
func Preparation(q Queues, b Bounds, lb LifecycleBounds) func(ctx workflow.Context, in Input) error {
	return func(ctx workflow.Context, in Input) (err error) {
		logger := workflow.GetLogger(ctx)
		var bounds Bounds
		if err := workflow.SideEffect(ctx, func(workflow.Context) any { return b }).Get(&bounds); err != nil {
			return err
		}
		var lbounds LifecycleBounds
		if err := workflow.SideEffect(ctx, func(workflow.Context) any { return lb }).Get(&lbounds); err != nil {
			return err
		}
		operationID, tenantID := in.OperationID, in.TenantID
		business := activityOptions(ctx, q, bounds)
		state := &preparationState{answers: map[uint64]activities.AnalysisAnswer{}, applied: map[string]bool{}}

		// The answer Update: the validator is deterministic and performs
		// no I/O (DD-01 §3); the handler reads the accepted answer through
		// an Activity and applies it idempotently.
		if err := workflow.SetUpdateHandlerWithOptions(ctx, AnswerUpdateName, func(ctx workflow.Context, arg AnswerUpdate) (UpdateResult, error) {
			if state.applied[arg.AnswerID] {
				return UpdateResult{Outcome: "applied"}, nil
			}
			var ref activities.AnswerRef
			if err := workflow.ExecuteActivity(activityOptions(ctx, q, bounds), activities.NameGetAnswer, activities.GetAnswerInput{OperationID: operationID, TenantID: tenantID, AnswerID: arg.AnswerID}).Get(ctx, &ref); err != nil {
				return UpdateResult{Outcome: "rejected", ReasonCode: firstNonEmpty(activities.RefusalCode(err), "ANSWER_UNAVAILABLE")}, nil
			}
			if !ref.Found || state.open == nil || ref.QuestionSetID != state.open.QuestionSetID || ref.QuestionSetRevision != state.open.Revision {
				return UpdateResult{Outcome: "rejected", ReasonCode: "STALE_QUESTION_SET"}, nil
			}
			state.applied[arg.AnswerID] = true
			state.answers[state.open.Round] = activities.AnalysisAnswer{Round: state.open.Round, Answer: ref.Answer, Question: *state.open}
			state.answered = true
			return UpdateResult{Outcome: "applied"}, nil
		}, workflow.UpdateHandlerOptions{Validator: func(ctx workflow.Context, arg AnswerUpdate) error {
			if arg.AnswerID == "" || arg.QuestionSetID == "" {
				return errors.New("STALE_QUESTION_SET")
			}
			if state.applied[arg.AnswerID] {
				return nil // the same Update again: applied once, answered again
			}
			if state.open == nil || arg.QuestionSetID != state.open.QuestionSetID || arg.QuestionSetRevision != state.open.Revision {
				return errors.New("STALE_QUESTION_SET")
			}
			return nil
		}}); err != nil {
			return err
		}
		defer awaitHandlers(ctx)

		var prep activities.PreparationView
		if err := workflow.ExecuteActivity(business, activities.NameGetPreparation, activities.GetPreparationInput{OperationID: operationID, TenantID: tenantID}).Get(ctx, &prep); err != nil {
			return err
		}
		// Answers accepted before a restart are Control's record: a run
		// replayed from history rebuilds them from its own events, a fresh
		// run of the same operation would not exist (the Workflow ID is the
		// operation), so the view only seeds the bounds.
		if prep.MaxRounds == 0 {
			prep.MaxRounds = 2
		}
		if prep.MaxQuestions == 0 {
			prep.MaxQuestions = 3
		}

		outcome, failureCode, phase := "failed", "", ""
		defer func() {
			disconnected, _ := workflow.NewDisconnectedContext(ctx)
			if serr := settleOperation(disconnected, q, bounds, operationID, tenantID, operationID+":settle", outcome, failureCode, phase); serr != nil {
				logger.Error("preparation settlement refused", "operationId", operationID, "error", serr)
				if err == nil {
					err = serr
				}
			}
		}()

		// Funding once under the operation identity (the analysis allowance).
		if err := workflow.ExecuteActivity(business, activities.NameRecordFunding, activities.RecordFundingInput{OperationID: operationID, TenantID: tenantID, CommandID: operationID + ":funding"}).Get(ctx, nil); err != nil {
			outcome, failureCode = settleOutcome(ctx, err, "BUDGET_EXHAUSTED")
			return nil
		}

		repairs := 0
		var requirements []byte
		for round := uint64(1); ; round++ {
			mustConclude := round > prep.MaxRounds
			analysis, attemptErr := analyzeRound(ctx, q, bounds, lbounds, operationID, tenantID, round, prep, state, mustConclude, &repairs)
			if attemptErr != nil {
				outcome, failureCode = settleOutcome(ctx, attemptErr, "OBSERVER_FAILED")
				return nil
			}
			if analysis.failureCode != "" {
				outcome, failureCode = "failed", analysis.failureCode
				return nil
			}
			if len(analysis.requirements) > 0 {
				requirements = analysis.requirements
				break
			}
			// A grouped round: recorded with Control (immutable expiry), then
			// the durable wait to that absolute instant.
			var qs activities.QuestionSetRef
			if err := workflow.ExecuteActivity(business, activities.NameRecordQuestionSet, activities.RecordQuestionSetInput{
				OperationID: operationID, TenantID: tenantID, CommandID: operationID + ":questions:" + strconv.FormatUint(round, 10), Round: round, Questions: analysis.questions,
			}).Get(ctx, &qs); err != nil {
				outcome, failureCode = settleOutcome(ctx, err, "OBSERVER_FAILED")
				return nil
			}
			state.open, state.answered = &qs, false
			expired := false
			timerCtx, cancelTimer := workflow.WithCancel(ctx)
			timer := workflow.NewTimer(timerCtx, qs.ExpiresAt.Sub(workflow.Now(ctx)))
			workflow.Go(ctx, func(gctx workflow.Context) {
				if terr := timer.Get(gctx, nil); terr == nil {
					expired = true
				}
			})
			if err := workflow.Await(ctx, func() bool { return state.answered || expired }); err != nil {
				cancelTimer()
				outcome, failureCode = settleOutcome(ctx, err, "CANCELED")
				return nil
			}
			cancelTimer()
			if !state.answered {
				// The timer fired: an answer Control accepted before the
				// instant still counts (its Update receipt may be on its
				// way); otherwise the wait ended without an answer.
				var late activities.AnswerRef
				if err := workflow.ExecuteActivity(business, activities.NameGetAnswer, activities.GetAnswerInput{OperationID: operationID, TenantID: tenantID, QuestionSetID: qs.QuestionSetID}).Get(ctx, &late); err != nil {
					outcome, failureCode = settleOutcome(ctx, err, "OBSERVER_FAILED")
					return nil
				}
				if late.Found && late.QuestionSetRevision == qs.Revision {
					state.applied[late.AnswerID] = true
					state.answers[qs.Round] = activities.AnalysisAnswer{Round: qs.Round, Answer: late.Answer, Question: qs}
					state.answered = true
				} else {
					outcome, failureCode, phase = "failed", "CLARIFICATION_EXPIRED", "clarification_expired"
					state.open = nil
					return nil
				}
			}
			state.open = nil
		}

		// Freeze: the content digests of the selected references, then the
		// brief artifact and its record.
		var frozen activities.FrozenReferences
		if err := workflow.ExecuteActivity(business, activities.NameFreezeReferences, activities.FreezeReferencesInput{TenantID: tenantID, BrandReferences: prep.BrandReferences, AssetReferences: prep.AssetReferences}).Get(ctx, &frozen); err != nil {
			outcome, failureCode = settleOutcome(ctx, err, "SOURCE_UNAVAILABLE")
			return nil
		}
		answers := make([]activities.AnalysisAnswer, 0, len(state.answers))
		for round := uint64(1); round <= prep.MaxRounds; round++ {
			if a, ok := state.answers[round]; ok {
				answers = append(answers, a)
			}
		}
		sources := append(append([]activities.SourceReference(nil), prep.BrandReferences...), prep.AssetReferences...)
		var brief activities.BriefRef
		if err := workflow.ExecuteActivity(business, activities.NameFreezeBrief, activities.FreezeBriefInput{
			OperationID: operationID, TenantID: tenantID, CommandID: operationID + ":brief", Requirements: requirements, Prompt: prep.Prompt, Answers: answers, SourceRevisions: sources, Frozen: frozen,
		}).Get(ctx, &brief); err != nil {
			outcome, failureCode = settleOutcome(ctx, err, "BRIEF_REFUSED")
			return nil
		}
		logger.Info("brief frozen", "operationId", operationID, "briefId", brief.BriefID)
		outcome, phase = "succeeded", "brief_frozen"
		return nil
	}
}

// roundResult is what one analysis round produced.
type roundResult struct {
	requirements []byte
	questions    []activities.Question
	failureCode  string
}

// analyzeRound runs one analysis round inside its own attempt (opened
// for the visit of the round, closed before any wait so the wait holds
// nothing): one controlled call per attempt under a stable call id; a
// content rejection consumes the frozen repair allowance and re-asks under
// a new call id; an unknown call is not a content error and never
// authorizes another call: the round fails EFFECT_UNCERTAIN.
func analyzeRound(ctx workflow.Context, q Queues, b Bounds, lb LifecycleBounds, operationID, tenantID string, round uint64, prep activities.PreparationView, state *preparationState, mustConclude bool, repairs *int) (roundResult, error) {
	business := activityOptions(ctx, q, b)
	visit := round - 1
	var attempt activities.AttemptRef
	if err := workflow.ExecuteActivity(business, activities.NameOpenAttempt, activities.OpenAttemptInput{
		OperationID: operationID, TenantID: tenantID, CommandID: fmt.Sprintf("%s:analysis:%d:open", operationID, round), StepID: analysisStepID, Visit: visit, ProfileID: preparationProfileID,
	}).Get(ctx, &attempt); err != nil {
		return roundResult{}, err
	}
	result := roundResult{}
	closeOutcome, closeCode := "completed", ""
	defer func() {
		disconnected, _ := workflow.NewDisconnectedContext(ctx)
		if cerr := closeAttempt(disconnected, q, b, attempt, attempt.AttemptID+":close", closeOutcome, "not_required", closeCode); cerr != nil {
			workflow.GetLogger(ctx).Error("analysis attempt close refused", "attemptId", attempt.AttemptID, "error", cerr)
		}
	}()
	answers := make([]activities.AnalysisAnswer, 0, len(state.answers))
	for r := uint64(1); r < round; r++ {
		if a, ok := state.answers[r]; ok {
			answers = append(answers, a)
		}
	}
	callCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue: q.Business, StartToCloseTimeout: lb.AnalysisCallTimeout, HeartbeatTimeout: b.ObserveHeartbeatTimeout,
		// A transport retry reenters the same call id: the Proxy answers
		// from its record and never sends twice (P11).
		RetryPolicy: &temporal.RetryPolicy{InitialInterval: b.ControlRetryInitial, BackoffCoefficient: 2, MaximumInterval: b.ControlRetryMaxInterval, MaximumAttempts: b.ControlRetryMaxAttempts},
	})
	epoch, _ := strconv.ParseUint(attempt.ExecutionEpoch, 10, 64)
	for call := 1; ; call++ {
		deadline := workflow.Now(ctx).Add(lb.AnalysisCallTimeout)
		if deadline.After(attempt.Deadline) {
			deadline = attempt.Deadline
		}
		var analysis activities.Analysis
		if err := workflow.ExecuteActivity(callCtx, activities.NameAnalyzeRequirements, activities.AnalyzeRequirementsInput{
			OperationID: operationID, TenantID: tenantID, AttemptID: attempt.AttemptID, ExecutionEpoch: epoch,
			CallID: fmt.Sprintf("%s:analysis:r%d:c%d", operationID, round, call), RouteID: lb.AnalysisRouteID, Round: round, Prompt: prep.Prompt, Answers: answers,
			MaxQuestions: prep.MaxQuestions, MustConclude: mustConclude, MaxOutputTokens: lb.AnalysisMaxOutputTokens, MaxExposure: lb.AnalysisMaxExposure, Deadline: deadline,
		}).Get(ctx, &analysis); err != nil {
			closeOutcome, closeCode = settleOutcome(ctx, err, "OBSERVER_FAILED")
			return roundResult{}, err
		}
		switch {
		case analysis.State == "unknown":
			closeOutcome, closeCode = "infrastructure_failed", "EFFECT_UNCERTAIN"
			result.failureCode = "EFFECT_UNCERTAIN"
			return result, nil
		case analysis.State != "succeeded":
			closeOutcome, closeCode = "failed", firstNonEmpty(analysis.ErrorCode, "MODEL_CALL_FAILED")
			result.failureCode = closeCode
			return result, nil
		case analysis.ErrorCode == "CONTENT_INVALID":
			if *repairs >= lb.ContentRepairAllowance {
				closeOutcome, closeCode = "failed", "CONTENT_INVALID"
				result.failureCode = "CONTENT_INVALID"
				return result, nil
			}
			*repairs++
			continue
		case len(analysis.Requirements) > 0:
			result.requirements = analysis.Requirements
			return result, nil
		default:
			result.questions = analysis.Questions
			return result, nil
		}
	}
}

// settleOutcome maps an Activity failure to the operation outcome.
func settleOutcome(ctx workflow.Context, err error, code string) (string, string) {
	if errors.Is(ctx.Err(), workflow.ErrCanceled) || temporal.IsCanceledError(err) {
		return "canceled", "CANCELED"
	}
	if refused := activities.RefusalCode(err); refused != "" {
		code = refused
	}
	return "failed", code
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
