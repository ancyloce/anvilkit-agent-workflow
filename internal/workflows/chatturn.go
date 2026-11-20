package workflows

import (
	"strconv"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

const (
	// ChatTurnWorkflowName is the registered name Control's relay starts a
	// chat_turn operation under (Control's bootstrap names the same).
	ChatTurnWorkflowName = "ChatTurnWorkflow"
	chatTurnProfileID    = "chat-turn-v1"
	chatReplyStepID      = "reply"
	// chatReplyActivityID is the chat Activity's fixed id: with the run, it
	// is the activity key Control binds the reply writer to.
	chatReplyActivityID = "chat-reply"
	// chatRecordMargin is the Activity's time after the call's deadline to
	// record the reply's outcome.
	chatRecordMargin = 30 * time.Second
)

// ChatTurn runs one conversation turn's reply (DEC-068, design 0001 D3
// §9.2): fund the turn once under the operation identity, open the Job-less
// reply attempt, run the fixed chat Activity (which claims the reply writer,
// makes the single reply call and records the reply's outcome with Control),
// close the attempt and settle the operation from the turn's state. The
// history carries identities, states and counts, never message text. A stop
// is the Workflow's cancellation: the Activity is waited for, so it seals the
// shown prefix before the attempt closes; the send is left to meter to its
// end, and the operation settles canceled.
func ChatTurn(q Queues, b Bounds, lb LifecycleBounds) func(ctx workflow.Context, in Input) error {
	return func(ctx workflow.Context, in Input) (err error) {
		if in.Settle != nil {
			return settleOnly(ctx, q, b, in)
		}
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

		// The settlement is registered before the first command (P1.6).
		outcome, failureCode := "failed", ""
		defer func() {
			disconnected, _ := workflow.NewDisconnectedContext(ctx)
			if serr := settleOperation(disconnected, q, bounds, operationID, tenantID, operationID+":settle", outcome, failureCode, ""); serr != nil {
				if IsContinueAsNew(serr) {
					err = serr
					return
				}
				logger.Error("chat turn settlement refused", "operationId", operationID, "error", serr)
				if err == nil {
					err = serr
				}
			}
		}()

		if err := workflow.ExecuteActivity(business, activities.NameRecordFunding, activities.RecordFundingInput{OperationID: operationID, TenantID: tenantID, CommandID: operationID + ":funding"}).Get(ctx, nil); err != nil {
			outcome, failureCode = fundingOutcome(ctx, err)
			return nil
		}
		var attempt activities.AttemptRef
		if err := workflow.ExecuteActivity(business, activities.NameOpenAttempt, activities.OpenAttemptInput{
			OperationID: operationID, TenantID: tenantID, CommandID: operationID + ":reply:open", StepID: chatReplyStepID, Visit: 0, ProfileID: chatTurnProfileID,
		}).Get(ctx, &attempt); err != nil {
			outcome, failureCode = settleOutcome(ctx, err, "CONTROL_UNAVAILABLE")
			return nil
		}
		closeOutcome, closeCode := "failed", "OBSERVER_FAILED"
		defer func() {
			disconnected, _ := workflow.NewDisconnectedContext(ctx)
			if cerr := closeForSettlement(disconnected, q, bounds, attempt, attempt.AttemptID+":close", closeOutcome, "not_required", closeCode); cerr != nil {
				logger.Error("chat reply attempt close refused", "attemptId", attempt.AttemptID, "error", cerr)
			}
		}()

		// The call's deadline is fixed here, once: a retry of the Activity
		// carries the same one and re-enters the same call.
		deadline := workflow.Now(ctx).Add(lbounds.ChatCallTimeout)
		if deadline.After(attempt.Deadline) {
			deadline = attempt.Deadline
		}
		epoch, _ := strconv.ParseUint(attempt.ExecutionEpoch, 10, 64)
		replyCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID: chatReplyActivityID, TaskQueue: q.Business, StartToCloseTimeout: lbounds.ChatCallTimeout + chatRecordMargin,
			HeartbeatTimeout: bounds.ObserveHeartbeatTimeout, WaitForCancellation: true,
			// A higher attempt takes the reply writer over and re-enters the
			// same call id: the Proxy answers from its record, never sending
			// twice (design 0001 §7.2).
			RetryPolicy: &temporal.RetryPolicy{InitialInterval: bounds.ControlRetryInitial, BackoffCoefficient: 2, MaximumInterval: bounds.ControlRetryMaxInterval, MaximumAttempts: bounds.ControlRetryMaxAttempts},
		})
		var reply activities.ChatReplyResult
		if err := workflow.ExecuteActivity(replyCtx, activities.NameChatReply, activities.ChatReplyInput{
			OperationID: operationID, TenantID: tenantID, AttemptID: attempt.AttemptID, ExecutionEpoch: epoch,
			RouteID: lbounds.ChatRouteID, MaxOutputTokens: lbounds.ChatMaxOutputTokens, MaxExposure: lbounds.ChatMaxExposure, Deadline: deadline,
		}).Get(ctx, &reply); err != nil {
			outcome, failureCode = settleOutcome(ctx, err, "OBSERVER_FAILED")
			closeOutcome, closeCode = attemptCloseOf(outcome), failureCode
			return nil
		}
		outcome, failureCode = turnOutcome(reply)
		closeOutcome, closeCode = attemptCloseOf(outcome), failureCode
		if reply.TurnState == "reconciling" {
			// The sender is unknown (a send reclaimed without a fence): the
			// effect is uncertain, and Control keeps its exposure.
			closeOutcome = "infrastructure_failed"
		}
		logger.Info("chat turn replied", "operationId", operationID, "turnId", reply.TurnID, "turnState", reply.TurnState, "sender", reply.SenderStatus, "callState", reply.CallState, "replyBytes", reply.ReplyBytes)
		return nil
	}
}

// turnOutcome maps the turn's state onto the operation's outcome: a
// completed reply succeeds, a stop cancels, and everything else fails with
// the turn's failure code.
func turnOutcome(r activities.ChatReplyResult) (string, string) {
	switch r.TurnState {
	case "completed":
		return "succeeded", ""
	case "canceled":
		return "canceled", firstNonEmpty(r.FailureCode, "CANCELED")
	default:
		return "failed", firstNonEmpty(r.FailureCode, "REPLY_NOT_RECORDED")
	}
}

func attemptCloseOf(outcome string) string {
	switch outcome {
	case "succeeded":
		return "completed"
	case "canceled":
		return "canceled"
	default:
		return "failed"
	}
}
