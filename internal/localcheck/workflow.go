// Package localcheck implements the one fixed, trusted local qualification flow.
package localcheck

import (
	"time"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/contracts"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// LocalCheckWorkflow consumes only its immutable history input and the recorded
// Activity result. Control owns starting and accepting this execution.
func LocalCheckWorkflow(ctx workflow.Context, input contracts.LocalCheckInputV1) (result contracts.LocalCheckWorkflowResultV1, err error) {
	if contracts.ValidateInput(input) != nil {
		return result, temporal.NewNonRetryableApplicationError("invalid local-check input", "InvalidLocalCheckInput", nil)
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
	if info.WorkflowExecution.ID != contracts.LocalCheckWorkflowIDPrefix+input.OperationID ||
		info.TaskQueueName != contracts.LocalCheckTaskQueue ||
		info.WorkflowExecutionTimeout != contracts.LocalCheckWorkflowExecutionTimeoutSeconds*time.Second ||
		info.RetryPolicy != nil || info.CronSchedule != "" || info.ContinuedExecutionRunID != "" || info.ParentWorkflowExecution != nil {
		return result, temporal.NewNonRetryableApplicationError("unsupported local-check execution profile", "InvalidLocalCheckExecution", nil)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:              contracts.LocalCheckTaskQueue,
		StartToCloseTimeout:    contracts.LocalCheckActivityStartToCloseSeconds * time.Second,
		ScheduleToCloseTimeout: contracts.LocalCheckActivityScheduleToCloseSeconds * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    contracts.LocalCheckActivityInitialIntervalSeconds * time.Second,
			BackoffCoefficient: contracts.LocalCheckActivityBackoffCoefficient,
			MaximumInterval:    contracts.LocalCheckActivityMaximumIntervalSeconds * time.Second,
			MaximumAttempts:    contracts.LocalCheckActivityMaximumAttempts,
		},
	})
	if err := workflow.ExecuteActivity(ctx, contracts.LocalCheckActivityType, input).Get(ctx, &result); err != nil {
		return contracts.LocalCheckWorkflowResultV1{}, err
	}
	if err := ctx.Err(); err != nil {
		return contracts.LocalCheckWorkflowResultV1{}, err
	}
	return result, nil
}
