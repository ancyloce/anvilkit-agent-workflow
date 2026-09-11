package localcheck

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strconv"
	"time"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/contracts"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// ComputeLocalCheck hashes the exact UTF-8 bytes in its retained input. It
// performs no external read or mutation; repeated attempts are safe.
func ComputeLocalCheck(ctx context.Context, input contracts.LocalCheckInputV1) (result contracts.LocalCheckWorkflowResultV1, err error) {
	if contracts.ValidateInput(input) != nil {
		return result, temporal.NewNonRetryableApplicationError("invalid local-check input", "InvalidLocalCheckInput", nil)
	}
	info := activity.GetInfo(ctx)
	logger := activity.GetLogger(ctx)
	fields := []any{"operationId", input.OperationID, "temporal.workflowId", info.WorkflowExecution.ID,
		"temporal.runId", info.WorkflowExecution.RunID, "temporal.activityType", info.ActivityType.Name,
		"activityAttempt", int(info.Attempt), "profileRef", contracts.LocalCheckProfileRef}
	started := time.Now()
	logger.Info("activity.started", fields...)
	defer func() {
		outcome := "ok"
		if err != nil {
			outcome = "canceled"
		}
		logger.Info("activity.completed", append(fields, "outcome", outcome, "durationMs", time.Since(started).Milliseconds())...)
	}()
	if ctx.Err() != nil {
		return result, temporal.NewCanceledError()
	}
	content := []byte(input.FixtureText)
	result = contracts.LocalCheckWorkflowResultV1{
		SchemaVersion: input.SchemaVersion, OperationID: input.OperationID, RequestDigest: input.RequestDigest,
		FixtureID: input.FixtureID, ProfileDigest: input.ProfileDigest,
		ExecutionGeneration: input.ExecutionGeneration, RecoveryGeneration: input.RecoveryGeneration,
		ByteLength: strconv.Itoa(len(content)), ContentDigest: fmt.Sprintf("sha256:%x", sha256.Sum256(content)),
	}
	if ctx.Err() != nil {
		return contracts.LocalCheckWorkflowResultV1{}, temporal.NewCanceledError()
	}
	return result, nil
}
