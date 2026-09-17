package workflows

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// Registered names of the P13 business workflows and their tracked Updates
// (DD-01 §7; the names are shared with Control's relay adapter).
const (
	PreparationWorkflowName = "PreparationWorkflow"
	GenerationWorkflowName  = "GenerationWorkflow"
	AnswerUpdateName        = "PreparationAnswer"
	CommandUpdateName       = "ControlCommand"
)

// AnswerUpdate is the argument of the answer Update (Control's relay
// adapter writes it): the identities the deterministic validator checks
// against the open question set.
type AnswerUpdate struct {
	AnswerID            string `json:"answerId"`
	QuestionSetID       string `json:"questionSetId"`
	QuestionSetRevision string `json:"questionSetRevision"`
	Handle              string `json:"handle"`
	Digest              string `json:"digest"`
}

// CommandUpdate is the argument of the control-command Update.
type CommandUpdate struct {
	CommandID                  string `json:"commandId"`
	Kind                       string `json:"kind"`
	ExpectedRevision           string `json:"expectedRevision"`
	TargetDefinitionActivation string `json:"targetDefinitionActivation,omitempty"`
}

// UpdateResult is what the handlers answer: applied, blocked or rejected
// with a reason; it is recorded by Control as the tracked outcome.
type UpdateResult struct {
	Outcome    string `json:"outcome"`
	ReasonCode string `json:"reasonCode,omitempty"`
}

// LifecycleBounds are the reviewed bounds of the P13 workflows the worker
// freezes into each run's history (like Bounds).
type LifecycleBounds struct {
	// Preparation: the analysis route and per-call bounds, the content
	// repair allowance (schema/content rejections that may be retried
	// under a new call id) and the Activity bound of one call.
	AnalysisRouteID         string
	AnalysisMaxOutputTokens int
	AnalysisMaxExposure     activities.Money
	AnalysisCallTimeout     time.Duration
	ContentRepairAllowance  int
	// Generation: the permit poll interval, the lease TTL asked of the
	// port, the renewal lead before the known expiry, the bound of a
	// codegen/validator launch window and the definition activations this
	// worker registered.
	PermitPollInterval time.Duration
	LeaseTTL           time.Duration
	LeaseRenewLead     time.Duration
	LeaseCallTimeout   time.Duration
	Definitions        []DefinitionActivation
}

// DefinitionActivation is one reviewed definition this worker registered
// for the Generation profile: the job profiles its steps launch and the
// repair bound. An empty profile or a negative bound inherits Control's
// profile value; a change_definition to an activation this worker does
// not list is rejected as unavailable.
type DefinitionActivation struct {
	ID               string
	CodegenProfileID string
	ValidatorProfile string
	MaxRepairs       int64
}

// definition resolves the activation in force against Control's profile.
func (lb LifecycleBounds) definition(id string, view activities.GenerationView) (codegen, validator string, maxRepairs uint64) {
	codegen, validator, maxRepairs = view.CodegenProfileID, view.ValidatorProfileID, view.MaxRepairs
	for _, d := range lb.Definitions {
		if d.ID != id {
			continue
		}
		if d.CodegenProfileID != "" {
			codegen = d.CodegenProfileID
		}
		if d.ValidatorProfile != "" {
			validator = d.ValidatorProfile
		}
		if d.MaxRepairs >= 0 {
			maxRepairs = uint64(d.MaxRepairs)
		}
	}
	return codegen, validator, maxRepairs
}

// activityOptions are the ordinary Control-command options: bounded,
// retried under the same command identity.
func activityOptions(ctx workflow.Context, q Queues, b Bounds) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue: q.Business, StartToCloseTimeout: b.ControlActivityTimeout,
		RetryPolicy: &temporal.RetryPolicy{InitialInterval: b.ControlRetryInitial, BackoffCoefficient: 2, MaximumInterval: b.ControlRetryMaxInterval, MaximumAttempts: b.ControlRetryMaxAttempts},
	})
}

// controlOptions are the protected control-queue options of the settling
// and closing commands: retried until Control answers, no attempt cap.
func controlOptions(ctx workflow.Context, q Queues, b Bounds) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue: q.Control, StartToCloseTimeout: b.ControlActivityTimeout,
		RetryPolicy: &temporal.RetryPolicy{InitialInterval: b.ControlRetryInitial, BackoffCoefficient: 2, MaximumInterval: b.ControlRetryMaxInterval, MaximumAttempts: 0},
	})
}

// settleOperation states the business outcome to Control under a stable
// command; a refusal is a visible non-retryable failure of the run.
func settleOperation(ctx workflow.Context, q Queues, b Bounds, operationID, tenantID, commandID, outcome, failureCode, phase string) error {
	var ref activities.OperationRef
	if err := workflow.ExecuteActivity(controlOptions(ctx, q, b), activities.NameSettleOperation, activities.SettleOperationInput{
		OperationID: operationID, TenantID: tenantID, CommandID: commandID, Outcome: outcome, FailureCode: failureCode, Phase: phase,
	}).Get(ctx, &ref); err != nil {
		return temporal.NewNonRetryableApplicationError("settle "+operationID+" refused; the operation is not settled", activities.RefusalCode(err), err)
	}
	return nil
}

// awaitHandlers lets every Update handler finish before the run returns
// (DD-01 §6: finish all Update handlers first).
func awaitHandlers(ctx workflow.Context) {
	_ = workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) })
}
