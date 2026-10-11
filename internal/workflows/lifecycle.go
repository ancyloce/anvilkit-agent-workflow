package workflows

import (
	"errors"
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
	PreviewWorkflowName     = "PreviewBuildWorkflow"
	ReleaseWorkflowName     = "ReleaseWorkflow"
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
	// Preview: the job profile that builds a preview's saved revision.
	PreviewBuildProfile string
	// Release (P21): the validator profile that certifies the exact source,
	// the release profile's destinations, the maintainer's approval wait
	// (bounded again by the operation deadline), its poll interval, the
	// polls one run makes before it continues as new, and the bounded
	// original-identity queries of an unknown target.
	ReleaseValidatorProfile string
	ReleaseDestinations     activities.ReleaseDestinations
	ReleaseApprovalWait     time.Duration
	ReleaseApprovalPoll     time.Duration
	ReleasePollsPerRun      int
	ReleaseReconcileRounds  int
	ReleaseReconcilePause   time.Duration
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
//
// A transient failure (Control unreachable, a deadline) is retried within
// the outage window rather than for a fixed attempt count (P1.6): a short
// Control outage is a retried success, never a business failure such as
// BUDGET_EXHAUSTED; a refusal is non-retryable and ends at once. A history
// recorded before the window existed holds zero and keeps the attempt cap.
func activityOptions(ctx workflow.Context, q Queues, b Bounds) workflow.Context {
	return workflow.WithActivityOptions(ctx, windowed(q.Business, b, b.ControlOutageWindow))
}

// readOptions are the options of read-only Control commands: retried
// within the wider read window (P1.6).
func readOptions(ctx workflow.Context, q Queues, b Bounds) workflow.Context {
	return workflow.WithActivityOptions(ctx, windowed(q.Business, b, b.ControlReadWindow))
}

func windowed(queue string, b Bounds, window time.Duration) workflow.ActivityOptions {
	o := workflow.ActivityOptions{
		TaskQueue: queue, StartToCloseTimeout: b.ControlActivityTimeout,
		RetryPolicy: &temporal.RetryPolicy{InitialInterval: b.ControlRetryInitial, BackoffCoefficient: 2, MaximumInterval: b.ControlRetryMaxInterval, MaximumAttempts: b.ControlRetryMaxAttempts},
	}
	if window > 0 {
		o.RetryPolicy.MaximumAttempts = 0
		o.ScheduleToCloseTimeout = window
	}
	return o
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
// command; a refusal is a visible non-retryable failure of the run. While
// Control keeps the operation reconciling the same command is re-polled
// with capped backoff; after the poll bound (or when the server suggests
// it) the run continues as new into a run that only settles, carrying the
// minimal settlement state (P1.6). Control is the authority for everything
// else and re-applies the stored intent itself once the last obligation
// resolves, so a run that stopped polling still settles.
func settleOperation(ctx workflow.Context, q Queues, b Bounds, operationID, tenantID, commandID, outcome, failureCode, phase string) error {
	return settleWith(ctx, q, b, operationID, tenantID, SettleCarry{CommandID: commandID, Outcome: outcome, FailureCode: failureCode, Phase: phase})
}

// SettleCarry is the settlement state a settling run continues as new
// with (P1.6): the command, the stated outcome and the current backoff.
type SettleCarry struct {
	CommandID   string        `json:"commandId"`
	Outcome     string        `json:"outcome"`
	FailureCode string        `json:"failureCode,omitempty"`
	Phase       string        `json:"phase,omitempty"`
	Interval    time.Duration `json:"interval,omitempty"`
}

func settleWith(ctx workflow.Context, q Queues, b Bounds, operationID, tenantID string, carry SettleCarry) error {
	p := newPoller(ctx, b, carry.Interval)
	for {
		var ref activities.OperationRef
		if err := workflow.ExecuteActivity(controlOptions(ctx, q, b), activities.NameSettleOperation, activities.SettleOperationInput{
			OperationID: operationID, TenantID: tenantID, CommandID: carry.CommandID, Outcome: carry.Outcome, FailureCode: carry.FailureCode, Phase: carry.Phase,
		}).Get(ctx, &ref); err != nil {
			return temporal.NewNonRetryableApplicationError("settle "+operationID+" refused; the operation is not settled", activities.RefusalCode(err), err)
		}
		if ref.Lifecycle != "reconciling" {
			return nil
		}
		// Temporal retains the original command and outcome through restarts.
		// No new effect is sent while Control waits for outstanding obligations.
		if p.exhausted(ctx) {
			carry.Interval = p.interval
			workflow.GetLogger(ctx).Info("settlement continues as new", "operationId", operationID, "polls", p.polls)
			return workflow.NewContinueAsNewError(ctx, workflow.GetInfo(ctx).WorkflowType.Name, Input{OperationID: operationID, TenantID: tenantID, Settle: &carry})
		}
		if err := p.wait(ctx); err != nil {
			return err
		}
	}
}

// settleOnly is the run a settlement continued as new into: it only
// re-polls the original settle command (P1.6). It is not canceled with the
// run: the settlement completes under its original identity.
func settleOnly(ctx workflow.Context, q Queues, b Bounds, in Input) error {
	var bounds Bounds
	if err := workflow.SideEffect(ctx, func(workflow.Context) any { return b }).Get(&bounds); err != nil {
		return err
	}
	disconnected, _ := workflow.NewDisconnectedContext(ctx)
	return settleWith(disconnected, q, bounds, in.OperationID, in.TenantID, *in.Settle)
}

// IsContinueAsNew reports whether err continues the run as new.
func IsContinueAsNew(err error) bool {
	var can *workflow.ContinueAsNewError
	return errors.As(err, &can)
}

// pollChange guards the P1.6 pacing of the settle, close and lease waits:
// a history recorded before it keeps the fixed interval and no bound.
const pollChange = "settle-poll-backoff"

// Pacing defaults for a history whose recorded Bounds predate P1.6.
const (
	defaultSettleMaxInterval = 5 * time.Minute
	defaultSettlePollBound   = 500
)

// poller paces one wait on Control: capped exponential backoff and a
// count of the polls this run made.
type poller struct {
	on           bool
	interval     time.Duration
	max          time.Duration
	polls, bound int32
}

func newPoller(ctx workflow.Context, b Bounds, start time.Duration) *poller {
	p := &poller{
		on:       workflow.GetVersion(ctx, pollChange, workflow.DefaultVersion, 1) == 1,
		interval: b.ReconcileInitialInterval, max: b.SettleMaxInterval, bound: b.SettlePollBound,
	}
	if p.max <= 0 {
		p.max = defaultSettleMaxInterval
	}
	if p.bound <= 0 {
		p.bound = defaultSettlePollBound
	}
	if p.on && start > 0 {
		p.interval = start
	}
	return p
}

// wait sleeps the next interval on a durable timer.
func (p *poller) wait(ctx workflow.Context) error {
	d := p.interval
	if p.on {
		p.polls++
		p.interval = min(2*p.interval, p.max)
	}
	return workflow.Sleep(ctx, d)
}

// exhausted reports whether this run has polled enough that its history
// should roll over.
func (p *poller) exhausted(ctx workflow.Context) bool {
	return p.on && (p.polls >= p.bound || workflow.GetInfo(ctx).GetContinueAsNewSuggested())
}

// awaitHandlers lets every Update handler finish before the run returns
// (DD-01 §6: finish all Update handlers first).
func awaitHandlers(ctx workflow.Context) {
	_ = workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) })
}
