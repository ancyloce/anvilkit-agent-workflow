// Package workflows holds the deterministic orchestration (DD-01). Only
// workflow.* time, timers and Activities are used; no wall clock, goroutines,
// unordered map decisions or network.
package workflows

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// LocalCheckWorkflowName is the stable registered Workflow Type
// (architecture.md "API, events and Temporal").
const LocalCheckWorkflowName = "LocalCheckWorkflow"

// Queues selects the Task Queues from one configuration snapshot: ordinary
// Activities on the business queue, protected control/cleanup Activities on
// the reserved control queue (DD-01 §7).
type Queues struct {
	Business string
	Control  string
}

// Bounds are the execution limits that shape Temporal decisions (timeouts,
// retry caps, launch window, cleanup and reconciliation pacing). The worker
// passes its validated snapshot; the Workflow records it once in history
// with workflow.SideEffect, so a redeployed worker with other values never
// changes the replay of an existing run.
type Bounds struct {
	ControlActivityTimeout   time.Duration
	ControlRetryInitial      time.Duration
	ControlRetryMaxInterval  time.Duration
	ControlRetryMaxAttempts  int32
	LaunchWindow             time.Duration
	ObserveHeartbeatTimeout  time.Duration
	ObserveMaxAttempts       int32
	CleanupTimeout           time.Duration
	CleanupMaxAttempts       int32
	UnresolvedSettleWindow   time.Duration
	ReconcileInitialInterval time.Duration
	ReconcileMaxInterval     time.Duration
	ReconcileMaxDuration     time.Duration
}

const (
	profileID = "local-check-v1"
	stepID    = "local-check"
	backend   = "kubernetes"
)

// launchKeyFor derives the stable DNS-label launch key from the operation id
// so a restarted Workflow reuses the same Job name.
func launchKeyFor(operationID string) string {
	key := strings.ToLower(strings.TrimPrefix(operationID, "op_"))
	key = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			return r
		}
		return '-'
	}, key)
	if len(key) > 40 {
		key = key[:40]
	}
	return "lc-" + strings.Trim(key, "-")
}

// Input is the Workflow argument written by Control's relay: identities only.
type Input struct {
	OperationID string `json:"operationId"`
	TenantID    string `json:"tenantId"`
}

// LocalCheck runs the fixed qualification fixture for one operation: open
// an attempt, inventory and create the fixed Job, register the real Pod,
// verify the result independently, accept it once, clean up and close.
// Cancellation stops the Job and closes the attempt as canceled; issued
// launches and results are never revived under a new identity.
//
// Cleanup is reported complete only when the launcher observed the Job and
// its Pods gone and every create request this run issued is accounted for.
// The Job is created under one launch key by numbered requests, one at a
// time; a request the API server answered, or that is known not to have
// been created, is settled by its answer, but a request that was written
// and never answered may still commit, also after a later request's Job
// was deleted and released the name. Such requests stay in the run's
// ledger (Workflow state, replayed from history) until the launch key
// shows the object they made. Absence for any length of time is not
// evidence, so the cleanup is evidence-first: while a request is
// unaccounted for, a read-only observation of the original launch key must
// report the Job or a Pod, with the request marker it carries, before
// anything is deleted. That observation is recorded in this run's history
// and every Pod it reports is registered with Control under the same
// command identities the running path uses, so the evidence survives a lost
// completion receipt, an Activity retry, a replaced worker and every later
// reconciliation round; the delete then confirms the absence of what was
// seen. When cleanup cannot be confirmed, the attempt is closed with cleanup
// unknown (Control keeps the operation reconciling and its cancel pending)
// and this same run remains the durable recovery owner: it keeps
// reconciling the original launch key with capped backoff until the backend
// evidence settles it, then closes the attempt again with the evidence.
// Only the reconciliation bound ends that loop, and then the Workflow fails
// visibly instead of completing with an unsettled operation. The close is a
// durable Control command under the attempt's identity: it is retried until
// Control answers, and a refusal fails the Workflow.
func LocalCheck(q Queues, b Bounds) func(ctx workflow.Context, in Input) error {
	return func(ctx workflow.Context, in Input) (err error) {
		operationID := in.OperationID
		logger := workflow.GetLogger(ctx)
		var bounds Bounds
		if err := workflow.SideEffect(ctx, func(workflow.Context) any { return b }).Get(&bounds); err != nil {
			return err
		}
		controlRetry := &temporal.RetryPolicy{InitialInterval: bounds.ControlRetryInitial, BackoffCoefficient: 2, MaximumInterval: bounds.ControlRetryMaxInterval, MaximumAttempts: bounds.ControlRetryMaxAttempts}
		business := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			TaskQueue: q.Business, StartToCloseTimeout: bounds.ControlActivityTimeout, RetryPolicy: controlRetry,
		})

		var attempt activities.AttemptRef
		if err := workflow.ExecuteActivity(business, activities.NameOpenAttempt, activities.OpenAttemptInput{
			OperationID: operationID, TenantID: in.TenantID, CommandID: operationID + ":open", StepID: stepID, Visit: 0, ProfileID: profileID,
		}).Get(ctx, &attempt); err != nil {
			return err // fenced or unknown operation: nothing was launched
		}

		outcome, cleanup, failureCode := "failed", "not_required", ""
		var launch activities.LaunchRef
		// launched: the launch obligation exists and the backend may hold the
		// Job; creates: the ledger of the create requests issued for it.
		launched := false
		var creates createLedger
		defer func() {
			// Protected cleanup and close on the reserved control queue, also
			// after cancellation (disconnected context).
			disconnected, _ := workflow.NewDisconnectedContext(ctx)
			if launched {
				cleanup = "unknown"
				if cleanupRound(disconnected, q, bounds, attempt, launch, &creates) {
					cleanup = "complete"
				}
			}
			if cerr := closeAttempt(disconnected, q, bounds, attempt, attempt.AttemptID+":close", outcome, cleanup, failureCode); cerr != nil {
				logger.Error("attempt close refused", "attemptId", attempt.AttemptID, "error", cerr)
				if err == nil {
					err = cerr
				}
				return
			}
			if cleanup != "unknown" {
				return
			}
			// Durable recovery owner: reconcile the original launch identity
			// until the backend settles it, then settle the operation and
			// its pending cancel with a second close carrying the evidence.
			if !reconcileCleanup(disconnected, q, bounds, attempt, launch, &creates) {
				logger.Error("cleanup reconciliation bound reached; the operation remains reconciling", "launchKey", launch.LaunchKey)
				if err == nil {
					err = temporal.NewNonRetryableApplicationError(fmt.Sprintf("cleanup of launch %s not confirmed within %s; operation not settled", launch.LaunchKey, bounds.ReconcileMaxDuration), "CLEANUP_UNCONFIRMED", nil)
				}
				return
			}
			if cerr := closeAttempt(disconnected, q, bounds, attempt, attempt.AttemptID+":close:settled", outcome, "complete", failureCode); cerr != nil {
				logger.Error("cleanup settlement refused", "attemptId", attempt.AttemptID, "error", cerr)
				if err == nil {
					err = cerr
				}
			}
		}()

		// The absolute deadline was set once at intake; nothing launches after it.
		if !attempt.Deadline.After(workflow.Now(ctx)) {
			outcome, failureCode = "infrastructure_failed", "DEADLINE_EXCEEDED"
			return nil
		}
		// The launch deadline never extends the attempt deadline (DD-01 §4).
		deadline := workflow.Now(ctx).Add(bounds.LaunchWindow)
		if deadline.After(attempt.Deadline) {
			deadline = attempt.Deadline
		}
		if err := workflow.ExecuteActivity(business, activities.NamePrepareLaunch, activities.PrepareLaunchInput{
			Attempt: attempt, CommandID: attempt.AttemptID + ":launch", LaunchKey: launchKeyFor(operationID), Deadline: deadline,
		}).Get(ctx, &launch); err != nil {
			outcome, failureCode = settle(ctx, err, "PROFILE_UNQUALIFIED")
			return nil
		}

		// Job create is idempotent by launch key; a retry queries the same
		// name. Every request is issued and accounted for by this run (see
		// createJob), and a cancellation waits for the request's real result
		// so the cleanup knows what the launch key may hold.
		launched = true
		if _, err := createJob(ctx, q, bounds, launch, deadline, &creates); err != nil {
			outcome, failureCode = settle(ctx, err, "OBSERVER_FAILED")
			return nil
		}

		observeTimeout := deadline.Sub(workflow.Now(ctx)) + time.Minute
		if observeTimeout < time.Minute {
			observeTimeout = time.Minute
		}
		observeCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			TaskQueue: q.Business, StartToCloseTimeout: observeTimeout, HeartbeatTimeout: bounds.ObserveHeartbeatTimeout,
			RetryPolicy: &temporal.RetryPolicy{InitialInterval: bounds.ControlRetryInitial, MaximumAttempts: bounds.ObserveMaxAttempts},
		})
		var observation activities.JobObservation
		if err := workflow.ExecuteActivity(observeCtx, activities.NameObserveJob, activities.ObserveJobInput{Launch: launch, Deadline: deadline}).Get(ctx, &observation); err != nil {
			outcome, failureCode = settle(ctx, err, "OBSERVER_FAILED")
			return nil
		}

		// Register every real Pod in order; the first becomes current, any
		// duplicate is recorded but can never own the result.
		var current activities.InstanceRef
		for i, pod := range observation.Pods {
			var inst activities.InstanceRef
			if err := workflow.ExecuteActivity(business, activities.NameRegisterInstance, activities.RegisterInstanceInput{
				Attempt: attempt, Launch: launch, CommandID: attempt.AttemptID + ":register:" + pod.PodUID, JobUID: observation.JobUID, PodUID: pod.PodUID,
			}).Get(ctx, &inst); err != nil {
				outcome, failureCode = settle(ctx, err, "OBSERVER_FAILED")
				return nil
			}
			// Record the backend-observed phase and exit code on the instance.
			if err := workflow.ExecuteActivity(business, activities.NameObserveInstance, activities.ObserveInstanceInput{
				Attempt: attempt, Instance: inst, Observation: pod,
			}).Get(ctx, nil); err != nil {
				outcome, failureCode = settle(ctx, err, "OBSERVER_FAILED")
				return nil
			}
			if i == 0 {
				current = inst
			}
		}
		if len(observation.Pods) == 0 || !current.Current {
			outcome, failureCode = "infrastructure_failed", "OBSERVER_FAILED"
			if observation.Reason == "DeadlineExceeded" {
				failureCode = "DEADLINE_EXCEEDED"
			}
			return nil
		}

		var verdict activities.Verdict
		if err := workflow.ExecuteActivity(business, activities.NameVerifyResult, activities.VerifyResultInput{
			Launch: launch, Attempt: attempt, ProfileID: profileID, Observation: observation, CompletedAt: workflow.Now(ctx),
		}).Get(ctx, &verdict); err != nil {
			outcome, failureCode = settle(ctx, err, "OBSERVER_FAILED")
			return nil
		}
		var stage activities.StageRef
		if err := workflow.ExecuteActivity(business, activities.NameAcceptResult, activities.AcceptResultInput{
			Attempt: attempt, Instance: current, CommandID: attempt.AttemptID + ":accept", ProfileID: profileID, Verdict: verdict,
		}).Get(ctx, &stage); err != nil {
			outcome, failureCode = settle(ctx, err, "OBSERVER_FAILED")
			if outcome != "canceled" && verdict.Verdict != "certified" {
				// Control refused to record a stage for an already failed
				// verdict (deadline passed, fenced): the observer's verdict
				// still settles the attempt; a certified result that cannot
				// be accepted stays a Control refusal.
				outcome, failureCode = verdictOutcome(verdict)
			}
			return nil
		}
		outcome, failureCode = verdictOutcome(verdict)
		return nil
	}
}

// createLedger is the run's account of the create requests issued for the
// launch. It lives in Workflow state, so it is replayed from history exactly
// as it was built: issued counts the requests, unaccounted holds the
// ordinals of those that were written to the backend without an answer
// (unanswered, timed out, canceled in flight) and have not been observed as
// the object they made. A request the backend answered, or that is known
// not to have been created, needs no evidence.
type createLedger struct {
	issued      int
	unaccounted []int
}

// account strikes the requests whose objects the backend showed.
func (l *createLedger) account(requests []int) {
	for _, r := range requests {
		l.unaccounted = slices.DeleteFunc(l.unaccounted, func(u int) bool { return u == r })
	}
}

// settled reports whether every request the run issued is accounted for.
func (l *createLedger) settled() bool { return len(l.unaccounted) == 0 }

// createJob issues create requests for the launch, one at a time under the
// same launch key, until one is answered with the Job or the requests are
// exhausted. Each request is a single Activity execution (no SDK retries)
// whose ordinal marks the object it creates; the run, not the worker,
// therefore knows every request it issued. A request the launcher reports
// NotCreated is settled by that answer; any other failure leaves the
// request unaccounted in the ledger, because the API server may still
// commit it, and the next request is issued after the retry backoff. A
// refusal or a cancellation ends the requests. The answer that carries the
// Job names the request that committed it, which accounts for that request
// even when its own answer was lost.
func createJob(ctx workflow.Context, q Queues, b Bounds, launch activities.LaunchRef, deadline time.Time, ledger *createLedger) (activities.JobRef, error) {
	logger := workflow.GetLogger(ctx)
	createCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue: q.Business, StartToCloseTimeout: b.ControlActivityTimeout, WaitForCancellation: true,
		RetryPolicy: &temporal.RetryPolicy{InitialInterval: b.ControlRetryInitial, MaximumAttempts: 1},
	})
	interval := b.ControlRetryInitial
	for {
		ledger.issued++
		request := ledger.issued
		var job activities.JobRef
		err := workflow.ExecuteActivity(createCtx, activities.NameCreateJob, activities.CreateJobInput{
			Launch: launch, ProfileID: profileID, Deadline: deadline, Request: request,
		}).Get(ctx, &job)
		if err == nil {
			ledger.account([]int{job.Request})
			return job, nil
		}
		switch {
		case activities.IsNotCreated(err):
			logger.Info("create request not created; retrying under the same launch key", "launchKey", launch.LaunchKey, "request", request, "error", err)
		case activities.RefusalCode(err) != "":
			return activities.JobRef{}, err
		default:
			// Written or possibly written without an answer: the backend may
			// still hold or produce its object under the launch key.
			ledger.unaccounted = append(ledger.unaccounted, request)
			logger.Warn("create request unanswered; it stays in the run's ledger until the launch key shows its object", "launchKey", launch.LaunchKey, "request", request, "error", err)
		}
		if ctx.Err() != nil || int32(ledger.issued) >= b.ControlRetryMaxAttempts {
			return activities.JobRef{}, err
		}
		if serr := workflow.Sleep(ctx, interval); serr != nil {
			return activities.JobRef{}, err
		}
		interval *= 2
		if interval > b.ControlRetryMaxInterval {
			interval = b.ControlRetryMaxInterval
		}
	}
}

// cleanupOptions are the protected control-queue options of one cleanup
// Activity: bounded, heartbeating, retried within the round's cap.
func cleanupOptions(ctx workflow.Context, q Queues, b Bounds) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue: q.Control, StartToCloseTimeout: b.CleanupTimeout, HeartbeatTimeout: b.ObserveHeartbeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{InitialInterval: b.ControlRetryInitial, MaximumAttempts: b.CleanupMaxAttempts},
	})
}

// cleanupRound is one protected attempt to confirm that the launch stopped.
// It is complete only when the backend reports neither the Job nor a Pod
// under the launch key and the ledger holds no unaccounted create request.
// While a request is unaccounted for, the round first needs the read-only
// observation of the launch key (observeLaunch), which records what it sees
// and strikes the requests whose objects it saw; the delete then confirms
// the absence of what was seen. The delete itself stops at an object of a
// request still unaccounted for (a create that lands while the earlier
// Job is being deleted) and returns it unstopped; the round records that
// evidence the same way and deletes again. A request still unaccounted for
// after an empty delete may land under the released name, so the round
// observes again; a pass that attributes nothing new ends the round
// unconfirmed, as does nothing observed within the settle window or a
// delete that is not confirmed: the caller records cleanup unknown and
// keeps recovering under the same launch identity.
func cleanupRound(ctx workflow.Context, q Queues, b Bounds, attempt activities.AttemptRef, launch activities.LaunchRef, ledger *createLedger) bool {
	logger := workflow.GetLogger(ctx)
	for observed := false; ; observed = true {
		before := len(ledger.unaccounted)
		if before > 0 && !observeLaunch(ctx, q, b, attempt, launch, ledger) {
			logger.Warn("launch not observed on the backend; a create request is still unaccounted for", "launchKey", launch.LaunchKey, "requests", ledger.unaccounted)
			return false
		}
		stopped, err := deleteJob(ctx, q, b, launch, ledger.unaccounted)
		if err != nil {
			logger.Warn("job cleanup not confirmed", "launchKey", launch.LaunchKey, "error", err)
			return false
		}
		if !stopped.Empty() {
			// The delete found the object of an unaccounted request and left
			// it: recorded here, in history and with Control, before the
			// next delete stops it.
			recordObservation(ctx, q, b, attempt, launch, ledger, stopped)
		}
		if ledger.settled() && stopped.Empty() {
			return true
		}
		if observed && len(ledger.unaccounted) == before {
			logger.Warn("observation attributes nothing to the unaccounted create requests; cleanup stays unknown", "launchKey", launch.LaunchKey, "requests", ledger.unaccounted)
			return false
		}
	}
}

// observeLaunch acquires the evidence an unaccounted create request lacks:
// a read-only observation of the original launch key (the settle window
// paces one observation; absence within it is not evidence and yields
// false), recorded with recordObservation.
func observeLaunch(ctx workflow.Context, q Queues, b Bounds, attempt activities.AttemptRef, launch activities.LaunchRef, ledger *createLedger) bool {
	logger := workflow.GetLogger(ctx)
	var observation activities.LaunchObservation
	if err := workflow.ExecuteActivity(cleanupOptions(ctx, q, b), activities.NameObserveLaunch, activities.ObserveLaunchInput{
		Launch: launch, SettleWindow: b.UnresolvedSettleWindow,
	}).Get(ctx, &observation); err != nil {
		logger.Warn("launch not observed", "launchKey", launch.LaunchKey, "error", err)
		return false
	}
	recordObservation(ctx, q, b, attempt, launch, ledger, observation)
	return true
}

// recordObservation strikes the request markers an observation reports
// from the ledger and registers every reported Pod with Control under the
// running path's command identities. The observation is already in history
// (the Activity result) and the registrations are durable Control records,
// so a lost receipt of the delete that follows can never turn a request
// back into an unaccounted one. A registration that Control cannot record
// is logged and does not stop the cleanup: the observation itself is the
// run's durable evidence.
func recordObservation(ctx workflow.Context, q Queues, b Bounds, attempt activities.AttemptRef, launch activities.LaunchRef, ledger *createLedger, observation activities.LaunchObservation) {
	logger := workflow.GetLogger(ctx)
	ledger.account(observation.Requests)
	business := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue: q.Business, StartToCloseTimeout: b.ControlActivityTimeout,
		RetryPolicy: &temporal.RetryPolicy{InitialInterval: b.ControlRetryInitial, BackoffCoefficient: 2, MaximumInterval: b.ControlRetryMaxInterval, MaximumAttempts: b.ControlRetryMaxAttempts},
	})
	for _, pod := range observation.Pods {
		var inst activities.InstanceRef
		if err := workflow.ExecuteActivity(business, activities.NameRegisterInstance, activities.RegisterInstanceInput{
			Attempt: attempt, Launch: launch, CommandID: attempt.AttemptID + ":register:" + pod.PodUID, JobUID: observation.JobUID, PodUID: pod.PodUID,
		}).Get(ctx, &inst); err != nil {
			logger.Error("observed pod not registered with Control", "launchKey", launch.LaunchKey, "podUid", pod.PodUID, "error", err)
			continue
		}
		if err := workflow.ExecuteActivity(business, activities.NameObserveInstance, activities.ObserveInstanceInput{
			Attempt: attempt, Instance: inst, Observation: pod,
		}).Get(ctx, nil); err != nil {
			logger.Error("observed pod state not recorded with Control", "launchKey", launch.LaunchKey, "instanceId", inst.InstanceID, "error", err)
		}
	}
}

// deleteJob stops the launch on the reserved control queue. It returns an
// empty observation only when the launcher observed the Job and its Pods
// gone; a non-empty one is the object of an unaccounted request the
// launcher left in place for the caller to record first; a bound reached
// first is an error the caller records as unknown.
func deleteJob(ctx workflow.Context, q Queues, b Bounds, launch activities.LaunchRef, unaccounted []int) (activities.LaunchObservation, error) {
	cleanupCtx := cleanupOptions(ctx, q, b)
	var stopped activities.LaunchObservation
	err := workflow.ExecuteActivity(cleanupCtx, activities.NameDeleteJob, activities.DeleteJobInput{Launch: launch, Unaccounted: unaccounted}).Get(cleanupCtx, &stopped)
	return stopped, err
}

// reconcileCleanup keeps this run as the recovery owner of an unconfirmed
// cleanup: rounds against the original launch key, paced by a capped
// exponential backoff on durable timers, until one round confirms or the
// reconciliation bound is reached. The ledger carries over from round to
// round, so evidence acquired earlier is never needed twice.
func reconcileCleanup(ctx workflow.Context, q Queues, b Bounds, attempt activities.AttemptRef, launch activities.LaunchRef, ledger *createLedger) bool {
	logger := workflow.GetLogger(ctx)
	start := workflow.Now(ctx)
	interval := b.ReconcileInitialInterval
	for round := 1; ; round++ {
		if workflow.Now(ctx).Sub(start)+interval > b.ReconcileMaxDuration {
			return false
		}
		if err := workflow.Sleep(ctx, interval); err != nil {
			return false
		}
		if cleanupRound(ctx, q, b, attempt, launch, ledger) {
			return true
		}
		logger.Warn("cleanup reconciliation round not confirmed", "launchKey", launch.LaunchKey, "round", round, "unaccountedCreates", ledger.unaccounted)
		interval *= 2
		if interval > b.ReconcileMaxInterval {
			interval = b.ReconcileMaxInterval
		}
	}
}

// closeAttempt reenters the same durable close command until Control
// answers (no attempt cap); only a refusal ends the retries, and it is
// returned as a visible non-retryable failure of the run.
func closeAttempt(ctx workflow.Context, q Queues, b Bounds, attempt activities.AttemptRef, commandID, outcome, cleanup, failureCode string) error {
	closeCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue: q.Control, StartToCloseTimeout: b.ControlActivityTimeout,
		RetryPolicy: &temporal.RetryPolicy{InitialInterval: b.ControlRetryInitial, BackoffCoefficient: 2, MaximumInterval: b.ControlRetryMaxInterval, MaximumAttempts: 0},
	})
	var closed activities.CloseResult
	if err := workflow.ExecuteActivity(closeCtx, activities.NameCloseAttempt, activities.CloseAttemptInput{
		Attempt: attempt, CommandID: commandID, Outcome: outcome, Cleanup: cleanup, FailureCode: failureCode,
	}).Get(closeCtx, &closed); err != nil {
		return temporal.NewNonRetryableApplicationError(fmt.Sprintf("close attempt %s (%s) refused; the operation is not settled", attempt.AttemptID, commandID), activities.RefusalCode(err), err)
	}
	return nil
}

// verdictOutcome maps the trusted observer's verdict to the attempt outcome.
func verdictOutcome(v activities.Verdict) (string, string) {
	switch v.Verdict {
	case "certified":
		return "completed", ""
	case "infrastructure_failed":
		return "infrastructure_failed", v.FailureCode
	default:
		return "failed", v.FailureCode
	}
}

// settle maps an Activity failure to the attempt outcome: cancellation
// becomes canceled, a Control or launcher refusal carries its public code,
// everything else the given failure code.
func settle(ctx workflow.Context, err error, code string) (string, string) {
	if errors.Is(ctx.Err(), workflow.ErrCanceled) || temporal.IsCanceledError(err) {
		return "canceled", "CANCELED"
	}
	if refused := activities.RefusalCode(err); refused != "" {
		code = refused
	}
	return "infrastructure_failed", code
}
