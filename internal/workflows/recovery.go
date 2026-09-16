package workflows

import (
	"fmt"
	"slices"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// RecoveryWorkflowName is the stable registered Workflow Type of the
// rollback-window reconciliation (delivery.md P07).
const RecoveryWorkflowName = "RecoveryWorkflow"

// findingsPage is the page size of one ListFindings Activity; the
// reconciliation walks every page of a run, so the size only bounds one
// Activity result.
const findingsPage = 200

// RecoveryInput is the run identity; every other fact is read through
// Control.
type RecoveryInput struct {
	RunID string `json:"runId"`
}

// Recovery drives the reconciliation of one recovery run in the order the
// design fixes. Control already closed admission for the scope and fenced
// every active execution identity under the new recovery epoch when it
// recorded the run (BeginRecovery is one commit; the relay starts this
// Workflow afterwards). This run then:
//
//  1. enumerates each obligation class of the rollback window completely,
//     page by page, resuming from Control's cursor: a page Control cannot
//     read fails the Activity and is retried, never treated as empty;
//  2. reconciles the findings, walking every page of them so a finding
//     late in the order is processed however many unresolved ones precede
//     it: every identity missing from the restored database is restored
//     under the run's epoch and its original outcome is queried by Control
//     (dispatch, business write); a launch whose cleanup Control's records
//     do not prove is settled by the trusted launcher on the reserved
//     control queue, evidence first: observe the original launch key,
//     record what was seen with Control, delete only what was recorded,
//     confirm the key empty;
//  3. evaluates the gate: the scope reopens only when every class is
//     enumerated completely and every finding is settled. Otherwise the
//     scope stays restricted and this run keeps reconciling on durable
//     timers with capped backoff (late outcomes, operator dispositions)
//     until the reconciliation bound, when it fails visibly with the scope
//     still restricted. Nothing here resends under a new identity, creates
//     a replacement launch or settles an unknown outcome as zero.
func Recovery(q Queues, b Bounds) func(ctx workflow.Context, in RecoveryInput) error {
	return func(ctx workflow.Context, in RecoveryInput) error {
		logger := workflow.GetLogger(ctx)
		var bounds Bounds
		if err := workflow.SideEffect(ctx, func(workflow.Context) any { return b }).Get(&bounds); err != nil {
			return err
		}
		control := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			TaskQueue: q.Control, StartToCloseTimeout: bounds.ControlActivityTimeout,
			RetryPolicy: &temporal.RetryPolicy{InitialInterval: bounds.ControlRetryInitial, BackoffCoefficient: 2, MaximumInterval: bounds.ControlRetryMaxInterval, MaximumAttempts: bounds.ControlRetryMaxAttempts},
		})
		// The evidence ledgers of the launches this run settles, by finding:
		// Workflow state, replayed from history, so a create request marker
		// recorded before a delete stays recorded across Worker restarts.
		ledgers := map[string]*launchLedger{}
		start := workflow.Now(ctx)
		interval := bounds.ReconcileInitialInterval
		for round := 1; ; round++ {
			if err := enumerate(control, in.RunID); err != nil {
				logger.Warn("inventory enumeration incomplete; the scope stays restricted", "runId", in.RunID, "round", round, "error", err)
			} else if err := reconcile(ctx, control, q, bounds, in.RunID, ledgers); err != nil {
				logger.Warn("reconciliation round incomplete", "runId", in.RunID, "round", round, "error", err)
			}
			var status activities.RecoveryStatus
			if err := workflow.ExecuteActivity(control, activities.NameEvaluateRecovery, in.RunID).Get(ctx, &status); err != nil {
				if activities.RefusalCode(err) != "" {
					return err
				}
				logger.Warn("recovery evaluation deferred", "runId", in.RunID, "error", err)
			} else if status.Reopened() {
				logger.Info("recovery scope reopened", "runId", in.RunID, "round", round)
				return nil
			} else {
				logger.Warn("recovery scope stays restricted", "runId", in.RunID, "round", round, "phase", status.Phase, "unsettled", status.Unsettled)
			}
			if workflow.Now(ctx).Sub(start)+interval > bounds.ReconcileMaxDuration {
				return temporal.NewNonRetryableApplicationError(fmt.Sprintf("recovery run %s not reopened within %s; the scope stays restricted", in.RunID, bounds.ReconcileMaxDuration), "RECOVERY_RESTRICTED", nil)
			}
			if err := workflow.Sleep(ctx, interval); err != nil {
				return err
			}
			interval *= 2
			if interval > bounds.ReconcileMaxInterval {
				interval = bounds.ReconcileMaxInterval
			}
		}
	}
}

// enumerate lists every class to completion; an Activity that exhausts its
// retries leaves the class's cursor with Control and returns the error.
func enumerate(ctx workflow.Context, runID string) error {
	for _, class := range activities.ObligationClasses {
		for {
			var progress activities.ClassProgress
			if err := workflow.ExecuteActivity(ctx, activities.NameEnumerateInventory, activities.EnumerateInventoryInput{RunID: runID, Class: class}).Get(ctx, &progress); err != nil {
				return fmt.Errorf("class %s from cursor: %w", class, err)
			}
			if progress.Complete {
				break
			}
		}
	}
	return nil
}

// reconcile advances every unsettled finding of the run once, walking the
// findings page by page from Control's cursor until the listing is
// complete: an unresolved finding never hides the ones after it, and a
// finding that cannot be advanced this round is logged and skipped. Control
// decides each finding from its records and the original-identity query
// (ReconcileFinding); a launch it cannot settle from its records gets the
// launcher's evidence (settleLaunch).
func reconcile(ctx workflow.Context, control workflow.Context, q Queues, b Bounds, runID string, ledgers map[string]*launchLedger) error {
	logger := workflow.GetLogger(ctx)
	var incomplete error
	cursor := ""
	for {
		var page activities.FindingPage
		if err := workflow.ExecuteActivity(control, activities.NameListFindings, activities.ListFindingsInput{RunID: runID, Limit: findingsPage, Cursor: cursor}).Get(ctx, &page); err != nil {
			return err
		}
		for _, f := range page.Findings {
			if f.Settled() {
				continue
			}
			var advanced activities.Finding
			if err := workflow.ExecuteActivity(control, activities.NameReconcileFinding, activities.ReconcileFindingInput{RunID: runID, FindingID: f.FindingID}).Get(ctx, &advanced); err != nil {
				logger.Warn("finding not reconciled", "findingId", f.FindingID, "class", f.Class, "error", err)
				incomplete = err
				continue
			}
			f = advanced
			if f.Settled() || f.Class != "job-launch" || f.LaunchKey == "" {
				continue
			}
			ledger := ledgers[f.FindingID]
			if ledger == nil {
				ledger = &launchLedger{}
				ledgers[f.FindingID] = ledger
			}
			if err := settleLaunch(ctx, control, q, b, runID, f, ledger); err != nil {
				logger.Warn("launch finding not settled", "findingId", f.FindingID, "launchKey", f.LaunchKey, "error", err)
				incomplete = err
			}
		}
		if page.Complete {
			return incomplete
		}
		cursor = page.NextCursor
	}
}

// launchLedger is the run's evidence account of one launch it settles: the
// create request markers the backend showed and Control recorded, and the
// stop confirmation still owed to Control once a delete observed the key
// empty. It lives in Workflow state, replayed from the Activity results in
// history, so evidence recorded before a delete is never needed twice,
// and a confirmed deletion whose confirmation to Control failed is
// resubmitted in a later round instead of being observed again (the
// deleted Job would show nothing, which is not cleanup evidence). Nothing
// here is lost to an Activity retry or a Worker restart.
type launchLedger struct {
	recorded []int
	// stopPending is set from the DeleteJob result that observed the launch
	// key empty and cleared when Control settles the finding on the
	// confirmation; stoppedJob is the Job UID that confirmation names.
	stopPending bool
	stoppedJob  string
}

func (l *launchLedger) record(requests []int) {
	for _, r := range requests {
		if !slices.Contains(l.recorded, r) {
			l.recorded = append(l.recorded, r)
		}
	}
}

// settleLaunch establishes a launch's outcome with the trusted launcher,
// evidence first, under the original launch identity (no replacement
// launch is ever created):
//
//  1. observe the original launch key, read-only, for the settle window. An
//     observation that shows nothing is reported to Control as such and
//     ends the round: absence within a window, an observation failure or a
//     temporary gap is not cleanup evidence, and nothing is deleted;
//  2. record what was seen with Control (Pods registered as physical
//     instances, the Job UID as the finding's evidence) and in this run's
//     ledger, before anything is deleted;
//  3. delete only objects whose markers were recorded; an object the delete
//     finds with another marker is returned unstopped, recorded the same
//     way and only then deleted;
//  4. confirm the key empty to Control, which accepts the confirmation only
//     against the recorded observation and settles the finding. A
//     confirmation Control could not take this round (the Activity
//     exhausted its retries) stays pending in the ledger and is
//     resubmitted, idempotently under the same finding and Job UID, by
//     the next round without observing the deleted Job again.
func settleLaunch(ctx workflow.Context, control workflow.Context, q Queues, b Bounds, runID string, f activities.Finding, ledger *launchLedger) error {
	logger := workflow.GetLogger(ctx)
	if ledger.stopPending {
		return confirmStop(ctx, control, runID, f.FindingID, ledger)
	}
	launch := activities.LaunchRef{LaunchID: f.ObligationID, LaunchKey: f.LaunchKey}
	cleanup := cleanupOptions(ctx, q, b)
	var observed activities.LaunchObservation
	if err := workflow.ExecuteActivity(cleanup, activities.NameObserveLaunch, activities.ObserveLaunchInput{Launch: launch, SettleWindow: b.UnresolvedSettleWindow}).Get(ctx, &observed); err != nil {
		logger.Info("nothing observed under the original launch key within the settle window; not cleanup evidence", "launchKey", f.LaunchKey, "error", err)
		var unresolved activities.Finding
		return workflow.ExecuteActivity(control, activities.NameRecordLaunchOutcome, activities.RecordLaunchOutcomeInput{RunID: runID, FindingID: f.FindingID}).Get(ctx, &unresolved)
	}
	if err := recordLaunch(ctx, control, runID, f.FindingID, observed, ledger); err != nil {
		return err
	}
	jobUID := observed.JobUID
	for {
		var stopped activities.LaunchObservation
		if err := workflow.ExecuteActivity(cleanup, activities.NameDeleteJob, activities.DeleteJobInput{Launch: launch, EvidenceFirst: true, Recorded: ledger.recorded}).Get(ctx, &stopped); err != nil {
			return err
		}
		if stopped.Empty() {
			break
		}
		// An object of a create request this run has not recorded yet:
		// evidence before deletion, then delete again.
		if stopped.JobUID != "" {
			jobUID = stopped.JobUID
		}
		if err := recordLaunch(ctx, control, runID, f.FindingID, stopped, ledger); err != nil {
			return err
		}
	}
	// The delete observed the key empty (an Activity result in history):
	// the confirmation is owed from now on, whatever happens to its
	// submission.
	ledger.stopPending, ledger.stoppedJob = true, jobUID
	return confirmStop(ctx, control, runID, f.FindingID, ledger)
}

// confirmStop submits the pending stop confirmation of the ledger to
// Control. The confirmation stays pending until Control settles the
// finding on it: a failed submission is resubmitted by a later round, and
// so is one Control answered without settling (late evidence, an attempt
// close it refused this time).
func confirmStop(ctx workflow.Context, control workflow.Context, runID, findingID string, ledger *launchLedger) error {
	var settled activities.Finding
	if err := workflow.ExecuteActivity(control, activities.NameRecordLaunchOutcome, activities.RecordLaunchOutcomeInput{RunID: runID, FindingID: findingID, JobUID: ledger.stoppedJob, Stopped: true}).Get(ctx, &settled); err != nil {
		return err
	}
	if settled.Settled() {
		ledger.stopPending = false
	}
	return nil
}

// recordLaunch makes an observation durable with Control (the Activity
// result is already in history) and strikes its markers into the ledger.
func recordLaunch(ctx workflow.Context, control workflow.Context, runID, findingID string, observation activities.LaunchObservation, ledger *launchLedger) error {
	var recorded activities.Finding
	if err := workflow.ExecuteActivity(control, activities.NameRecordLaunchOutcome, activities.RecordLaunchOutcomeInput{
		RunID: runID, FindingID: findingID, JobUID: observation.JobUID, Pods: observation.Pods, Stopped: false,
	}).Get(ctx, &recorded); err != nil {
		return err
	}
	ledger.record(observation.Requests)
	return nil
}
