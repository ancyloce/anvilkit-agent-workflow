package workflows_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/workflows"
)

func registerRecovery(env *testsuite.TestWorkflowEnvironment, b workflows.Bounds) {
	var a stub
	env.RegisterWorkflowWithOptions(workflows.Recovery(queues, b), workflow.RegisterOptions{Name: workflows.RecoveryWorkflowName})
	for name, fn := range map[string]any{
		activities.NameEnumerateInventory: a.EnumerateInventory, activities.NameListFindings: a.ListFindings, activities.NameReconcileFinding: a.ReconcileFinding,
		activities.NameRecordLaunchOutcome: a.RecordLaunchOutcome, activities.NameEvaluateRecovery: a.EvaluateRecovery,
		activities.NameObserveLaunch: a.ObserveLaunch, activities.NameDeleteJob: a.DeleteJob,
	} {
		env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
}

const runID = "rec_1"

// The run enumerates every class to completion (resuming from Control's
// cursor after an interrupted page), restores the missing identities,
// settles a restored launch with the launcher's evidence and reopens when
// Control's evaluation says every finding is settled.
func TestRecoveryEnumeratesCompletelyReconcilesAndReopens(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	registerRecovery(env, bounds)
	var pages, listFailures atomic.Int32
	env.OnActivity(activities.NameEnumerateInventory, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.EnumerateInventoryInput) (activities.ClassProgress, error) {
		pages.Add(1)
		if in.Class == "job-launch" && listFailures.Add(1) == 1 {
			return activities.ClassProgress{}, errors.New("EFFECT_UNCERTAIN: listing job-launch: backend unavailable")
		}
		// Two pages per class: the first is not complete.
		if pages.Load()%2 == 1 && in.Class != "job-launch" {
			return activities.ClassProgress{Class: in.Class, Cursor: "next", Complete: false}, nil
		}
		return activities.ClassProgress{Class: in.Class, Cursor: "", Complete: true}, nil
	})
	findings := []activities.Finding{
		{FindingID: "f_present", Class: "intake", ObligationID: "op_1", Status: "present"},
		{FindingID: "f_intake", Class: "intake", ObligationID: "op_2", Status: "missing"},
		{FindingID: "f_launch", Class: "job-launch", ObligationID: "lch_2", Status: "missing"},
		{FindingID: "f_dispatch", Class: "model-dispatch", ObligationID: "dsp_2", Status: "unresolved"},
	}
	env.OnActivity(activities.NameListFindings, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.ListFindingsInput) (activities.FindingPage, error) {
		return activities.FindingPage{Findings: findings, Complete: true}, nil
	})
	var reconciled []string
	env.OnActivity(activities.NameReconcileFinding, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.ReconcileFindingInput) (activities.Finding, error) {
		reconciled = append(reconciled, in.FindingID)
		switch in.FindingID {
		case "f_intake":
			return activities.Finding{FindingID: in.FindingID, Class: "intake", Status: "resolved"}, nil
		case "f_launch":
			return activities.Finding{FindingID: in.FindingID, Class: "job-launch", ObligationID: "lch_2", Status: "restored", LaunchKey: "lc-op2"}, nil
		case "f_dispatch":
			return activities.Finding{FindingID: in.FindingID, Class: "model-dispatch", Status: "resolved"}, nil
		}
		return activities.Finding{}, errors.New("unexpected finding")
	})
	env.OnActivity(activities.NameObserveLaunch, mock.Anything, mock.MatchedBy(func(in activities.ObserveLaunchInput) bool {
		return in.Launch.LaunchKey == "lc-op2" && in.Launch.LaunchID == "lch_2"
	})).Return(activities.LaunchObservation{JobUID: "job-2", Pods: []activities.PodObservation{{PodUID: "pod-2", Phase: "succeeded"}}}, nil)
	// Evidence first: the observation is recorded with Control before the
	// delete, the delete stops only recorded objects, and the stop is
	// confirmed against the recorded observation.
	var order []string
	env.OnActivity(activities.NameDeleteJob, mock.Anything, mock.MatchedBy(func(in activities.DeleteJobInput) bool {
		return in.Launch.LaunchKey == "lc-op2" && in.EvidenceFirst && len(in.Unaccounted) == 0
	})).Return(func(context.Context, activities.DeleteJobInput) (activities.LaunchObservation, error) {
		order = append(order, "delete")
		return activities.LaunchObservation{}, nil
	})
	env.OnActivity(activities.NameRecordLaunchOutcome, mock.Anything, mock.MatchedBy(func(in activities.RecordLaunchOutcomeInput) bool {
		return in.FindingID == "f_launch" && in.JobUID == "job-2" && len(in.Pods) == 1 && !in.Stopped
	})).Return(func(context.Context, activities.RecordLaunchOutcomeInput) (activities.Finding, error) {
		order = append(order, "observed")
		return activities.Finding{FindingID: "f_launch", Status: "restored", Outcome: "observed"}, nil
	})
	env.OnActivity(activities.NameRecordLaunchOutcome, mock.Anything, mock.MatchedBy(func(in activities.RecordLaunchOutcomeInput) bool {
		return in.FindingID == "f_launch" && in.JobUID == "job-2" && len(in.Pods) == 0 && in.Stopped
	})).Return(func(context.Context, activities.RecordLaunchOutcomeInput) (activities.Finding, error) {
		order = append(order, "stopped")
		return activities.Finding{FindingID: "f_launch", Status: "resolved"}, nil
	})
	env.OnActivity(activities.NameEvaluateRecovery, mock.Anything, runID).Return(activities.RecoveryStatus{Phase: "reopened"}, nil)

	env.ExecuteWorkflow(workflows.RecoveryWorkflowName, workflows.RecoveryInput{RunID: runID})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.GreaterOrEqual(t, pages.Load(), int32(10), "every class is enumerated to completion, the interrupted page retried")
	require.ElementsMatch(t, []string{"f_intake", "f_launch", "f_dispatch"}, reconciled, "missing identities are reconciled, unresolved sends queried again, present ones untouched")
	require.Equal(t, []string{"observed", "delete", "stopped"}, order, "the observation is recorded before the delete, the stop confirmed after it")
	env.AssertExpectations(t)
}

// Nothing observed under the original launch key is not evidence: the run
// records that with Control and deletes nothing. An object the delete
// finds with a marker the run has not recorded is recorded first and only
// then deleted.
func TestRecoveryLaunchEvidenceBeforeDeletion(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	registerRecovery(env, bounds)
	env.OnActivity(activities.NameEnumerateInventory, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.EnumerateInventoryInput) (activities.ClassProgress, error) {
		return activities.ClassProgress{Class: in.Class, Complete: true}, nil
	})
	env.OnActivity(activities.NameListFindings, mock.Anything, mock.Anything).Return(activities.FindingPage{Findings: []activities.Finding{
		{FindingID: "f_gone", Class: "job-launch", ObligationID: "lch_gone", Status: "restored", LaunchKey: "lc-gone"},
		{FindingID: "f_late", Class: "job-launch", ObligationID: "lch_late", Status: "unresolved", LaunchKey: "lc-late"},
	}, Complete: true}, nil)
	env.OnActivity(activities.NameReconcileFinding, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.ReconcileFindingInput) (activities.Finding, error) {
		switch in.FindingID {
		case "f_gone":
			return activities.Finding{FindingID: "f_gone", Class: "job-launch", ObligationID: "lch_gone", Status: "restored", LaunchKey: "lc-gone"}, nil
		case "f_late":
			return activities.Finding{FindingID: "f_late", Class: "job-launch", ObligationID: "lch_late", Status: "unresolved", LaunchKey: "lc-late"}, nil
		}
		return activities.Finding{}, errors.New("unexpected finding")
	})
	// lc-gone: nothing within the settle window.
	env.OnActivity(activities.NameObserveLaunch, mock.Anything, mock.MatchedBy(func(in activities.ObserveLaunchInput) bool { return in.Launch.LaunchKey == "lc-gone" })).
		Return(activities.LaunchObservation{}, errors.New("job lc-gone create unresolved: neither the Job nor a Pod was observed within 60s"))
	var recorded []activities.RecordLaunchOutcomeInput
	env.OnActivity(activities.NameRecordLaunchOutcome, mock.Anything, mock.MatchedBy(func(in activities.RecordLaunchOutcomeInput) bool { return in.FindingID == "f_gone" })).
		Return(func(_ context.Context, in activities.RecordLaunchOutcomeInput) (activities.Finding, error) {
			recorded = append(recorded, in)
			return activities.Finding{FindingID: "f_gone", Status: "restored", Detail: "nothing observed; absence is not cleanup evidence"}, nil
		})
	// lc-late: request 1 is observed; the delete finds an object of
	// request 2 that the run has not recorded and returns it unstopped.
	env.OnActivity(activities.NameObserveLaunch, mock.Anything, mock.MatchedBy(func(in activities.ObserveLaunchInput) bool { return in.Launch.LaunchKey == "lc-late" })).
		Return(activities.LaunchObservation{JobUID: "job-late-1", Pods: []activities.PodObservation{{PodUID: "pod-late-1", Phase: "failed"}}, Requests: []int{1}}, nil)
	var deletes []activities.DeleteJobInput
	env.OnActivity(activities.NameDeleteJob, mock.Anything, mock.MatchedBy(func(in activities.DeleteJobInput) bool { return in.Launch.LaunchKey == "lc-late" })).
		Return(func(_ context.Context, in activities.DeleteJobInput) (activities.LaunchObservation, error) {
			deletes = append(deletes, in)
			if len(deletes) == 1 {
				return activities.LaunchObservation{JobUID: "job-late-2", Pods: []activities.PodObservation{{PodUID: "pod-late-2", Phase: "running"}}, Requests: []int{2}}, nil
			}
			return activities.LaunchObservation{}, nil
		})
	env.OnActivity(activities.NameRecordLaunchOutcome, mock.Anything, mock.MatchedBy(func(in activities.RecordLaunchOutcomeInput) bool { return in.FindingID == "f_late" })).
		Return(func(_ context.Context, in activities.RecordLaunchOutcomeInput) (activities.Finding, error) {
			recorded = append(recorded, in)
			if in.Stopped {
				return activities.Finding{FindingID: "f_late", Status: "resolved"}, nil
			}
			return activities.Finding{FindingID: "f_late", Status: "unresolved", Outcome: "observed"}, nil
		})
	env.OnActivity(activities.NameEvaluateRecovery, mock.Anything, runID).Return(activities.RecoveryStatus{Phase: "reopened"}, nil)

	env.ExecuteWorkflow(workflows.RecoveryWorkflowName, workflows.RecoveryInput{RunID: runID})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var gone, late []activities.RecordLaunchOutcomeInput
	for _, r := range recorded {
		if r.FindingID == "f_gone" {
			gone = append(gone, r)
		} else {
			late = append(late, r)
		}
	}
	require.Len(t, gone, 1)
	require.False(t, gone[0].Stopped)
	require.Empty(t, gone[0].JobUID)
	require.Empty(t, gone[0].Pods, "an observation that showed nothing records nothing and never a stop")
	require.Len(t, deletes, 2)
	require.True(t, deletes[0].EvidenceFirst)
	require.Equal(t, []int{1}, deletes[0].Recorded, "the first delete may stop only what the observation recorded")
	require.Equal(t, []int{1, 2}, deletes[1].Recorded, "the object of request 2 was recorded before the second delete")
	require.Len(t, late, 3)
	require.Equal(t, "job-late-1", late[0].JobUID)
	require.False(t, late[0].Stopped)
	require.Equal(t, "job-late-2", late[1].JobUID)
	require.False(t, late[1].Stopped)
	require.True(t, late[2].Stopped, "the stop is confirmed last")
	env.AssertExpectations(t)
}

// A deletion the launcher confirmed whose confirmation Control could not
// take (the RecordLaunchOutcome Activity exhausted its retries) is not
// lost: the pending confirmation stays in the Workflow's ledger and a
// later round resubmits it under the same finding and Job UID until
// Control settles the finding. The deleted Job is never observed again
// (it would show nothing, which is not cleanup evidence), no stop is
// fabricated before the delete and no replacement is launched.
//
// Evidence boundary: the Temporal testsuite executes the Workflow with
// mocked Activities, the retry policy and durable timers on its simulated
// clock; it does not restart a Worker or replay against a Temporal server.
func TestRecoveryResubmitsAPendingStopConfirmationAcrossRounds(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	narrow := bounds
	narrow.ControlRetryMaxAttempts, narrow.ReconcileInitialInterval, narrow.ReconcileMaxInterval, narrow.ReconcileMaxDuration = 3, time.Minute, 5*time.Minute, time.Hour
	registerRecovery(env, narrow)
	env.OnActivity(activities.NameEnumerateInventory, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.EnumerateInventoryInput) (activities.ClassProgress, error) {
		return activities.ClassProgress{Class: in.Class, Complete: true}, nil
	})
	var settled atomic.Bool
	finding := func() activities.Finding {
		f := activities.Finding{FindingID: "f_launch", Class: "job-launch", ObligationID: "lch_7", Status: "restored", LaunchKey: "lc-op7", Outcome: "observed"}
		if settled.Load() {
			f.Status, f.Outcome = "resolved", "job_stopped"
		}
		return f
	}
	env.OnActivity(activities.NameListFindings, mock.Anything, mock.Anything).Return(func(context.Context, activities.ListFindingsInput) (activities.FindingPage, error) {
		return activities.FindingPage{Findings: []activities.Finding{finding()}, Complete: true}, nil
	})
	var reconciles atomic.Int32
	env.OnActivity(activities.NameReconcileFinding, mock.Anything, mock.Anything).Return(func(context.Context, activities.ReconcileFindingInput) (activities.Finding, error) {
		reconciles.Add(1)
		return finding(), nil
	})
	var observations, deletes atomic.Int32
	env.OnActivity(activities.NameObserveLaunch, mock.Anything, mock.Anything).Return(func(context.Context, activities.ObserveLaunchInput) (activities.LaunchObservation, error) {
		observations.Add(1)
		return activities.LaunchObservation{JobUID: "job-7", Pods: []activities.PodObservation{{PodUID: "pod-7", Phase: "running"}}, Requests: []int{1}}, nil
	})
	env.OnActivity(activities.NameDeleteJob, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.DeleteJobInput) (activities.LaunchObservation, error) {
		deletes.Add(1)
		require.True(t, in.EvidenceFirst)
		require.Equal(t, []int{1}, in.Recorded)
		return activities.LaunchObservation{}, nil
	})
	var observed []activities.RecordLaunchOutcomeInput
	env.OnActivity(activities.NameRecordLaunchOutcome, mock.Anything, mock.MatchedBy(func(in activities.RecordLaunchOutcomeInput) bool { return !in.Stopped })).
		Return(func(_ context.Context, in activities.RecordLaunchOutcomeInput) (activities.Finding, error) {
			observed = append(observed, in)
			return finding(), nil
		})
	// The stop confirmation: Control is unreachable for the first
	// submissions (more than the retry policy allows in one round), then
	// answers without settling once, then settles.
	var stops atomic.Int32
	env.OnActivity(activities.NameRecordLaunchOutcome, mock.Anything, mock.MatchedBy(func(in activities.RecordLaunchOutcomeInput) bool { return in.Stopped })).
		Return(func(_ context.Context, in activities.RecordLaunchOutcomeInput) (activities.Finding, error) {
			require.Equal(t, "f_launch", in.FindingID)
			require.Equal(t, "job-7", in.JobUID, "the confirmation names the Job the observation recorded")
			require.Empty(t, in.Pods)
			n := stops.Add(1)
			switch {
			case n <= 4:
				return activities.Finding{}, errors.New("DEPENDENCY_UNAVAILABLE: control unreachable")
			case n == 5:
				return finding(), nil // taken, not settled yet
			}
			settled.Store(true)
			return finding(), nil
		})
	var evaluations atomic.Int32
	env.OnActivity(activities.NameEvaluateRecovery, mock.Anything, runID).Return(func(context.Context, string) (activities.RecoveryStatus, error) {
		evaluations.Add(1)
		if settled.Load() {
			return activities.RecoveryStatus{Phase: "reopened"}, nil
		}
		return activities.RecoveryStatus{Phase: "restricted", Unsettled: 1}, nil
	})

	env.ExecuteWorkflow(workflows.RecoveryWorkflowName, workflows.RecoveryInput{RunID: runID})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError(), "the stop is eventually confirmed and the scope reopens")
	require.Equal(t, int32(1), observations.Load(), "the original launch key is observed once; the deleted Job is never observed again")
	require.Equal(t, int32(1), deletes.Load(), "one delete")
	require.Len(t, observed, 1, "one observation recorded, before the delete")
	require.Equal(t, int32(6), stops.Load(), "the confirmation is resubmitted across rounds until Control settles the finding")
	require.GreaterOrEqual(t, evaluations.Load(), int32(3), "the rounds in between leave the scope restricted")
	require.Equal(t, evaluations.Load(), reconciles.Load(), "every round reads Control's view of the finding first")
	env.AssertExpectations(t)
}

// Findings beyond the first page, and beyond the thousand a single
// listing can return, are reconciled: the loop walks Control's cursor
// to the end however many earlier findings stay unresolved.
func TestRecoveryReconcilesEveryFindingBeyondTheFirstThousand(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	registerRecovery(env, bounds)
	const total = 1200
	env.OnActivity(activities.NameEnumerateInventory, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.EnumerateInventoryInput) (activities.ClassProgress, error) {
		return activities.ClassProgress{Class: in.Class, Complete: true}, nil
	})
	env.OnActivity(activities.NameListFindings, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.ListFindingsInput) (activities.FindingPage, error) {
		start := 0
		if in.Cursor != "" {
			_, err := fmt.Sscanf(in.Cursor, "after:%d", &start)
			require.NoError(t, err)
			start++
		}
		end := min(start+in.Limit, total)
		page := activities.FindingPage{Complete: end == total}
		for i := start; i < end; i++ {
			page.Findings = append(page.Findings, activities.Finding{FindingID: fmt.Sprintf("f_%04d", i), Class: "model-dispatch", ObligationID: fmt.Sprintf("dsp_%04d", i), Status: "unresolved"})
		}
		if !page.Complete {
			page.NextCursor = fmt.Sprintf("after:%d", end-1)
		}
		return page, nil
	})
	reconciled := map[string]int{}
	env.OnActivity(activities.NameReconcileFinding, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.ReconcileFindingInput) (activities.Finding, error) {
		reconciled[in.FindingID]++
		return activities.Finding{FindingID: in.FindingID, Class: "model-dispatch", Status: "unresolved", Detail: "no upstream record"}, nil
	})
	env.OnActivity(activities.NameEvaluateRecovery, mock.Anything, runID).Return(activities.RecoveryStatus{Phase: "reopened"}, nil)

	env.ExecuteWorkflow(workflows.RecoveryWorkflowName, workflows.RecoveryInput{RunID: runID})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, reconciled, total, "every finding of every page got its round")
	require.Equal(t, 1, reconciled["f_1199"], "the last finding, beyond the first thousand, was reconciled once")
	env.AssertExpectations(t)
}

// An unresolved outcome keeps the scope restricted: the run re-evaluates on
// durable timers with capped backoff and, at the reconciliation bound,
// fails visibly instead of reopening.
func TestRecoveryStaysRestrictedUntilTheBound(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	narrow := bounds
	narrow.ReconcileInitialInterval, narrow.ReconcileMaxInterval, narrow.ReconcileMaxDuration = time.Minute, 5*time.Minute, 30*time.Minute
	registerRecovery(env, narrow)
	env.OnActivity(activities.NameEnumerateInventory, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.EnumerateInventoryInput) (activities.ClassProgress, error) {
		return activities.ClassProgress{Class: in.Class, Complete: true}, nil
	})
	env.OnActivity(activities.NameListFindings, mock.Anything, mock.Anything).Return(activities.FindingPage{Findings: []activities.Finding{
		{FindingID: "f_dispatch", Class: "model-dispatch", ObligationID: "dsp_9", Status: "unresolved"},
	}, Complete: true}, nil)
	var queries atomic.Int32
	env.OnActivity(activities.NameReconcileFinding, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.ReconcileFindingInput) (activities.Finding, error) {
		queries.Add(1)
		return activities.Finding{FindingID: in.FindingID, Class: "model-dispatch", Status: "unresolved", Detail: "no upstream record"}, nil
	})
	var evaluations atomic.Int32
	env.OnActivity(activities.NameEvaluateRecovery, mock.Anything, runID).Return(func(_ context.Context, _ string) (activities.RecoveryStatus, error) {
		evaluations.Add(1)
		return activities.RecoveryStatus{Phase: "restricted", Unsettled: 1}, nil
	})

	env.ExecuteWorkflow(workflows.RecoveryWorkflowName, workflows.RecoveryInput{RunID: runID})
	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	var app *temporal.ApplicationError
	require.ErrorAs(t, err, &app)
	require.Equal(t, "RECOVERY_RESTRICTED", app.Type())
	require.GreaterOrEqual(t, evaluations.Load(), int32(4), "the run keeps re-evaluating for late outcomes and dispositions")
	require.Equal(t, evaluations.Load(), queries.Load(), "every round queries the unresolved identity again")
	env.AssertExpectations(t)
}

// A refusal from Control (an unknown run) ends the workflow with that code.
func TestRecoveryRefusalIsVisible(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	registerRecovery(env, bounds)
	env.OnActivity(activities.NameEnumerateInventory, mock.Anything, mock.Anything).Return(activities.ClassProgress{}, activities.Refused("NOT_FOUND", errors.New("run rec_1 not found")))
	env.OnActivity(activities.NameEvaluateRecovery, mock.Anything, runID).Return(activities.RecoveryStatus{}, activities.Refused("NOT_FOUND", errors.New("run rec_1 not found")))
	env.ExecuteWorkflow(workflows.RecoveryWorkflowName, workflows.RecoveryInput{RunID: runID})
	require.True(t, env.IsWorkflowCompleted())
	require.Equal(t, "NOT_FOUND", activities.RefusalCode(env.GetWorkflowError()), "%v", env.GetWorkflowError())
}
