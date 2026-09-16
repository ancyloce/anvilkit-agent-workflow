package activities

import (
	"context"
)

// Stable Activity names of the recovery reconciliation (DD-01 §7).
const (
	NameEnumerateInventory  = "EnumerateInventory"
	NameListFindings        = "ListFindings"
	NameReconcileFinding    = "ReconcileFinding"
	NameRecordLaunchOutcome = "RecordLaunchOutcome"
	NameEvaluateRecovery    = "EvaluateRecovery"
)

// ObligationClasses are the five inventory classes in enumeration order
// (DD-02 §5); the Workflow enumerates each completely before reconciling.
var ObligationClasses = []string{"intake", "job-launch", "model-dispatch", "tool-dispatch", "business-write"}

type EnumerateInventoryInput struct {
	RunID string
	Class string
}

// ClassProgress is the resumable cursor of one class: Complete only when
// the backend said the listing ended.
type ClassProgress struct {
	Class    string
	Cursor   string
	Complete bool
	Seen     uint64
}

type ListFindingsInput struct {
	RunID  string
	Status string // present | missing | restored | resolved | unresolved | disposed | outside_scope | "" for all
	Limit  int
	// Cursor resumes the listing after the previous page (FindingPage.NextCursor).
	Cursor string
}

// FindingPage is one page of a run's findings in their fixed order;
// Complete is true when no finding follows it.
type FindingPage struct {
	Findings   []Finding
	NextCursor string
	Complete   bool
}

// Finding is one obligation of the rollback window as Control knows it.
type Finding struct {
	FindingID    string
	Class        string
	ObligationID string
	TenantID     string
	Status       string
	Outcome      string
	Detail       string
	LaunchKey    string // job-launch findings whose launch the database holds
}

// Settled reports whether the finding no longer keeps the scope restricted.
func (f Finding) Settled() bool {
	switch f.Status {
	case "present", "resolved", "disposed", "outside_scope":
		return true
	}
	return false
}

type ReconcileFindingInput struct {
	RunID     string
	FindingID string
}

// RecordLaunchOutcomeInput carries the trusted launcher's evidence about a
// job-launch finding in two steps: first the observation of the original
// launch key (Job UID and Pods, Stopped false), which Control records
// before anything is deleted; then, only after that record, the stop
// confirmation (Stopped true) once the delete observed the key empty.
type RecordLaunchOutcomeInput struct {
	RunID     string
	FindingID string
	JobUID    string
	Pods      []PodObservation
	Stopped   bool
}

// RecoveryStatus is the run's gate after an evaluation.
type RecoveryStatus struct {
	Phase     string // fenced | enumerating | reconciling | restricted | reopened
	Unsettled uint64
	Progress  []ClassProgress
}

// Reopened reports whether admission for the scope is open again.
func (s RecoveryStatus) Reopened() bool { return s.Phase == "reopened" }

// RecoveryControl is the Workflow's port to anvilkit.control.v1.RecoveryService.
type RecoveryControl interface {
	EnumerateInventory(ctx context.Context, in EnumerateInventoryInput) (ClassProgress, error)
	ListFindings(ctx context.Context, in ListFindingsInput) (FindingPage, error)
	ReconcileFinding(ctx context.Context, in ReconcileFindingInput) (Finding, error)
	RecordLaunchOutcome(ctx context.Context, in RecordLaunchOutcomeInput) (Finding, error)
	EvaluateRecovery(ctx context.Context, runID string) (RecoveryStatus, error)
}

func (a *Activities) EnumerateInventory(ctx context.Context, in EnumerateInventoryInput) (ClassProgress, error) {
	return a.Recovery.EnumerateInventory(ctx, in)
}

func (a *Activities) ListFindings(ctx context.Context, in ListFindingsInput) (FindingPage, error) {
	return a.Recovery.ListFindings(ctx, in)
}

func (a *Activities) ReconcileFinding(ctx context.Context, in ReconcileFindingInput) (Finding, error) {
	return a.Recovery.ReconcileFinding(ctx, in)
}

func (a *Activities) RecordLaunchOutcome(ctx context.Context, in RecordLaunchOutcomeInput) (Finding, error) {
	return a.Recovery.RecordLaunchOutcome(ctx, in)
}

func (a *Activities) EvaluateRecovery(ctx context.Context, runID string) (RecoveryStatus, error) {
	return a.Recovery.EvaluateRecovery(ctx, runID)
}
