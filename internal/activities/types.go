// Package activities holds the LocalCheck Activities: thin, testable use
// cases over the Control and launcher ports (DD-01 §1). All I/O lives here;
// the Workflow never touches the network or the wall clock.
package activities

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/sdk/temporal"
)

// Stable Activity names (DD-01 §7): Go refactoring must not change stored
// identities.
const (
	NameOpenAttempt      = "OpenAttempt"
	NamePrepareLaunch    = "PrepareLaunch"
	NameCreateJob        = "CreateJob"
	NameObserveJob       = "ObserveJob"
	NameRegisterInstance = "RegisterInstance"
	NameObserveInstance  = "ObserveInstance"
	NameVerifyResult     = "VerifyResult"
	NameAcceptResult     = "AcceptResult"
	NameObserveLaunch    = "ObserveLaunch"
	NameDeleteJob        = "DeleteJob"
	NameCloseAttempt     = "CloseAttempt"
)

type OpenAttemptInput struct {
	OperationID string
	TenantID    string
	CommandID   string
	StepID      string
	Visit       uint64
	ProfileID   string
}

type AttemptRef struct {
	AttemptID string
	TenantID  string
	ProfileID string
	Deadline  time.Time
	// ExecutionEpoch is the epoch the attempt was opened under; the result
	// acceptance names it so a stage is accepted only under the current one.
	ExecutionEpoch string
}

type PrepareLaunchInput struct {
	Attempt   AttemptRef
	CommandID string
	LaunchKey string
	Deadline  time.Time
}

type LaunchRef struct {
	LaunchID    string
	AttemptID   string
	LaunchKey   string
	ImageDigest string
}

// CreateJobInput is one create request for the launch. Request is its
// ordinal within the launch (1, 2, ...): every request reuses the launch
// key, so the backend stores at most one Job, but only the marker a request
// leaves on the object it creates tells which request committed. The
// Workflow issues one request at a time and keeps the ledger of those that
// were written but never answered, so cleanup can account for each of them.
type CreateJobInput struct {
	Launch    LaunchRef
	ProfileID string
	Deadline  time.Time
	Request   int
	// Envelope facts of a harness profile (DD-03 §4): the typed inputs by
	// name and digest and the identities the launch envelope carries. The
	// LocalCheck fixture leaves them empty.
	OperationID    string
	ExecutionEpoch string
	LaunchEpoch    string
	Inputs         []LaunchInput
}

// LaunchInput is one typed input of a launch: a name bound to a digest and,
// when the bytes live in the artifact store, the opaque handle.
type LaunchInput struct {
	Name   string
	Digest string
	Handle string
}

// JobRef identifies the Job under the launch key and the create request
// whose marker it carries (0 when the object carries none).
type JobRef struct {
	JobUID  string
	Request int
}

type ObserveJobInput struct {
	Launch   LaunchRef
	Deadline time.Time
}

// PodObservation is trusted backend evidence: identities and states come
// from the Kubernetes API, never from the candidate.
type PodObservation struct {
	PodUID             string
	Phase              string // pending | running | succeeded | failed | unknown
	ExitCode           *int32
	Reason             string
	TerminationMessage string
	ObservedAt         time.Time
}

type JobObservation struct {
	JobUID string
	Pods   []PodObservation // in creation order; the first is the physical owner
	Reason string           // Job-level condition reason (for example DeadlineExceeded)
}

type RegisterInstanceInput struct {
	Attempt   AttemptRef
	Launch    LaunchRef
	CommandID string
	JobUID    string
	PodUID    string
}

type InstanceRef struct {
	InstanceID string
	Current    bool
}

type ObserveInstanceInput struct {
	Attempt     AttemptRef
	Instance    InstanceRef
	Observation PodObservation
}

type VerifyResultInput struct {
	Launch      LaunchRef
	Attempt     AttemptRef
	ProfileID   string
	Observation JobObservation
	CompletedAt time.Time
}

// Verdict is the trusted observer's decision with the manifest Control will
// accept and its digest.
type Verdict struct {
	Verdict      string
	FailureCode  string
	Manifest     []byte
	ResultDigest string
}

type AcceptResultInput struct {
	Attempt   AttemptRef
	Instance  InstanceRef
	CommandID string
	ProfileID string
	Verdict   Verdict
}

type StageRef struct {
	StageID string
}

// ObserveLaunchInput asks the backend what exists under the original launch
// key while a create request of the launch is unaccounted for (written,
// never answered, its object not yet seen). The call is a read: it deletes
// nothing, so a lost completion receipt is repaired by repeating it while
// the objects are still there.
type ObserveLaunchInput struct {
	Launch LaunchRef
	// SettleWindow is how long one observation waits for the Job or a Pod
	// to appear before reporting that the create is not known to have
	// finished. Absence for the window is an error, never an observation
	// of "nothing": the in-flight create may still land.
	SettleWindow time.Duration
}

// LaunchObservation is the trusted backend evidence that a create
// finished: the Job under the launch key and every Pod reported for it, the
// Job's own Pods first in creation order. JobUID comes from the Job or,
// when only lingering Pods remain, from their owner reference. Requests are
// the distinct create request markers found on the Job and the Pods,
// ascending: the Workflow strikes them from its ledger of unanswered
// requests.
type LaunchObservation struct {
	JobUID   string
	Pods     []PodObservation
	Requests []int
}

// Empty reports that the backend showed nothing under the launch key.
func (o LaunchObservation) Empty() bool {
	return o.JobUID == "" && len(o.Pods) == 0 && len(o.Requests) == 0
}

// DeleteJobInput names a launch to stop. Unaccounted are the create
// requests of the launch whose objects the Workflow has not recorded yet:
// an object carrying one of their markers is evidence the Workflow needs
// before the object disappears, so the launcher reports it instead of
// deleting it. The backend reporting neither the Job nor a Pod afterwards
// is complete cleanup only when the Workflow's ledger holds no unaccounted
// request; the Workflow decides that, the launcher only confirms the
// absence.
type DeleteJobInput struct {
	Launch      LaunchRef
	Unaccounted []int
	// EvidenceFirst inverts the ledger for a caller that owns no ledger of
	// the launch's create requests (the recovery of another run's launch):
	// only objects whose create request marker is in Recorded are deleted,
	// every other marked object is reported unstopped so the caller records
	// it first. Objects without a marker are deleted in both modes.
	EvidenceFirst bool
	Recorded      []int
}

// Refused marks an adapter error that must not be retried by Temporal: a
// Control precondition failure (fence, conflict, unknown identity) or a
// launch that the backend must not create (deadline passed). The public code
// becomes the application error type so the Workflow can settle the attempt
// with the actual reason (DD-01 §5).
func Refused(code string, err error) error {
	return temporal.NewNonRetryableApplicationError("refused: "+err.Error(), code, err)
}

// RefusalCode returns the public code of a Refused error, or "" when the
// error is not a refusal (transient failures, cancellation).
func RefusalCode(err error) string {
	var app *temporal.ApplicationError
	if errors.As(err, &app) && app.NonRetryable() {
		return app.Type()
	}
	return ""
}

// notCreatedType marks a failed create request the backend is known not to
// have stored.
const notCreatedType = "NOT_CREATED"

// NotCreated marks a create request that stored nothing on the backend: the
// API server answered with a definitive rejection, or the request never
// reached the wire. Such a request needs no cleanup evidence. Every other
// failed create (written without an answer, timed out, a server-side
// error) is unresolved: the request may still commit under the launch key,
// so it stays in the Workflow's ledger until the key shows the object it
// made.
func NotCreated(err error) error {
	return temporal.NewApplicationError("not created: "+err.Error(), notCreatedType, err)
}

// IsNotCreated reports whether a failed create request is known to have
// stored nothing.
func IsNotCreated(err error) bool {
	var app *temporal.ApplicationError
	return errors.As(err, &app) && app.Type() == notCreatedType
}

type CloseAttemptInput struct {
	Attempt     AttemptRef
	CommandID   string
	Outcome     string // completed | failed | infrastructure_failed | canceled | unknown
	Cleanup     string // not_required | complete | unknown
	FailureCode string
}

type CloseResult struct {
	Lifecycle string
}

// Control is the Workflow's port to anvilkit.control.v1.ExecutionService.
type Control interface {
	OpenAttempt(ctx context.Context, in OpenAttemptInput) (AttemptRef, error)
	PrepareLaunch(ctx context.Context, in PrepareLaunchInput) (LaunchRef, error)
	RegisterInstance(ctx context.Context, in RegisterInstanceInput) (InstanceRef, error)
	ObserveInstance(ctx context.Context, attemptID, instanceID string, obs PodObservation) error
	AcceptResult(ctx context.Context, in AcceptResultInput) (StageRef, error)
	CloseAttempt(ctx context.Context, in CloseAttemptInput) (CloseResult, error)
}

// Launcher is the fixed-template Kubernetes port (DD-03 §4): it accepts a
// reviewed profile id and typed inputs only.
type Launcher interface {
	// CreateJob issues one marked create request under the launch key and
	// returns the Job with the marker of the request that committed it. A
	// failed request is NotCreated when the backend is known to hold
	// nothing from it; any other error leaves the request unresolved.
	CreateJob(ctx context.Context, in CreateJobInput) (JobRef, error)
	ObserveJob(ctx context.Context, in ObserveJobInput, heartbeat func()) (JobObservation, error)
	// ObserveLaunch reports the Job and Pods under the launch key as soon as
	// the backend shows any, waiting up to SettleWindow; it never deletes.
	// An error means nothing was observed: the create is still unresolved.
	ObserveLaunch(ctx context.Context, in ObserveLaunchInput, heartbeat func()) (LaunchObservation, error)
	// DeleteJob stops the launch and returns an empty observation only
	// after the backend no longer reports the Job or any of its Pods. It
	// deletes nothing while an object carries the marker of an unaccounted
	// request: that object is returned as the observation, unstopped, so
	// the caller records it first. An error means cleanup is unknown.
	DeleteJob(ctx context.Context, in DeleteJobInput, heartbeat func()) (LaunchObservation, error)
}
