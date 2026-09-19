package activities

import (
	"context"
	"time"
)

// The Generation Activities (DD-01 §4, delivery.md P13-03/05/06): the
// execution permit, the lease (through the LeasePort, only confirmed
// results reported to Control), the funding, the source scope check, the
// accepted stage read of an attempt and the candidate registration under
// an effect identity (a lost receipt queries the original effect, never
// registers again).
const (
	NameGetGeneration          = "GetGeneration"
	NameRequestExecutionPermit = "RequestExecutionPermit"
	NameRecordFunding          = "RecordFunding"
	NameCheckSourceScope       = "CheckSourceScope"
	NameAcquireLease           = "AcquireLease"
	NameRenewLease             = "RenewLease"
	NameQueryLease             = "QueryLease"
	NameReleaseLease           = "ReleaseLease"
	NameRecordLease            = "RecordLease"
	NameGetAcceptedStage       = "GetAcceptedStage"
	NameRegisterCandidate      = "RegisterCandidate"
)

type GetGenerationInput struct {
	OperationID string
	TenantID    string
}

// GenerationView is Control's admission record of a Generation.
type GenerationView struct {
	ProfileID            string
	SubjectDigest        string
	SourceRevision       string
	ActorID              string
	Brief                BriefRef
	SourceRevisions      []SourceReference
	QueueDeadline        time.Time
	ActiveDeadline       *time.Time
	PermitActive         bool
	Lease                LeaseRecord
	Funded               bool
	DefinitionActivation string
	MaxRepairs           uint64
	CodegenProfileID     string
	ValidatorProfileID   string
	CandidateEffectID    string
}

// LeaseRecord is the last confirmed lease fact Control holds.
type LeaseRecord struct {
	State      string // none | held | lost | released
	LeaseID    string
	Fence      string
	ExpiresAt  *time.Time
	Occurrence uint64
}

type RequestExecutionPermitInput struct {
	OperationID string
	TenantID    string
	CommandID   string
}

// PermitAnswer is Control's admission decision.
type PermitAnswer struct {
	Granted        bool
	PermitID       string
	ActiveDeadline *time.Time
	QueueDeadline  time.Time
	QueuedAhead    uint64
}

type RecordFundingInput struct {
	OperationID string
	TenantID    string
	CommandID   string
}

type FundingRef struct {
	Currency string
	Amount   string
	Existing bool
}

// CheckSourceScopeInput asks the SourcePort whether the brief's source
// scope may be generated against for the tenant (DD-06 SourcePort).
type CheckSourceScopeInput struct {
	OperationID     string
	TenantID        string
	BriefID         string
	SubjectDigest   string
	SourceRevisions []SourceReference
}

type ScopeDecision struct {
	Allowed    bool
	ReasonCode string
	Revision   string
}

// LeaseInput is one lease call under a stable occurrence.
type LeaseInput struct {
	OperationID string
	TenantID    string
	Subject     string
	Owner       string
	Occurrence  uint64
	LeaseID     string
	Fence       string
	TTL         time.Duration
}

// LeaseResult is what the port answered: held (confirmed with expiry),
// lost (confirmed loss: another owner, released, expired upstream), or
// unknown (no answer). An unknown result never extends known validity.
type LeaseResult struct {
	State     string // held | lost | unknown
	LeaseID   string
	Fence     string
	ExpiresAt *time.Time
	Reason    string
}

type RecordLeaseInput struct {
	OperationID string
	TenantID    string
	CommandID   string
	Occurrence  uint64
	State       string // held | lost | released
	LeaseID     string
	Fence       string
	ExpiresAt   *time.Time
}

type GetAcceptedStageInput struct {
	AttemptID   string
	TenantID    string
	OperationID string
}

// AcceptedStage is the stage Control accepted for an attempt.
type AcceptedStage struct {
	Found          bool
	StageID        string
	AttemptID      string
	Verdict        string
	FailureCode    string
	ResultDigest   string
	ExecutionEpoch string
	Artifacts      []StageArtifact
}

// StageArtifact is one artifact an accepted stage binds.
type StageArtifact struct {
	Handle        string
	Class         string
	Digest        string
	SizeBytes     string
	TransferID    string
	ObjectVersion string
}

// RegisterCandidateInput registers the certified source as a candidate
// through the SourcePort under an effect identity: PrepareEffect under
// CommandID (occurrence stable), one registration when permitted, the
// outcome observed; a permit that already exists is not a second
// permission: the original effect is queried instead.
type RegisterCandidateInput struct {
	OperationID    string
	TenantID       string
	AttemptID      string
	InstanceID     string
	ExecutionEpoch uint64
	CommandID      string
	Occurrence     uint64
	Subject        string
	SourceRevision string
	Source         StageArtifact
	Certification  StageArtifact
	Lease          LeaseRecord
	Deadline       time.Time
}

// CandidateRef is the registration outcome.
type CandidateRef struct {
	EffectID    string
	State       string // succeeded | failed | unknown | denied
	CandidateID string
	Revision    string
	DenialCode  string
}

// GenerationControl is the Workflow's port to Control's GenerationService
// and EffectService.
type GenerationControl interface {
	GetGeneration(ctx context.Context, in GetGenerationInput) (GenerationView, error)
	RequestExecutionPermit(ctx context.Context, in RequestExecutionPermitInput) (PermitAnswer, error)
	RecordFunding(ctx context.Context, in RecordFundingInput) (FundingRef, error)
	RecordLease(ctx context.Context, in RecordLeaseInput) (LeaseRecord, error)
	GetAcceptedStage(ctx context.Context, in GetAcceptedStageInput) (AcceptedStage, error)
	// PrepareEffect, ObserveEffect and GetEffect are the EffectService
	// path of the candidate registration.
	PrepareEffect(ctx context.Context, in RegisterCandidateInput) (EffectPermit, error)
	ObserveEffect(ctx context.Context, tenantID, effectID, source string, sequence uint64, outcome, receiptDigest, nativeRef string, at time.Time) error
	GetEffect(ctx context.Context, tenantID, effectID string) (EffectPermit, error)
}

// EffectPermit is Control's answer to a prepare or a query.
type EffectPermit struct {
	EffectID   string
	Permitted  bool
	State      string
	Outcome    string
	DenialCode string
	OutcomeRef string
}

// LeasePort is the source lease upstream (DD-06 §1 LeasePort): acquire,
// renew, query and release with a stable occurrence, explicit expiry and
// explicit loss. Until ENV-07 supplies the Pagix declaration it is an
// explicitly labeled DEVELOPMENT_ONLY double.
type LeasePort interface {
	Acquire(ctx context.Context, in LeaseInput) (LeaseResult, error)
	Renew(ctx context.Context, in LeaseInput) (LeaseResult, error)
	Query(ctx context.Context, in LeaseInput) (LeaseResult, error)
	Release(ctx context.Context, in LeaseInput) (LeaseResult, error)
}

// SourcePort is the source authority upstream (DD-06 §1 SourcePort): the
// scope decision and the candidate registration with original-identity
// query. DEVELOPMENT_ONLY double until ENV-07.
type SourcePort interface {
	CheckScope(ctx context.Context, in CheckSourceScopeInput) (ScopeDecision, error)
	RegisterCandidate(ctx context.Context, effectID string, in RegisterCandidateInput) (CandidateRef, error)
	QueryRegistration(ctx context.Context, effectID string) (CandidateRef, bool, error)
}
