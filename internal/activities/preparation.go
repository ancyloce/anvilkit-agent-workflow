package activities

import (
	"context"
	"encoding/json"
	"time"
)

// The Preparation Activities (DD-01 §3, delivery.md P13-02): thin use
// cases over the Control preparation port, the trusted artifact port, the
// controlled model caller and the Knowledge double. Every durable command
// carries a stable identity derived by the Workflow; the analysis reads
// the prompt and the accepted answers through Control's artifact read and
// asks the Proxy for one structured call under a stable call id.
const (
	NameGetPreparation      = "GetPreparation"
	NameAnalyzeRequirements = "AnalyzeRequirements"
	NameRecordQuestionSet   = "RecordQuestionSet"
	NameGetAnswer           = "GetAnswer"
	NameFreezeReferences    = "FreezeReferences"
	NameFreezeBrief         = "FreezeBrief"
	NameSettleOperation     = "SettleOperation"
)

// ArtifactBinding names a finalized transfer and the digest it holds.
type ArtifactBinding struct {
	TransferID string
	Digest     string
	Handle     string
}

// SourceReference names an exact revision of a Knowledge source.
type SourceReference struct {
	SourceID string
	Revision string
}

// ContentDigest freezes the exact content of a referenced revision.
type ContentDigest struct {
	SourceID string
	Revision string
	Digest   string
}

// Question is one clarification question.
type Question struct {
	QuestionID string
	Text       string
}

// QuestionSetRef is a recorded question set with its immutable expiry.
type QuestionSetRef struct {
	QuestionSetID string
	Revision      string
	Round         uint64
	Questions     []Question
	AskedAt       time.Time
	ExpiresAt     time.Time
	State         string
}

// BriefRef is a frozen brief.
type BriefRef struct {
	BriefID            string
	Brief              ArtifactBinding
	RequirementsDigest string
	Revision           string
	State              string
}

type GetPreparationInput struct {
	OperationID string
	TenantID    string
}

// PreparationView is what Control records about a Preparation.
type PreparationView struct {
	Prompt          ArtifactBinding
	BrandReferences []SourceReference
	AssetReferences []SourceReference
	QuestionSets    []QuestionSetRef
	Brief           *BriefRef
	MaxRounds       uint64
	MaxQuestions    uint64
	Wait            time.Duration
}

// AnalysisAnswer is one accepted answer the analysis reads (by transfer).
type AnalysisAnswer struct {
	Round    uint64
	Answer   ArtifactBinding
	Question QuestionSetRef
}

// AnalyzeRequirementsInput asks for one structured analysis call: the
// prompt and the accepted answers so far are read through Control, the
// call goes to the route under CallID (a retry reenters the same call),
// MaxQuestions bounds a question round and MustConclude forbids one
// (the rounds are exhausted: the analysis states its unknowns instead).
type AnalyzeRequirementsInput struct {
	OperationID     string
	TenantID        string
	AttemptID       string
	ExecutionEpoch  uint64
	CallID          string
	RouteID         string
	Round           uint64
	Prompt          ArtifactBinding
	Answers         []AnalysisAnswer
	MaxQuestions    uint64
	MustConclude    bool
	MaxOutputTokens int
	MaxExposure     Money
	Deadline        time.Time
}

// Analysis is the structured outcome of one call: either the requirements
// (Requirements set, Questions empty) or a grouped question round. State is
// the call state the Proxy answered (succeeded, failed, canceled, unknown);
// an unknown call is a business outcome to reconcile under the call id,
// never a reason to call again; a content refusal (the model produced
// neither) consumes the content-repair allowance of the caller.
type Analysis struct {
	CallID    string
	State     string
	ErrorCode string
	// Requirements is omitted when absent: a JSON null would otherwise be
	// four bytes of "requirements" in the Workflow's eyes.
	Requirements json.RawMessage `json:"requirements,omitempty"`
	Questions    []Question
}

type RecordQuestionSetInput struct {
	OperationID string
	TenantID    string
	CommandID   string
	Round       uint64
	Questions   []Question
}

// GetAnswerInput reads an accepted answer by id (the Update's argument) or
// by question set (the timer's re-check: an answer Control accepted before
// the expiry counts even when its Update receipt is still on its way).
type GetAnswerInput struct {
	OperationID   string
	TenantID      string
	AnswerID      string
	QuestionSetID string
}

// AnswerRef.Found is false when no answer is accepted for the question set.

// AnswerRef is an accepted answer as Control records it.
type AnswerRef struct {
	Found               bool
	AnswerID            string
	QuestionSetID       string
	QuestionSetRevision string
	Answer              ArtifactBinding
	Relay               string
}

type FreezeReferencesInput struct {
	TenantID        string
	BrandReferences []SourceReference
	AssetReferences []SourceReference
}

// FrozenReferences are the content digests of the exact revisions.
type FrozenReferences struct {
	BrandDigests []ContentDigest
	AssetDigests []ContentDigest
}

// FreezeBriefInput freezes the brief: the document is composed here from
// the requirements and the bindings, uploaded as a brief artifact bound to
// the operation (stable transfer command ids), then recorded with Control
// under CommandID.
type FreezeBriefInput struct {
	OperationID     string
	TenantID        string
	CommandID       string
	Requirements    json.RawMessage
	Prompt          ArtifactBinding
	Answers         []AnalysisAnswer
	SourceRevisions []SourceReference
	Frozen          FrozenReferences
}

type SettleOperationInput struct {
	OperationID string
	TenantID    string
	CommandID   string
	Outcome     string // succeeded | failed | canceled
	FailureCode string
	Phase       string
}

type OperationRef struct {
	Lifecycle string
	Phase     string
	Revision  string
}

// PreparationControl is the Workflow's port to Control's PreparationService
// and ExecutionService.SettleOperation.
type PreparationControl interface {
	GetPreparation(ctx context.Context, in GetPreparationInput) (PreparationView, error)
	RecordQuestionSet(ctx context.Context, in RecordQuestionSetInput) (QuestionSetRef, error)
	GetAnswer(ctx context.Context, in GetAnswerInput) (AnswerRef, error)
	RecordBrief(ctx context.Context, operationID, tenantID, commandID string, brief ArtifactBinding, requirementsDigest string, sources []SourceReference, frozen FrozenReferences) (BriefRef, error)
	SettleOperation(ctx context.Context, in SettleOperationInput) (OperationRef, error)
}

// Artifacts is the Workflow's trusted artifact port over Control's
// ArtifactService and the object store capabilities: Upload begins a
// transfer bound to the operation under the given command id, puts the
// bytes through the capability and finalizes with the object version;
// Read obtains the download capability of an artifact under the reading
// operation's relationship and verifies the bytes against the digest and
// size Control answered.
type Artifacts interface {
	Upload(ctx context.Context, tenantID, operationID, commandID, class, mediaType string, body []byte) (ArtifactBinding, error)
	Read(ctx context.Context, operationID, transferID, handle string, maxBytes int64) ([]byte, ArtifactBinding, error)
}

// Knowledge is the port that freezes the content digests of the selected
// source revisions (DD-07); until P15/P16 deliver Knowledge it is an
// explicitly labeled DEVELOPMENT_ONLY double.
type Knowledge interface {
	ContentDigest(ctx context.Context, tenantID string, ref SourceReference) (ContentDigest, error)
}
