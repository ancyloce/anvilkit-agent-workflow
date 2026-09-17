package activities

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
)

// The controlled model call Activities (DD-01 §5 "External send after
// admission", DD-03 §2 ControlledModelPort, delivery.md P11): the Workflow's
// fixed Activities reach the Model Proxy over the frozen transport
// (openapi/model-proxy.yaml). The Proxy asks Control for the single-use
// permission and sends once; a retry of the Activity under the same call id
// is a reentry the Proxy answers from its record — it never sends again —
// so transport loss between the worker and the Proxy is safe to retry while
// the original deadline holds, and a query never sends. Preparation and the
// agent team (P12/P13) compose these; nothing here orchestrates a call.
const (
	NameCallModel       = "CallModel"
	NameGetModelCall    = "GetModelCall"
	NameCancelModelCall = "CancelModelCall"
)

// ModelMessage is one message of the frozen contract (roles system, user,
// assistant, tool).
type ModelMessage struct {
	Role       string
	Content    string
	ToolCallID string
}

// ModelTool names a reviewed tool of the route by the digest of its
// reviewed input schema; the Proxy holds the schema.
type ModelTool struct {
	Name              string
	Description       string
	InputSchemaDigest string
}

// ModelCallInput is the typed input of one controlled call. CallID is the
// stable identity the Workflow derives (a retry carries the same one); the
// execution binding is the Workflow's current attempt and instance; the
// deadline is the original one and is never extended by a retry.
type ModelCallInput struct {
	CallID           string
	TenantID         string
	OperationID      string
	AttemptID        string
	InstanceID       string
	ExecutionEpoch   uint64
	RouteID          string
	Messages         []ModelMessage
	Tools            []ModelTool
	MaxOutputTokens  int
	MaxExposure      Money
	Deadline         time.Time
	SupersedesCallID string
	EvidenceRef      string
}

// Money is a scale-6 integer amount as a decimal string.
type Money struct {
	Currency string
	Amount   string
}

// ModelUsage carries the native usage counters as the Proxy observed them.
type ModelUsage struct {
	InputUnits       string
	OutputUnits      string
	ReasoningUnits   string
	CachedInputUnits string
}

// ModelToolCall is one tool call the model produced.
type ModelToolCall struct {
	ToolCallID      string
	Name            string
	Arguments       string
	ArgumentsDigest string
}

// ModelCallResult is the outcome of a call as the Proxy answered it: the
// call state (succeeded, failed, canceled, unknown), the assembled text and
// tool calls of the frames, the usage when the Proxy had it, and the
// Proxy's error code otherwise. An unknown state is a business outcome to
// reconcile under the original call id, never a reason to call again.
type ModelCallResult struct {
	CallID          string
	DispatchID      string
	State           string
	Text            string
	ToolCalls       []ModelToolCall
	Usage           *ModelUsage
	NativeReference string
	ErrorCode       string
	Frames          int
}

// ModelCaller is the port to the Model Proxy transport.
type ModelCaller interface {
	CallModel(ctx context.Context, in ModelCallInput, heartbeat func()) (ModelCallResult, error)
	GetModelCall(ctx context.Context, callID string) (ModelCallResult, error)
	CancelModelCall(ctx context.Context, callID string) (ModelCallResult, error)
}

func (a *Activities) CallModel(ctx context.Context, in ModelCallInput) (ModelCallResult, error) {
	return a.Model.CallModel(ctx, in, func() { activity.RecordHeartbeat(ctx) })
}

func (a *Activities) GetModelCall(ctx context.Context, callID string) (ModelCallResult, error) {
	return a.Model.GetModelCall(ctx, callID)
}

func (a *Activities) CancelModelCall(ctx context.Context, callID string) (ModelCallResult, error) {
	return a.Model.CancelModelCall(ctx, callID)
}
