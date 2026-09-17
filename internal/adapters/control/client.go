// Package control is the Workflow's grpc-go client for
// anvilkit.control.v1.ExecutionService. Every durable command carries a
// stable identity derived by the Workflow, so retries reenter the same
// command and never create a second attempt, launch or result.
package control

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-contracts/go/jobschema"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

type Client struct {
	conn     *grpc.ClientConn
	exec     controlv1.ExecutionServiceClient
	recovery controlv1.RecoveryServiceClient
	backend  string
	worker   string
}

// Dial connects to Control; backend names the launch backend identity and
// worker the actor recorded on Workflow-issued commands.
func Dial(address, backend, worker string) (*Client, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, exec: controlv1.NewExecutionServiceClient(conn), recovery: controlv1.NewRecoveryServiceClient(conn), backend: backend, worker: worker}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

// Conn is the established connection, shared with the artifact port.
func (c *Client) Conn() *grpc.ClientConn { return c.conn }

// command builds the durable identity: the digest covers the typed input so
// a changed input for the same command id is refused by Control.
func (c *Client) command(tenantID, commandID string, input any) *controlv1.CommandIdentity {
	raw, _ := json.Marshal(input)
	return &controlv1.CommandIdentity{TenantId: tenantID, CommandId: commandID, ActorId: c.worker, RequestDigest: fmt.Sprintf("sha256:%x", sha256.Sum256(raw))}
}

// nonRetryable marks Control's precondition failures so Temporal does not
// retry a fenced or conflicting command (DD-01 §5). The public code that
// Control put in front of the status message becomes the refusal type;
// transient statuses (Unavailable, DeadlineExceeded, ...) stay retryable.
func nonRetryable(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	switch st.Code() {
	case codes.FailedPrecondition, codes.Aborted, codes.InvalidArgument, codes.NotFound:
		code, _, _ := strings.Cut(st.Message(), ":")
		if code == "" {
			code = st.Code().String()
		}
		return activities.Refused(strings.TrimSpace(code), err)
	}
	return err
}

func (c *Client) OpenAttempt(ctx context.Context, in activities.OpenAttemptInput) (activities.AttemptRef, error) {
	tenant := in.TenantID
	resp, err := c.exec.OpenAttempt(ctx, &controlv1.OpenAttemptRequest{
		Command: c.command(tenant, in.CommandID, in), OperationId: in.OperationID, StepId: in.StepID,
		VisitOrdinal: strconv.FormatUint(in.Visit, 10), ProfileId: in.ProfileID,
	})
	if err != nil {
		return activities.AttemptRef{}, nonRetryable(err)
	}
	at := resp.GetAttempt()
	return activities.AttemptRef{AttemptID: at.GetAttemptId(), TenantID: tenant, ProfileID: at.GetProfileId(), Deadline: at.GetDeadline().AsTime(), ExecutionEpoch: at.GetExecutionEpoch()}, nil
}

func (c *Client) PrepareLaunch(ctx context.Context, in activities.PrepareLaunchInput) (activities.LaunchRef, error) {
	profile, err := jobschema.ProfileByID(in.Attempt.ProfileID)
	if err != nil {
		return activities.LaunchRef{}, activities.Refused("PROFILE_UNQUALIFIED", err)
	}
	image := profile.Image.Digest
	resp, err := c.exec.PrepareLaunch(ctx, &controlv1.PrepareLaunchRequest{
		Command: c.command(in.Attempt.TenantID, in.CommandID, in), AttemptId: in.Attempt.AttemptID, LaunchKey: in.LaunchKey,
		Backend: c.backend, ImageDigest: image, Deadline: timestamppb.New(in.Deadline),
	})
	if err != nil {
		return activities.LaunchRef{}, nonRetryable(err)
	}
	return activities.LaunchRef{LaunchID: resp.GetLaunchId(), AttemptID: resp.GetAttemptId(), LaunchKey: resp.GetLaunchKey(), ImageDigest: image, LaunchEpoch: resp.GetLaunchEpoch()}, nil
}

func (c *Client) RegisterInstance(ctx context.Context, in activities.RegisterInstanceInput) (activities.InstanceRef, error) {
	resp, err := c.exec.RegisterInstance(ctx, &controlv1.RegisterInstanceRequest{
		Command: c.command(in.Attempt.TenantID, in.CommandID, in), AttemptId: in.Attempt.AttemptID, LaunchKey: in.Launch.LaunchKey,
		Backend: c.backend, JobUid: in.JobUID, PodUid: in.PodUID, ImageDigest: in.Launch.ImageDigest,
	})
	if err != nil {
		return activities.InstanceRef{}, nonRetryable(err)
	}
	return activities.InstanceRef{InstanceID: resp.GetInstance().GetInstanceId(), Current: resp.GetInstance().GetCurrent()}, nil
}

var phases = map[string]controlv1.InstancePhase{
	"pending": controlv1.InstancePhase_INSTANCE_PHASE_PENDING, "running": controlv1.InstancePhase_INSTANCE_PHASE_RUNNING,
	"succeeded": controlv1.InstancePhase_INSTANCE_PHASE_SUCCEEDED, "failed": controlv1.InstancePhase_INSTANCE_PHASE_FAILED,
}

func (c *Client) ObserveInstance(ctx context.Context, attemptID, instanceID string, obs activities.PodObservation) error {
	phase, ok := phases[obs.Phase]
	if !ok {
		phase = controlv1.InstancePhase_INSTANCE_PHASE_UNKNOWN
	}
	_, err := c.exec.ObserveInstance(ctx, &controlv1.ObserveInstanceRequest{AttemptId: attemptID, InstanceId: instanceID, Phase: phase, ExitCode: obs.ExitCode, ObservedAt: timestamppb.New(obs.ObservedAt)})
	return nonRetryable(err)
}

var verdicts = map[string]controlv1.Verdict{
	"certified": controlv1.Verdict_VERDICT_CERTIFIED, "repairable": controlv1.Verdict_VERDICT_REPAIRABLE, "invalid": controlv1.Verdict_VERDICT_INVALID,
	"infrastructure_failed": controlv1.Verdict_VERDICT_INFRASTRUCTURE_FAILED, "canceled": controlv1.Verdict_VERDICT_CANCELED,
}

func (c *Client) AcceptResult(ctx context.Context, in activities.AcceptResultInput) (activities.StageRef, error) {
	req := &controlv1.AcceptResultRequest{
		Command: c.command(in.Attempt.TenantID, in.CommandID, in), AttemptId: in.Attempt.AttemptID, InstanceId: in.Instance.InstanceID,
		ProfileId: in.ProfileID, Verdict: verdicts[in.Verdict.Verdict], ResultDigest: in.Verdict.ResultDigest, ResultManifest: in.Verdict.Manifest,
		ObserverIdentity: c.worker, ExecutionEpoch: in.Attempt.ExecutionEpoch,
	}
	if in.Verdict.FailureCode != "" {
		req.FailureCode = &in.Verdict.FailureCode
	}
	resp, err := c.exec.AcceptResult(ctx, req)
	if err != nil {
		return activities.StageRef{}, nonRetryable(err)
	}
	return activities.StageRef{StageID: resp.GetStage().GetStageId()}, nil
}

var outcomes = map[string]controlv1.AttemptOutcome{
	"completed": controlv1.AttemptOutcome_ATTEMPT_OUTCOME_COMPLETED, "failed": controlv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED,
	"infrastructure_failed": controlv1.AttemptOutcome_ATTEMPT_OUTCOME_INFRASTRUCTURE_FAILED, "canceled": controlv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELED,
	"unknown": controlv1.AttemptOutcome_ATTEMPT_OUTCOME_UNKNOWN,
}

var cleanups = map[string]controlv1.CleanupState{
	"not_required": controlv1.CleanupState_CLEANUP_STATE_NOT_REQUIRED, "pending": controlv1.CleanupState_CLEANUP_STATE_PENDING,
	"complete": controlv1.CleanupState_CLEANUP_STATE_COMPLETE, "unknown": controlv1.CleanupState_CLEANUP_STATE_UNKNOWN,
}

func (c *Client) CloseAttempt(ctx context.Context, in activities.CloseAttemptInput) (activities.CloseResult, error) {
	req := &controlv1.CloseAttemptRequest{Command: c.command(in.Attempt.TenantID, in.CommandID, in), AttemptId: in.Attempt.AttemptID, Outcome: outcomes[in.Outcome], Cleanup: cleanups[in.Cleanup]}
	if in.FailureCode != "" {
		req.FailureCode = &in.FailureCode
	}
	resp, err := c.exec.CloseAttempt(ctx, req)
	if err != nil {
		return activities.CloseResult{}, nonRetryable(err)
	}
	return activities.CloseResult{Lifecycle: strings.ToLower(strings.TrimPrefix(resp.GetOperation().GetLifecycle().String(), "LIFECYCLE_"))}, nil
}

// ---- RecoveryService (P07) ----

func toProgress(p *controlv1.ClassProgress) activities.ClassProgress {
	seen, _ := strconv.ParseUint(p.GetSeen(), 10, 64)
	return activities.ClassProgress{Class: p.GetClass(), Cursor: p.GetCursor(), Complete: p.GetComplete(), Seen: seen}
}

func toFinding(f *controlv1.Finding) activities.Finding {
	return activities.Finding{
		FindingID: f.GetFindingId(), Class: f.GetClass(), ObligationID: f.GetObligationId(), TenantID: f.GetTenantId(),
		Status: strings.ToLower(strings.TrimPrefix(f.GetStatus().String(), "FINDING_STATUS_")), Outcome: f.GetOutcome(), Detail: f.GetDetail(), LaunchKey: f.GetLaunchKey(),
	}
}

var findingStatuses = map[string]controlv1.FindingStatus{
	"present": controlv1.FindingStatus_FINDING_STATUS_PRESENT, "missing": controlv1.FindingStatus_FINDING_STATUS_MISSING, "restored": controlv1.FindingStatus_FINDING_STATUS_RESTORED,
	"resolved": controlv1.FindingStatus_FINDING_STATUS_RESOLVED, "unresolved": controlv1.FindingStatus_FINDING_STATUS_UNRESOLVED, "disposed": controlv1.FindingStatus_FINDING_STATUS_DISPOSED,
	"outside_scope": controlv1.FindingStatus_FINDING_STATUS_OUTSIDE_SCOPE,
}

func (c *Client) EnumerateInventory(ctx context.Context, in activities.EnumerateInventoryInput) (activities.ClassProgress, error) {
	resp, err := c.recovery.EnumerateInventory(ctx, &controlv1.EnumerateInventoryRequest{RunId: in.RunID, Class: in.Class})
	if err != nil {
		return activities.ClassProgress{}, nonRetryable(err)
	}
	return toProgress(resp.GetProgress()), nil
}

func (c *Client) ListFindings(ctx context.Context, in activities.ListFindingsInput) (activities.FindingPage, error) {
	resp, err := c.recovery.ListFindings(ctx, &controlv1.ListFindingsRequest{RunId: in.RunID, Status: findingStatuses[in.Status], Limit: int32(in.Limit), Cursor: in.Cursor})
	if err != nil {
		return activities.FindingPage{}, nonRetryable(err)
	}
	page := activities.FindingPage{Findings: make([]activities.Finding, 0, len(resp.GetFindings())), NextCursor: resp.GetNextCursor(), Complete: resp.GetComplete()}
	for _, f := range resp.GetFindings() {
		page.Findings = append(page.Findings, toFinding(f))
	}
	return page, nil
}

func (c *Client) ReconcileFinding(ctx context.Context, in activities.ReconcileFindingInput) (activities.Finding, error) {
	resp, err := c.recovery.ReconcileFinding(ctx, &controlv1.ReconcileFindingRequest{RunId: in.RunID, FindingId: in.FindingID})
	if err != nil {
		return activities.Finding{}, nonRetryable(err)
	}
	return toFinding(resp.GetFinding()), nil
}

func (c *Client) RecordLaunchOutcome(ctx context.Context, in activities.RecordLaunchOutcomeInput) (activities.Finding, error) {
	req := &controlv1.RecordLaunchOutcomeRequest{RunId: in.RunID, FindingId: in.FindingID, JobUid: in.JobUID, Stopped: in.Stopped}
	for _, p := range in.Pods {
		phase, ok := phases[p.Phase]
		if !ok {
			phase = controlv1.InstancePhase_INSTANCE_PHASE_UNKNOWN
		}
		req.Pods = append(req.Pods, &controlv1.LaunchPod{PodUid: p.PodUID, Phase: phase, ExitCode: p.ExitCode, ObservedAt: timestamppb.New(p.ObservedAt)})
	}
	resp, err := c.recovery.RecordLaunchOutcome(ctx, req)
	if err != nil {
		return activities.Finding{}, nonRetryable(err)
	}
	return toFinding(resp.GetFinding()), nil
}

func (c *Client) EvaluateRecovery(ctx context.Context, runID string) (activities.RecoveryStatus, error) {
	resp, err := c.recovery.EvaluateRecovery(ctx, &controlv1.EvaluateRecoveryRequest{RunId: runID})
	if err != nil {
		return activities.RecoveryStatus{}, nonRetryable(err)
	}
	run := resp.GetRun()
	unsettled, _ := strconv.ParseUint(run.GetUnsettledFindings(), 10, 64)
	status := activities.RecoveryStatus{Phase: strings.ToLower(strings.TrimPrefix(run.GetPhase().String(), "RECOVERY_PHASE_")), Unsettled: unsettled}
	for _, p := range run.GetProgress() {
		status.Progress = append(status.Progress, toProgress(p))
	}
	return status, nil
}
