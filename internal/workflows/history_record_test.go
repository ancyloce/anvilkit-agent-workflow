//go:build historyrecord

package workflows_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/workflows"
)

// TestRecordHistories runs scripted scenarios of the Generation,
// Preparation and Recovery Workflows on the development foundation's
// Temporal (ANVILKIT_DEV_TEMPORAL_ADDRESS) with stub Activities and writes
// each run's history to testdata/histories as
// <Type>-<ANVILKIT_HISTORY_LABEL>-<scenario>.json, the replay evidence of
// P1.7. The scenarios cross the P1.6 version markers (settle backoff and
// continue-as-new, the final close straight to the settlement, the hold
// accounting, the batched recovery rounds). The file compiles against the
// Workflow code before P1.6 as well (Activity names as strings, inputs read
// as JSON), so the same scenarios recorded by that code are the "before"
// histories:
//
//	ANVILKIT_HISTORY_LABEL=after go test -tags historyrecord -run TestRecordHistories ./internal/workflows/
func TestRecordHistories(t *testing.T) {
	address, label := os.Getenv("ANVILKIT_DEV_TEMPORAL_ADDRESS"), os.Getenv("ANVILKIT_HISTORY_LABEL")
	if address == "" || label == "" {
		t.Skip("ANVILKIT_DEV_TEMPORAL_ADDRESS and ANVILKIT_HISTORY_LABEL are required")
	}
	var suffix [4]byte
	_, _ = rand.Read(suffix[:])
	tag := hex.EncodeToString(suffix[:])
	q := workflows.Queues{Business: "history-record-" + tag, Control: "history-record-" + tag + "-control"}
	b := workflows.Bounds{
		ControlActivityTimeout: 10 * time.Second, ControlRetryInitial: time.Second, ControlRetryMaxInterval: 2 * time.Second, ControlRetryMaxAttempts: 3,
		LaunchWindow: 2 * time.Minute, ObserveHeartbeatTimeout: 30 * time.Second, ObserveMaxAttempts: 3,
		CleanupTimeout: 30 * time.Second, CleanupMaxAttempts: 2, UnresolvedSettleWindow: 5 * time.Second, CreateRequestLifetime: 30 * time.Second,
		ReconcileInitialInterval: time.Second, ReconcileMaxInterval: 2 * time.Second, ReconcileMaxDuration: 10 * time.Minute,
	}
	c, err := client.Dial(client.Options{HostPort: address, Namespace: "anvilkit"})
	require.NoError(t, err)
	defer c.Close()

	s := &recorder{settles: map[string]int{}, closes: map[string]int{}, reconcilingSettles: map[string]int{}, reconcilingCloses: map[string]int{}, rounds: map[string]int{}}
	for _, queue := range []string{q.Business, q.Control} {
		w := worker.New(c, queue, worker.Options{})
		w.RegisterWorkflowWithOptions(workflows.Preparation(q, b, lifecycleBounds), workflow.RegisterOptions{Name: workflows.PreparationWorkflowName})
		w.RegisterWorkflowWithOptions(workflows.Generation(q, b, lifecycleBounds), workflow.RegisterOptions{Name: workflows.GenerationWorkflowName})
		w.RegisterWorkflowWithOptions(workflows.Recovery(q, b), workflow.RegisterOptions{Name: workflows.RecoveryWorkflowName})
		for name, fn := range s.activities() {
			w.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
		}
		require.NoError(t, w.Start())
		defer w.Stop()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	record := func(typ, scenario string, run client.WorkflowRun, all bool) {
		t.Helper()
		first := run.GetRunID() // Get follows a continuation and moves the run id
		_ = run.Get(ctx, nil)
		runs := []string{first}
		if all {
			// A continued run's successors share the Workflow ID.
			d, err := c.DescribeWorkflowExecution(ctx, run.GetID(), "")
			require.NoError(t, err)
			if last := d.GetWorkflowExecutionInfo().GetExecution().GetRunId(); last != first {
				runs = append(runs, last)
			}
		}
		for i, id := range runs {
			h := &historypb.History{}
			it := c.GetWorkflowHistory(ctx, run.GetID(), id, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
			for it.HasNext() {
				ev, err := it.Next()
				require.NoError(t, err)
				h.Events = append(h.Events, ev)
			}
			raw, err := protojson.MarshalOptions{Indent: "  "}.Marshal(h)
			require.NoError(t, err)
			name := fmt.Sprintf("%s-%s-%s.json", typ, label, scenario)
			if i > 0 {
				name = fmt.Sprintf("%s-%s-%s-run%d.json", typ, label, scenario, i+1)
			}
			require.NoError(t, os.WriteFile(filepath.Join("testdata", "histories", name), raw, 0o644))
			t.Logf("%s: %d events", name, len(h.Events))
		}
	}
	start := func(id, wf string, in any) client.WorkflowRun {
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id + "-" + tag, TaskQueue: q.Business, WorkflowExecutionTimeout: 4 * time.Minute}, wf, in)
		require.NoError(t, err)
		return run
	}

	// Generation: certified and registered; settlement kept reconciling
	// for three polls; a failed validation whose final close meets an
	// operation Control still reconciles.
	record("Generation", "success", start("op_HISTGENOK", workflows.GenerationWorkflowName, workflows.Input{OperationID: "op_HISTGENOK", TenantID: "tenant_a"}), false)
	s.set(func() { s.reconcilingSettles["op_HISTGENRECON"] = 3 })
	record("Generation", "settle-reconciling", start("op_HISTGENRECON", workflows.GenerationWorkflowName, workflows.Input{OperationID: "op_HISTGENRECON", TenantID: "tenant_a"}), true)
	s.set(func() { s.reconcilingCloses["op_HISTGENFAIL"] = 1 })
	record("Generation", "validation-failed", start("op_HISTGENFAIL", workflows.GenerationWorkflowName, workflows.Input{OperationID: "op_HISTGENFAIL", TenantID: "tenant_a"}), false)

	// Preparation: sufficient on the first round; canceled during the
	// analysis call, its close meeting a reconciling operation.
	record("Preparation", "success", start("op_HISTPREPOK", workflows.PreparationWorkflowName, workflows.Input{OperationID: "op_HISTPREPOK", TenantID: "tenant_a"}), false)
	s.set(func() { s.reconcilingCloses["op_HISTPREPCANCEL"] = 1; s.holdAnalysis = true })
	canceled := start("op_HISTPREPCANCEL", workflows.PreparationWorkflowName, workflows.Input{OperationID: "op_HISTPREPCANCEL", TenantID: "tenant_a"})
	require.Eventually(t, func() bool { return s.get(func() bool { return s.analysisHeld }) }, time.Minute, 50*time.Millisecond)
	require.NoError(t, c.CancelWorkflow(ctx, canceled.GetID(), ""))
	record("Preparation", "canceled", canceled, false)

	// Recovery: the first round leaves the scope restricted, the second
	// reopens it.
	record("Recovery", "two-rounds", start("rec_HISTTWO", workflows.RecoveryWorkflowName, workflows.RecoveryInput{RunID: "rec_HISTTWO"}), true)
}

// recorder holds the stub Activities' script and state.
type recorder struct {
	mu                                    sync.Mutex
	settles, closes                       map[string]int
	reconcilingSettles, reconcilingCloses map[string]int
	rounds                                map[string]int
	holdAnalysis, analysisHeld            bool
}

func (s *recorder) set(f func()) { s.mu.Lock(); defer s.mu.Unlock(); f() }
func (s *recorder) get(f func() bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return f()
}

type opIn struct {
	OperationID string
	CommandID   string
	ProfileID   string
	StepID      string
	AttemptID   string
	Outcome     string
	Phase       string
	Attempt     struct{ AttemptID string }
	Launch      struct{ AttemptID string }
	RunID       string
	FindingID   string
	FindingIDs  []string
	CallID      string
}

func decode(raw json.RawMessage) opIn {
	var in opIn
	_ = json.Unmarshal(raw, &in)
	return in
}

func (s *recorder) activities() map[string]any {
	exit := int32(0)
	now := func() time.Time { return time.Now() }
	ok := func(context.Context, json.RawMessage) error { return nil }
	return map[string]any{
		"GetGeneration": func(_ context.Context, _ json.RawMessage) (activities.GenerationView, error) {
			return activities.GenerationView{SubjectDigest: "sha256:subject", Brief: brief, QueueDeadline: now().Add(2 * time.Hour), DefinitionActivation: "generation-v1:def-1", MaxRepairs: 1,
				CodegenProfileID: "codegen-team-dev-v1", ValidatorProfileID: "validator-source-v1"}, nil
		},
		"RequestExecutionPermit": func(_ context.Context, _ json.RawMessage) (activities.PermitAnswer, error) {
			d := now().Add(time.Hour)
			return activities.PermitAnswer{Granted: true, PermitID: "prm_1", ActiveDeadline: &d, QueueDeadline: now().Add(2 * time.Hour)}, nil
		},
		"CheckSourceScope": func(_ context.Context, _ json.RawMessage) (activities.ScopeDecision, error) {
			return activities.ScopeDecision{Allowed: true}, nil
		},
		"RecordFunding": func(_ context.Context, _ json.RawMessage) (activities.FundingRef, error) {
			return activities.FundingRef{Currency: "USD", Amount: "1000000"}, nil
		},
		"BriefIdentity": func(_ context.Context, _ json.RawMessage) (activities.ComponentIdentity, error) {
			return briefIdentity, nil
		},
		"AcquireLease": func(_ context.Context, _ json.RawMessage) (activities.LeaseResult, error) {
			return held(now().Add(10 * time.Minute)), nil
		},
		"RenewLease": func(_ context.Context, _ json.RawMessage) (activities.LeaseResult, error) {
			return held(now().Add(10 * time.Minute)), nil
		},
		"QueryLease": func(_ context.Context, _ json.RawMessage) (activities.LeaseResult, error) {
			return held(now().Add(10 * time.Minute)), nil
		},
		"ReleaseLease": func(_ context.Context, _ json.RawMessage) (activities.LeaseResult, error) {
			return activities.LeaseResult{State: "lost", Reason: "RELEASED"}, nil
		},
		"RecordLease": func(_ context.Context, raw json.RawMessage) (activities.LeaseRecord, error) {
			var in struct {
				State, LeaseID, Fence string
				Occurrence            uint64
				ExpiresAt             *time.Time
			}
			_ = json.Unmarshal(raw, &in)
			return activities.LeaseRecord{State: in.State, LeaseID: in.LeaseID, Fence: in.Fence, ExpiresAt: in.ExpiresAt, Occurrence: in.Occurrence}, nil
		},
		"OpenAttempt": func(_ context.Context, raw json.RawMessage) (activities.AttemptRef, error) {
			in := decode(raw)
			return activities.AttemptRef{AttemptID: "att_" + in.CommandID, TenantID: "tenant_a", ProfileID: in.ProfileID, Deadline: now().Add(time.Hour), ExecutionEpoch: "1"}, nil
		},
		"PrepareLaunch": func(_ context.Context, raw json.RawMessage) (activities.LaunchRef, error) {
			in := decode(raw)
			return activities.LaunchRef{LaunchID: "lch_" + in.Attempt.AttemptID, AttemptID: in.Attempt.AttemptID, LaunchKey: "hist-launch", ImageDigest: "sha256:img", LaunchEpoch: "1"}, nil
		},
		"CreateJob": func(_ context.Context, raw json.RawMessage) (activities.JobRef, error) {
			in := decode(raw)
			return activities.JobRef{JobUID: "job-" + in.Launch.AttemptID, Request: 1}, nil
		},
		"AwaitJobOwner": func(_ context.Context, raw json.RawMessage) (activities.JobObservation, error) {
			in := decode(raw)
			return activities.JobObservation{JobUID: "job-" + in.Launch.AttemptID, Pods: []activities.PodObservation{{PodUID: "pod-" + in.Launch.AttemptID, Phase: "running"}}}, nil
		},
		"ObserveJob": func(_ context.Context, raw json.RawMessage) (activities.JobObservation, error) {
			in := decode(raw)
			return activities.JobObservation{JobUID: "job-" + in.Launch.AttemptID, Pods: []activities.PodObservation{{PodUID: "pod-" + in.Launch.AttemptID, Phase: "succeeded", ExitCode: &exit}}}, nil
		},
		"RegisterInstance": func(_ context.Context, raw json.RawMessage) (activities.InstanceRef, error) {
			in := decode(raw)
			return activities.InstanceRef{InstanceID: "inst-" + in.Attempt.AttemptID, Current: true}, nil
		},
		"ObserveInstance": ok,
		"DeleteJob": func(_ context.Context, _ json.RawMessage) (activities.LaunchObservation, error) {
			return activities.LaunchObservation{}, nil
		},
		"ObserveLaunch": func(_ context.Context, _ json.RawMessage) (activities.LaunchObservation, error) {
			return activities.LaunchObservation{}, nil
		},
		"GetAcceptedStage": func(_ context.Context, raw json.RawMessage) (activities.AcceptedStage, error) {
			in := decode(raw)
			if strings.Contains(in.AttemptID, ":codegen:") {
				return certified(in.AttemptID, sourceA, stageA), nil
			}
			if strings.HasPrefix(in.OperationID, "op_HISTGENFAIL") {
				return activities.AcceptedStage{Found: true, StageID: "stg_v", AttemptID: in.AttemptID, Verdict: "failed", FailureCode: "CANDIDATE_INVALID", ExecutionEpoch: "1", Artifacts: []activities.StageArtifact{evidence}}, nil
			}
			return certified(in.AttemptID, evidence), nil
		},
		"RegisterCandidate": func(_ context.Context, _ json.RawMessage) (activities.CandidateRef, error) {
			return activities.CandidateRef{EffectID: "eff_1", State: "succeeded", CandidateID: "cand_1"}, nil
		},
		"SettleOperation": func(_ context.Context, raw json.RawMessage) (activities.OperationRef, error) {
			in := decode(raw)
			id := strings.Split(in.OperationID, "-")[0]
			s.mu.Lock()
			defer s.mu.Unlock()
			s.settles[id]++
			if s.reconcilingSettles[id] > 0 {
				s.reconcilingSettles[id]--
				return activities.OperationRef{Lifecycle: "reconciling", Phase: in.Phase}, nil
			}
			return activities.OperationRef{Lifecycle: in.Outcome, Phase: in.Phase}, nil
		},
		"CloseAttempt": func(_ context.Context, raw json.RawMessage) (activities.CloseResult, error) {
			in := decode(raw)
			id := strings.Split(strings.TrimPrefix(in.Attempt.AttemptID, "att_"), ":")[0]
			id = strings.Split(id, "-")[0]
			s.mu.Lock()
			defer s.mu.Unlock()
			s.closes[id]++
			if s.reconcilingCloses[id] > 0 {
				s.reconcilingCloses[id]--
				return activities.CloseResult{Lifecycle: "reconciling"}, nil
			}
			return activities.CloseResult{Lifecycle: "running"}, nil
		},
		// Preparation.
		"GetPreparation": func(_ context.Context, _ json.RawMessage) (activities.PreparationView, error) { return prepView, nil },
		"AnalyzeRequirements": func(ctx context.Context, raw json.RawMessage) (activities.Analysis, error) {
			in := decode(raw)
			hold := false
			s.set(func() {
				hold = s.holdAnalysis
				if hold {
					s.analysisHeld = true
				}
			})
			if hold {
				for {
					select {
					case <-ctx.Done():
						return activities.Analysis{}, ctx.Err()
					case <-time.After(200 * time.Millisecond):
						activity.RecordHeartbeat(ctx)
					}
				}
			}
			return activities.Analysis{CallID: in.CallID, State: "succeeded", Requirements: reqs}, nil
		},
		"FreezeReferences": func(_ context.Context, _ json.RawMessage) (activities.FrozenReferences, error) {
			return activities.FrozenReferences{BrandDigests: []activities.ContentDigest{{SourceID: "brand_1", Revision: "3", Digest: "sha256:bb"}}}, nil
		},
		"FreezeBrief": func(_ context.Context, _ json.RawMessage) (activities.BriefRef, error) {
			return activities.BriefRef{BriefID: "brf_1"}, nil
		},
		// Recovery.
		"EnumerateInventory": func(_ context.Context, raw json.RawMessage) (activities.ClassProgress, error) {
			var in struct{ Class string }
			_ = json.Unmarshal(raw, &in)
			return activities.ClassProgress{Class: in.Class, Complete: true}, nil
		},
		"ListFindings": func(_ context.Context, raw json.RawMessage) (activities.FindingPage, error) {
			in := decode(raw)
			status := "unresolved"
			s.mu.Lock()
			if s.rounds[in.RunID] >= 1 {
				status = "resolved"
			}
			s.mu.Unlock()
			return activities.FindingPage{Complete: true, Findings: []activities.Finding{{FindingID: "f_1", Class: "model-dispatch", ObligationID: "dsp_1", Status: status}}}, nil
		},
		"ReconcileFinding": func(_ context.Context, raw json.RawMessage) (activities.Finding, error) {
			in := decode(raw)
			return activities.Finding{FindingID: in.FindingID, Class: "model-dispatch", Status: "unresolved"}, nil
		},
		"ReconcileFindings": func(_ context.Context, raw json.RawMessage) (struct {
			Findings []activities.Finding
			Failures map[string]string
		}, error) {
			in := decode(raw)
			out := struct {
				Findings []activities.Finding
				Failures map[string]string
			}{Failures: map[string]string{}}
			for _, id := range in.FindingIDs {
				out.Findings = append(out.Findings, activities.Finding{FindingID: id, Class: "model-dispatch", Status: "unresolved"})
			}
			return out, nil
		},
		"EvaluateRecovery": func(_ context.Context, raw json.RawMessage) (activities.RecoveryStatus, error) {
			var runID string
			if err := json.Unmarshal(raw, &runID); err != nil {
				return activities.RecoveryStatus{}, errors.New("run id")
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			s.rounds[runID]++
			if s.rounds[runID] >= 2 {
				return activities.RecoveryStatus{Phase: "reopened"}, nil
			}
			return activities.RecoveryStatus{Phase: "restricted", Unsettled: 1}, nil
		},
	}
}
