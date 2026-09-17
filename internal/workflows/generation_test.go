package workflows_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/workflows"
)

var (
	genInput = workflows.Input{OperationID: "op_GEN", TenantID: "tenant_a"}
	brief    = activities.BriefRef{BriefID: "brf_1", Brief: activities.ArtifactBinding{TransferID: "xfer_brief", Digest: "sha256:b1", Handle: "h_brief"}, State: "current"}
	sourceA  = activities.StageArtifact{Handle: "h_src1", Class: "source", Digest: "sha256:s1", SizeBytes: "10", TransferID: "xfer_s1", ObjectVersion: "v1"}
	stageA   = activities.StageArtifact{Handle: "h_stg1", Class: "stage", Digest: "sha256:t1", SizeBytes: "10", TransferID: "xfer_t1", ObjectVersion: "v1"}
	evidence = activities.StageArtifact{Handle: "h_ev1", Class: "evidence", Digest: "sha256:e1", SizeBytes: "10", TransferID: "xfer_e1", ObjectVersion: "v1"}
)

// genScript scripts one generation run: the permit answers in order, the
// verdicts of the codegen and validator steps per visit, the lease port's
// answers per occurrence and what the registration answers.
type genScript struct {
	// createNotCreated makes the first n create requests of every step
	// answer NotCreated, so the step waits on the run's durable retry
	// timers (a step in flight while the lease clock runs).
	createNotCreated int
	permitErr        error
	permits          []bool
	codegen          map[uint64]activities.AcceptedStage // by visit
	validator        map[uint64]activities.AcceptedStage
	scope            activities.ScopeDecision
	lease            map[string]activities.LeaseResult // "acquire", "renew:<occ>", "query", "release"
	candidate        activities.CandidateRef
	definition       string
	leaseState       activities.LeaseRecord
}

// genRecord is what a run issued.
type genRecord struct {
	settled
	permits   []string
	fundings  []string
	leases    []activities.RecordLeaseInput
	opens     []activities.OpenAttemptInput
	creates   []activities.CreateJobInput
	renewals  []uint64
	registers []activities.RegisterCandidateInput
}

func (r *genRecord) add(f func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f()
}

func held(exp time.Time) activities.LeaseResult {
	return activities.LeaseResult{State: "held", LeaseID: "lease_1", Fence: "1", ExpiresAt: &exp}
}

func certified(attempt string, artifacts ...activities.StageArtifact) activities.AcceptedStage {
	return activities.AcceptedStage{Found: true, StageID: "stg_" + attempt, AttemptID: attempt, Verdict: "certified", ExecutionEpoch: "1", Artifacts: artifacts}
}

func generationEnv(t *testing.T, lb workflows.LifecycleBounds, script genScript) (*testsuite.TestWorkflowEnvironment, *genRecord) {
	return generationEnvWith(t, bounds, lb, script)
}

func generationEnvWith(t *testing.T, b workflows.Bounds, lb workflows.LifecycleBounds, script genScript) (*testsuite.TestWorkflowEnvironment, *genRecord) {
	t.Helper()
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	registerWith(env, b)
	var a lifecycleStub
	env.RegisterWorkflowWithOptions(workflows.Generation(queues, b, lb), workflow.RegisterOptions{Name: workflows.GenerationWorkflowName})
	for name, fn := range map[string]any{
		activities.NameSettleOperation: a.SettleOperation, activities.NameRecordFunding: a.RecordFunding,
		activities.NameGetGeneration: a.GetGeneration, activities.NameRequestExecutionPermit: a.RequestExecutionPermit, activities.NameCheckSourceScope: a.CheckSourceScope,
		activities.NameAcquireLease: a.AcquireLease, activities.NameRenewLease: a.RenewLease, activities.NameQueryLease: a.QueryLease, activities.NameReleaseLease: a.ReleaseLease,
		activities.NameRecordLease: a.RecordLease, activities.NameGetAcceptedStage: a.GetAcceptedStage, activities.NameRegisterCandidate: a.RegisterCandidate,
	} {
		env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
	r := &genRecord{}
	r.record(env)
	definition := script.definition
	if definition == "" {
		definition = "generation-v1:def-1"
	}
	env.OnActivity(activities.NameGetGeneration, mock.Anything, mock.Anything).Return(activities.GenerationView{
		SubjectDigest: "sha256:subject", Brief: brief, QueueDeadline: env.Now().Add(2 * time.Hour), DefinitionActivation: definition, MaxRepairs: 1,
		CodegenProfileID: "codegen-team-dev-v1", ValidatorProfileID: "validator-fixed-dev-v1", Lease: script.leaseState,
	}, nil)
	permits := append([]bool(nil), script.permits...)
	env.OnActivity(activities.NameRequestExecutionPermit, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.RequestExecutionPermitInput) (activities.PermitAnswer, error) {
		r.add(func() { r.permits = append(r.permits, in.CommandID) })
		if script.permitErr != nil {
			return activities.PermitAnswer{}, script.permitErr
		}
		granted := true
		if len(permits) > 0 {
			granted, permits = permits[0], permits[1:]
		}
		deadline := env.Now().Add(time.Hour)
		return activities.PermitAnswer{Granted: granted, PermitID: "prm_1", ActiveDeadline: &deadline, QueueDeadline: env.Now().Add(2 * time.Hour)}, nil
	})
	env.OnActivity(activities.NameCheckSourceScope, mock.Anything, mock.Anything).Return(script.scope, nil)
	env.OnActivity(activities.NameRecordFunding, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.RecordFundingInput) (activities.FundingRef, error) {
		r.add(func() { r.fundings = append(r.fundings, in.CommandID) })
		return activities.FundingRef{Currency: "USD", Amount: "1000000"}, nil
	})
	leaseAnswer := func(kind string) func(_ context.Context, in activities.LeaseInput) (activities.LeaseResult, error) {
		return func(_ context.Context, in activities.LeaseInput) (activities.LeaseResult, error) {
			if kind == "renew" {
				r.add(func() { r.renewals = append(r.renewals, in.Occurrence) })
				if res, ok := script.lease["renew:"+itoa(in.Occurrence)]; ok {
					return res, nil
				}
				if res, ok := script.lease["renew:*"]; ok {
					return res, nil
				}
				return held(env.Now().Add(lb.LeaseTTL)), nil
			}
			if res, ok := script.lease[kind]; ok {
				return res, nil
			}
			if kind == "release" {
				return activities.LeaseResult{State: "lost", Reason: "RELEASED"}, nil
			}
			return held(env.Now().Add(lb.LeaseTTL)), nil
		}
	}
	env.OnActivity(activities.NameAcquireLease, mock.Anything, mock.Anything).Return(leaseAnswer("acquire"))
	env.OnActivity(activities.NameRenewLease, mock.Anything, mock.Anything).Return(leaseAnswer("renew"))
	env.OnActivity(activities.NameQueryLease, mock.Anything, mock.Anything).Return(leaseAnswer("query"))
	env.OnActivity(activities.NameReleaseLease, mock.Anything, mock.Anything).Return(leaseAnswer("release"))
	env.OnActivity(activities.NameRecordLease, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.RecordLeaseInput) (activities.LeaseRecord, error) {
		r.add(func() { r.leases = append(r.leases, in) })
		return activities.LeaseRecord{State: in.State, LeaseID: in.LeaseID, Fence: in.Fence, ExpiresAt: in.ExpiresAt, Occurrence: in.Occurrence}, nil
	})
	env.OnActivity(activities.NameOpenAttempt, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.OpenAttemptInput) (activities.AttemptRef, error) {
		r.add(func() { r.opens = append(r.opens, in) })
		return activities.AttemptRef{AttemptID: "att_" + in.CommandID, TenantID: in.TenantID, ProfileID: in.ProfileID, Deadline: env.Now().Add(time.Hour), ExecutionEpoch: "1"}, nil
	})
	env.OnActivity(activities.NamePrepareLaunch, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.PrepareLaunchInput) (activities.LaunchRef, error) {
		return activities.LaunchRef{LaunchID: "lch_" + in.Attempt.AttemptID, AttemptID: in.Attempt.AttemptID, LaunchKey: in.LaunchKey, ImageDigest: "sha256:img", LaunchEpoch: "1"}, nil
	})
	env.OnActivity(activities.NameCreateJob, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.CreateJobInput) (activities.JobRef, error) {
		r.add(func() { r.creates = append(r.creates, in) })
		if script.createNotCreated > 0 && in.Request <= script.createNotCreated {
			return activities.JobRef{}, activities.NotCreated(errors.New("admission webhook refused the create"))
		}
		return activities.JobRef{JobUID: "job-" + in.Launch.AttemptID, Request: in.Request}, nil
	})
	exit := int32(0)
	env.OnActivity(activities.NameObserveJob, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.ObserveJobInput) (activities.JobObservation, error) {
		return activities.JobObservation{JobUID: "job-" + in.Launch.AttemptID, Pods: []activities.PodObservation{{PodUID: "pod-" + in.Launch.AttemptID, Phase: "succeeded", ExitCode: &exit}}}, nil
	})
	env.OnActivity(activities.NameRegisterInstance, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.RegisterInstanceInput) (activities.InstanceRef, error) {
		return activities.InstanceRef{InstanceID: "inst-" + in.Attempt.AttemptID, Current: true}, nil
	})
	env.OnActivity(activities.NameObserveInstance, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(activities.NameDeleteJob, mock.Anything, mock.Anything).Return(activities.LaunchObservation{}, nil)
	env.OnActivity(activities.NameGetAcceptedStage, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.GetAcceptedStageInput) (activities.AcceptedStage, error) {
		// The attempt id carries the step, visit and ordinal of its open command.
		parts := strings.Split(strings.TrimPrefix(in.AttemptID, "att_op_GEN:"), ":")
		visit := parseU(parts[1])
		var st activities.AcceptedStage
		var ok bool
		if parts[0] == "codegen" {
			st, ok = script.codegen[visit]
		} else {
			st, ok = script.validator[visit]
		}
		if !ok {
			return activities.AcceptedStage{Found: false}, nil
		}
		st.AttemptID = in.AttemptID
		return st, nil
	})
	env.OnActivity(activities.NameRegisterCandidate, mock.Anything, mock.Anything).Return(func(_ context.Context, in activities.RegisterCandidateInput) (activities.CandidateRef, error) {
		r.add(func() { r.registers = append(r.registers, in) })
		return script.candidate, nil
	})
	return env, r
}

func itoa(v uint64) string { return strings.TrimLeft(strings.Repeat("0", 0)+uint64String(v), "") }

func uint64String(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b []byte
	for v > 0 {
		b = append([]byte{byte('0' + v%10)}, b...)
		v /= 10
	}
	return string(b)
}

func parseU(s string) uint64 {
	var v uint64
	for _, c := range s {
		v = v*10 + uint64(c-'0')
	}
	return v
}

func TestGenerationCandidateReadyAfterIndependentCertification(t *testing.T) {
	env, r := generationEnv(t, lifecycleBounds, genScript{
		permits: []bool{false, false, true}, scope: activities.ScopeDecision{Allowed: true},
		codegen:   map[uint64]activities.AcceptedStage{0: certified("c0", sourceA, stageA)},
		validator: map[uint64]activities.AcceptedStage{0: certified("v0", evidence)},
		candidate: activities.CandidateRef{EffectID: "eff_1", State: "succeeded", CandidateID: "cand_1"},
	})
	env.ExecuteWorkflow(workflows.GenerationWorkflowName, genInput)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "succeeded", r.last().Outcome)
	require.Equal(t, "candidate_ready", r.last().Phase)
	require.Equal(t, []string{"op_GEN:permit", "op_GEN:permit", "op_GEN:permit"}, r.permits, "the queue wait asks under one command identity")
	require.Equal(t, []string{"op_GEN:funding"}, r.fundings)
	require.Len(t, r.opens, 2)
	require.Equal(t, "codegen", r.opens[0].StepID)
	require.Equal(t, "codegen-team-dev-v1", r.opens[0].ProfileID)
	require.Equal(t, "validate", r.opens[1].StepID)
	require.Equal(t, "validator-fixed-dev-v1", r.opens[1].ProfileID)
	require.Len(t, r.creates, 2)
	require.Equal(t, []activities.LaunchInput{{Name: "brief", Digest: "sha256:b1", Handle: "h_brief"}}, r.creates[0].Inputs, "the codegen Job receives the brief by handle")
	require.Equal(t, []activities.LaunchInput{{Name: "source", Digest: "sha256:s1", Handle: "h_src1"}}, r.creates[1].Inputs, "the validator receives the accepted source by handle")
	require.Equal(t, "1", r.creates[0].LaunchEpoch)
	require.Len(t, r.registers, 1)
	require.Equal(t, "op_GEN:candidate:1", r.registers[0].CommandID)
	require.Equal(t, uint64(1), r.registers[0].Occurrence)
	require.Equal(t, "h_src1", r.registers[0].Source.Handle)
	require.Len(t, r.leases, 2, "acquire and release, each a confirmed fact under its occurrence")
	require.Equal(t, uint64(1), r.leases[0].Occurrence)
	require.Equal(t, "held", r.leases[0].State)
	require.Equal(t, "released", r.leases[1].State)
	require.Equal(t, "op_GEN:lease:1", r.leases[0].CommandID)
}

func TestGenerationRejectedScopeStopsBeforeFundingAndLease(t *testing.T) {
	env, r := generationEnv(t, lifecycleBounds, genScript{scope: activities.ScopeDecision{Allowed: false, ReasonCode: "SCOPE_DENIED"}})
	env.ExecuteWorkflow(workflows.GenerationWorkflowName, genInput)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "failed", r.last().Outcome)
	require.Equal(t, "SCOPE_DENIED", r.last().FailureCode)
	require.Empty(t, r.fundings)
	require.Empty(t, r.leases)
	require.Empty(t, r.opens)
}

func TestGenerationStaleBriefRefusedAtAdmission(t *testing.T) {
	env, r := generationEnv(t, lifecycleBounds, genScript{scope: activities.ScopeDecision{Allowed: true}, permitErr: activities.Refused("STALE_EXECUTION", errors.New("brief brf_1 is superseded"))})
	env.ExecuteWorkflow(workflows.GenerationWorkflowName, genInput)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "failed", r.last().Outcome)
	require.Equal(t, "STALE_EXECUTION", r.last().FailureCode)
	require.Empty(t, r.opens)
	require.Empty(t, r.leases)
}

func TestGenerationBoundedRepairOnAcceptedRepairableFindings(t *testing.T) {
	repairable := activities.AcceptedStage{Found: true, StageID: "stg_v0", Verdict: "repairable", FailureCode: "CANDIDATE_TEST_FAILED", ExecutionEpoch: "1", Artifacts: []activities.StageArtifact{evidence}}
	env, r := generationEnv(t, lifecycleBounds, genScript{
		scope:     activities.ScopeDecision{Allowed: true},
		codegen:   map[uint64]activities.AcceptedStage{0: certified("c0", sourceA, stageA), 1: certified("c1", sourceA, stageA)},
		validator: map[uint64]activities.AcceptedStage{0: repairable, 1: certified("v1", evidence)},
		candidate: activities.CandidateRef{EffectID: "eff_1", State: "succeeded", CandidateID: "cand_1"},
	})
	env.ExecuteWorkflow(workflows.GenerationWorkflowName, genInput)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "succeeded", r.last().Outcome)
	require.Len(t, r.opens, 4, "codegen, validate, one repair round, validate")
	require.Equal(t, uint64(1), r.opens[2].Visit)
	require.Equal(t, "op_GEN:codegen:1:1:open", r.opens[2].CommandID)
	names := func(in []activities.LaunchInput) []string {
		var out []string
		for _, i := range in {
			out = append(out, i.Name+"="+i.Handle)
		}
		return out
	}
	require.Equal(t, []string{"brief=h_brief", "stage=h_stg1", "source=h_src1", "evidence=h_ev1"}, names(r.creates[2].Inputs), "the repair round receives the prior accepted stage and the validator's findings by handle")
}

func TestGenerationRepairBoundNeverResets(t *testing.T) {
	repairable := activities.AcceptedStage{Found: true, StageID: "stg_v", Verdict: "repairable", FailureCode: "CANDIDATE_TEST_FAILED", ExecutionEpoch: "1", Artifacts: []activities.StageArtifact{evidence}}
	env, r := generationEnv(t, lifecycleBounds, genScript{
		scope:     activities.ScopeDecision{Allowed: true},
		codegen:   map[uint64]activities.AcceptedStage{0: certified("c0", sourceA, stageA), 1: certified("c1", sourceA, stageA), 2: certified("c2", sourceA, stageA)},
		validator: map[uint64]activities.AcceptedStage{0: repairable, 1: repairable, 2: certified("v2", evidence)},
	})
	env.ExecuteWorkflow(workflows.GenerationWorkflowName, genInput)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "failed", r.last().Outcome)
	require.Equal(t, "REPAIRS_EXHAUSTED", r.last().FailureCode)
	require.Len(t, r.opens, 4, "one repair round only; the second repairable finding ends the generation")
	require.Empty(t, r.registers)
}

func TestGenerationDoesNotRegenerateOnInfrastructureOrTeamFindings(t *testing.T) {
	for name, script := range map[string]genScript{
		"team repairable is no independent classification": {codegen: map[uint64]activities.AcceptedStage{0: {Found: true, StageID: "s", Verdict: "repairable", FailureCode: "CANDIDATE_TEST_FAILED", ExecutionEpoch: "1"}}},
		"infrastructure failure":                           {codegen: map[uint64]activities.AcceptedStage{0: {Found: true, StageID: "s", Verdict: "infrastructure_failed", FailureCode: "OBSERVER_FAILED", ExecutionEpoch: "1"}}},
		"no accepted result":                               {codegen: map[uint64]activities.AcceptedStage{}},
		"invalid input":                                    {codegen: map[uint64]activities.AcceptedStage{0: certified("c0", sourceA, stageA)}, validator: map[uint64]activities.AcceptedStage{0: {Found: true, StageID: "v", Verdict: "invalid", FailureCode: "PATH_ESCAPE", ExecutionEpoch: "1"}}},
	} {
		t.Run(name, func(t *testing.T) {
			script.scope = activities.ScopeDecision{Allowed: true}
			env, r := generationEnv(t, lifecycleBounds, script)
			env.ExecuteWorkflow(workflows.GenerationWorkflowName, genInput)
			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())
			require.Equal(t, "failed", r.last().Outcome)
			require.LessOrEqual(t, len(r.opens), 2, "nothing is regenerated")
			require.Empty(t, r.registers)
		})
	}
}

func TestGenerationLostRegistrationReceiptIsQueried(t *testing.T) {
	// The Activity's own path: the registration answer is lost once; the
	// retry reenters PrepareEffect (not permitted: the permit is consumed),
	// queries the original identity and observes it. One physical
	// registration, one observation from the query.
	ports := &fakeGenerationPorts{}
	acts := &activities.LifecycleActivities{Generation: ports, Source: ports}
	in := activities.RegisterCandidateInput{OperationID: "op_GEN", TenantID: "tenant_a", AttemptID: "att_v", CommandID: "op_GEN:candidate:1", Occurrence: 1, Subject: "source:x", Source: sourceA}
	ports.registerErr = errors.New("connection reset")
	_, err := acts.RegisterCandidate(context.Background(), in)
	require.Error(t, err, "the lost answer is reported, never assumed")
	ports.registerErr = nil
	ref, err := acts.RegisterCandidate(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, "succeeded", ref.State)
	require.Equal(t, 1, ports.registrations, "the original effect was queried, not registered again")
	require.Equal(t, 1, ports.queries)
	require.Equal(t, []string{"workflow-query"}, ports.observed)
}

// slowBounds make the create requests of a step wait minutes between
// attempts, so a step is in flight while the lease supervisor runs.
var slowBounds = func() workflows.Bounds {
	b := bounds
	b.ControlRetryInitial, b.ControlRetryMaxInterval, b.ControlRetryMaxAttempts = 2*time.Minute, 4*time.Minute, 6
	return b
}()

func TestGenerationLeaseSupervision(t *testing.T) {
	t.Run("an unknown renewal never extends known validity and the passed expiry fences the step in flight", func(t *testing.T) {
		env, r := generationEnvWith(t, slowBounds, lifecycleBounds, genScript{
			scope: activities.ScopeDecision{Allowed: true}, createNotCreated: 5,
			lease:   map[string]activities.LeaseResult{"renew:*": {State: "unknown"}, "query": {State: "unknown"}},
			codegen: map[uint64]activities.AcceptedStage{0: certified("c0", sourceA, stageA)}, validator: map[uint64]activities.AcceptedStage{0: certified("v0", evidence)},
		})
		env.ExecuteWorkflow(workflows.GenerationWorkflowName, genInput)
		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError())
		require.Equal(t, "failed", r.last().Outcome)
		require.Equal(t, "LEASE_EXPIRED", r.last().FailureCode)
		require.NotEmpty(t, r.renewals, "the supervisor renewed under stable occurrences")
		var states []string
		for _, l := range r.leases {
			states = append(states, l.State)
		}
		require.Equal(t, "held", states[0])
		require.Equal(t, "lost", states[len(states)-1], "the passed expiry is recorded as a loss under its occurrence")
		require.NotContains(t, states[1:], "held", "no unknown renewal extended the known validity")
		require.Len(t, r.opens, 1, "the fenced step ended; no validation, no registration")
		require.Empty(t, r.registers)
		require.Less(t, len(r.creates), 6, "the step's remaining create requests were fenced")
	})
	t.Run("confirmed renewals under stable occurrences extend validity while a step is in flight", func(t *testing.T) {
		env, r := generationEnvWith(t, slowBounds, lifecycleBounds, genScript{
			scope: activities.ScopeDecision{Allowed: true}, createNotCreated: 5,
			codegen: map[uint64]activities.AcceptedStage{0: certified("c0", sourceA, stageA)}, validator: map[uint64]activities.AcceptedStage{0: certified("v0", evidence)},
			candidate: activities.CandidateRef{EffectID: "eff_1", State: "succeeded", CandidateID: "cand_1"},
		})
		env.ExecuteWorkflow(workflows.GenerationWorkflowName, genInput)
		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError())
		require.Equal(t, "succeeded", r.last().Outcome, "the step's sixth create request was answered; the run completed under the renewed lease")
		require.GreaterOrEqual(t, len(r.renewals), 2, "one renewal per lead window while the steps ran")
		for i, occ := range r.renewals {
			require.Equal(t, uint64(i+2), occ, "each renewal carries the next occurrence")
		}
		var recorded []string
		for _, l := range r.leases {
			recorded = append(recorded, l.State+":"+uint64String(l.Occurrence))
		}
		expected := []string{"held:1"}
		for _, occ := range r.renewals {
			expected = append(expected, "held:"+uint64String(occ))
		}
		expected = append(expected, "released:"+uint64String(uint64(len(r.renewals)+2)))
		require.Equal(t, expected, recorded, "every confirmed renewal is recorded under its occurrence; the release follows")
		require.Len(t, r.creates, 12, "five refused create requests then the sixth answered, for the codegen and the validator step alike")
	})
	t.Run("a lease that cannot be confirmed ends the generation before any send", func(t *testing.T) {
		env, r := generationEnv(t, lifecycleBounds, genScript{
			scope: activities.ScopeDecision{Allowed: true},
			lease: map[string]activities.LeaseResult{"acquire": {State: "lost", Reason: "HELD_BY_ANOTHER_OWNER"}, "query": {State: "lost"}},
		})
		env.ExecuteWorkflow(workflows.GenerationWorkflowName, genInput)
		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError())
		require.Equal(t, "failed", r.last().Outcome)
		require.Equal(t, "LEASE_UNAVAILABLE", r.last().FailureCode)
		require.Empty(t, r.leases, "an unconfirmed lease is never recorded")
		require.Empty(t, r.opens)
	})
}

func TestGenerationDefinitionChange(t *testing.T) {
	repairable := activities.AcceptedStage{Found: true, StageID: "stg_v", Verdict: "repairable", FailureCode: "CANDIDATE_TEST_FAILED", ExecutionEpoch: "1", Artifacts: []activities.StageArtifact{evidence}}
	env, r := generationEnv(t, lifecycleBounds, genScript{
		permits: []bool{false, true}, scope: activities.ScopeDecision{Allowed: true},
		codegen:   map[uint64]activities.AcceptedStage{0: certified("c0", sourceA, stageA), 1: certified("c1", sourceA, stageA)},
		validator: map[uint64]activities.AcceptedStage{0: repairable, 1: certified("v1", evidence)},
	})
	var unknown, applied workflows.UpdateResult
	var rejected error
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflowNoRejection(workflows.CommandUpdateName, "hold_1", t, workflows.CommandUpdate{CommandID: "hold_1", Kind: "hold", ExpectedRevision: "3"})
		env.UpdateWorkflow(workflows.CommandUpdateName, "chg_unknown", &testsuite.TestUpdateCallback{
			OnAccept: func() {}, OnReject: func(err error) { rejected = err }, OnComplete: func(result interface{}, err error) { unknown = result.(workflows.UpdateResult) },
		}, workflows.CommandUpdate{CommandID: "chg_unknown", Kind: "change_definition", ExpectedRevision: "4", TargetDefinitionActivation: "generation-v1:def-9"})
		env.UpdateWorkflow(workflows.CommandUpdateName, "chg_1", &testsuite.TestUpdateCallback{
			OnAccept: func() {}, OnReject: func(err error) { t.Errorf("change rejected: %v", err) }, OnComplete: func(result interface{}, err error) { applied = result.(workflows.UpdateResult) },
		}, workflows.CommandUpdate{CommandID: "chg_1", Kind: "change_definition", ExpectedRevision: "4", TargetDefinitionActivation: "generation-v1:def-2"})
		env.UpdateWorkflowNoRejection(workflows.CommandUpdateName, "resume_1", t, workflows.CommandUpdate{CommandID: "resume_1", Kind: "resume", ExpectedRevision: "5"})
	}, time.Second)
	env.ExecuteWorkflow(workflows.GenerationWorkflowName, genInput)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Error(t, rejected, "an activation this worker does not register is rejected by the validator (worker availability)")
	require.Contains(t, rejected.Error(), "DEFINITION_UNAVAILABLE")
	require.Equal(t, workflows.UpdateResult{}, unknown)
	require.Equal(t, "applied", applied.Outcome)
	require.Equal(t, "failed", r.last().Outcome)
	require.Equal(t, "REPAIRS_EXHAUSTED", r.last().FailureCode, "def-2 allows no repair round: the accepted repairable finding ends the generation")
	require.Len(t, r.opens, 2)
}

// fakeGenerationPorts is the Control and Source double of the registration
// Activity: the permit is consumed once; the registration answer can be
// lost after the upstream recorded it.
type fakeGenerationPorts struct {
	permitted     bool
	registrations int
	queries       int
	observed      []string
	registerErr   error
}

func (f *fakeGenerationPorts) GetGeneration(context.Context, activities.GetGenerationInput) (activities.GenerationView, error) {
	return activities.GenerationView{}, nil
}
func (f *fakeGenerationPorts) RequestExecutionPermit(context.Context, activities.RequestExecutionPermitInput) (activities.PermitAnswer, error) {
	return activities.PermitAnswer{}, nil
}
func (f *fakeGenerationPorts) RecordFunding(context.Context, activities.RecordFundingInput) (activities.FundingRef, error) {
	return activities.FundingRef{}, nil
}
func (f *fakeGenerationPorts) RecordLease(context.Context, activities.RecordLeaseInput) (activities.LeaseRecord, error) {
	return activities.LeaseRecord{}, nil
}
func (f *fakeGenerationPorts) GetAcceptedStage(context.Context, activities.GetAcceptedStageInput) (activities.AcceptedStage, error) {
	return activities.AcceptedStage{}, nil
}
func (f *fakeGenerationPorts) PrepareEffect(context.Context, activities.RegisterCandidateInput) (activities.EffectPermit, error) {
	if f.permitted {
		return activities.EffectPermit{EffectID: "eff_1", Permitted: false, State: "permitted"}, nil
	}
	f.permitted = true
	return activities.EffectPermit{EffectID: "eff_1", Permitted: true, State: "permitted"}, nil
}
func (f *fakeGenerationPorts) ObserveEffect(_ context.Context, _, _, source string, _ uint64, _, _, _ string, _ time.Time) error {
	f.observed = append(f.observed, source)
	return nil
}
func (f *fakeGenerationPorts) GetEffect(context.Context, string, string) (activities.EffectPermit, error) {
	return activities.EffectPermit{EffectID: "eff_1", State: "permitted"}, nil
}
func (f *fakeGenerationPorts) CheckScope(context.Context, activities.CheckSourceScopeInput) (activities.ScopeDecision, error) {
	return activities.ScopeDecision{Allowed: true}, nil
}
func (f *fakeGenerationPorts) RegisterCandidate(_ context.Context, effectID string, _ activities.RegisterCandidateInput) (activities.CandidateRef, error) {
	f.registrations++
	if f.registerErr != nil {
		return activities.CandidateRef{}, f.registerErr
	}
	return activities.CandidateRef{EffectID: effectID, State: "succeeded", CandidateID: "cand_1"}, nil
}
func (f *fakeGenerationPorts) QueryRegistration(context.Context, string) (activities.CandidateRef, bool, error) {
	f.queries++
	return activities.CandidateRef{EffectID: "eff_1", State: "succeeded", CandidateID: "cand_1"}, true, nil
}

var _ sync.Locker = (*sync.Mutex)(nil)
