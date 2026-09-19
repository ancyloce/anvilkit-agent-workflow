package workflows

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

const (
	codegenStepID   = "codegen"
	validateStepID  = "validate"
	candidateEffect = 1
)

// generationState is the run's deterministic control record: the hold
// applied by a tracked command, the sends in flight (a Job attempt or a
// registration) that a hold or a change waits out, the lease fence and the
// definition activation in force.
type generationState struct {
	held       bool
	inflight   int
	fenced     bool
	fenceCode  string
	definition string
	lease      activities.LeaseRecord
	occurrence uint64
	done       bool
	// cancelWork ends the in-flight step when the lease fence falls: no
	// new send after a confirmed loss or a passed expiry.
	cancelWork workflow.CancelFunc
	// ordinals counts the attempts opened per step visit, so a step rerun
	// after a hold opens a new attempt under a new command identity.
	ordinals map[string]uint64
}

func (s *generationState) nextOrdinal(stepID string, visit uint64) uint64 {
	if s.ordinals == nil {
		s.ordinals = map[string]uint64{}
	}
	key := fmt.Sprintf("%s:%d", stepID, visit)
	s.ordinals[key]++
	return s.ordinals[key]
}

// Generation runs the Generation lifecycle (DD-01 §4, P13-03/05/06):
// bootstrap (execution permit that sets the active deadline once, source
// scope, funding, lease under stable occurrences, every confirmed result
// reported to Control before authored execution), the codegen Job, the
// independent validator Job, bounded repair on accepted repairable
// findings only, candidate registration under an effect identity, then
// candidate_ready. The protected lease supervisor renews on durable timers
// with short Activities under stable occurrence ids: only a confirmed
// renewal moves the known expiry, an unknown one never does, and a
// confirmed loss or the passing of the known expiry fences new sends for
// good. Hold, resume and change_definition arrive as tracked Updates and
// apply only after the affected senders are quiescent; none of them, and
// no retry or replacement, resets the active deadline, the budget or the
// repair counter.
func Generation(q Queues, b Bounds, lb LifecycleBounds) func(ctx workflow.Context, in Input) error {
	return func(ctx workflow.Context, in Input) (err error) {
		logger := workflow.GetLogger(ctx)
		var bounds Bounds
		if err := workflow.SideEffect(ctx, func(workflow.Context) any { return b }).Get(&bounds); err != nil {
			return err
		}
		var lbounds LifecycleBounds
		if err := workflow.SideEffect(ctx, func(workflow.Context) any { return lb }).Get(&lbounds); err != nil {
			return err
		}
		operationID, tenantID := in.OperationID, in.TenantID
		business := activityOptions(ctx, q, bounds)
		state := &generationState{}

		if err := workflow.SetUpdateHandlerWithOptions(ctx, CommandUpdateName, func(ctx workflow.Context, arg CommandUpdate) (UpdateResult, error) {
			switch arg.Kind {
			case "hold":
				// The fence is installed by Control; the hold applies once
				// no sender of this run is in flight.
				if err := workflow.Await(ctx, func() bool { return state.inflight == 0 || state.done }); err != nil {
					return UpdateResult{Outcome: "blocked", ReasonCode: "CANCELED"}, nil
				}
				if state.done {
					return UpdateResult{Outcome: "rejected", ReasonCode: "TERMINAL"}, nil
				}
				state.held = true
				return UpdateResult{Outcome: "applied"}, nil
			case "resume":
				if !state.held {
					return UpdateResult{Outcome: "rejected", ReasonCode: "NOT_HELD"}, nil
				}
				if state.fenced {
					return UpdateResult{Outcome: "rejected", ReasonCode: state.fenceCode}, nil
				}
				state.held = false
				return UpdateResult{Outcome: "applied"}, nil
			case "change_definition":
				if err := workflow.Await(ctx, func() bool { return state.inflight == 0 || state.done }); err != nil {
					return UpdateResult{Outcome: "blocked", ReasonCode: "CANCELED"}, nil
				}
				if state.done {
					return UpdateResult{Outcome: "rejected", ReasonCode: "TERMINAL"}, nil
				}
				state.definition = arg.TargetDefinitionActivation
				return UpdateResult{Outcome: "applied"}, nil
			}
			return UpdateResult{Outcome: "rejected", ReasonCode: "UNSUPPORTED_BY_PROFILE"}, nil
		}, workflow.UpdateHandlerOptions{Validator: func(ctx workflow.Context, arg CommandUpdate) error {
			switch arg.Kind {
			case "hold", "resume":
				return nil
			case "change_definition":
				// Worker availability: only an activation this worker
				// registered can be applied by it.
				for _, d := range lbounds.Definitions {
					if d.ID == arg.TargetDefinitionActivation {
						return nil
					}
				}
				return errors.New("DEFINITION_UNAVAILABLE")
			}
			return errors.New("UNSUPPORTED_BY_PROFILE")
		}}); err != nil {
			return err
		}
		defer awaitHandlers(ctx)

		var view activities.GenerationView
		if err := workflow.ExecuteActivity(business, activities.NameGetGeneration, activities.GetGenerationInput{OperationID: operationID, TenantID: tenantID}).Get(ctx, &view); err != nil {
			return err
		}
		state.definition = view.DefinitionActivation
		if view.Lease.State == "lost" {
			state.fenced, state.fenceCode = true, "LEASE_LOST"
		}

		outcome, failureCode, phase := "failed", "", ""
		defer func() {
			state.done = true
			disconnected, _ := workflow.NewDisconnectedContext(ctx)
			releaseLease(disconnected, q, bounds, lbounds, operationID, tenantID, view, state)
			if serr := settleOperation(disconnected, q, bounds, operationID, tenantID, operationID+":settle", outcome, failureCode, phase); serr != nil {
				logger.Error("generation settlement refused", "operationId", operationID, "error", serr)
				if err == nil {
					err = serr
				}
			}
		}()

		// 1. Admission: the permit, asked under one command identity until
		// granted or the original queue deadline passes; the first grant
		// sets the active deadline once.
		if code := awaitPermit(ctx, q, bounds, lbounds, operationID, tenantID, view, state); code != "" {
			outcome, failureCode = settleOutcome(ctx, ctx.Err(), code)
			return nil
		}
		// 2. Bootstrap under fixed occurrence identities: scope, funding, lease.
		var scope activities.ScopeDecision
		if err := workflow.ExecuteActivity(business, activities.NameCheckSourceScope, activities.CheckSourceScopeInput{
			OperationID: operationID, TenantID: tenantID, BriefID: view.Brief.BriefID, SubjectDigest: view.SubjectDigest, SourceRevisions: view.SourceRevisions,
		}).Get(ctx, &scope); err != nil {
			outcome, failureCode = settleOutcome(ctx, err, "SOURCE_UNAVAILABLE")
			return nil
		}
		if !scope.Allowed {
			outcome, failureCode, phase = "failed", firstNonEmpty(scope.ReasonCode, "SCOPE_REJECTED"), "scope_rejected"
			return nil
		}
		if err := workflow.ExecuteActivity(business, activities.NameRecordFunding, activities.RecordFundingInput{OperationID: operationID, TenantID: tenantID, CommandID: operationID + ":funding"}).Get(ctx, nil); err != nil {
			outcome, failureCode = settleOutcome(ctx, err, "BUDGET_EXHAUSTED")
			return nil
		}
		if code := acquireLease(ctx, q, bounds, lbounds, operationID, tenantID, view, state); code != "" {
			outcome, failureCode, phase = "failed", code, "lease_unavailable"
			return nil
		}
		supervisorCtx, stopSupervisor := workflow.WithCancel(ctx)
		defer stopSupervisor()
		// The steps run under a context the lease fence cancels; the
		// protected close and settle use disconnected contexts.
		workCtx, cancelWork := workflow.WithCancel(ctx)
		defer cancelWork()
		state.cancelWork = cancelWork
		workflow.Go(supervisorCtx, func(gctx workflow.Context) {
			superviseLease(gctx, q, bounds, lbounds, operationID, tenantID, view, state)
		})

		// 3. Codegen, independent validation, bounded repair.
		repairs := uint64(0)
		var prior *activities.AcceptedStage
		var findings *activities.AcceptedStage
		// certifiedSource is the codegen stage the independent validator
		// certified; certification is the validator's accepted stage.
		var certifiedSource, certification *activities.AcceptedStage
		var lastAttempt activities.AttemptRef
		for {
			if code := awaitRunnable(ctx, state); code != "" {
				outcome, failureCode = settleOutcome(ctx, ctx.Err(), code)
				return nil
			}
			codegenProfile, validatorProfile, maxRepairs := lbounds.definition(state.definition, view)
			inputs := []activities.LaunchInput{{Name: "brief", Digest: view.Brief.Brief.Digest, Handle: view.Brief.Brief.Handle}}
			if prior != nil {
				inputs = append(inputs, stageInputs(prior, "stage", "source")...)
			}
			if findings != nil {
				inputs = append(inputs, stageInputs(findings, "evidence")...)
			}
			stage, attempt, instance, code := runJobStep(workCtx, q, bounds, stepSpec{
				operationID: operationID, tenantID: tenantID, stepID: codegenStepID, visit: repairs, profileID: codegenProfile, launchPrefix: "gen-c", inputs: inputs,
			}, state)
			if state.fenced {
				outcome, failureCode = "failed", state.fenceCode
				return nil
			}
			if state.held && !state.fenced && ctx.Err() == nil {
				// A hold applied while the step ran: its sends were fenced by
				// Control, the attempt is closed with what the evidence
				// supports, and the step reruns after the resume under a new
				// attempt; the repair counter and the deadline stay.
				logger.Info("step ended under a hold; it reruns after the resume", "operationId", operationID, "step", codegenStepID)
				continue
			}
			if code != "" {
				outcome, failureCode = settleOutcome(ctx, ctx.Err(), code)
				return nil
			}
			if stage.Verdict != "certified" {
				// The team's own outcome: an accepted repairable stage from the
				// team is not an independent classification and starts no
				// repair; nothing here regenerates.
				outcome, failureCode = "failed", firstNonEmpty(stage.FailureCode, strings.ToUpper(stage.Verdict))
				return nil
			}
			if code := awaitRunnable(ctx, state); code != "" {
				outcome, failureCode = settleOutcome(ctx, ctx.Err(), code)
				return nil
			}
			validation, vattempt, vinstance, code := runJobStep(workCtx, q, bounds, stepSpec{
				operationID: operationID, tenantID: tenantID, stepID: validateStepID, visit: repairs, profileID: validatorProfile, launchPrefix: "gen-v", inputs: stageInputs(&stage, "source"),
			}, state)
			if state.fenced {
				outcome, failureCode = "failed", state.fenceCode
				return nil
			}
			if state.held && !state.fenced && ctx.Err() == nil {
				logger.Info("validation ended under a hold; the round reruns after the resume", "operationId", operationID)
				continue
			}
			if code != "" {
				outcome, failureCode = settleOutcome(ctx, ctx.Err(), code)
				return nil
			}
			switch validation.Verdict {
			case "certified":
				certifiedSource, certification, lastAttempt = &stage, &validation, vattempt
				_ = vinstance
				_ = attempt
				_ = instance
			case "repairable":
				// Only an accepted, independently classified repairable
				// finding returns to the coder, within the bound; the
				// counter never resets.
				if repairs >= maxRepairs {
					outcome, failureCode, phase = "failed", "REPAIRS_EXHAUSTED", "repairs_exhausted"
					return nil
				}
				repairs++
				prior, findings = &stage, &validation
				logger.Info("independent validator found repairable findings; one bounded repair round", "operationId", operationID, "repairs", repairs)
				continue
			default:
				outcome, failureCode = "failed", firstNonEmpty(validation.FailureCode, strings.ToUpper(validation.Verdict))
				return nil
			}
			break
		}

		// 4. Candidate registration under the effect identity.
		if code := awaitRunnable(ctx, state); code != "" {
			outcome, failureCode = settleOutcome(ctx, ctx.Err(), code)
			return nil
		}
		source, cert := stageArtifact(certifiedSource, "source"), stageArtifact(certification, "evidence")
		if source == nil {
			outcome, failureCode = "failed", "SOURCE_MISSING"
			return nil
		}
		if cert == nil {
			outcome, failureCode = "failed", "CERTIFICATION_MISSING"
			return nil
		}
		// Registration is trusted Workflow execution after the Validator Job
		// closes. It owns a current attempt, never the closed candidate binding.
		var registration activities.AttemptRef
		if openErr := workflow.ExecuteActivity(business, activities.NameOpenAttempt, activities.OpenAttemptInput{
			OperationID: operationID, TenantID: tenantID, CommandID: operationID + ":candidate:1:open",
			StepID: "register_candidate", ProfileID: view.ProfileID,
		}).Get(ctx, &registration); openErr != nil {
			outcome, failureCode = settleOutcome(ctx, openErr, "STALE_EXECUTION")
			return nil
		}
		defer func() {
			disconnected, _ := workflow.NewDisconnectedContext(ctx)
			if closeErr := closeAttempt(disconnected, q, bounds, registration, registration.AttemptID+":close", "completed", "not_required", ""); closeErr != nil {
				err = closeErr
			}
		}()
		epoch, _ := strconv.ParseUint(registration.ExecutionEpoch, 10, 64)
		var candidate activities.CandidateRef
		registrationDeadline := lastAttempt.Deadline
		if state.lease.ExpiresAt != nil && state.lease.ExpiresAt.Before(registrationDeadline) {
			registrationDeadline = *state.lease.ExpiresAt
		}
		state.inflight++
		regErr := workflow.ExecuteActivity(controlOptions(workCtx, q, bounds), activities.NameRegisterCandidate, activities.RegisterCandidateInput{
			OperationID: operationID, TenantID: tenantID, AttemptID: registration.AttemptID, ExecutionEpoch: epoch,
			CommandID: operationID + ":candidate:" + strconv.Itoa(candidateEffect), Occurrence: candidateEffect, Subject: "source:" + view.SubjectDigest, SourceRevision: view.SourceRevision,
			Source: *source, Certification: derefArtifact(cert), Lease: state.lease, Deadline: registrationDeadline,
		}).Get(ctx, &candidate)
		state.inflight--
		if state.fenced {
			outcome, failureCode = "failed", state.fenceCode
			return nil
		}
		if regErr != nil {
			outcome, failureCode = settleOutcome(ctx, regErr, "EFFECT_UNCERTAIN")
			return nil
		}
		switch candidate.State {
		case "succeeded":
			outcome, phase = "succeeded", "candidate_ready"
			logger.Info("candidate registered", "operationId", operationID, "effectId", candidate.EffectID, "candidateId", candidate.CandidateID)
		case "unknown":
			outcome, failureCode, phase = "failed", "EFFECT_UNCERTAIN", "candidate_unknown"
		case "denied":
			outcome, failureCode, phase = "failed", firstNonEmpty(candidate.DenialCode, "EFFECT_DENIED"), "candidate_denied"
		default:
			outcome, failureCode, phase = "failed", "CANDIDATE_REJECTED", "candidate_rejected"
		}
		return nil
	}
}

// awaitPermit asks for the execution permit until granted; the wait is a
// durable timer paced by the poll interval and bounded by the original
// queue deadline, which nothing here extends. A hold or a cancellation
// during the wait ends it.
func awaitPermit(ctx workflow.Context, q Queues, b Bounds, lb LifecycleBounds, operationID, tenantID string, view activities.GenerationView, state *generationState) string {
	business := activityOptions(ctx, q, b)
	for {
		if code := awaitRunnable(ctx, state); code != "" {
			return code
		}
		var answer activities.PermitAnswer
		if err := workflow.ExecuteActivity(business, activities.NameRequestExecutionPermit, activities.RequestExecutionPermitInput{OperationID: operationID, TenantID: tenantID, CommandID: operationID + ":permit"}).Get(ctx, &answer); err != nil {
			if refused := activities.RefusalCode(err); refused != "" {
				return refused
			}
			return "OBSERVER_FAILED"
		}
		if answer.Granted {
			return ""
		}
		remaining := answer.QueueDeadline.Sub(workflow.Now(ctx))
		if remaining <= 0 {
			return "QUEUE_DEADLINE_EXCEEDED"
		}
		wait := lb.PermitPollInterval
		if wait <= 0 {
			wait = 30 * time.Second
		}
		if remaining < wait {
			wait = remaining
		}
		if err := workflow.Sleep(ctx, wait); err != nil {
			return "CANCELED"
		}
	}
}

// awaitRunnable waits out a hold and reports a fence or a cancellation.
func awaitRunnable(ctx workflow.Context, state *generationState) string {
	if err := workflow.Await(ctx, func() bool { return !state.held || state.fenced }); err != nil {
		return "CANCELED"
	}
	if state.fenced {
		return state.fenceCode
	}
	return ""
}

func leaseInput(operationID, tenantID string, view activities.GenerationView, state *generationState, lb LifecycleBounds) activities.LeaseInput {
	return activities.LeaseInput{OperationID: operationID, TenantID: tenantID, Subject: "source:" + tenantID + "/" + view.SubjectDigest, Owner: operationID, Occurrence: state.occurrence, LeaseID: state.lease.LeaseID, Fence: state.lease.Fence, TTL: lb.LeaseTTL}
}

// leaseOptions bound one short lease call; a transport retry reenters the
// same occurrence, which the port answers with the original result.
func leaseOptions(ctx workflow.Context, q Queues, b Bounds, lb LifecycleBounds) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue: q.Control, StartToCloseTimeout: lb.LeaseCallTimeout,
		RetryPolicy: &temporal.RetryPolicy{InitialInterval: b.ControlRetryInitial, BackoffCoefficient: 2, MaximumInterval: b.ControlRetryMaxInterval, MaximumAttempts: b.ControlRetryMaxAttempts},
	})
}

// recordLease reports a confirmed lease fact to Control under the
// occurrence; unknown results are never reported.
func recordLease(ctx workflow.Context, q Queues, b Bounds, operationID, tenantID string, occurrence uint64, res activities.LeaseResult, stateName string) (activities.LeaseRecord, error) {
	var rec activities.LeaseRecord
	err := workflow.ExecuteActivity(controlOptions(ctx, q, b), activities.NameRecordLease, activities.RecordLeaseInput{
		OperationID: operationID, TenantID: tenantID, CommandID: fmt.Sprintf("%s:lease:%d", operationID, occurrence), Occurrence: occurrence, State: stateName, LeaseID: res.LeaseID, Fence: res.Fence, ExpiresAt: res.ExpiresAt,
	}).Get(ctx, &rec)
	return rec, err
}

// acquireLease obtains the lease under occurrence 1 (a reentry reaches the
// same occurrence), resolves an unknown answer by a query, and reports the
// confirmed result to Control before anything authored runs. A lease that
// cannot be confirmed ends the generation before any send.
func acquireLease(ctx workflow.Context, q Queues, b Bounds, lb LifecycleBounds, operationID, tenantID string, view activities.GenerationView, state *generationState) string {
	state.occurrence = 1
	if view.Lease.State == "held" && view.Lease.ExpiresAt != nil && view.Lease.ExpiresAt.After(workflow.Now(ctx)) {
		// A restarted run finds its confirmed lease in Control's record.
		state.lease, state.occurrence = view.Lease, view.Lease.Occurrence
		return ""
	}
	var res activities.LeaseResult
	if err := workflow.ExecuteActivity(leaseOptions(ctx, q, b, lb), activities.NameAcquireLease, leaseInput(operationID, tenantID, view, state, lb)).Get(ctx, &res); err != nil {
		return "LEASE_UNAVAILABLE"
	}
	if res.State == "unknown" {
		var queried activities.LeaseResult
		if err := workflow.ExecuteActivity(leaseOptions(ctx, q, b, lb), activities.NameQueryLease, leaseInput(operationID, tenantID, view, state, lb)).Get(ctx, &queried); err == nil {
			res = queried
		}
	}
	if res.State != "held" {
		return "LEASE_UNAVAILABLE"
	}
	rec, err := recordLease(ctx, q, b, operationID, tenantID, state.occurrence, res, "held")
	if err != nil {
		return firstNonEmpty(activities.RefusalCode(err), "LEASE_UNAVAILABLE")
	}
	state.lease = rec
	return ""
}

// superviseLease is the protected supervisor (DD-01 §4): it sleeps on a
// durable timer until the renewal lead before the known expiry, renews
// under the next occurrence and records only confirmed results. An
// unknown renewal is queried; an answer that stays unknown extends
// nothing, and once the known expiry passes the lease is recorded lost
// and every new send is fenced for good. A confirmed loss fences at once.
func superviseLease(ctx workflow.Context, q Queues, b Bounds, lb LifecycleBounds, operationID, tenantID string, view activities.GenerationView, state *generationState) {
	logger := workflow.GetLogger(ctx)
	fence := func(code string, res activities.LeaseResult) {
		state.fenced, state.fenceCode = true, code
		if state.cancelWork != nil {
			state.cancelWork()
		}
		disconnected, _ := workflow.NewDisconnectedContext(ctx)
		if _, err := recordLease(disconnected, q, b, operationID, tenantID, state.occurrence, res, "lost"); err != nil {
			logger.Error("lease loss not recorded", "operationId", operationID, "error", err)
		}
	}
	for !state.done && !state.fenced {
		known := state.lease.ExpiresAt
		if known == nil {
			fence("LEASE_LOST", activities.LeaseResult{LeaseID: state.lease.LeaseID, Fence: state.lease.Fence})
			return
		}
		wait := known.Sub(workflow.Now(ctx)) - lb.LeaseRenewLead
		if wait > 0 {
			if err := workflow.Sleep(ctx, wait); err != nil {
				return // the run ended; the deferred release settles the lease
			}
		}
		if state.done {
			return
		}
		state.occurrence++
		in := leaseInput(operationID, tenantID, view, state, lb)
		var res activities.LeaseResult
		if err := workflow.ExecuteActivity(leaseOptions(ctx, q, b, lb), activities.NameRenewLease, in).Get(ctx, &res); err != nil {
			if temporal.IsCanceledError(err) || ctx.Err() != nil {
				return
			}
			res = activities.LeaseResult{State: "unknown", Reason: "RENEW_FAILED"}
		}
		if res.State == "unknown" {
			var queried activities.LeaseResult
			if err := workflow.ExecuteActivity(leaseOptions(ctx, q, b, lb), activities.NameQueryLease, in).Get(ctx, &queried); err == nil && queried.State != "unknown" {
				res = queried
			}
		}
		switch res.State {
		case "held":
			rec, err := recordLease(ctx, q, b, operationID, tenantID, state.occurrence, res, "held")
			if err != nil {
				if temporal.IsCanceledError(err) || ctx.Err() != nil {
					return
				}
				fence(firstNonEmpty(activities.RefusalCode(err), "LEASE_LOST"), res)
				return
			}
			state.lease = rec
		case "lost":
			fence("LEASE_LOST", res)
			return
		default:
			// Unknown: the known expiry stands; once it passes without a
			// confirmed renewal the lease is expired for this generation.
			if !workflow.Now(ctx).Before(*known) {
				fence("LEASE_EXPIRED", activities.LeaseResult{LeaseID: state.lease.LeaseID, Fence: state.lease.Fence})
				return
			}
			if err := workflow.Sleep(ctx, minDuration(lb.LeaseRenewLead/2, known.Sub(workflow.Now(ctx)))); err != nil {
				return
			}
		}
	}
}

// releaseLease ends the lease when the run ends: released upstream and
// recorded released with Control (a lost lease stays lost).
func releaseLease(ctx workflow.Context, q Queues, b Bounds, lb LifecycleBounds, operationID, tenantID string, view activities.GenerationView, state *generationState) {
	if state.lease.LeaseID == "" || state.fenced {
		return
	}
	state.occurrence++
	in := leaseInput(operationID, tenantID, view, state, lb)
	var res activities.LeaseResult
	if err := workflow.ExecuteActivity(leaseOptions(ctx, q, b, lb), activities.NameReleaseLease, in).Get(ctx, &res); err != nil {
		res.State = "unknown"
	}
	for res.State != "lost" && res.State != "released" {
		// A timeout or UNKNOWN is not release evidence. Observe the original
		// occurrence through durable history; never issue another release id.
		if err := workflow.Sleep(ctx, b.ReconcileInitialInterval); err != nil {
			return
		}
		if err := workflow.ExecuteActivity(leaseOptions(ctx, q, b, lb), activities.NameQueryLease, in).Get(ctx, &res); err != nil {
			res.State = "unknown"
		}
	}
	confirmed := "lost"
	if res.State == "released" || res.Reason == "RELEASED" {
		confirmed = "released"
	}
	if _, err := recordLease(ctx, q, b, operationID, tenantID, state.occurrence, activities.LeaseResult{LeaseID: state.lease.LeaseID, Fence: state.lease.Fence}, confirmed); err != nil {
		workflow.GetLogger(ctx).Warn("lease release not recorded", "operationId", operationID, "error", err)
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// stepSpec is one Job step of the generation.
type stepSpec struct {
	operationID  string
	tenantID     string
	stepID       string
	visit        uint64
	profileID    string
	launchPrefix string
	inputs       []activities.LaunchInput
}

// launchKeyForAttempt derives the stable DNS-label launch key of an attempt.
func launchKeyForAttempt(prefix, attemptID string) string {
	key := strings.ToLower(strings.TrimPrefix(attemptID, "att_"))
	key = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			return r
		}
		return '-'
	}, key)
	if len(key) > 40 {
		key = key[:40]
	}
	return prefix + "-" + strings.Trim(key, "-")
}

// runJobStep runs one Job attempt of a step the way the LocalCheck fixture
// does (open, launch obligation, marked create requests, observation,
// instance registration, evidence-first cleanup, close) and reads the
// stage the Job's trusted process had Control accept for the attempt. It
// returns the accepted stage, or a code when the step produced none: the
// attempt is closed with the outcome the evidence supports and nothing is
// regenerated for an infrastructure failure, an unknown or an invalid
// input.
func runJobStep(ctx workflow.Context, q Queues, b Bounds, spec stepSpec, state *generationState) (stage activities.AcceptedStage, attempt activities.AttemptRef, current activities.InstanceRef, code string) {
	logger := workflow.GetLogger(ctx)
	business := activityOptions(ctx, q, b)
	ordinal := state.nextOrdinal(spec.stepID, spec.visit)
	if err := workflow.ExecuteActivity(business, activities.NameOpenAttempt, activities.OpenAttemptInput{
		OperationID: spec.operationID, TenantID: spec.tenantID, CommandID: fmt.Sprintf("%s:%s:%d:%d:open", spec.operationID, spec.stepID, spec.visit, ordinal), StepID: spec.stepID, Visit: spec.visit, ProfileID: spec.profileID,
	}).Get(ctx, &attempt); err != nil {
		return stage, attempt, current, firstNonEmpty(activities.RefusalCode(err), "OBSERVER_FAILED")
	}
	state.inflight++
	outcome, cleanup, failureCode := "failed", "not_required", ""
	launched := false
	var launch activities.LaunchRef
	var creates createLedger
	defer func() {
		state.inflight--
		disconnected, _ := workflow.NewDisconnectedContext(ctx)
		if launched {
			cleanup = "unknown"
			if cleanupRound(disconnected, q, b, attempt, launch, &creates) {
				cleanup = "complete"
			}
		}
		if cerr := closeAttempt(disconnected, q, b, attempt, attempt.AttemptID+":close", outcome, cleanup, failureCode); cerr != nil {
			logger.Error("attempt close refused", "attemptId", attempt.AttemptID, "error", cerr)
			if code == "" {
				code = activities.RefusalCode(cerr)
			}
			return
		}
		if cleanup != "unknown" {
			return
		}
		if !reconcileCleanup(disconnected, q, b, attempt, launch, &creates) {
			logger.Error("cleanup reconciliation bound reached; the operation remains reconciling", "launchKey", launch.LaunchKey)
			code = "CLEANUP_UNCONFIRMED"
			return
		}
		if cerr := closeAttempt(disconnected, q, b, attempt, attempt.AttemptID+":close:settled", outcome, "complete", failureCode); cerr != nil {
			logger.Error("cleanup settlement refused", "attemptId", attempt.AttemptID, "error", cerr)
		}
	}()
	if !attempt.Deadline.After(workflow.Now(ctx)) {
		outcome, failureCode = "infrastructure_failed", "DEADLINE_EXCEEDED"
		return stage, attempt, current, "DEADLINE_EXCEEDED"
	}
	deadline := workflow.Now(ctx).Add(b.LaunchWindow)
	if deadline.After(attempt.Deadline) {
		deadline = attempt.Deadline
	}
	// The Job's own deadline covers the profile's run: the launch window
	// bounds the create, the attempt deadline bounds the run.
	jobDeadline := attempt.Deadline
	if err := workflow.ExecuteActivity(business, activities.NamePrepareLaunch, activities.PrepareLaunchInput{
		Attempt: attempt, CommandID: attempt.AttemptID + ":launch", LaunchKey: launchKeyForAttempt(spec.launchPrefix, attempt.AttemptID), Deadline: jobDeadline,
	}).Get(ctx, &launch); err != nil {
		outcome, failureCode = settle(ctx, err, "PROFILE_UNQUALIFIED")
		return stage, attempt, current, failureCode
	}
	launched = true
	if _, err := createJob(ctx, q, b, jobSpec{ProfileID: spec.profileID, OperationID: spec.operationID, ExecutionEpoch: attempt.ExecutionEpoch, Inputs: spec.inputs}, launch, jobDeadline, &creates); err != nil {
		outcome, failureCode = settle(ctx, err, "OBSERVER_FAILED")
		return stage, attempt, current, failureCode
	}
	observeTimeout := jobDeadline.Sub(workflow.Now(ctx)) + time.Minute
	if observeTimeout < time.Minute {
		observeTimeout = time.Minute
	}
	observeCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue: q.Business, StartToCloseTimeout: observeTimeout, HeartbeatTimeout: b.ObserveHeartbeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{InitialInterval: b.ControlRetryInitial, MaximumAttempts: b.ObserveMaxAttempts},
	})
	var observation activities.JobObservation
	if err := workflow.ExecuteActivity(observeCtx, activities.NameObserveJob, activities.ObserveJobInput{Launch: launch, Deadline: jobDeadline}).Get(ctx, &observation); err != nil {
		outcome, failureCode = settle(ctx, err, "OBSERVER_FAILED")
		return stage, attempt, current, failureCode
	}
	for i, pod := range observation.Pods {
		var inst activities.InstanceRef
		if err := workflow.ExecuteActivity(business, activities.NameRegisterInstance, activities.RegisterInstanceInput{
			Attempt: attempt, Launch: launch, CommandID: attempt.AttemptID + ":register:" + pod.PodUID, JobUID: observation.JobUID, PodUID: pod.PodUID,
		}).Get(ctx, &inst); err != nil {
			outcome, failureCode = settle(ctx, err, "OBSERVER_FAILED")
			return stage, attempt, current, failureCode
		}
		if err := workflow.ExecuteActivity(business, activities.NameObserveInstance, activities.ObserveInstanceInput{Attempt: attempt, Instance: inst, Observation: pod}).Get(ctx, nil); err != nil {
			outcome, failureCode = settle(ctx, err, "OBSERVER_FAILED")
			return stage, attempt, current, failureCode
		}
		if i == 0 {
			current = inst
		}
	}
	if len(observation.Pods) == 0 || !current.Current {
		outcome, failureCode = "infrastructure_failed", "OBSERVER_FAILED"
		if observation.Reason == "DeadlineExceeded" {
			failureCode = "DEADLINE_EXCEEDED"
		}
		return stage, attempt, current, failureCode
	}
	// The stage the Job's trusted process had Control accept: Control's
	// record is the only evidence; the Pod's exit code and message never
	// certify (DD-04 §3).
	if err := workflow.ExecuteActivity(business, activities.NameGetAcceptedStage, activities.GetAcceptedStageInput{AttemptID: attempt.AttemptID, TenantID: spec.tenantID, OperationID: spec.operationID}).Get(ctx, &stage); err != nil {
		outcome, failureCode = settle(ctx, err, "OBSERVER_FAILED")
		return stage, attempt, current, failureCode
	}
	if !stage.Found {
		outcome, failureCode = "infrastructure_failed", "RESULT_MISSING"
		if observation.Reason == "DeadlineExceeded" {
			failureCode = "DEADLINE_EXCEEDED"
		}
		return stage, attempt, current, failureCode
	}
	switch stage.Verdict {
	case "certified", "repairable", "invalid":
		outcome = "completed"
		if stage.Verdict != "certified" {
			outcome = "failed"
		}
	case "canceled":
		outcome, failureCode = "canceled", "CANCELED"
	default:
		outcome, failureCode = "infrastructure_failed", firstNonEmpty(stage.FailureCode, "OBSERVER_FAILED")
	}
	return stage, attempt, current, ""
}

// stageInputs names the artifacts of the classes an accepted stage binds
// as typed launch inputs (name = class).
func stageInputs(stage *activities.AcceptedStage, classes ...string) []activities.LaunchInput {
	var out []activities.LaunchInput
	for _, class := range classes {
		if a := stageArtifact(stage, class); a != nil {
			out = append(out, activities.LaunchInput{Name: class, Digest: a.Digest, Handle: a.Handle})
		}
	}
	return out
}

func stageArtifact(stage *activities.AcceptedStage, class string) *activities.StageArtifact {
	if stage == nil {
		return nil
	}
	for i := range stage.Artifacts {
		if stage.Artifacts[i].Class == class {
			return &stage.Artifacts[i]
		}
	}
	return nil
}

func derefArtifact(a *activities.StageArtifact) activities.StageArtifact {
	if a == nil {
		return activities.StageArtifact{}
	}
	return *a
}
