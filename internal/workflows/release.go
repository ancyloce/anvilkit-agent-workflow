package workflows

import (
	"strconv"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// Release effect occurrences: one review registration, the npm target and
// the browser/CSS target (separate identities, never merged), one
// activation.
const (
	releaseCertifyStep    = "certify"
	releaseReviewEffect   = 1
	releaseNpmEffect      = 1
	releaseBrowserEffect  = 2
	releaseActivateEffect = 1
)

// ReleaseCarry is what a release carries into the run it continues as
// during the long approval wait (DD-01 §6): the projection revision, the
// binding, the exact subject and its artifacts, the review identity and
// the absolute approval deadline. Nothing here is reset by the rollover.
type ReleaseCarry struct {
	Revision         uint64                     `json:"revision"`
	Binding          activities.ReleaseBinding  `json:"binding"`
	Built            activities.BuiltSubject    `json:"built"`
	ReleaseID        string                     `json:"releaseId"`
	ReviewEffectID   string                     `json:"reviewEffectId"`
	ApprovalDeadline time.Time                  `json:"approvalDeadline"`
	Approval         activities.ReleaseApproval `json:"approval"`
}

// releaseRun is the run's deterministic view of the projection it records.
type releaseRun struct {
	ReleaseCarry
	npm, browser, activation activities.ReleaseTargetRecord
	catalogRevision          string
	// recorded is the state of the last recorded transition.
	recorded string
}

func (r *releaseRun) record(state, failureCode string) activities.ReleaseRecordInput {
	in := activities.ReleaseRecordInput{
		ExpectedRevision: r.Revision, State: state, ReleaseID: r.ReleaseID, ReviewEffectID: r.ReviewEffectID,
		Npm: r.npm, Browser: r.browser, Activation: r.activation, CatalogRevision: r.catalogRevision, FailureCode: failureCode,
	}
	if r.Built.Subject.SubjectDigest != "" {
		s := r.Built.Subject
		in.Subject = &s
	}
	if r.Approval.State != "" {
		a := r.Approval
		in.Approval = &a
	}
	if !r.ApprovalDeadline.IsZero() {
		d := r.ApprovalDeadline
		in.ApprovalDeadline = &d
	}
	return in
}

// Release is the ReleaseWorkflow (P21, DD-04 §4, DD-06 §4): it certifies
// the exact saved source the release binds (the independent validator Job
// over the bytes the source authority names for that revision), builds the
// exact ReleaseSubject from the certification, registers it for review,
// waits for the maintainer's decision on durable timers up to an absolute
// approval deadline (continuing as new at a qualified boundary so the long
// wait keeps a bounded history), and publishes only under an approval of
// exactly that digest: the npm target and the browser/CSS target each
// under its own effect identity, each receipt verified against the subject
// and read back from its destination. A lost answer is asked about the
// original identity, never resent; an unknown target keeps the release
// reconciling; npm-only success is partially published and never
// activated; a published target is never republished. Activation runs
// only after both verified receipts and a re-read approval, as a
// conditional write under the catalog revision it read; a conflict is not
// overwritten. Cancellation stops further effects and keeps what was sent.
func Release(q Queues, b Bounds, lb LifecycleBounds) func(ctx workflow.Context, in Input) error {
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
		rel := &releaseRun{npm: pending(), browser: pending(), activation: pending()}
		if in.Release != nil {
			rel.ReleaseCarry = *in.Release
		}

		outcome, failureCode, phase := "failed", "", ""
		continued := false
		defer func() {
			if continued {
				return
			}
			disconnected, _ := workflow.NewDisconnectedContext(ctx)
			if serr := settleOperation(disconnected, q, bounds, operationID, tenantID, operationID+":settle", outcome, failureCode, phase); serr != nil {
				logger.Error("release settlement refused", "operationId", operationID, "error", serr)
				if err == nil {
					err = serr
				}
			}
		}()
		// record commits one transition of the projection; the projection
		// is recorded even after a cancellation.
		record := func(state, code string) bool {
			disconnected, _ := workflow.NewDisconnectedContext(ctx)
			r := rel.record(state, code)
			r.OperationID, r.TenantID = operationID, tenantID
			var next uint64
			if err := workflow.ExecuteActivity(controlOptions(disconnected, q, bounds), activities.NameRecordRelease, r).Get(disconnected, &next); err != nil {
				logger.Error("release projection refused", "operationId", operationID, "state", state, "error", err)
				outcome, failureCode = settleOutcome(ctx, err, "RELEASE_UNRECORDED")
				return false
			}
			rel.Revision, rel.recorded = next, state
			return true
		}
		fail := func(code, ph string) error {
			outcome, failureCode, phase = "failed", code, ph
			record("failed", code)
			return nil
		}
		refusal := func(err error, fallback string) string {
			_, code := settleOutcome(ctx, err, fallback)
			return code
		}

		if in.Release == nil {
			if !record("certifying", "") {
				return nil
			}
			// 1. The exact source: Control's bound artifact, proven by the
			// source authority to be the named revision.
			if err := workflow.ExecuteActivity(business, activities.NameGetReleaseBinding, tenantID, operationID).Get(ctx, &rel.Binding); err != nil {
				return fail(refusal(err, "SOURCE_UNAVAILABLE"), "source_rejected")
			}
			// 2. Independent certification of exactly those bytes.
			stage, _, _, code := runJobStep(ctx, q, bounds, stepSpec{
				operationID: operationID, tenantID: tenantID, stepID: releaseCertifyStep, profileID: lbounds.ReleaseValidatorProfile, launchPrefix: "rel",
				inputs: []activities.LaunchInput{{Name: "source", Digest: rel.Binding.Source.Digest, Handle: rel.Binding.Source.Handle}},
			}, state)
			if code != "" {
				return fail(code, "certification_failed")
			}
			if stage.Verdict != "certified" {
				return fail(firstNonEmpty(stage.FailureCode, strings.ToUpper(stage.Verdict)), "certification_failed")
			}
			// 3. The exact subject.
			if err := workflow.ExecuteActivity(business, activities.NameBuildReleaseSubj, activities.BuildSubjectInput{
				OperationID: operationID, TenantID: tenantID, Binding: rel.Binding, Stage: stage, Destinations: lbounds.ReleaseDestinations,
			}).Get(ctx, &rel.Built); err != nil {
				return fail(refusal(err, "CERTIFICATION_MISMATCH"), "subject_rejected")
			}
			// 4. Review registration of exactly that subject.
			res, code := releaseEffect(ctx, q, bounds, operationID, tenantID, rel, "review", releaseReviewEffect, activities.NameRegisterReview, "review", "", "")
			if code != "" {
				return fail(code, "review_unregistered")
			}
			if res.State != "succeeded" {
				return fail(firstNonEmpty(res.Target.FailureCode, reviewCode(res.State)), "review_unregistered")
			}
			rel.ReleaseID, rel.ReviewEffectID = res.ReleaseID, res.EffectID
			rel.ApprovalDeadline = workflow.Now(ctx).Add(lbounds.ReleaseApprovalWait)
			if rel.Binding.Deadline.Before(rel.ApprovalDeadline) {
				rel.ApprovalDeadline = rel.Binding.Deadline
			}
			rel.Approval = activities.ReleaseApproval{State: "pending", SubjectDigest: rel.Built.Subject.SubjectDigest}
			if !record("awaiting_approval", "") {
				return nil
			}
		}

		// 5. The maintainer's decision, polled on durable timers up to the
		// absolute approval deadline; the run continues as new after a
		// bounded number of polls, carrying every durable fact.
		digest := rel.Built.Subject.SubjectDigest
		decision, code := awaitApproval(ctx, business, lbounds, rel)
		switch code {
		case "":
		case "CONTINUE":
			continued = true
			carry := rel.ReleaseCarry
			return workflow.NewContinueAsNewError(ctx, ReleaseWorkflowName, Input{OperationID: operationID, TenantID: tenantID, Release: &carry})
		case "CANCELED":
			outcome, failureCode = settleOutcome(ctx, ctx.Err(), "CANCELED")
			record("failed", "CANCELED")
			return nil
		default:
			return fail(code, "approval_unavailable")
		}
		rel.Approval = activities.ReleaseApproval{State: decision.State, SubjectDigest: decision.SubjectDigest, ApproverID: decision.ApproverID, DecidedAt: decision.DecidedAt, ReasonCode: decision.ReasonCode}
		switch {
		case decision.State == "expired":
			rel.Approval = activities.ReleaseApproval{State: "pending", SubjectDigest: digest}
			return fail("APPROVAL_EXPIRED", "approval_expired")
		case decision.State == "approved" && decision.SubjectDigest != digest:
			// A stale approval: made for another subject; it authorizes nothing.
			outcome, failureCode, phase = "failed", "APPROVAL_SUBJECT_MISMATCH", "approval_rejected"
			record("rejected", "APPROVAL_SUBJECT_MISMATCH")
			return nil
		case decision.State == "rejected" || decision.State == "invalidated":
			code := "APPROVAL_" + strings.ToUpper(decision.State)
			outcome, failureCode, phase = "failed", code, "approval_rejected"
			record("rejected", code)
			return nil
		}
		if !record("publishing", "") {
			return nil
		}

		// 6. Both targets under their own identities; the approval is read
		// again before each send.
		for _, t := range []struct {
			name       string
			occurrence uint64
			target     *activities.ReleaseTargetRecord
		}{{"npm", releaseNpmEffect, &rel.npm}, {"browser", releaseBrowserEffect, &rel.browser}} {
			if ctx.Err() != nil {
				outcome, failureCode = settleOutcome(ctx, ctx.Err(), "CANCELED")
				record(publicationState(rel), "CANCELED")
				return nil
			}
			if code := stillApproved(ctx, business, rel); code != "" {
				return fail(code, "approval_invalidated")
			}
			res, code := releaseEffect(ctx, q, bounds, operationID, tenantID, rel, "publication", t.occurrence, activities.NamePublishTarget, t.name, t.name, "")
			if code != "" {
				return fail(code, "publication_failed")
			}
			*t.target = targetOf(res)
			if !record(publicationState(rel), "") {
				return nil
			}
			// An unknown target: its original identity is asked, bounded,
			// before anything else is sent (Control opens no new attempt
			// while an effect of the operation is unresolved).
			for round := 0; t.target.State == "unknown" && round < lbounds.ReleaseReconcileRounds; round++ {
				if err := workflow.Sleep(ctx, lbounds.ReleaseReconcilePause); err != nil {
					break
				}
				res, code := releaseEffect(ctx, q, bounds, operationID, tenantID, rel, "publication", t.occurrence, activities.NamePublishTarget, t.name, t.name, "")
				if code == "" && res.State != "unknown" {
					*t.target = targetOf(res)
					record(publicationState(rel), "")
				}
			}
			if t.target.State == "unknown" {
				break
			}
			if t.name == "npm" && rel.npm.State == "failed" {
				// A definitely failed npm target: the browser target is not
				// sent, so no partial publication is created knowingly.
				return fail(firstNonEmpty(rel.npm.FailureCode, "PUBLICATION_FAILED"), "publication_failed")
			}
		}
		switch publicationState(rel) {
		case "reconciling":
			outcome, failureCode, phase = "failed", "EFFECT_UNCERTAIN", "publication_unknown"
			return nil
		case "partially_published":
			outcome, failureCode, phase = "failed", "PARTIALLY_PUBLISHED", "partially_published"
			return nil
		case "published":
		default:
			return fail(firstNonEmpty(rel.npm.FailureCode, rel.browser.FailureCode, "PUBLICATION_FAILED"), "publication_failed")
		}
		if rel.recorded != "published" && !record("published", "") {
			return nil
		}

		// 8. Activation after both verified receipts and a re-read approval,
		// conditional on the catalog revision read now.
		if ctx.Err() != nil {
			outcome, failureCode = settleOutcome(ctx, ctx.Err(), "CANCELED")
			return nil
		}
		if code := stillApproved(ctx, business, rel); code != "" {
			outcome, failureCode, phase = "failed", code, "approval_invalidated"
			return nil
		}
		var catalog string
		if err := workflow.ExecuteActivity(business, activities.NameCatalogRevision).Get(ctx, &catalog); err != nil {
			outcome, failureCode = settleOutcome(ctx, err, "DEPENDENCY_UNAVAILABLE")
			return nil
		}
		res, code := releaseEffect(ctx, q, bounds, operationID, tenantID, rel, "activation", releaseActivateEffect, activities.NameActivateRelease, "activation", "", catalog)
		for round := 0; code == "" && res.State == "unknown" && round < lbounds.ReleaseReconcileRounds; round++ {
			if round == 0 {
				rel.activation = targetOf(res)
				record("reconciling", "")
			}
			if err := workflow.Sleep(ctx, lbounds.ReleaseReconcilePause); err != nil {
				break
			}
			res, code = releaseEffect(ctx, q, bounds, operationID, tenantID, rel, "activation", releaseActivateEffect, activities.NameActivateRelease, "activation", "", catalog)
		}
		if code != "" {
			outcome, failureCode, phase = "failed", code, "activation_failed"
			return nil
		}
		rel.activation = targetOf(res)
		switch res.State {
		case "succeeded":
			rel.catalogRevision = res.CatalogRevision
			if !record("activated", "") {
				return nil
			}
			outcome, phase = "succeeded", "activated"
			logger.Info("release activated", "operationId", operationID, "releaseId", rel.ReleaseID, "catalogRevision", res.CatalogRevision)
		case "unknown":
			outcome, failureCode, phase = "failed", "EFFECT_UNCERTAIN", "activation_unknown"
		default:
			code := firstNonEmpty(rel.activation.FailureCode, "ACTIVATION_FAILED")
			return fail(code, "activation_failed")
		}
		return nil
	}
}

func pending() activities.ReleaseTargetRecord {
	return activities.ReleaseTargetRecord{State: "pending"}
}

func reviewCode(state string) string {
	if state == "unknown" {
		return "EFFECT_UNCERTAIN"
	}
	return "REVIEW_UNREGISTERED"
}

func targetOf(res activities.ReleaseEffectResult) activities.ReleaseTargetRecord {
	t := res.Target
	if t.State == "" {
		t.State = "failed"
	}
	return t
}

func hasUnknown(targets ...activities.ReleaseTargetRecord) bool {
	for _, t := range targets {
		if t.State == "unknown" {
			return true
		}
	}
	return false
}

// publicationState is the release state the two targets support.
func publicationState(rel *releaseRun) string {
	n, b := rel.npm.State, rel.browser.State
	switch {
	case n == "unknown" || b == "unknown":
		return "reconciling"
	case n == "succeeded" && b == "succeeded":
		return "published"
	case (n == "succeeded" && b == "failed") || (n == "failed" && b == "succeeded"):
		return "partially_published"
	case n == "failed" && b == "failed":
		return "failed"
	}
	return "publishing"
}

// releaseEffect runs one guarded release mutation under its own attempt
// (the attempt binds the effect's execution scope and deadline) and a
// stable command identity; the same identity reentered answers the
// original outcome. The mutation Activity runs under a disconnected
// context so a cancellation never abandons a send in flight; the caller
// checks the cancellation between mutations.
func releaseEffect(ctx workflow.Context, q Queues, b Bounds, operationID, tenantID string, rel *releaseRun, kind string, occurrence uint64, activity, target, publishTarget, catalog string) (activities.ReleaseEffectResult, string) {
	logger := workflow.GetLogger(ctx)
	business := activityOptions(ctx, q, b)
	commandID := operationID + ":" + kind + ":" + target + ":" + strconv.FormatUint(occurrence, 10)
	// One attempt per mutation identity: a reentered mutation (a bounded
	// query round) reenters the same attempt, so the effect's binding is
	// the one it was prepared with.
	var attempt activities.AttemptRef
	if err := workflow.ExecuteActivity(business, activities.NameOpenAttempt, activities.OpenAttemptInput{
		OperationID: operationID, TenantID: tenantID, CommandID: commandID + ":open",
		StepID: kind + "_" + target, ProfileID: rel.Binding.ProfileID,
	}).Get(ctx, &attempt); err != nil {
		_, code := settleOutcome(ctx, err, "STALE_EXECUTION")
		return activities.ReleaseEffectResult{}, code
	}
	epoch, _ := strconv.ParseUint(attempt.ExecutionEpoch, 10, 64)
	call := activities.ReleaseEffectCall{
		Effect: activities.ReleaseEffectInput{
			OperationID: operationID, TenantID: tenantID, CommandID: commandID, AttemptID: attempt.AttemptID, ExecutionEpoch: epoch, Kind: kind,
			Occurrence: occurrence, CanonicalSubject: "release:" + rel.Built.Subject.SubjectDigest + ":" + target, Deadline: attempt.Deadline,
		},
		Subject: rel.Built.Subject, ReleaseID: rel.ReleaseID, Target: publishTarget, Artifacts: rel.Built.Artifacts, ExpectedCatalogRevision: catalog,
	}
	disconnected, _ := workflow.NewDisconnectedContext(ctx)
	opts := workflow.WithActivityOptions(disconnected, workflow.ActivityOptions{
		TaskQueue: q.Control, StartToCloseTimeout: b.ControlActivityTimeout,
		RetryPolicy: &temporal.RetryPolicy{InitialInterval: b.ControlRetryInitial, BackoffCoefficient: 2, MaximumInterval: b.ControlRetryMaxInterval, MaximumAttempts: b.ControlRetryMaxAttempts},
	})
	var res activities.ReleaseEffectResult
	callErr := workflow.ExecuteActivity(opts, activity, call).Get(disconnected, &res)
	// The attempt records whether the guarded call completed; the target's
	// outcome lives in its effect and the release record. While the effect
	// is unknown Control keeps the operation reconciling, and this run is
	// the one that queries the original identity next, so the close is
	// issued once without waiting; once a query round resolves the effect,
	// the same close command settles the obligations (the operation runs
	// again) before the next mutation opens its attempt.
	closeOutcome := "completed"
	if callErr != nil {
		closeOutcome = "failed"
	}
	closeCmd := attempt.AttemptID + ":close"
	if callErr != nil || res.State == "unknown" {
		var closed activities.CloseResult
		if cerr := workflow.ExecuteActivity(controlOptions(disconnected, q, b), activities.NameCloseAttempt, activities.CloseAttemptInput{
			Attempt: attempt, CommandID: closeCmd, Outcome: closeOutcome, Cleanup: "not_required",
		}).Get(disconnected, &closed); cerr != nil {
			logger.Error("release attempt close refused", "attemptId", attempt.AttemptID, "error", cerr)
		}
	} else if cerr := closeAttempt(disconnected, q, b, attempt, closeCmd, closeOutcome, "not_required", ""); cerr != nil {
		logger.Error("release attempt close refused", "attemptId", attempt.AttemptID, "error", cerr)
	}
	if callErr != nil {
		_, code := settleOutcome(ctx, callErr, "EFFECT_UNCERTAIN")
		return activities.ReleaseEffectResult{}, code
	}
	return res, ""
}

// awaitApproval polls the review port on durable timers until a decision,
// the absolute approval deadline, a cancellation, or the poll bound of this
// run ("CONTINUE"). An expired wait answers the state "expired".
func awaitApproval(ctx workflow.Context, business workflow.Context, lb LifecycleBounds, rel *releaseRun) (activities.ReviewDecision, string) {
	for polls := 0; ; polls++ {
		if ctx.Err() != nil {
			return activities.ReviewDecision{}, "CANCELED"
		}
		var d activities.ReviewDecision
		if err := workflow.ExecuteActivity(business, activities.NameReviewDecision, rel.ReleaseID).Get(ctx, &d); err != nil {
			if ctx.Err() != nil {
				return d, "CANCELED"
			}
			_, code := settleOutcome(ctx, err, "DEPENDENCY_UNAVAILABLE")
			return d, code
		}
		if d.State == "approved" || d.State == "rejected" || d.State == "invalidated" {
			return d, ""
		}
		now := workflow.Now(ctx)
		if !now.Before(rel.ApprovalDeadline) {
			return activities.ReviewDecision{State: "expired"}, ""
		}
		if polls+1 >= lb.ReleasePollsPerRun {
			return d, "CONTINUE"
		}
		wait := lb.ReleaseApprovalPoll
		if left := rel.ApprovalDeadline.Sub(now); left < wait {
			wait = left
		}
		if err := workflow.Sleep(ctx, wait); err != nil {
			return d, "CANCELED"
		}
	}
}

// stillApproved re-reads the decision: only an approval of exactly the
// recorded subject lets the next effect be asked for.
func stillApproved(ctx workflow.Context, business workflow.Context, rel *releaseRun) string {
	var d activities.ReviewDecision
	if err := workflow.ExecuteActivity(business, activities.NameReviewDecision, rel.ReleaseID).Get(ctx, &d); err != nil {
		_, code := settleOutcome(ctx, err, "DEPENDENCY_UNAVAILABLE")
		return code
	}
	if d.State != "approved" || d.SubjectDigest != rel.Built.Subject.SubjectDigest {
		return "APPROVAL_INVALIDATED"
	}
	return ""
}
