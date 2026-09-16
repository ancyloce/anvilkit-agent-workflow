package activities

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"go.temporal.io/sdk/activity"

	"github.com/ancyloce/anvilkit-agent-contracts/go/jobschema"
)

// Activities binds the ports; every method is registered under its stable
// name by the bootstrap.
type Activities struct {
	Control  Control
	Recovery RecoveryControl
	Launcher Launcher
	Observer string // observer identity recorded on accepted results
}

func (a *Activities) OpenAttempt(ctx context.Context, in OpenAttemptInput) (AttemptRef, error) {
	return a.Control.OpenAttempt(ctx, in)
}

func (a *Activities) PrepareLaunch(ctx context.Context, in PrepareLaunchInput) (LaunchRef, error) {
	return a.Control.PrepareLaunch(ctx, in)
}

func (a *Activities) CreateJob(ctx context.Context, in CreateJobInput) (JobRef, error) {
	return a.Launcher.CreateJob(ctx, in)
}

func (a *Activities) ObserveJob(ctx context.Context, in ObserveJobInput) (JobObservation, error) {
	return a.Launcher.ObserveJob(ctx, in, func() { activity.RecordHeartbeat(ctx) })
}

func (a *Activities) RegisterInstance(ctx context.Context, in RegisterInstanceInput) (InstanceRef, error) {
	return a.Control.RegisterInstance(ctx, in)
}

func (a *Activities) ObserveInstance(ctx context.Context, in ObserveInstanceInput) error {
	return a.Control.ObserveInstance(ctx, in.Attempt.AttemptID, in.Instance.InstanceID, in.Observation)
}

func (a *Activities) AcceptResult(ctx context.Context, in AcceptResultInput) (StageRef, error) {
	return a.Control.AcceptResult(ctx, in)
}

func (a *Activities) ObserveLaunch(ctx context.Context, in ObserveLaunchInput) (LaunchObservation, error) {
	return a.Launcher.ObserveLaunch(ctx, in, func() { activity.RecordHeartbeat(ctx) })
}

func (a *Activities) DeleteJob(ctx context.Context, in DeleteJobInput) (LaunchObservation, error) {
	return a.Launcher.DeleteJob(ctx, in, func() { activity.RecordHeartbeat(ctx) })
}

func (a *Activities) CloseAttempt(ctx context.Context, in CloseAttemptInput) (CloseResult, error) {
	return a.Control.CloseAttempt(ctx, in)
}

// VerifyResult is the trusted observer of the LocalCheck fixture (DD-04 §3
// in miniature): it decides from Kubernetes-reported phase, exit code and
// termination message, validates the manifest against the jobs contract and
// compares the reported output digest and byte size with the reviewed
// profile's expected result. The candidate's own claims never certify.
func (a *Activities) VerifyResult(ctx context.Context, in VerifyResultInput) (Verdict, error) {
	profile, err := jobschema.ProfileByID(in.ProfileID)
	if err != nil {
		return Verdict{}, err
	}
	fail := func(verdict, code string) (Verdict, error) {
		manifest, err := json.Marshal(map[string]any{
			"schemaVersion": 1, "launchId": in.Launch.LaunchID, "attemptId": in.Attempt.AttemptID, "jobKind": profile.JobKind,
			"profileId": in.ProfileID, "verdict": verdict, "failureCode": code, "outputs": []any{},
			"completedAt": in.CompletedAt.UTC().Format("2006-01-02T15:04:05Z"),
		})
		if err != nil {
			return Verdict{}, err
		}
		return Verdict{Verdict: verdict, FailureCode: code, Manifest: manifest, ResultDigest: digestOf(manifest)}, nil
	}
	if len(in.Observation.Pods) == 0 {
		if in.Observation.Reason == "DeadlineExceeded" {
			return fail("infrastructure_failed", "DEADLINE_EXCEEDED")
		}
		return fail("infrastructure_failed", "OBSERVER_FAILED")
	}
	owner := in.Observation.Pods[0]
	switch {
	case in.Observation.Reason == "DeadlineExceeded":
		return fail("infrastructure_failed", "DEADLINE_EXCEEDED")
	case owner.Reason == "ErrImagePull" || owner.Reason == "ImagePullBackOff":
		return fail("infrastructure_failed", "IMAGE_PULL_FAILED")
	case owner.Reason == "Evicted":
		return fail("infrastructure_failed", "POD_EVICTED")
	case owner.Phase != "succeeded" || owner.ExitCode == nil || *owner.ExitCode != 0:
		return fail("invalid", "CANDIDATE_TEST_FAILED")
	}
	manifest := []byte(owner.TerminationMessage)
	if err := jobschema.ValidateResultManifest(manifest); err != nil {
		return fail("invalid", "PROTECTED_FIXTURE_ALTERED")
	}
	var reported struct {
		LaunchID  string `json:"launchId"`
		AttemptID string `json:"attemptId"`
		ProfileID string `json:"profileId"`
		Verdict   string `json:"verdict"`
		Outputs   []struct {
			Class     string `json:"class"`
			Digest    string `json:"digest"`
			SizeBytes string `json:"sizeBytes"`
		} `json:"outputs"`
	}
	if err := json.Unmarshal(manifest, &reported); err != nil {
		return fail("invalid", "PROTECTED_FIXTURE_ALTERED")
	}
	if reported.LaunchID != in.Launch.LaunchID || reported.AttemptID != in.Attempt.AttemptID || reported.ProfileID != in.ProfileID || reported.Verdict != "certified" {
		return fail("invalid", "PROTECTED_FIXTURE_ALTERED")
	}
	// The reviewed expectation is the digest and the byte size together
	// (both canonical strings of the contract); a report that matches only
	// one of them is not the fixed result.
	expected := profile.ExpectedResult
	if expected == nil || expected.ResultSizeBytes == "" || len(reported.Outputs) != 1 || reported.Outputs[0].Class != "result" ||
		reported.Outputs[0].Digest != expected.ResultDigest || reported.Outputs[0].SizeBytes != expected.ResultSizeBytes {
		return fail("invalid", "RESULT_DIGEST_MISMATCH")
	}
	return Verdict{Verdict: "certified", Manifest: manifest, ResultDigest: digestOf(manifest)}, nil
}

func digestOf(b []byte) string { return fmt.Sprintf("sha256:%x", sha256.Sum256(b)) }
