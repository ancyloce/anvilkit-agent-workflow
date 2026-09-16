package activities_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-contracts/go/jobschema"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

func manifest(t *testing.T, digest, sizeBytes string, extra map[string]any) string {
	t.Helper()
	m := map[string]any{
		"schemaVersion": 1, "launchId": "lch_1", "attemptId": "att_1", "jobKind": "validator", "profileId": "local-check-v1",
		"verdict": "certified", "outputs": []map[string]any{{"class": "result", "digest": digest, "sizeBytes": sizeBytes}}, "completedAt": "2026-09-14T12:00:00Z",
	}
	for k, v := range extra {
		m[k] = v
	}
	raw, err := json.Marshal(m)
	require.NoError(t, err)
	return string(raw)
}

func TestVerifyResult(t *testing.T) {
	profile, err := jobschema.ProfileByID("local-check-v1")
	require.NoError(t, err)
	expected, size := profile.ExpectedResult.ResultDigest, profile.ExpectedResult.ResultSizeBytes
	require.Equal(t, "24", size, "the reviewed fixed output of local-check-v1 is 24 bytes")
	a := &activities.Activities{Observer: "test"}
	zero, one := int32(0), int32(1)
	base := activities.VerifyResultInput{
		Launch: activities.LaunchRef{LaunchID: "lch_1", AttemptID: "att_1", LaunchKey: "lc-x"}, Attempt: activities.AttemptRef{AttemptID: "att_1", ProfileID: "local-check-v1"},
		ProfileID: "local-check-v1", CompletedAt: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC),
	}
	obs := func(phase string, code *int32, msg, reason string) activities.JobObservation {
		return activities.JobObservation{JobUID: "job", Pods: []activities.PodObservation{{PodUID: "pod", Phase: phase, ExitCode: code, TerminationMessage: msg, Reason: reason}}}
	}
	cases := []struct {
		name          string
		observation   activities.JobObservation
		verdict, code string
	}{
		{"certified when the reported digest and size equal the reviewed expectation", obs("succeeded", &zero, manifest(t, expected, size, nil), ""), "certified", ""},
		{"digest mismatch is invalid", obs("succeeded", &zero, manifest(t, "sha256:1111111111111111111111111111111111111111111111111111111111111111", size, nil), ""), "invalid", "RESULT_DIGEST_MISMATCH"},
		{"the reviewed digest with another size is invalid", obs("succeeded", &zero, manifest(t, expected, "25", nil), ""), "invalid", "RESULT_DIGEST_MISMATCH"},
		{"self-reported extra field is a fixture alteration", obs("succeeded", &zero, manifest(t, expected, size, map[string]any{"exitCode": 0}), ""), "invalid", "PROTECTED_FIXTURE_ALTERED"},
		{"manifest for another attempt is a fixture alteration", obs("succeeded", &zero, manifest(t, expected, size, map[string]any{"attemptId": "att_other"}), ""), "invalid", "PROTECTED_FIXTURE_ALTERED"},
		{"exit code 0 claimed in the manifest cannot override a failed pod", obs("failed", &one, manifest(t, expected, size, nil), ""), "invalid", "CANDIDATE_TEST_FAILED"},
		{"image pull failure is infrastructure", obs("pending", nil, "", "ErrImagePull"), "infrastructure_failed", "IMAGE_PULL_FAILED"},
		{"no pod before the deadline is infrastructure", activities.JobObservation{JobUID: "job", Reason: "DeadlineExceeded"}, "infrastructure_failed", "DEADLINE_EXCEEDED"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			in := base
			in.Observation = c.observation
			v, err := a.VerifyResult(context.Background(), in)
			require.NoError(t, err)
			require.Equal(t, c.verdict, v.Verdict)
			require.Equal(t, c.code, v.FailureCode)
			require.NoError(t, jobschema.ValidateResultManifest(v.Manifest), "the accepted manifest is always a valid contract document")
			require.Regexp(t, `^sha256:[0-9a-f]{64}$`, v.ResultDigest)
		})
	}
}
