package activities_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// recordingPreview is Control's preview projection as RecordPreview sees
// it: it keeps the transitions it was sent.
type recordingPreview struct {
	activities.PreviewControl
	records []activities.PreviewRecordInput
}

func (r *recordingPreview) RecordPreview(_ context.Context, in activities.PreviewRecordInput) (uint64, error) {
	r.records = append(r.records, in)
	return uint64(len(r.records)), nil
}

func previewEvidence(verdict string, complete bool, buildProfile, hostAbi string) []byte {
	return []byte(fmt.Sprintf(`{"verdict":%q,"certification":{"verdict":%q,"complete":%t,"bindings":{"componentId":"cmp_x","puckType":"Hero","sourceRevision":"4","buildProfileId":%q,"hostAbi":%q}}}`,
		verdict, verdict, complete, buildProfile, hostAbi))
}

// TestPreviewProfilesAreTheCertifiedOnes (RB-A03a): the preview records the
// build-support and host ABI profiles the validator certified the build
// under, and those must be the ones the environment configures.
func TestPreviewProfilesAreTheCertifiedOnes(t *testing.T) {
	ctx := context.Background()
	read := func(acts *activities.LifecycleActivities, body []byte) (activities.PreviewProfiles, error) {
		acts.Artifacts = fakeArtifacts{bodies: map[string][]byte{"h_ev": body}}
		return acts.CertifiedPreviewProfiles(ctx, activities.PreviewCertificationInput{OperationID: "op", TenantID: "t", Evidence: art("h_ev", "evidence", body)})
	}
	configured := func() *activities.LifecycleActivities {
		return &activities.LifecycleActivities{BuildSupportProfile: "build-support-dev-v1", HostAbiProfile: "host-abi-dev-v1"}
	}

	got, err := read(configured(), previewEvidence("certified", true, "build-support-dev-v1", "host-abi-dev-v1"))
	require.NoError(t, err)
	require.Equal(t, activities.PreviewProfiles{BuildProfileID: "build-support-dev-v1", HostProfileID: "host-abi-dev-v1"}, got)

	_, err = read(configured(), previewEvidence("certified", true, "build-support-2026-11", "host-abi-dev-v1"))
	require.Equal(t, "CERTIFICATION_MISMATCH", activities.RefusalCode(err), "built under another build-support profile than the configured one")
	_, err = read(configured(), previewEvidence("certified", true, "build-support-dev-v1", "host-abi-studio-2026-11"))
	require.Equal(t, "CERTIFICATION_MISMATCH", activities.RefusalCode(err), "built for another host ABI than the one served")
	_, err = read(configured(), previewEvidence("certified", false, "build-support-dev-v1", "host-abi-dev-v1"))
	require.Equal(t, "CERTIFICATION_MISSING", activities.RefusalCode(err), "an incomplete certification")
	_, err = read(configured(), previewEvidence("invalid", true, "build-support-dev-v1", "host-abi-dev-v1"))
	require.Equal(t, "CERTIFICATION_MISSING", activities.RefusalCode(err), "not certified")
	_, err = read(configured(), previewEvidence("certified", true, "", "host-abi-dev-v1"))
	require.Equal(t, "CERTIFICATION_MISSING", activities.RefusalCode(err), "a certification that names no build-support profile")
	_, err = read(configured(), []byte(`{"verdict":"certified"}`))
	require.Equal(t, "CERTIFICATION_MISSING", activities.RefusalCode(err), "evidence without a certification")

	got, err = read(&activities.LifecycleActivities{}, previewEvidence("certified", true, "build-support-2026-11", "host-abi-studio-2026-11"))
	require.NoError(t, err, "an environment that configures no profile binds nothing")
	require.Equal(t, activities.PreviewProfiles{BuildProfileID: "build-support-2026-11", HostProfileID: "host-abi-studio-2026-11"}, got)

	// RecordPreview: a transition without profiles records the configured
	// ones; a built preview keeps the certified ones it names.
	ctl := &recordingPreview{}
	acts := configured()
	acts.Preview = ctl
	_, err = acts.RecordPreview(ctx, activities.PreviewRecordInput{State: "building"})
	require.NoError(t, err)
	_, err = acts.RecordPreview(ctx, activities.PreviewRecordInput{State: "ready", BuildProfileID: "build-support-dev-v1", HostProfileID: "host-abi-dev-v1"})
	require.NoError(t, err)
	require.Len(t, ctl.records, 2)
	require.Equal(t, "build-support-dev-v1", ctl.records[0].BuildProfileID)
	require.Equal(t, "host-abi-dev-v1", ctl.records[0].HostProfileID)
	require.Equal(t, "build-support-dev-v1", ctl.records[1].BuildProfileID)
}
