package activities_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// P0.8: a generation's launches carry the identity and the source revision
// its frozen brief allocated; a generation naming another revision, a brief
// without a whole identity or with a non-sequence revision is refused.
func TestBriefIdentity(t *testing.T) {
	brief := func(revision string) activities.BriefIdentityInput {
		body := []byte(fmt.Sprintf(`{"schemaVersion":1,"componentId":"cmp_hero","puckType":"Hero","packageName":"@anvilkit/hero","sourceRevision":%q}`, revision))
		return activities.BriefIdentityInput{TenantID: "tenant_a", OperationID: "op_1",
			Brief: activities.BriefRef{BriefID: "brf_1", Brief: activities.ArtifactBinding{Handle: "h_brief", Digest: activities.Digest(body)}}}
	}
	acts := func(in activities.BriefIdentityInput, revision string) *activities.LifecycleActivities {
		body := []byte(fmt.Sprintf(`{"schemaVersion":1,"componentId":"cmp_hero","puckType":"Hero","packageName":"@anvilkit/hero","sourceRevision":%q}`, revision))
		return &activities.LifecycleActivities{Artifacts: fakeArtifacts{bodies: map[string][]byte{in.Brief.Brief.Handle: body}}}
	}
	ctx := context.Background()

	in := brief("1")
	id, err := acts(in, "1").BriefIdentity(ctx, in)
	require.NoError(t, err)
	require.Equal(t, activities.ComponentIdentity{ComponentID: "cmp_hero", PuckType: "Hero", PackageName: "@anvilkit/hero", SourceRevision: "1"}, id,
		"a generation naming no revision certifies the brief's frozen one")

	in.SourceRevision = "1"
	id, err = acts(in, "1").BriefIdentity(ctx, in)
	require.NoError(t, err)
	require.Equal(t, "1", id.SourceRevision)

	in.SourceRevision = "2"
	_, err = acts(in, "1").BriefIdentity(ctx, in)
	require.Equal(t, "BRIEF_INVALID", activities.RefusalCode(err), "another revision than the brief's")

	bad := brief("01")
	_, err = acts(bad, "01").BriefIdentity(ctx, bad)
	require.Equal(t, "BRIEF_INVALID", activities.RefusalCode(err), "a revision that is not a sequence")

	tampered := brief("1")
	tampered.Brief.Brief.Digest = "sha256:" + fmt.Sprintf("%064d", 0)
	_, err = acts(tampered, "1").BriefIdentity(ctx, tampered)
	require.Equal(t, "BRIEF_DIGEST_MISMATCH", activities.RefusalCode(err))
}
