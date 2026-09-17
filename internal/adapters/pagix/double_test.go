package pagix

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// The DEVELOPMENT_ONLY double keeps the lease semantics the supervisor
// relies on: a lease is one owner's until released or expired, a renewal
// under a repeated occurrence answers the original expiry, another fence
// is a confirmed loss, a released lease is lost, a fault answers unknown.
func TestLeaseDouble(t *testing.T) {
	d, err := New(t.TempDir())
	require.NoError(t, err)
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	d.now = func() time.Time { return now }
	ctx := context.Background()
	in := activities.LeaseInput{Subject: "source:tenant_a/sha256:x", Owner: "op_1", Occurrence: 1, TTL: 10 * time.Minute}

	held, err := d.Acquire(ctx, in)
	require.NoError(t, err)
	require.Equal(t, "held", held.State)
	require.Equal(t, now.Add(10*time.Minute), *held.ExpiresAt)
	other := in
	other.Owner = "op_2"
	lost, err := d.Acquire(ctx, other)
	require.NoError(t, err)
	require.Equal(t, "lost", lost.State, "another owner cannot take an unexpired lease")
	again, err := d.Acquire(ctx, in)
	require.NoError(t, err)
	require.Equal(t, held.LeaseID, again.LeaseID, "the owner re-acquires its own lease")

	in.LeaseID, in.Fence = held.LeaseID, held.Fence
	now = now.Add(5 * time.Minute)
	in.Occurrence = 2
	renewed, err := d.Renew(ctx, in)
	require.NoError(t, err)
	require.Equal(t, now.Add(10*time.Minute), *renewed.ExpiresAt)
	now = now.Add(time.Minute)
	repeat, err := d.Renew(ctx, in)
	require.NoError(t, err)
	require.Equal(t, *renewed.ExpiresAt, *repeat.ExpiresAt, "the same occurrence answers the original expiry")

	d.InstallFault("renew", 3, "unknown")
	in.Occurrence = 3
	unknown, err := d.Renew(ctx, in)
	require.NoError(t, err)
	require.Equal(t, "unknown", unknown.State)
	q, err := d.Query(ctx, in)
	require.NoError(t, err)
	require.Equal(t, "held", q.State, "a query after an unknown renewal answers the confirmed state")

	stale := in
	stale.Fence = "99"
	stale.Occurrence = 4
	res, err := d.Renew(ctx, stale)
	require.NoError(t, err)
	require.Equal(t, "lost", res.State, "another fence is a confirmed loss")

	in.Occurrence = 5
	released, err := d.Release(ctx, in)
	require.NoError(t, err)
	require.Equal(t, "lost", released.State)
	after, err := d.Query(ctx, in)
	require.NoError(t, err)
	require.Equal(t, "lost", after.State, "a released lease is lost for its owner")
	taken, err := d.Acquire(ctx, other)
	require.NoError(t, err)
	require.Equal(t, "held", taken.State, "the subject is free after the release")
	require.NotEqual(t, held.Fence, taken.Fence)

	require.NoError(t, d.DenyScope("sha256:denied"))
	decision, err := d.CheckScope(ctx, activities.CheckSourceScopeInput{SubjectDigest: "sha256:denied"})
	require.NoError(t, err)
	require.False(t, decision.Allowed)
	require.Equal(t, "SCOPE_DENIED", decision.ReasonCode)

	reg, err := d.RegisterCandidate(ctx, "eff_1", activities.RegisterCandidateInput{Occurrence: 1, Subject: "s", Source: activities.StageArtifact{Digest: "sha256:s"}})
	require.NoError(t, err)
	same, err := d.RegisterCandidate(ctx, "eff_1", activities.RegisterCandidateInput{Occurrence: 1, Subject: "s", Source: activities.StageArtifact{Digest: "sha256:s"}})
	require.NoError(t, err)
	require.Equal(t, reg.CandidateID, same.CandidateID, "the same effect id registers once")
	found, known, err := d.QueryRegistration(ctx, "eff_1")
	require.NoError(t, err)
	require.True(t, known)
	require.Equal(t, reg.CandidateID, found.CandidateID)
	_, known, err = d.QueryRegistration(ctx, "eff_none")
	require.NoError(t, err)
	require.False(t, known)
}
