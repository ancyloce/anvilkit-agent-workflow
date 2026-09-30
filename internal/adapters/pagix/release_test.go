package pagix

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

func releaseSubject() activities.ReleaseSubject {
	return activities.ReleaseSubject{SchemaVersion: 1, ComponentID: "cmp_x", PuckType: "Hero", PackageName: "@anvilkit/x", Version: "1.0.0", SubjectDigest: "sha256:subject"}
}

// The review, publication and activation doubles (P21): idempotency by
// effect id, maintainer decisions bound to a digest, immutable versions,
// read-back of published bytes, the injected faults and the conditional
// activation into the Studio-readable catalog.
func TestReleasePorts(t *testing.T) {
	ctx := context.Background()
	d, err := New(t.TempDir())
	require.NoError(t, err)
	origin := t.TempDir()
	require.NoError(t, d.SetOrigin(origin))
	subject := releaseSubject()

	// Exact-revision digests: a save records its bytes' digest.
	_, err = d.SaveRevision(ctx, "eff_s", activities.SaveSourceInput{Lineage: "sha256:lin", BaseRevision: "1", Occurrence: 1, Source: activities.StageArtifact{Digest: "sha256:saved"}})
	require.NoError(t, err)
	dg, known, err := d.RevisionDigest(ctx, "sha256:lin", "2")
	require.NoError(t, err)
	require.True(t, known)
	require.Equal(t, "sha256:saved", dg)
	_, known, err = d.RevisionDigest(ctx, "sha256:lin", "7")
	require.NoError(t, err)
	require.False(t, known)

	// Review: one release id per effect, pending until the maintainer decides.
	review, err := d.RegisterReview(ctx, "eff_r", activities.RegisterReviewInput{Occurrence: 1, Subject: subject})
	require.NoError(t, err)
	again, err := d.RegisterReview(ctx, "eff_r", activities.RegisterReviewInput{Occurrence: 1, Subject: subject})
	require.NoError(t, err)
	require.Equal(t, review, again)
	dec, err := d.ReviewDecision(ctx, review.ReleaseID)
	require.NoError(t, err)
	require.Equal(t, "pending", dec.State)
	require.NoError(t, d.Decide(review.ReleaseID, "approved", "sha256:other", "maintainer_a"))
	dec, err = d.ReviewDecision(ctx, review.ReleaseID)
	require.NoError(t, err)
	require.Equal(t, "sha256:other", dec.SubjectDigest, "the decision names the digest it was made for")

	// npm: published once; the version is immutable.
	tarball := []byte("tarball")
	res, err := d.Publish(ctx, "eff_n", activities.PublishInput{Target: "npm", Occurrence: 1, ReleaseID: review.ReleaseID, Subject: subject, Destination: "https://registry.example.invalid/", Files: []activities.PublishFile{{Path: "x-1.0.0.tgz", Bytes: tarball}}})
	require.NoError(t, err)
	require.Equal(t, "published", res.State)
	var receipt activities.PublicationReceipt
	require.NoError(t, json.Unmarshal(res.Receipt, &receipt))
	require.Equal(t, activities.Digest(tarball), receipt.Files[0].Digest)
	back, err := d.FetchPublished(ctx, "npm", "", "", "@anvilkit/x@1.0.0")
	require.NoError(t, err)
	require.Equal(t, tarball, back)
	dup, err := d.Publish(ctx, "eff_n2", activities.PublishInput{Target: "npm", Occurrence: 3, Subject: subject, Files: []activities.PublishFile{{Path: "x-1.0.0.tgz", Bytes: tarball}}})
	require.NoError(t, err)
	require.Equal(t, activities.PublishResult{State: "failed", FailureCode: "VERSION_EXISTS"}, dup, "a published version is never written again")

	// Browser: a lost answer is recorded and answered by the query.
	d.InstallFault("publish", 2, "unknown")
	manifest := []byte(`{"schemaVersion":1}`)
	_, err = d.Publish(ctx, "eff_b", activities.PublishInput{Target: "browser", Occurrence: 2, ReleaseID: review.ReleaseID, Subject: subject, Destination: "https://components.example.invalid", Files: []activities.PublishFile{{Path: "index.js", Bytes: []byte("js")}, {Path: "styles/x.css", Bytes: []byte("css")}}, Manifest: manifest})
	require.Error(t, err)
	queried, ok, err := d.QueryPublication(ctx, "eff_b")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "published", queried.State)
	css, err := d.FetchPublished(ctx, "browser", "", review.ReleaseID, "styles/x.css")
	require.NoError(t, err)
	require.Equal(t, []byte("css"), css)

	// Activation: conditional on the catalog revision, only for a published manifest.
	lock := activities.LockEntry{SchemaVersion: 1, ComponentID: "cmp_x", PuckType: "Hero", ReleaseID: review.ReleaseID, PackageName: "@anvilkit/x", PackageVersion: "1.0.0", BrowserManifestDigest: activities.Digest(manifest), HostProfileID: "host-abi-dev-v1"}
	stale, err := d.Activate(ctx, "eff_a0", activities.ActivateInput{Occurrence: 1, Subject: subject, Lock: lock, ExpectedCatalogRevision: "5"})
	require.NoError(t, err)
	require.Equal(t, activities.ActivationResult{State: "conflict", CurrentCatalogRevision: "0"}, stale)
	wrong := lock
	wrong.BrowserManifestDigest = "sha256:mismatch"
	missing, err := d.Activate(ctx, "eff_a1", activities.ActivateInput{Occurrence: 1, Subject: subject, Lock: wrong, ExpectedCatalogRevision: "0"})
	require.NoError(t, err)
	require.Equal(t, "RELEASE_UNAVAILABLE", missing.FailureCode)
	act, err := d.Activate(ctx, "eff_a2", activities.ActivateInput{Occurrence: 1, Subject: subject, Lock: lock, ExpectedCatalogRevision: "0"})
	require.NoError(t, err)
	require.Equal(t, "activated", act.State)
	require.Equal(t, "1", act.CurrentCatalogRevision)
	var doc catalogDoc
	ok, err = d.readJSON(origin+"/catalog.json", &doc)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []catalogEntry{{LockEntry: lock, Label: "Hero 1.0.0"}}, doc.Releases)
	orig, ok, err := d.QueryActivation(ctx, "eff_a2")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, act, orig)
}

// The publication faults a test drives the release through.
func TestPublicationFaults(t *testing.T) {
	ctx := context.Background()
	d, err := New(t.TempDir())
	require.NoError(t, err)
	subject := releaseSubject()
	d.InstallFault("publish", 1, "wrong-destination")
	res, err := d.Publish(ctx, "eff_w", activities.PublishInput{Target: "npm", Occurrence: 1, Subject: subject, Destination: "https://registry.example.invalid/", Files: []activities.PublishFile{{Path: "a.tgz", Bytes: []byte("a")}}})
	require.NoError(t, err)
	var receipt activities.PublicationReceipt
	require.NoError(t, json.Unmarshal(res.Receipt, &receipt))
	require.NotEqual(t, "https://registry.example.invalid/", receipt.Destination)

	d.InstallFault("publish", 2, "fail")
	failed, err := d.Publish(ctx, "eff_f", activities.PublishInput{Target: "browser", Occurrence: 2, ReleaseID: "rel_f", Subject: subject, Files: []activities.PublishFile{{Path: "index.js", Bytes: []byte("js")}}, Manifest: []byte("{}")})
	require.NoError(t, err)
	require.Equal(t, activities.PublishResult{State: "failed", FailureCode: "REGISTRY_REJECTED"}, failed)

	d.InstallFault("publish", 3, "unavailable")
	un, err := d.Publish(ctx, "eff_u", activities.PublishInput{Target: "browser", Occurrence: 3, ReleaseID: "rel_u", Subject: subject, Files: []activities.PublishFile{{Path: "index.js", Bytes: []byte("js")}}, Manifest: []byte("{}")})
	require.NoError(t, err)
	require.Equal(t, "published", un.State)
	_, err = d.FetchPublished(ctx, "browser", "", "rel_u", "index.js")
	require.Error(t, err, "the receipt names a file the origin does not serve")
}
