//go:build conformance

package pagix

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
	pagixadapter "github.com/ancyloce/anvilkit-agent-workflow/internal/adapters/pagix"
)

// target is one implementation of the three Pagix ports under test.
type target struct {
	name    string
	lease   activities.LeasePort
	source  activities.SourcePort
	release activities.ReleasePort
	// double is the controllable upstream (nil for a real environment):
	// cases that must arrange upstream state run only where it exists.
	double *pagixadapter.Double
	// seen lists the wire exchanges (nil when they cannot be observed).
	seen func() []Exchange
}

func referenceDeclaration(t *testing.T) *pagixadapter.Declaration {
	t.Helper()
	d, err := pagixadapter.LoadDeclaration("reference-declaration.yaml")
	require.NoError(t, err)
	require.Empty(t, d.Missing(), "the reference declaration names every port operation")
	return d
}

func targets(t *testing.T) []target {
	t.Helper()
	inProcess, err := pagixadapter.New(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, inProcess.SetOrigin(t.TempDir()))
	served, err := pagixadapter.New(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, served.SetOrigin(t.TempDir()))
	decl := referenceDeclaration(t)
	ref := NewReference(served, decl)
	// The adapter appends the declared paths to its base path.
	srv := httptest.NewServer(http.StripPrefix("/pagix", ref))
	t.Cleanup(srv.Close)
	client, err := pagixadapter.NewClient(decl, pagixadapter.ClientOptions{BaseURL: srv.URL + "/pagix", HTTPClient: srv.Client()})
	require.NoError(t, err)
	out := []target{
		{name: "double", lease: inProcess, source: inProcess, release: inProcess, double: inProcess},
		{name: "declared-adapter", lease: client, source: client, release: client, double: served, seen: func() []Exchange { return ref.Seen }},
	}
	if real := realTarget(t); real != nil {
		out = append(out, *real)
	}
	return out
}

// realTarget is the ENV-07 environment when its inputs are given; without
// them the run is reported UNEXECUTED with the missing item named.
func realTarget(t *testing.T) *target {
	url, declPath := os.Getenv("ANVILKIT_PAGIX_URL"), os.Getenv("ANVILKIT_PAGIX_DECLARATION")
	switch {
	case url == "":
		t.Log("UNEXECUTED real Pagix environment: ANVILKIT_PAGIX_URL (ENV-07 Pagix environment) is not set")
		return nil
	case declPath == "":
		t.Log("UNEXECUTED real Pagix environment: ANVILKIT_PAGIX_DECLARATION (the ENV-07 adapter declaration) is not set")
		return nil
	}
	decl, err := pagixadapter.LoadDeclaration(declPath)
	require.NoError(t, err)
	if missing := decl.Missing(); len(missing) > 0 {
		t.Logf("the ENV-07 declaration omits %v: those operations are refused (UNVERIFIED upstream capability)", missing)
	}
	client, err := pagixadapter.NewClient(decl, pagixadapter.ClientOptions{
		BaseURL: url, TokenFile: os.Getenv("ANVILKIT_PAGIX_TOKEN_FILE"),
		CertFile: os.Getenv("ANVILKIT_PAGIX_CERT_FILE"), KeyFile: os.Getenv("ANVILKIT_PAGIX_KEY_FILE"), CAFile: os.Getenv("ANVILKIT_PAGIX_CA_FILE"),
	})
	require.NoError(t, err)
	return &target{name: "real-" + decl.APIVersion, lease: client, source: client, release: client}
}

func TestPagixPortConformance(t *testing.T) {
	ctx := context.Background()
	for _, tg := range targets(t) {
		t.Run(tg.name, func(t *testing.T) {
			stamp := time.Now().UTC().Format("150405.000000")

			t.Run("lease acquire, renew under the same occurrence, query and release", func(t *testing.T) {
				subject := "source:conformance-" + stamp
				in := activities.LeaseInput{OperationID: "op_c", TenantID: "tenant_c", Subject: subject, Owner: "op_c", Occurrence: 1, TTL: 10 * time.Minute}
				held, err := tg.lease.Acquire(ctx, in)
				require.NoError(t, err)
				require.Equal(t, "held", held.State)
				require.NotEmpty(t, held.LeaseID)
				require.NotNil(t, held.ExpiresAt)
				renew := in
				renew.Occurrence, renew.LeaseID, renew.Fence = 2, held.LeaseID, held.Fence
				first, err := tg.lease.Renew(ctx, renew)
				require.NoError(t, err)
				require.Equal(t, "held", first.State)
				again, err := tg.lease.Renew(ctx, renew)
				require.NoError(t, err)
				require.Equal(t, first.ExpiresAt.UTC(), again.ExpiresAt.UTC(), "the same occurrence answers the expiry it produced")
				other := in
				other.Owner = "op_other"
				contested, err := tg.lease.Acquire(ctx, other)
				require.NoError(t, err)
				require.Equal(t, "lost", contested.State, "another owner never takes a held lease")
				q, err := tg.lease.Query(ctx, renew)
				require.NoError(t, err)
				require.Equal(t, "held", q.State)
				rel, err := tg.lease.Release(ctx, renew)
				require.NoError(t, err)
				require.Equal(t, "lost", rel.State)
				after, err := tg.lease.Query(ctx, renew)
				require.NoError(t, err)
				require.Equal(t, "lost", after.State, "a released lease is never held again")
				if tg.seen != nil {
					for _, e := range tg.seen() {
						if e.Operation == pagixadapter.OpLeaseRenew {
							require.Equal(t, held.Fence, e.Condition, "the renewal is conditional on the fence")
						}
					}
				}
			})

			t.Run("a save under a stale revision is a conflict naming the current one", func(t *testing.T) {
				if tg.double == nil {
					t.Skip("needs an arranged lineage revision (upstream setup); covered by the ENV-07 sandbox procedure")
				}
				lineage := "sha256:" + stamp
				require.NoError(t, tg.double.SetRevision("source:"+lineage, 3))
				save := activities.SaveSourceInput{OperationID: "op_s", TenantID: "tenant_c", Lineage: lineage, BaseRevision: "3", Source: activities.StageArtifact{Digest: "sha256:" + stamp, Handle: "h"}}
				saved, err := tg.source.SaveRevision(ctx, "eff_save_1_"+stamp, save)
				require.NoError(t, err)
				require.Equal(t, "saved", saved.State)
				require.Equal(t, "4", saved.Revision)
				stale, err := tg.source.SaveRevision(ctx, "eff_save_2_"+stamp, save)
				require.NoError(t, err)
				require.Equal(t, "conflict", stale.State)
				require.Equal(t, "4", stale.CurrentRevision)
				orig, known, err := tg.source.QuerySave(ctx, "eff_save_1_"+stamp)
				require.NoError(t, err)
				require.True(t, known)
				require.Equal(t, saved, orig, "the original identity answers the original result")
				_, known, err = tg.source.QuerySave(ctx, "eff_never_"+stamp)
				require.NoError(t, err)
				require.False(t, known, "an identity never sent has no record")
			})

			t.Run("publication: one effect identity, the same answer twice, the receipt queried by original identity", func(t *testing.T) {
				if tg.double == nil {
					t.Skip("publishes a fixture package; run against the ENV-07 sandbox only with its own approved fixture")
				}
				subject := activities.ReleaseSubject{SchemaVersion: 1, ComponentID: "cmp_c", PuckType: "Hero", SourceRevision: "1", PackageName: "@anvilkit/conformance-" + stamp, Version: "1.0.0", SubjectDigest: "sha256:subject-" + stamp}
				in := activities.PublishInput{Target: "npm", Occurrence: 1, ReleaseID: "rel_" + stamp, Subject: subject, Destination: "https://registry.anvilkit.invalid/", Files: []activities.PublishFile{{Path: "package.tgz", Bytes: []byte("tarball " + stamp)}}}
				effect := "eff_pub_" + stamp
				first, err := tg.release.Publish(ctx, effect, in)
				require.NoError(t, err)
				require.Equal(t, "published", first.State)
				again, err := tg.release.Publish(ctx, effect, in)
				require.NoError(t, err)
				require.Equal(t, first, again, "a duplicate under the same idempotency key answers the original")
				queried, known, err := tg.release.QueryPublication(ctx, effect)
				require.NoError(t, err)
				require.True(t, known)
				require.Equal(t, first.Receipt, queried.Receipt)
				var receipt activities.PublicationReceipt
				require.NoError(t, json.Unmarshal(queried.Receipt, &receipt))
				require.Equal(t, effect, receipt.EffectID, "the receipt names its original identity")
				_, known, err = tg.release.QueryPublication(ctx, "eff_never_"+stamp)
				require.NoError(t, err)
				require.False(t, known)
				if tg.seen != nil {
					var keys []string
					for _, e := range tg.seen() {
						if e.Operation == pagixadapter.OpPublish {
							keys = append(keys, e.IdempotencyKey)
						}
					}
					require.Equal(t, []string{effect, effect}, keys, "every publication carries its effect identity as the idempotency key")
				}
			})

			t.Run("a lost publication answer is resolved by the original identity, never by a second effect", func(t *testing.T) {
				if tg.double == nil {
					t.Skip("needs fault injection (the conformance double's); a real sandbox run follows the ENV-07 procedure")
				}
				tg.double.InstallFault("publish", 2, "unknown")
				subject := activities.ReleaseSubject{SchemaVersion: 1, ComponentID: "cmp_l", PuckType: "Hero", SourceRevision: "1", PackageName: "@anvilkit/lost-" + stamp, Version: "1.0.0", SubjectDigest: "sha256:lost-" + stamp}
				in := activities.PublishInput{Target: "npm", Occurrence: 2, ReleaseID: "rel_l_" + stamp, Subject: subject, Destination: "https://registry.anvilkit.invalid/", Files: []activities.PublishFile{{Path: "package.tgz", Bytes: []byte("lost " + stamp)}}}
				effect := "eff_lost_" + stamp
				_, err := tg.release.Publish(ctx, effect, in)
				require.Error(t, err, "the answer is lost: an error, never a result")
				res, known, err := tg.release.QueryPublication(ctx, effect)
				require.NoError(t, err)
				require.True(t, known, "the upstream recorded the effect under its original identity")
				require.Equal(t, "published", res.State)
			})

			t.Run("activation is conditional on the catalog revision and needs the published release", func(t *testing.T) {
				if tg.double == nil {
					t.Skip("activates a fixture release; run against the ENV-07 sandbox only with its own approved fixture")
				}
				rev, err := tg.release.CatalogRevision(ctx)
				require.NoError(t, err)
				lock := activities.LockEntry{SchemaVersion: 1, ComponentID: "cmp_a", PuckType: "Hero", ReleaseID: "rel_a_" + stamp, PackageName: "@anvilkit/a", PackageVersion: "1.0.0", BrowserManifestDigest: "sha256:none"}
				unpublished, err := tg.release.Activate(ctx, "eff_act_0_"+stamp, activities.ActivateInput{Occurrence: 1, Lock: lock, ExpectedCatalogRevision: rev})
				require.NoError(t, err)
				require.Equal(t, "failed", unpublished.State)
				require.Equal(t, "RELEASE_UNAVAILABLE", unpublished.FailureCode, "nothing activates a release that is not published")
				stale, err := tg.release.Activate(ctx, "eff_act_1_"+stamp, activities.ActivateInput{Occurrence: 2, Lock: lock, ExpectedCatalogRevision: rev + "9"})
				require.NoError(t, err)
				require.Equal(t, "conflict", stale.State)
				require.Equal(t, rev, stale.CurrentCatalogRevision)
				q, known, err := tg.release.QueryActivation(ctx, "eff_act_1_"+stamp)
				require.NoError(t, err)
				require.True(t, known)
				require.Equal(t, stale, q)
			})
		})
	}
}

// TestDeclarationRefusesUndeclaredOperations: an operation the declaration
// omits refuses without a request (never a guessed path).
func TestDeclarationRefusesUndeclaredOperations(t *testing.T) {
	raw, err := os.ReadFile("reference-declaration.yaml")
	require.NoError(t, err)
	decl, err := pagixadapter.ParseDeclaration(raw)
	require.NoError(t, err)
	delete(decl.Operations, pagixadapter.OpActivate)
	served, err := pagixadapter.New(t.TempDir())
	require.NoError(t, err)
	ref := NewReference(served, decl)
	srv := httptest.NewServer(ref)
	defer srv.Close()
	client, err := pagixadapter.NewClient(decl, pagixadapter.ClientOptions{BaseURL: srv.URL, HTTPClient: srv.Client()})
	require.NoError(t, err)
	_, err = client.Activate(context.Background(), "eff_x", activities.ActivateInput{})
	require.Error(t, err)
	require.Equal(t, "DEPENDENCY_UNAVAILABLE", activities.RefusalCode(err))
	require.Empty(t, ref.Seen, "nothing was sent")
	_, err = pagixadapter.ParseDeclaration([]byte("schemaVersion: 1\napiVersion: x\noperations:\n  lease.steal: {method: POST, path: /x}\n"))
	require.ErrorContains(t, err, "unknown operation lease.steal")
	_, err = pagixadapter.ParseDeclaration([]byte("schemaVersion: 1\napiVersion: x\noperations:\n  lease.acquire: {method: POST, path: \"/x/{tenantSecret}\"}\n"))
	require.ErrorContains(t, err, "path variable {tenantSecret}")
}
