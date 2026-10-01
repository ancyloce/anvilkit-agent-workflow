package activities_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/adapters/pagix"
)

// fakeControl is Control's effect permits and observations as the release
// Activities see them: the first prepare of a command consumes the permit,
// later prepares answer the recorded state, a kind can be denied.
type fakeControl struct {
	activities.GenerationControl
	mu           sync.Mutex
	effects      map[string]*activities.EffectPermit
	observations map[string][]string
	deny         map[string]string
	binding      activities.ReleaseBinding
}

func newFakeControl() *fakeControl {
	return &fakeControl{effects: map[string]*activities.EffectPermit{}, observations: map[string][]string{}, deny: map[string]string{}}
}

func (f *fakeControl) PrepareReleaseEffect(_ context.Context, in activities.ReleaseEffectInput) (activities.EffectPermit, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e, ok := f.effects[in.CommandID]; ok {
		out := *e
		out.Permitted = false
		return out, nil
	}
	e := &activities.EffectPermit{EffectID: fmt.Sprintf("eff_%d", len(f.effects)+1), State: "permitted", Permitted: true}
	if code, ok := f.deny[in.Kind]; ok {
		e.State, e.Permitted, e.DenialCode = "denied", false, code
	}
	f.effects[in.CommandID] = e
	return *e, nil
}

func (f *fakeControl) GetReleaseBinding(context.Context, string, string) (activities.ReleaseBinding, error) {
	return f.binding, nil
}

func (f *fakeControl) RecordRelease(context.Context, activities.ReleaseRecordInput) (uint64, error) {
	return 0, errors.New("not used")
}

type observingControl struct{ *fakeControl }

func (o observingControl) ObserveEffect(_ context.Context, _ string, effectID, source string, _ uint64, outcome, _, _ string, _ time.Time) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.observations[effectID] = append(o.observations[effectID], source+":"+outcome)
	for _, e := range o.effects {
		if e.EffectID == effectID && outcome != "unknown" {
			e.State = outcome
		} else if e.EffectID == effectID {
			e.State = "unknown"
		}
	}
	return nil
}

// fakeArtifacts serves the certified bytes by handle.
type fakeArtifacts struct {
	activities.Artifacts
	bodies map[string][]byte
}

func (f fakeArtifacts) Read(_ context.Context, _, _, handle string, _ int64) ([]byte, activities.ArtifactBinding, error) {
	b, ok := f.bodies[handle]
	if !ok {
		return nil, activities.ArtifactBinding{}, errors.New("artifact unavailable")
	}
	return b, activities.ArtifactBinding{Handle: handle, Digest: activities.Digest(b)}, nil
}

type releaseFixture struct {
	acts    *activities.LifecycleActivities
	ctl     observingControl
	double  *pagix.Double
	subject activities.ReleaseSubject
	arts    activities.ReleaseArtifacts
	dir     string
}

func art(handle, class string, body []byte) activities.StageArtifact {
	return activities.StageArtifact{Handle: handle, Class: class, Digest: activities.Digest(body), SizeBytes: fmt.Sprint(len(body)), TransferID: "x" + handle, ObjectVersion: "v1"}
}

func newReleaseFixture(t *testing.T) *releaseFixture {
	t.Helper()
	dir := t.TempDir()
	d, err := pagix.New(dir)
	require.NoError(t, err)
	tarball, module, css := []byte("npm tarball"), []byte("export default {}"), []byte(".x{}")
	bodies := map[string][]byte{"h_npm": tarball, "h_mod": module, "h_css": css}
	arts := activities.ReleaseArtifacts{Npm: art("h_npm", "npm", tarball), Browser: art("h_mod", "browser", module),
		CSS: []activities.ReleaseStyle{{Path: "styles/x.css", Artifact: art("h_css", "css", css)}}}
	s := activities.ReleaseSubject{SchemaVersion: 1, ComponentID: "cmp_x", PuckType: "Hero", SourceRevision: "2", PackageName: "@anvilkit/x", Version: "1.0.0",
		Npm: activities.ArtifactDigest{Digest: arts.Npm.Digest, SizeBytes: arts.Npm.SizeBytes}, Browser: activities.ArtifactDigest{Digest: arts.Browser.Digest, SizeBytes: arts.Browser.SizeBytes},
		CSS: []activities.ArtifactDigest{{Digest: arts.CSS[0].Artifact.Digest, SizeBytes: arts.CSS[0].Artifact.SizeBytes}}, HostAbi: "host-abi-dev-v1",
		Destinations: activities.ReleaseDestinations{NpmRegistry: "https://registry.anvilkit.invalid/", BrowserOrigin: "https://components.anvilkit.invalid"}}
	s.SubjectDigest, err = activities.ComputeSubjectDigest(s)
	require.NoError(t, err)
	ctl := observingControl{newFakeControl()}
	acts := &activities.LifecycleActivities{Release: ctl, ReleasePort: d, Generation: ctl, Artifacts: fakeArtifacts{bodies: bodies}}
	return &releaseFixture{acts: acts, ctl: ctl, double: d, subject: s, arts: arts, dir: dir}
}

func (f *releaseFixture) call(kind, target string, occurrence uint64) activities.ReleaseEffectCall {
	return activities.ReleaseEffectCall{
		Effect:  activities.ReleaseEffectInput{OperationID: "op", TenantID: "t", CommandID: "op:" + kind + ":" + target, AttemptID: "att", ExecutionEpoch: 1, Kind: kind, Occurrence: occurrence},
		Subject: f.subject, ReleaseID: "rel_x", Target: target, Artifacts: f.arts,
	}
}

func TestPublishTarget(t *testing.T) {
	ctx := context.Background()

	t.Run("both targets publish once with verified, read-back receipts", func(t *testing.T) {
		f := newReleaseFixture(t)
		npm, err := f.acts.PublishTarget(ctx, f.call("publication", "npm", 1))
		require.NoError(t, err)
		require.Equal(t, "succeeded", npm.State)
		require.Equal(t, f.subject.SubjectDigest, npm.Target.SubjectDigest)
		browser, err := f.acts.PublishTarget(ctx, f.call("publication", "browser", 2))
		require.NoError(t, err)
		require.Equal(t, "succeeded", browser.State, browser.Target.FailureCode)
		manifest, err := activities.BrowserManifestOf(f.subject, "rel_x", f.arts.CSS)
		require.NoError(t, err)
		require.Equal(t, activities.Digest(manifest), browser.Target.ManifestDigest)
		again, err := f.acts.PublishTarget(ctx, f.call("publication", "browser", 2))
		require.NoError(t, err)
		require.Equal(t, browser.Target, again.Target, "a repeated call answers the original identity without a second send")
	})

	t.Run("a receipt naming another destination is a failed target, never a delivery", func(t *testing.T) {
		f := newReleaseFixture(t)
		f.double.InstallFault("publish", 1, "wrong-destination")
		res, err := f.acts.PublishTarget(ctx, f.call("publication", "npm", 1))
		require.NoError(t, err)
		require.Equal(t, "failed", res.State)
		require.Equal(t, "DESTINATION_MISMATCH", res.Target.FailureCode)
		require.Equal(t, []string{"workflow:succeeded"}, f.ctl.observations[res.EffectID], "the effect happened upstream and is recorded so")
	})

	t.Run("a receipt whose bytes the destination does not serve is unavailable", func(t *testing.T) {
		f := newReleaseFixture(t)
		f.double.InstallFault("publish", 2, "unavailable")
		res, err := f.acts.PublishTarget(ctx, f.call("publication", "browser", 2))
		require.NoError(t, err)
		require.Equal(t, "ARTIFACT_UNAVAILABLE", res.Target.FailureCode)
	})

	t.Run("a lost answer stays unknown and the original identity answers later; nothing is resent", func(t *testing.T) {
		f := newReleaseFixture(t)
		f.double.InstallFault("publish", 1, "unknown")
		first, err := f.acts.PublishTarget(ctx, f.call("publication", "npm", 1))
		require.NoError(t, err)
		require.Equal(t, "unknown", first.State)
		second, err := f.acts.PublishTarget(ctx, f.call("publication", "npm", 1))
		require.NoError(t, err)
		require.Equal(t, "succeeded", second.State)
		require.Equal(t, first.EffectID, second.EffectID)
		require.Equal(t, []string{"workflow:unknown", "workflow-query:succeeded"}, f.ctl.observations[first.EffectID])
		entries, err := os.ReadDir(filepath.Join(f.dir, "publications"))
		require.NoError(t, err)
		require.Len(t, entries, 1, "one publication under one effect")
	})

	t.Run("a denied permit sends nothing", func(t *testing.T) {
		f := newReleaseFixture(t)
		f.ctl.deny["publication"] = "APPROVAL_REQUIRED"
		res, err := f.acts.PublishTarget(ctx, f.call("publication", "npm", 1))
		require.NoError(t, err)
		require.Equal(t, "denied", res.State)
		require.Equal(t, "APPROVAL_REQUIRED", res.Target.FailureCode)
		_, known, err := f.double.QueryPublication(ctx, res.EffectID)
		require.NoError(t, err)
		require.False(t, known)
	})

	t.Run("an unavailable certified artifact consumes no permit", func(t *testing.T) {
		f := newReleaseFixture(t)
		f.acts.Artifacts = fakeArtifacts{bodies: map[string][]byte{}}
		_, err := f.acts.PublishTarget(ctx, f.call("publication", "npm", 1))
		require.Error(t, err)
		require.Empty(t, f.ctl.effects, "no permit was asked for")
	})

	t.Run("activation is conditional on the catalog revision and exact", func(t *testing.T) {
		f := newReleaseFixture(t)
		_, err := f.acts.PublishTarget(ctx, f.call("publication", "browser", 2))
		require.NoError(t, err)
		stale := f.call("activation", "activation", 1)
		stale.ExpectedCatalogRevision = "3"
		res, err := f.acts.ActivateRelease(ctx, stale)
		require.NoError(t, err)
		require.Equal(t, "conflict", res.State)
		require.Equal(t, "0", res.CatalogRevision)
		ok := f.call("activation", "activation", 2)
		ok.Effect.CommandID = "op:activation:retry"
		ok.ExpectedCatalogRevision = "0"
		res, err = f.acts.ActivateRelease(ctx, ok)
		require.NoError(t, err)
		require.Equal(t, "succeeded", res.State)
		require.Equal(t, "1", res.CatalogRevision)
	})
}

func TestReleaseBindingAndSubject(t *testing.T) {
	ctx := context.Background()
	f := newReleaseFixture(t)
	_, err := f.double.SaveRevision(ctx, "eff_save", activities.SaveSourceInput{Lineage: "sha256:lin", BaseRevision: "1", Occurrence: 1, Source: activities.StageArtifact{Digest: "sha256:saved"}})
	require.NoError(t, err)
	f.ctl.binding = activities.ReleaseBinding{Lineage: "sha256:lin", SourceRevision: "2", PackageVersion: "1.0.0", Source: activities.StageArtifact{Digest: "sha256:other"}}
	_, err = f.acts.GetReleaseBinding(ctx, "t", "op")
	require.Equal(t, "SOURCE_REVISION_MISMATCH", activities.RefusalCode(err), "bytes that are not the revision's")
	f.ctl.binding.Source.Digest = "sha256:saved"
	b, err := f.acts.GetReleaseBinding(ctx, "t", "op")
	require.NoError(t, err)

	evidence := []byte(fmt.Sprintf(`{"verdict":"certified","certification":{"verdict":"certified","complete":true,"bindings":{"componentId":"cmp_x","puckType":"Hero","sourceDigest":"sha256:m","packageName":"@anvilkit/x","version":"%%s","buildProfileId":"b","buildProfileDigest":"sha256:b","validatorProfileId":"v","validatorProfileDigest":"sha256:v","hostAbi":"host-abi-dev-v1","hostAbiDigest":"sha256:h","npm":{"digest":"%s","sizeBytes":"%s"},"browser":{"digest":"%s","sizeBytes":"%s"},"css":[{"path":"styles/x.css","digest":"%s","sizeBytes":"%s"}]}}}`,
		f.arts.Npm.Digest, f.arts.Npm.SizeBytes, f.arts.Browser.Digest, f.arts.Browser.SizeBytes, f.arts.CSS[0].Artifact.Digest, f.arts.CSS[0].Artifact.SizeBytes))
	build := func(version string) (activities.BuiltSubject, error) {
		body := []byte(fmt.Sprintf(string(evidence), version))
		f.acts.Artifacts = fakeArtifacts{bodies: map[string][]byte{"h_ev": body}}
		stage := activities.AcceptedStage{Verdict: "certified", Artifacts: []activities.StageArtifact{f.arts.Npm, f.arts.Browser, f.arts.CSS[0].Artifact, art("h_ev", "evidence", body)}}
		return f.acts.BuildReleaseSubject(ctx, activities.BuildSubjectInput{Binding: b, Stage: stage, Destinations: f.subject.Destinations})
	}
	_, err = build("1.0.1")
	require.Equal(t, "VERSION_MISMATCH", activities.RefusalCode(err))
	built, err := build("1.0.0")
	require.NoError(t, err)
	require.Equal(t, "2", built.Subject.SourceRevision, "the revision is the source authority's")
	require.Equal(t, []activities.ReleaseStyle{{Path: "styles/x.css", Artifact: f.arts.CSS[0].Artifact}}, built.Artifacts.CSS)
	d, err := activities.ComputeSubjectDigest(built.Subject)
	require.NoError(t, err)
	require.Equal(t, d, built.Subject.SubjectDigest)
}
