package activities

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
)

// Release Activities (P21, DD-04 §4, DD-06 §3/§4). Every mutation of the
// release side of Pagix — the review registration, the npm and the
// browser/CSS publication, the activation — is one guarded effect: Control
// records the obligation and issues its single permit (publication only
// under an approval of exactly the subject, activation only after both
// verified receipts), the port is called once under the effect id, and a
// lost answer asks the port about that original identity; nothing is
// sent again. A receipt counts only when it names the subject, the
// release, the version and the subject's destination and the destination
// serves exactly the named bytes.
const (
	NameGetReleaseBinding  = "GetReleaseBinding"
	NameBuildReleaseSubj   = "BuildReleaseSubject"
	NameRegisterReview     = "RegisterReview"
	NameReviewDecision     = "ReviewDecision"
	NamePublishTarget      = "PublishTarget"
	NameCatalogRevision    = "CatalogRevision"
	NameActivateRelease    = "ActivateRelease"
	NameRecordRelease      = "RecordRelease"
	releaseEntryPath       = "index.js"
	releaseManifestPath    = "browser-manifest.json"
	maxCertificationBytes  = 4 << 20
	maxPublicationArtifact = 64 << 20
)

// ReleaseBinding is a release operation's accepted binding: the lineage,
// the exact revision, the package version, the source artifact Control
// bound and the operation's execution epoch and deadline.
type ReleaseBinding struct {
	ProfileID      string
	Lineage        string
	SourceRevision string
	PackageVersion string
	Source         StageArtifact
	ExecutionEpoch uint64
	Deadline       time.Time
}

// ReleaseTargetRecord is one target as the release records it.
type ReleaseTargetRecord struct {
	State          string // pending | succeeded | failed | unknown
	EffectID       string
	ReceiptID      string
	ReceiptDigest  string
	SubjectDigest  string
	Destination    string
	Version        string
	ManifestDigest string
	FailureCode    string
}

// ReleaseApproval is the recorded decision, bound to its digest.
type ReleaseApproval struct {
	State         string
	SubjectDigest string
	ApproverID    string
	DecidedAt     *time.Time
	ReasonCode    string
}

// ReleaseRecordInput is one transition of the release projection.
type ReleaseRecordInput struct {
	OperationID      string
	TenantID         string
	ExpectedRevision uint64
	State            string
	Subject          *ReleaseSubject
	ReleaseID        string
	ReviewEffectID   string
	Approval         *ReleaseApproval
	ApprovalDeadline *time.Time
	Npm              ReleaseTargetRecord
	Browser          ReleaseTargetRecord
	Activation       ReleaseTargetRecord
	CatalogRevision  string
	FailureCode      string
}

// ReleaseEffectInput prepares one guarded release mutation.
type ReleaseEffectInput struct {
	OperationID      string
	TenantID         string
	CommandID        string
	AttemptID        string
	ExecutionEpoch   uint64
	Kind             string // review | publication | activation
	Occurrence       uint64
	CanonicalSubject string
	Deadline         time.Time
}

// ReleaseControl is the Workflow's port to Control for releases.
type ReleaseControl interface {
	GetReleaseBinding(ctx context.Context, tenantID, operationID string) (ReleaseBinding, error)
	RecordRelease(ctx context.Context, in ReleaseRecordInput) (uint64, error)
	PrepareReleaseEffect(ctx context.Context, in ReleaseEffectInput) (EffectPermit, error)
}

// ReleaseArtifacts are the certified stage's artifacts the targets publish.
type ReleaseArtifacts struct {
	Npm     StageArtifact
	Browser StageArtifact
	// CSS pairs each stylesheet artifact with the path the certification
	// bound it under (its source path).
	CSS []ReleaseStyle
}

type ReleaseStyle struct {
	Path     string
	Artifact StageArtifact
}

// BuildSubjectInput builds the subject from the certified stage.
type BuildSubjectInput struct {
	OperationID  string
	TenantID     string
	Binding      ReleaseBinding
	Stage        AcceptedStage
	Destinations ReleaseDestinations
}

// BuiltSubject is the subject and the artifacts its targets publish.
type BuiltSubject struct {
	Subject   ReleaseSubject
	Artifacts ReleaseArtifacts
}

// ReleaseEffectResult is the outcome of one guarded release mutation.
type ReleaseEffectResult struct {
	State    string // succeeded | failed | unknown | denied | conflict
	EffectID string
	Target   ReleaseTargetRecord
	// Review registration
	ReleaseID string
	// Activation
	CatalogRevision string
}

// ReleaseEffectCall is one mutation the Workflow asks for.
type ReleaseEffectCall struct {
	Effect    ReleaseEffectInput
	Subject   ReleaseSubject
	ReleaseID string
	Target    string // npm | browser (publication)
	Artifacts ReleaseArtifacts
	// Activation: the catalog revision the conditional write expects.
	ExpectedCatalogRevision string
}

func refusedf(code, format string, args ...any) error {
	return Refused(code, fmt.Errorf(format, args...))
}

// GetReleaseBinding reads the release's binding and proves with the source
// authority that the bound bytes are the named revision (the exact-revision
// read of the SourcePort): an artifact whose digest is not the revision's
// is SOURCE_REVISION_MISMATCH, an unknown revision SOURCE_UNAVAILABLE.
func (a *LifecycleActivities) GetReleaseBinding(ctx context.Context, tenantID, operationID string) (ReleaseBinding, error) {
	b, err := a.Release.GetReleaseBinding(ctx, tenantID, operationID)
	if err != nil {
		return ReleaseBinding{}, err
	}
	digest, known, err := a.ReleasePort.RevisionDigest(ctx, b.Lineage, b.SourceRevision)
	if err != nil {
		return ReleaseBinding{}, err
	}
	if !known {
		return ReleaseBinding{}, refusedf("SOURCE_UNAVAILABLE", "the source authority holds no revision %s of the lineage", b.SourceRevision)
	}
	if digest != b.Source.Digest {
		return ReleaseBinding{}, refusedf("SOURCE_REVISION_MISMATCH", "revision %s holds %s, the release binds %s", b.SourceRevision, digest, b.Source.Digest)
	}
	return b, nil
}

type certificationDoc struct {
	Verdict       string `json:"verdict"`
	Certification *struct {
		Verdict  string `json:"verdict"`
		Complete bool   `json:"complete"`
		Bindings struct {
			ComponentID            string         `json:"componentId"`
			PuckType               string         `json:"puckType"`
			SourceDigest           string         `json:"sourceDigest"`
			PackageName            string         `json:"packageName"`
			Version                string         `json:"version"`
			BuildProfileID         string         `json:"buildProfileId"`
			BuildProfileDigest     string         `json:"buildProfileDigest"`
			ValidatorProfileID     string         `json:"validatorProfileId"`
			ValidatorProfileDigest string         `json:"validatorProfileDigest"`
			HostAbi                string         `json:"hostAbi"`
			HostAbiDigest          string         `json:"hostAbiDigest"`
			Npm                    ArtifactDigest `json:"npm"`
			Browser                ArtifactDigest `json:"browser"`
			CSS                    []FileReceipt  `json:"css"`
		} `json:"bindings"`
	} `json:"certification"`
}

// BuildReleaseSubject builds the exact subject from the operation's
// accepted certified stage: the certification evidence (read from Control's
// store, bytes verified) names the component, the package, the profiles
// and the artifact digests; the certified stage must hold exactly those
// artifacts; the version must be the one the release was accepted for;
// the revision is the source authority's, the destinations the release
// profile's. A mismatch is a refusal, never a repaired subject.
func (a *LifecycleActivities) BuildReleaseSubject(ctx context.Context, in BuildSubjectInput) (BuiltSubject, error) {
	var evidence, npm, browser *StageArtifact
	var css []StageArtifact
	for i := range in.Stage.Artifacts {
		art := &in.Stage.Artifacts[i]
		switch art.Class {
		case "evidence":
			evidence = art
		case "npm":
			npm = art
		case "browser":
			browser = art
		case "css":
			css = append(css, *art)
		}
	}
	if in.Stage.Verdict != "certified" || evidence == nil || npm == nil || browser == nil || len(css) == 0 {
		return BuiltSubject{}, refusedf("CERTIFICATION_MISSING", "the certified stage lacks its evidence, npm, browser or css artifacts")
	}
	raw, _, err := a.Artifacts.Read(ctx, in.OperationID, evidence.TransferID, evidence.Handle, maxCertificationBytes)
	if err != nil {
		return BuiltSubject{}, err
	}
	var doc certificationDoc
	if err := json.Unmarshal(raw, &doc); err != nil || doc.Certification == nil {
		return BuiltSubject{}, refusedf("CERTIFICATION_MISSING", "the evidence holds no certification")
	}
	c := doc.Certification
	b := c.Bindings
	switch {
	case doc.Verdict != "certified" || c.Verdict != "certified" || !c.Complete:
		return BuiltSubject{}, refusedf("CERTIFICATION_MISSING", "the evidence is not a complete certification")
	case b.Npm.Digest != npm.Digest || b.Npm.SizeBytes != npm.SizeBytes || b.Browser.Digest != browser.Digest || b.Browser.SizeBytes != browser.SizeBytes:
		return BuiltSubject{}, refusedf("CERTIFICATION_MISMATCH", "the certified npm or browser digests are not the stage's artifacts")
	case len(b.CSS) != len(css):
		return BuiltSubject{}, refusedf("CERTIFICATION_MISMATCH", "the certification binds %d stylesheets, the stage holds %d", len(b.CSS), len(css))
	case b.Version != in.Binding.PackageVersion:
		return BuiltSubject{}, refusedf("VERSION_MISMATCH", "the certified source declares version %s, the release names %s", b.Version, in.Binding.PackageVersion)
	}
	out := BuiltSubject{Artifacts: ReleaseArtifacts{Npm: *npm, Browser: *browser}}
	subject := ReleaseSubject{
		SchemaVersion: 1, ComponentID: b.ComponentID, PuckType: b.PuckType, SourceRevision: in.Binding.SourceRevision, SourceDigest: b.SourceDigest,
		PackageName: b.PackageName, Version: b.Version, Npm: b.Npm, Browser: b.Browser, BuildProfileID: b.BuildProfileID, BuildProfileDigest: b.BuildProfileDigest,
		ValidatorProfileID: b.ValidatorProfileID, ValidatorProfileDigest: b.ValidatorProfileDigest, HostAbi: b.HostAbi, HostAbiDigest: b.HostAbiDigest,
		Destinations: in.Destinations, CertificationEvidenceDigest: evidence.Digest,
	}
	for _, f := range b.CSS {
		i := slices.IndexFunc(css, func(s StageArtifact) bool { return s.Digest == f.Digest && s.SizeBytes == f.SizeBytes })
		if i < 0 || !validReleasePath(f.Path) {
			return BuiltSubject{}, refusedf("CERTIFICATION_MISMATCH", "stylesheet %s is not a stage artifact under a publishable path", f.Path)
		}
		subject.CSS = append(subject.CSS, ArtifactDigest{Digest: f.Digest, SizeBytes: f.SizeBytes})
		out.Artifacts.CSS = append(out.Artifacts.CSS, ReleaseStyle{Path: f.Path, Artifact: css[i]})
		css = slices.Delete(css, i, i+1)
	}
	if subject.SubjectDigest, err = ComputeSubjectDigest(subject); err != nil {
		return BuiltSubject{}, err
	}
	out.Subject = subject
	return out, nil
}

func validReleasePath(p string) bool {
	if p == "" || len(p) > 512 || p == releaseEntryPath || p == releaseManifestPath {
		return false
	}
	for _, seg := range bytes.Split([]byte(p), []byte("/")) {
		if len(seg) == 0 || string(seg) == "." || string(seg) == ".." {
			return false
		}
		for _, r := range string(seg) {
			if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-') {
				return false
			}
		}
	}
	return true
}

// BrowserManifestOf is the deterministic browser manifest of the release:
// the canonical JSON of components' browserManifest.
func BrowserManifestOf(s ReleaseSubject, releaseID string, styles []ReleaseStyle) ([]byte, error) {
	m := BrowserManifest{
		SchemaVersion: 1, HostProfileID: s.HostAbi,
		Identity: ManifestIdentity{ComponentID: s.ComponentID, PuckType: s.PuckType, ReleaseID: releaseID, PackageName: s.PackageName, PackageVersion: s.Version},
		Entry:    FileReceipt{Path: releaseEntryPath, Digest: s.Browser.Digest, SizeBytes: s.Browser.SizeBytes},
	}
	for _, st := range styles {
		m.Styles = append(m.Styles, FileReceipt{Path: st.Path, Digest: st.Artifact.Digest, SizeBytes: st.Artifact.SizeBytes})
	}
	return CanonicalJSON(m)
}

// LockOf is the exact lock entry of an activated release.
func LockOf(s ReleaseSubject, releaseID, manifestDigest string) LockEntry {
	return LockEntry{SchemaVersion: 1, ComponentID: s.ComponentID, PuckType: s.PuckType, ReleaseID: releaseID, PackageName: s.PackageName,
		PackageVersion: s.Version, BrowserManifestDigest: manifestDigest, HostProfileID: s.HostAbi}
}

func (a *LifecycleActivities) RecordRelease(ctx context.Context, in ReleaseRecordInput) (uint64, error) {
	return a.Release.RecordRelease(ctx, in)
}

func (a *LifecycleActivities) ReviewDecision(ctx context.Context, releaseID string) (ReviewDecision, error) {
	return a.ReleasePort.ReviewDecision(ctx, releaseID)
}

func (a *LifecycleActivities) CatalogRevision(ctx context.Context) (string, error) {
	return a.ReleasePort.CatalogRevision(ctx)
}

// observe records one outcome of the effect with Control (idempotent per
// source and sequence).
func (a *LifecycleActivities) observe(ctx context.Context, tenantID, effectID, source, outcome, receiptDigest, ref string) error {
	return a.Generation.ObserveEffect(ctx, tenantID, effectID, source, 1, outcome, receiptDigest, ref, time.Now().UTC())
}

// guarded runs the permit protocol shared by every release mutation: a
// consumed permit sends once through send, a permit consumed earlier (a
// lost answer, a replaced worker) asks query about the original identity,
// a denial is answered as such. settle turns the port's answer into the
// result and the observation Control records; an answer that never came
// is recorded unknown.
func (a *LifecycleActivities) guarded(ctx context.Context, e ReleaseEffectInput,
	send func(effectID string) (any, error), query func(effectID string) (any, bool, error),
	settle func(effectID string, answer any) (ReleaseEffectResult, string, string, string),
) (ReleaseEffectResult, error) {
	permit, err := a.Release.PrepareReleaseEffect(ctx, e)
	if err != nil {
		return ReleaseEffectResult{}, err
	}
	if permit.State == "denied" {
		return ReleaseEffectResult{State: "denied", EffectID: permit.EffectID, Target: ReleaseTargetRecord{State: "failed", EffectID: permit.EffectID, FailureCode: permit.DenialCode}}, nil
	}
	source := "workflow-query"
	var answer any
	if permit.Permitted {
		source = "workflow"
		answer, err = send(permit.EffectID)
		if err != nil {
			// The mutation may have happened: recorded unknown, never resent.
			if oerr := a.observe(ctx, e.TenantID, permit.EffectID, "workflow", "unknown", "", ""); oerr != nil {
				return ReleaseEffectResult{}, oerr
			}
			return ReleaseEffectResult{State: "unknown", EffectID: permit.EffectID, Target: ReleaseTargetRecord{State: "unknown", EffectID: permit.EffectID}}, nil
		}
	} else {
		var known bool
		answer, known, err = query(permit.EffectID)
		if err != nil {
			return ReleaseEffectResult{}, err
		}
		if !known {
			return ReleaseEffectResult{State: "unknown", EffectID: permit.EffectID, Target: ReleaseTargetRecord{State: "unknown", EffectID: permit.EffectID}}, nil
		}
	}
	res, outcome, receiptDigest, ref := settle(permit.EffectID, answer)
	if permit.State != "succeeded" && permit.State != "failed" {
		if err := a.observe(ctx, e.TenantID, permit.EffectID, source, outcome, receiptDigest, ref); err != nil {
			return ReleaseEffectResult{}, err
		}
	}
	return res, nil
}

// RegisterReview registers the exact subject for review under the review
// effect; the release id is the port's.
func (a *LifecycleActivities) RegisterReview(ctx context.Context, call ReleaseEffectCall) (ReleaseEffectResult, error) {
	return a.guarded(ctx, call.Effect,
		func(id string) (any, error) {
			return a.ReleasePort.RegisterReview(ctx, id, RegisterReviewInput{Occurrence: call.Effect.Occurrence, Subject: call.Subject})
		},
		func(id string) (any, bool, error) { return a.ReleasePort.QueryReview(ctx, id) },
		func(id string, answer any) (ReleaseEffectResult, string, string, string) {
			ref := answer.(ReviewRef)
			if ref.SubjectDigest != call.Subject.SubjectDigest || ref.ReleaseID == "" {
				return ReleaseEffectResult{State: "failed", EffectID: id}, "succeeded", ref.SubjectDigest, "review:" + ref.ReleaseID
			}
			return ReleaseEffectResult{State: "succeeded", EffectID: id, ReleaseID: ref.ReleaseID}, "succeeded", ref.SubjectDigest, "review:" + ref.ReleaseID
		})
}

// PublishTarget publishes one target of the approved subject under its
// publication effect, or reads the original identity's outcome.
func (a *LifecycleActivities) PublishTarget(ctx context.Context, call ReleaseEffectCall) (ReleaseEffectResult, error) {
	s := call.Subject
	manifest, err := BrowserManifestOf(s, call.ReleaseID, call.Artifacts.CSS)
	if err != nil {
		return ReleaseEffectResult{}, err
	}
	destination := s.Destinations.NpmRegistry
	if call.Target == "browser" {
		destination = s.Destinations.BrowserOrigin
	}
	// The certified bytes are read from Control's store before the permit
	// is asked for: an unavailable artifact fails here, retried as a safe
	// read, and never consumes a permit or becomes a possible send.
	in := PublishInput{Target: call.Target, Occurrence: call.Effect.Occurrence, ReleaseID: call.ReleaseID, Subject: s, Destination: destination}
	read := func(art StageArtifact, path string) error {
		body, _, err := a.Artifacts.Read(ctx, call.Effect.OperationID, art.TransferID, art.Handle, maxPublicationArtifact)
		if err != nil {
			return fmt.Errorf("certified artifact %s: %w", art.Class, err)
		}
		in.Files = append(in.Files, PublishFile{Path: path, Bytes: body})
		return nil
	}
	if call.Target == "npm" {
		if err := read(call.Artifacts.Npm, tarballName(s)); err != nil {
			return ReleaseEffectResult{}, err
		}
	} else {
		if err := read(call.Artifacts.Browser, releaseEntryPath); err != nil {
			return ReleaseEffectResult{}, err
		}
		for _, st := range call.Artifacts.CSS {
			if err := read(st.Artifact, st.Path); err != nil {
				return ReleaseEffectResult{}, err
			}
		}
		in.Manifest = manifest
	}
	return a.guarded(ctx, call.Effect,
		func(id string) (any, error) { return a.ReleasePort.Publish(ctx, id, in) },
		func(id string) (any, bool, error) { return a.ReleasePort.QueryPublication(ctx, id) },
		func(id string, answer any) (ReleaseEffectResult, string, string, string) {
			res := answer.(PublishResult)
			if res.State != "published" {
				code := firstCode(res.FailureCode, "PUBLICATION_FAILED")
				return ReleaseEffectResult{State: "failed", EffectID: id, Target: ReleaseTargetRecord{State: "failed", EffectID: id, FailureCode: code}}, "failed", s.SubjectDigest, "failed:" + code
			}
			receiptDigest := Digest(res.Receipt)
			target, code := a.verifyPublication(ctx, call, id, destination, manifest, res.Receipt)
			target.ReceiptDigest = receiptDigest
			// The upstream answered with a receipt: the effect happened. A
			// receipt that does not bind this subject, its destination or
			// bytes the destination serves is a failed target, never a
			// delivered one.
			if code != "" {
				target.State, target.FailureCode = "failed", code
				return ReleaseEffectResult{State: "failed", EffectID: id, Target: target}, "succeeded", receiptDigest, "receipt:" + target.ReceiptID
			}
			target.State = "succeeded"
			return ReleaseEffectResult{State: "succeeded", EffectID: id, Target: target}, "succeeded", receiptDigest, "receipt:" + target.ReceiptID
		})
}

func firstCode(codes ...string) string {
	for _, c := range codes {
		if c != "" {
			return c
		}
	}
	return ""
}

func tarballName(s ReleaseSubject) string {
	name := s.PackageName
	if i := bytes.LastIndexByte([]byte(name), '/'); i >= 0 {
		name = name[i+1:]
	}
	return name + "-" + s.Version + ".tgz"
}

// verifyPublication checks the receipt against the subject and reads every
// named file back from the destination.
func (a *LifecycleActivities) verifyPublication(ctx context.Context, call ReleaseEffectCall, effectID, destination string, manifest, raw []byte) (ReleaseTargetRecord, string) {
	s := call.Subject
	var r PublicationReceipt
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return ReleaseTargetRecord{EffectID: effectID}, "RECEIPT_INVALID"
	}
	t := ReleaseTargetRecord{EffectID: effectID, ReceiptID: r.ReceiptID, SubjectDigest: r.SubjectDigest, Destination: r.Destination, Version: r.Version, ManifestDigest: r.ManifestDigest}
	want := []FileReceipt{}
	if call.Target == "npm" {
		want = append(want, FileReceipt{Path: tarballName(s), Digest: s.Npm.Digest, SizeBytes: s.Npm.SizeBytes})
	} else {
		want = append(want, FileReceipt{Path: releaseEntryPath, Digest: s.Browser.Digest, SizeBytes: s.Browser.SizeBytes})
		for _, st := range call.Artifacts.CSS {
			want = append(want, FileReceipt{Path: st.Path, Digest: st.Artifact.Digest, SizeBytes: st.Artifact.SizeBytes})
		}
	}
	switch {
	case r.SchemaVersion != 1 || r.Target != call.Target || r.EffectID != effectID || r.ReceiptID == "":
		return t, "RECEIPT_INVALID"
	case r.SubjectDigest != s.SubjectDigest || r.ReleaseID != call.ReleaseID || r.PackageName != s.PackageName || r.Version != s.Version:
		return t, "RECEIPT_SUBJECT_MISMATCH"
	case r.Destination != destination:
		return t, "DESTINATION_MISMATCH"
	case call.Target == "browser" && r.ManifestDigest != Digest(manifest):
		return t, "RECEIPT_SUBJECT_MISMATCH"
	case !slices.Equal(r.Files, want):
		return t, "RECEIPT_SUBJECT_MISMATCH"
	}
	// Availability: the destination serves exactly the receipt's bytes.
	check := func(path, digest string) bool {
		name := path
		if call.Target == "npm" {
			name = s.PackageName + "@" + s.Version
		}
		body, err := a.ReleasePort.FetchPublished(ctx, call.Target, destination, call.ReleaseID, name)
		return err == nil && Digest(body) == digest
	}
	for _, f := range want {
		if !check(f.Path, f.Digest) {
			return t, "ARTIFACT_UNAVAILABLE"
		}
	}
	if call.Target == "browser" && !check(releaseManifestPath, Digest(manifest)) {
		return t, "ARTIFACT_UNAVAILABLE"
	}
	return t, ""
}

// ActivateRelease makes the exact published release current under the
// activation effect (Control permits it only after both verified receipts
// under the approval) with the catalog revision the caller read.
func (a *LifecycleActivities) ActivateRelease(ctx context.Context, call ReleaseEffectCall) (ReleaseEffectResult, error) {
	s := call.Subject
	manifest, err := BrowserManifestOf(s, call.ReleaseID, call.Artifacts.CSS)
	if err != nil {
		return ReleaseEffectResult{}, err
	}
	lock := LockOf(s, call.ReleaseID, Digest(manifest))
	return a.guarded(ctx, call.Effect,
		func(id string) (any, error) {
			return a.ReleasePort.Activate(ctx, id, ActivateInput{Occurrence: call.Effect.Occurrence, Subject: s, Lock: lock, ExpectedCatalogRevision: call.ExpectedCatalogRevision})
		},
		func(id string) (any, bool, error) { return a.ReleasePort.QueryActivation(ctx, id) },
		func(id string, answer any) (ReleaseEffectResult, string, string, string) {
			res := answer.(ActivationResult)
			switch res.State {
			case "conflict":
				return ReleaseEffectResult{State: "conflict", EffectID: id, CatalogRevision: res.CurrentCatalogRevision,
					Target: ReleaseTargetRecord{State: "failed", EffectID: id, FailureCode: "ACTIVATION_CONFLICT"}}, "failed", s.SubjectDigest, "conflict:" + res.CurrentCatalogRevision
			case "activated":
			default:
				code := firstCode(res.FailureCode, "ACTIVATION_FAILED")
				return ReleaseEffectResult{State: "failed", EffectID: id, Target: ReleaseTargetRecord{State: "failed", EffectID: id, FailureCode: code}}, "failed", s.SubjectDigest, "failed:" + code
			}
			var r ActivationReceipt
			digest := Digest(res.Receipt)
			if err := json.Unmarshal(res.Receipt, &r); err != nil || r.EffectID != id || r.SubjectDigest != s.SubjectDigest || r.Lock != lock || r.CatalogRevision == "" {
				return ReleaseEffectResult{State: "failed", EffectID: id, Target: ReleaseTargetRecord{State: "failed", EffectID: id, ReceiptDigest: digest, FailureCode: "RECEIPT_SUBJECT_MISMATCH"}},
					"succeeded", digest, "receipt:" + r.ReceiptID
			}
			return ReleaseEffectResult{State: "succeeded", EffectID: id, CatalogRevision: r.CatalogRevision,
				Target: ReleaseTargetRecord{State: "succeeded", EffectID: id, ReceiptID: r.ReceiptID, ReceiptDigest: digest, SubjectDigest: r.SubjectDigest}}, "succeeded", digest, "receipt:" + r.ReceiptID
		})
}

// errUnavailablePort answers every release call when no port is configured.
var errUnavailablePort = errors.New("no Pagix release port is configured (ENV-07)")

// ReleaseUnavailable is the release port of a worker without a Pagix
// placement: every call is DEPENDENCY_UNAVAILABLE.
type ReleaseUnavailable struct{}

func (ReleaseUnavailable) RevisionDigest(context.Context, string, string) (string, bool, error) {
	return "", false, Refused("DEPENDENCY_UNAVAILABLE", errUnavailablePort)
}
func (ReleaseUnavailable) RegisterReview(context.Context, string, RegisterReviewInput) (ReviewRef, error) {
	return ReviewRef{}, Refused("DEPENDENCY_UNAVAILABLE", errUnavailablePort)
}
func (ReleaseUnavailable) QueryReview(context.Context, string) (ReviewRef, bool, error) {
	return ReviewRef{}, false, Refused("DEPENDENCY_UNAVAILABLE", errUnavailablePort)
}
func (ReleaseUnavailable) ReviewDecision(context.Context, string) (ReviewDecision, error) {
	return ReviewDecision{}, Refused("DEPENDENCY_UNAVAILABLE", errUnavailablePort)
}
func (ReleaseUnavailable) Publish(context.Context, string, PublishInput) (PublishResult, error) {
	return PublishResult{}, Refused("DEPENDENCY_UNAVAILABLE", errUnavailablePort)
}
func (ReleaseUnavailable) QueryPublication(context.Context, string) (PublishResult, bool, error) {
	return PublishResult{}, false, Refused("DEPENDENCY_UNAVAILABLE", errUnavailablePort)
}
func (ReleaseUnavailable) FetchPublished(context.Context, string, string, string, string) ([]byte, error) {
	return nil, Refused("DEPENDENCY_UNAVAILABLE", errUnavailablePort)
}
func (ReleaseUnavailable) Activate(context.Context, string, ActivateInput) (ActivationResult, error) {
	return ActivationResult{}, Refused("DEPENDENCY_UNAVAILABLE", errUnavailablePort)
}
func (ReleaseUnavailable) QueryActivation(context.Context, string) (ActivationResult, bool, error) {
	return ActivationResult{}, false, Refused("DEPENDENCY_UNAVAILABLE", errUnavailablePort)
}
func (ReleaseUnavailable) CatalogRevision(context.Context) (string, error) {
	return "", Refused("DEPENDENCY_UNAVAILABLE", errUnavailablePort)
}
