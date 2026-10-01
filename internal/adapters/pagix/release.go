package pagix

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// ---- ReviewPort, PublicationPort, ActivationPort (P21) ----
//
// DEVELOPMENT_ONLY doubles of Pagix's release ports (DD-06 §1/§4) until
// ENV-07 names the real ones. Layout under the double's directory:
//
//	reviews/<effect>.json         the subject registered under an effect id
//	decisions/<releaseId>.json    the maintainer's decision (written by the
//	                              maintainer's tool or a test, never by the
//	                              Workflow): {state, subjectDigest, approverId}
//	publications/<effect>.json    the answer recorded under an effect id
//	npm/<package>/<version>.tgz   the npm registry (immutable versions)
//	activations/<effect>.json     the activation answer under an effect id
//	<origin>/<releaseId>/...      the browser origin: browser-manifest.json,
//	                              the entry and its stylesheets
//	<origin>/catalog.json         the activated catalog {schemaVersion,
//	                              revision, releases: [lock entries]}
//
// <origin> is the Studio's DEVELOPMENT_ONLY release directory
// (ANVILKIT_DEV_RELEASES_DIR), so an activated release is what the host's
// loader can pin and reopen. Faults a test installs (kind:occurrence):
// review, publish (unknown | fail | wrong-destination | unavailable) and
// activate (unknown | conflict).

var releaseIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// SetOrigin places the browser origin (default <dir>/origin).
func (d *Double) SetOrigin(dir string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	d.origin = dir
	return nil
}

func (d *Double) originDir() string {
	if d.origin != "" {
		return d.origin
	}
	return filepath.Join(d.dir, "origin")
}

func (d *Double) readJSON(path string, v any) (bool, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, json.Unmarshal(raw, v)
}

func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, mode); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (d *Double) writeJSON(path string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return writeFileAtomic(path, raw, 0o600)
}

// RevisionDigest answers the archive digest recorded for the lineage's
// revision: a save's bytes, or the registered candidate's for revision 1.
func (d *Double) RevisionDigest(ctx context.Context, lineageDigest, revision string) (string, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	l, err := d.readLineage("source:" + lineageDigest)
	if err != nil {
		return "", false, err
	}
	if dg, ok := l.Revisions[revision]; ok {
		return dg, true, nil
	}
	if revision != "1" {
		return "", false, nil
	}
	entries, err := os.ReadDir(filepath.Join(d.dir, "registrations"))
	if err != nil {
		return "", false, err
	}
	for _, e := range entries {
		var rec registration
		if ok, err := d.readJSON(filepath.Join(d.dir, "registrations", e.Name()), &rec); err == nil && ok && rec.Subject == "source:"+lineageDigest {
			return rec.Digest, true, nil
		}
	}
	return "", false, nil
}

type reviewRecord struct {
	activities.ReviewRef
	Subject activities.ReleaseSubject `json:"subject"`
}

// RegisterReview records the subject for review under the effect id; the
// same effect id answers the original registration. The release id is
// derived from the effect id, so it is stable across a lost answer.
func (d *Double) RegisterReview(ctx context.Context, effectID string, in activities.RegisterReviewInput) (activities.ReviewRef, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	path := filepath.Join(d.dir, "reviews", safe(effectID)+".json")
	var rec reviewRecord
	if ok, err := d.readJSON(path, &rec); err != nil || ok {
		return rec.ReviewRef, err
	}
	rec = reviewRecord{ReviewRef: activities.ReviewRef{EffectID: effectID, ReleaseID: "rel_" + safe(effectID)[:20], SubjectDigest: in.Subject.SubjectDigest}, Subject: in.Subject}
	if err := d.writeJSON(path, rec); err != nil {
		return activities.ReviewRef{}, err
	}
	if err := d.writeJSON(filepath.Join(d.dir, "decisions", rec.ReleaseID+".json"), decisionRecord{State: "pending", SubjectDigest: in.Subject.SubjectDigest}); err != nil {
		return activities.ReviewRef{}, err
	}
	if d.fault("review", in.Occurrence) == "unknown" {
		return activities.ReviewRef{}, errors.New("review registration answer lost (fault injected)")
	}
	return rec.ReviewRef, nil
}

func (d *Double) QueryReview(ctx context.Context, effectID string) (activities.ReviewRef, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var rec reviewRecord
	ok, err := d.readJSON(filepath.Join(d.dir, "reviews", safe(effectID)+".json"), &rec)
	return rec.ReviewRef, ok, err
}

type decisionRecord struct {
	State         string     `json:"state"`
	SubjectDigest string     `json:"subjectDigest"`
	ApproverID    string     `json:"approverId,omitempty"`
	DecidedAt     *time.Time `json:"decidedAt,omitempty"`
	ReasonCode    string     `json:"reasonCode,omitempty"`
}

// Decide records a maintainer's decision on a release (the maintainer's
// tool and tests; the Workflow never calls it). The decision names the
// digest it was made for, which may be another subject's.
func (d *Double) Decide(releaseID, state, subjectDigest, approverID string) error {
	if !releaseIDPattern.MatchString(releaseID) {
		return fmt.Errorf("release id %q", releaseID)
	}
	switch state {
	case "approved", "rejected", "invalidated", "pending":
	default:
		return fmt.Errorf("decision %q", state)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now()
	return d.writeJSON(filepath.Join(d.dir, "decisions", releaseID+".json"), decisionRecord{State: state, SubjectDigest: subjectDigest, ApproverID: approverID, DecidedAt: &now})
}

// ReviewDecision answers the recorded decision of the release.
func (d *Double) ReviewDecision(ctx context.Context, releaseID string) (activities.ReviewDecision, error) {
	if !releaseIDPattern.MatchString(releaseID) {
		return activities.ReviewDecision{}, fmt.Errorf("release id %q", releaseID)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	var rec decisionRecord
	ok, err := d.readJSON(filepath.Join(d.dir, "decisions", releaseID+".json"), &rec)
	if err != nil {
		return activities.ReviewDecision{}, err
	}
	if !ok {
		return activities.ReviewDecision{}, fmt.Errorf("release %s has no review", releaseID)
	}
	return activities.ReviewDecision{State: rec.State, SubjectDigest: rec.SubjectDigest, ApproverID: rec.ApproverID, DecidedAt: rec.DecidedAt, ReasonCode: rec.ReasonCode}, nil
}

func (d *Double) npmPath(pkg, version string) string {
	return filepath.Join(d.dir, "npm", safe(pkg), version+".tgz")
}

// Publish publishes one target under the effect id: the npm tarball into
// the registry (a version holding other bytes is refused VERSION_EXISTS,
// never overwritten), or the browser manifest, entry and stylesheets
// under the release in the origin. The same effect id answers its
// original result.
func (d *Double) Publish(ctx context.Context, effectID string, in activities.PublishInput) (activities.PublishResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	recPath := filepath.Join(d.dir, "publications", safe(effectID)+".json")
	var res activities.PublishResult
	if ok, err := d.readJSON(recPath, &res); err != nil || ok {
		return res, err
	}
	fault := d.fault("publish", in.Occurrence)
	if fault == "fail" {
		res = activities.PublishResult{State: "failed", FailureCode: "REGISTRY_REJECTED"}
		return res, d.writeJSON(recPath, res)
	}
	receipt := activities.PublicationReceipt{
		SchemaVersion: 1, Target: in.Target, ReceiptID: "rcpt_" + safe(effectID + "/receipt")[:20], EffectID: effectID, SubjectDigest: in.Subject.SubjectDigest,
		ReleaseID: in.ReleaseID, PackageName: in.Subject.PackageName, Version: in.Subject.Version, Destination: in.Destination,
	}
	switch in.Target {
	case "npm":
		if len(in.Files) != 1 {
			return activities.PublishResult{}, errors.New("an npm publication carries one tarball")
		}
		path := d.npmPath(in.Subject.PackageName, in.Subject.Version)
		if _, err := os.Stat(path); err == nil {
			// Published versions are immutable; identical bytes under another
			// effect are not reused by this double either.
			res = activities.PublishResult{State: "failed", FailureCode: "VERSION_EXISTS"}
			return res, d.writeJSON(recPath, res)
		}
		body := in.Files[0].Bytes
		if fault == "unavailable" {
			body = append(append([]byte{}, body...), 0) // the registry serves other bytes
		}
		if err := writeFileAtomic(path, body, 0o644); err != nil {
			return activities.PublishResult{}, err
		}
		receipt.Files = []activities.FileReceipt{{Path: in.Files[0].Path, Digest: activities.Digest(in.Files[0].Bytes), SizeBytes: strconv.Itoa(len(in.Files[0].Bytes))}}
	case "browser":
		if !releaseIDPattern.MatchString(in.ReleaseID) {
			return activities.PublishResult{}, fmt.Errorf("release id %q", in.ReleaseID)
		}
		dir := filepath.Join(d.originDir(), in.ReleaseID)
		if _, err := os.Stat(filepath.Join(dir, "browser-manifest.json")); err == nil {
			res = activities.PublishResult{State: "failed", FailureCode: "VERSION_EXISTS"}
			return res, d.writeJSON(recPath, res)
		}
		for _, f := range in.Files {
			if !validRelative(f.Path) {
				return activities.PublishResult{}, fmt.Errorf("file path %q", f.Path)
			}
			body := f.Bytes
			if fault == "unavailable" {
				continue // the origin never serves the files
			}
			if err := writeFileAtomic(filepath.Join(dir, filepath.FromSlash(f.Path)), body, 0o644); err != nil {
				return activities.PublishResult{}, err
			}
			receipt.Files = append(receipt.Files, activities.FileReceipt{Path: f.Path, Digest: activities.Digest(f.Bytes), SizeBytes: strconv.Itoa(len(f.Bytes))})
		}
		if fault == "unavailable" {
			for _, f := range in.Files {
				receipt.Files = append(receipt.Files, activities.FileReceipt{Path: f.Path, Digest: activities.Digest(f.Bytes), SizeBytes: strconv.Itoa(len(f.Bytes))})
			}
		}
		if err := writeFileAtomic(filepath.Join(dir, "browser-manifest.json"), in.Manifest, 0o644); err != nil {
			return activities.PublishResult{}, err
		}
		receipt.ManifestDigest = activities.Digest(in.Manifest)
	default:
		return activities.PublishResult{}, fmt.Errorf("target %q", in.Target)
	}
	if fault == "wrong-destination" {
		receipt.Destination = "https://elsewhere.anvilkit.invalid/"
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		return activities.PublishResult{}, err
	}
	res = activities.PublishResult{State: "published", Receipt: raw}
	if err := d.writeJSON(recPath, res); err != nil {
		return activities.PublishResult{}, err
	}
	if fault == "unknown" {
		return activities.PublishResult{}, errors.New("publication answer lost (fault injected)")
	}
	return res, nil
}

func validRelative(p string) bool {
	if p == "" || strings.HasPrefix(p, "/") || len(p) > 512 {
		return false
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == "" || seg == "." || seg == ".." || !releaseIDPattern.MatchString(seg) {
			return false
		}
	}
	return true
}

func (d *Double) QueryPublication(ctx context.Context, effectID string) (activities.PublishResult, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var res activities.PublishResult
	ok, err := d.readJSON(filepath.Join(d.dir, "publications", safe(effectID)+".json"), &res)
	return res, ok, err
}

// FetchPublished reads a published file back from its destination.
func (d *Double) FetchPublished(ctx context.Context, target, destination, releaseID, path string) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	switch target {
	case "npm":
		// path is "<package>@<version>"
		at := strings.LastIndex(path, "@")
		if at <= 0 {
			return nil, fmt.Errorf("npm path %q", path)
		}
		return os.ReadFile(d.npmPath(path[:at], path[at+1:]))
	case "browser":
		if !releaseIDPattern.MatchString(releaseID) || !validRelative(path) {
			return nil, fmt.Errorf("browser path %q/%q", releaseID, path)
		}
		return os.ReadFile(filepath.Join(d.originDir(), releaseID, filepath.FromSlash(path)))
	}
	return nil, fmt.Errorf("target %q", target)
}

type catalogEntry struct {
	activities.LockEntry
	Label string `json:"label,omitempty"`
}

type catalogDoc struct {
	SchemaVersion int            `json:"schemaVersion"`
	Revision      string         `json:"revision"`
	Releases      []catalogEntry `json:"releases"`
}

func (d *Double) readCatalog() (catalogDoc, error) {
	doc := catalogDoc{SchemaVersion: 1, Revision: "0", Releases: []catalogEntry{}}
	if _, err := d.readJSON(filepath.Join(d.originDir(), "catalog.json"), &doc); err != nil {
		return doc, err
	}
	if doc.Releases == nil {
		doc.Releases = []catalogEntry{}
	}
	return doc, nil
}

// CatalogRevision answers the activated catalog's revision.
func (d *Double) CatalogRevision(ctx context.Context) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	doc, err := d.readCatalog()
	return doc.Revision, err
}

// Activate makes the exact release current for its component under the
// expected catalog revision (a conditional write: another revision is a
// conflict naming the current one). The release must be published at the
// origin with the lock's manifest digest (RELEASE_UNAVAILABLE otherwise).
// The same effect id answers its original result.
func (d *Double) Activate(ctx context.Context, effectID string, in activities.ActivateInput) (activities.ActivationResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	recPath := filepath.Join(d.dir, "activations", safe(effectID)+".json")
	var res activities.ActivationResult
	if ok, err := d.readJSON(recPath, &res); err != nil || ok {
		return res, err
	}
	doc, err := d.readCatalog()
	if err != nil {
		return activities.ActivationResult{}, err
	}
	fault := d.fault("activate", in.Occurrence)
	if fault == "conflict" {
		// Another activation moved the catalog between the caller's read and
		// this conditional write.
		rev, _ := strconv.ParseUint(doc.Revision, 10, 64)
		doc.Revision = strconv.FormatUint(rev+1, 10)
		raw, err := json.Marshal(doc)
		if err != nil {
			return activities.ActivationResult{}, err
		}
		if err := writeFileAtomic(filepath.Join(d.originDir(), "catalog.json"), raw, 0o644); err != nil {
			return activities.ActivationResult{}, err
		}
	}
	switch {
	case doc.Revision != in.ExpectedCatalogRevision:
		res = activities.ActivationResult{State: "conflict", CurrentCatalogRevision: doc.Revision}
	default:
		manifest, err := os.ReadFile(filepath.Join(d.originDir(), in.Lock.ReleaseID, "browser-manifest.json"))
		if err != nil || activities.Digest(manifest) != in.Lock.BrowserManifestDigest {
			res = activities.ActivationResult{State: "failed", FailureCode: "RELEASE_UNAVAILABLE"}
			break
		}
		rev, _ := strconv.ParseUint(doc.Revision, 10, 64)
		doc.Revision = strconv.FormatUint(rev+1, 10)
		kept := doc.Releases[:0]
		for _, e := range doc.Releases {
			if e.ComponentID != in.Lock.ComponentID {
				kept = append(kept, e)
			}
		}
		doc.Releases = append(kept, catalogEntry{LockEntry: in.Lock, Label: in.Lock.PuckType + " " + in.Lock.PackageVersion})
		raw, err := json.Marshal(doc)
		if err != nil {
			return activities.ActivationResult{}, err
		}
		if err := writeFileAtomic(filepath.Join(d.originDir(), "catalog.json"), raw, 0o644); err != nil {
			return activities.ActivationResult{}, err
		}
		receipt, err := json.Marshal(activities.ActivationReceipt{
			SchemaVersion: 1, ReceiptID: "rcpt_" + safe(effectID + "/activation")[:20], EffectID: effectID, SubjectDigest: in.Subject.SubjectDigest,
			CatalogRevision: doc.Revision, Lock: in.Lock,
		})
		if err != nil {
			return activities.ActivationResult{}, err
		}
		res = activities.ActivationResult{State: "activated", Receipt: receipt, CurrentCatalogRevision: doc.Revision}
	}
	if err := d.writeJSON(recPath, res); err != nil {
		return activities.ActivationResult{}, err
	}
	if fault == "unknown" {
		return activities.ActivationResult{}, errors.New("activation answer lost (fault injected)")
	}
	return res, nil
}

func (d *Double) QueryActivation(ctx context.Context, effectID string) (activities.ActivationResult, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var res activities.ActivationResult
	ok, err := d.readJSON(filepath.Join(d.dir, "activations", safe(effectID)+".json"), &res)
	return res, ok, err
}

// SetCatalogRevision moves the catalog's revision (tests: a concurrent
// activation by another release).
func (d *Double) SetCatalogRevision(revision uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	doc, err := d.readCatalog()
	if err != nil {
		return err
	}
	doc.Revision = strconv.FormatUint(revision, 10)
	raw, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	return writeFileAtomic(filepath.Join(d.originDir(), "catalog.json"), bytes.TrimSpace(raw), 0o644)
}
