package activities

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"
)

// Release types and ports (P21, DD-04 §4, DD-06 §1/§4). The ports are the
// target-domain contracts of Pagix's ReviewPort, PublicationPort and
// ActivationPort and the exact-revision read of its SourcePort; no upstream
// URL or field is claimed (ENV-07). Receipts are the component contract's
// publicationReceipt and activationReceipt.

// ArtifactDigest is one certified artifact's digest and size.
type ArtifactDigest struct {
	Digest    string `json:"digest"`
	SizeBytes string `json:"sizeBytes"`
}

// ReleaseDestinations are the release profile's reviewed destinations.
type ReleaseDestinations struct {
	NpmRegistry   string `json:"npmRegistry"`
	BrowserOrigin string `json:"browserOrigin"`
}

// ReleaseSubject is components/component.schema.json#/$defs/releaseSubject.
type ReleaseSubject struct {
	SchemaVersion               int                 `json:"schemaVersion"`
	ComponentID                 string              `json:"componentId"`
	PuckType                    string              `json:"puckType"`
	SourceRevision              string              `json:"sourceRevision"`
	SourceDigest                string              `json:"sourceDigest"`
	PackageName                 string              `json:"packageName"`
	Version                     string              `json:"version"`
	Npm                         ArtifactDigest      `json:"npm"`
	Browser                     ArtifactDigest      `json:"browser"`
	CSS                         []ArtifactDigest    `json:"css"`
	BuildProfileID              string              `json:"buildProfileId"`
	BuildProfileDigest          string              `json:"buildProfileDigest"`
	ValidatorProfileID          string              `json:"validatorProfileId"`
	ValidatorProfileDigest      string              `json:"validatorProfileDigest"`
	HostAbi                     string              `json:"hostAbi"`
	HostAbiDigest               string              `json:"hostAbiDigest"`
	Destinations                ReleaseDestinations `json:"destinations"`
	CertificationEvidenceDigest string              `json:"certificationEvidenceDigest"`
	SubjectDigest               string              `json:"subjectDigest"`
}

// CanonicalJSON is the RFC 8785 form of a JSON value made of objects,
// arrays, strings and small integers: members sorted by key, no
// whitespace, no HTML escaping (encoding/json sorts map keys).
func CanonicalJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var generic any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&generic); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(generic); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// Digest is the sha256 digest of the canonical bytes in contract form.
func Digest(b []byte) string { return fmt.Sprintf("sha256:%x", sha256.Sum256(b)) }

// ComputeSubjectDigest is the subject digest of the contract: the RFC 8785
// canonical JSON of the subject without subjectDigest.
func ComputeSubjectDigest(s ReleaseSubject) (string, error) {
	s.SubjectDigest = ""
	raw, err := CanonicalJSON(s)
	if err != nil {
		return "", err
	}
	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		return "", err
	}
	delete(m, "subjectDigest")
	canon, err := CanonicalJSON(m)
	if err != nil {
		return "", err
	}
	return Digest(canon), nil
}

// FileReceipt is one published file.
type FileReceipt struct {
	Path      string `json:"path"`
	Digest    string `json:"digest"`
	SizeBytes string `json:"sizeBytes"`
}

// BrowserManifest is components/component.schema.json#/$defs/browserManifest.
type BrowserManifest struct {
	SchemaVersion int              `json:"schemaVersion"`
	HostProfileID string           `json:"hostProfileId"`
	Identity      ManifestIdentity `json:"identity"`
	Entry         FileReceipt      `json:"entry"`
	Styles        []FileReceipt    `json:"styles"`
}

type ManifestIdentity struct {
	ComponentID    string `json:"componentId"`
	PuckType       string `json:"puckType"`
	ReleaseID      string `json:"releaseId"`
	PackageName    string `json:"packageName"`
	PackageVersion string `json:"packageVersion"`
}

// PublicationReceipt is components/component.schema.json#/$defs/publicationReceipt.
type PublicationReceipt struct {
	SchemaVersion  int           `json:"schemaVersion"`
	Target         string        `json:"target"`
	ReceiptID      string        `json:"receiptId"`
	EffectID       string        `json:"effectId"`
	SubjectDigest  string        `json:"subjectDigest"`
	ReleaseID      string        `json:"releaseId"`
	PackageName    string        `json:"packageName"`
	Version        string        `json:"version"`
	Destination    string        `json:"destination"`
	ManifestDigest string        `json:"manifestDigest,omitempty"`
	Files          []FileReceipt `json:"files"`
}

// LockEntry is components/component.schema.json#/$defs/remoteComponentLock.
type LockEntry struct {
	SchemaVersion         int    `json:"schemaVersion"`
	ComponentID           string `json:"componentId"`
	PuckType              string `json:"puckType"`
	ReleaseID             string `json:"releaseId"`
	PackageName           string `json:"packageName"`
	PackageVersion        string `json:"packageVersion"`
	BrowserManifestDigest string `json:"browserManifestDigest"`
	HostProfileID         string `json:"hostProfileId"`
}

// ActivationReceipt is components/component.schema.json#/$defs/activationReceipt.
type ActivationReceipt struct {
	SchemaVersion   int       `json:"schemaVersion"`
	ReceiptID       string    `json:"receiptId"`
	EffectID        string    `json:"effectId"`
	SubjectDigest   string    `json:"subjectDigest"`
	CatalogRevision string    `json:"catalogRevision"`
	Lock            LockEntry `json:"lock"`
}

// RegisterReviewInput registers the exact subject for review.
type RegisterReviewInput struct {
	Occurrence uint64
	Lineage    string
	Subject    ReleaseSubject
}

// ReviewRef is the review the port registered for a subject.
type ReviewRef struct {
	EffectID      string
	ReleaseID     string
	SubjectDigest string
}

// ReviewDecision is the maintainer's decision as the review port answers
// it, bound to the digest it was made for.
type ReviewDecision struct {
	State         string // pending | approved | rejected | invalidated
	SubjectDigest string
	ApproverID    string
	DecidedAt     *time.Time
	ReasonCode    string
}

// PublishFile is one file of a target with its exact bytes.
type PublishFile struct {
	Path  string
	Bytes []byte
}

// PublishInput publishes one target of the approved subject.
type PublishInput struct {
	Target      string // npm | browser
	Occurrence  uint64
	ReleaseID   string
	Subject     ReleaseSubject
	Destination string
	Files       []PublishFile
	Manifest    []byte // browser only: the browser manifest bytes
}

// PublishResult is the port's answer: published with the receipt bytes,
// or a definite refusal (FailureCode). An answer that never arrives is an
// error of the call, never a result.
type PublishResult struct {
	State       string // published | failed
	Receipt     []byte
	FailureCode string
}

// ActivateInput activates the exact release under the catalog revision
// the caller read (a conditional write).
type ActivateInput struct {
	Occurrence              uint64
	Subject                 ReleaseSubject
	Lock                    LockEntry
	ExpectedCatalogRevision string
}

// ActivationResult is activated with the receipt bytes, conflict (the
// catalog moved: CurrentCatalogRevision) or failed.
type ActivationResult struct {
	State                  string // activated | conflict | failed
	Receipt                []byte
	CurrentCatalogRevision string
	FailureCode            string
}

// ReleasePort is the Workflow's port to the release side of Pagix.
type ReleasePort interface {
	// RevisionDigest answers the digest of the source archive the source
	// authority holds for the lineage's revision.
	RevisionDigest(ctx context.Context, lineage, revision string) (digest string, known bool, err error)
	RegisterReview(ctx context.Context, effectID string, in RegisterReviewInput) (ReviewRef, error)
	QueryReview(ctx context.Context, effectID string) (ReviewRef, bool, error)
	ReviewDecision(ctx context.Context, releaseID string) (ReviewDecision, error)
	Publish(ctx context.Context, effectID string, in PublishInput) (PublishResult, error)
	QueryPublication(ctx context.Context, effectID string) (PublishResult, bool, error)
	// FetchPublished reads a published file back from the destination:
	// the npm tarball by its name, a browser file by its path.
	FetchPublished(ctx context.Context, target, destination, releaseID, path string) ([]byte, error)
	Activate(ctx context.Context, effectID string, in ActivateInput) (ActivationResult, error)
	QueryActivation(ctx context.Context, effectID string) (ActivationResult, bool, error)
	CatalogRevision(ctx context.Context) (string, error)
}
