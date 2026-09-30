// Package pagix holds the DEVELOPMENT_ONLY doubles of the Pagix-owned
// ports the Generation, Preview and Release lifecycles need (DD-06 §1
// LeasePort, SourcePort and, since P21, ReviewPort, PublicationPort and
// ActivationPort)
// and of the Knowledge content-digest read (DD-07) until their real
// declarations exist (ENV-07, P15/P16). The doubles are explicit: every
// answer is recorded under a directory so two worker processes and a
// restart see the same lease and registration facts, and no answer is
// ever a claim about the real upstream. Nothing here reaches a network.
package pagix

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// Double is the filesystem-backed double of the three ports.
type Double struct {
	dir string
	// origin is the browser origin directory of the release double (P21).
	origin string
	mu     sync.Mutex
	now    func() time.Time
	// Faults are the next answers of the lease port a test installs:
	// keyed by "renew"/"query"/"acquire" plus occurrence, each consumed once.
	faults map[string]string
}

// New opens the double under dir (created when missing).
func New(dir string) (*Double, error) {
	if dir == "" {
		return nil, errors.New("pagix double: a directory is required")
	}
	for _, sub := range []string{"leases", "registrations", "faults", "sources", "reviews", "decisions", "publications", "npm", "activations"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o700); err != nil {
			return nil, err
		}
	}
	return &Double{dir: dir, now: func() time.Time { return time.Now().UTC() }, faults: map[string]string{}}, nil
}

// leaseRecord is the double's lease fact for one subject.
type leaseRecord struct {
	LeaseID   string    `json:"leaseId"`
	Subject   string    `json:"subject"`
	Owner     string    `json:"owner"`
	Fence     uint64    `json:"fence"`
	ExpiresAt time.Time `json:"expiresAt"`
	Released  bool      `json:"released"`
	// Renewals records the occurrences applied so a repeated occurrence
	// answers the original result.
	Renewals map[string]time.Time `json:"renewals"`
}

func (d *Double) leasePath(subject string) string {
	return filepath.Join(d.dir, "leases", fmt.Sprintf("%x.json", sha256.Sum256([]byte(subject))))
}

func (d *Double) readLease(subject string) (*leaseRecord, error) {
	raw, err := os.ReadFile(d.leasePath(subject))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var rec leaseRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, err
	}
	if rec.Renewals == nil {
		rec.Renewals = map[string]time.Time{}
	}
	return &rec, nil
}

func (d *Double) writeLease(rec *leaseRecord) error {
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	tmp := d.leasePath(rec.Subject) + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, d.leasePath(rec.Subject))
}

// fault consumes an installed fault for the call kind and occurrence:
// "unknown" answers no result, "lost" answers a confirmed loss.
func (d *Double) fault(kind string, occurrence uint64) string {
	key := kind + ":" + strconv.FormatUint(occurrence, 10)
	if f, ok := d.faults[key]; ok {
		delete(d.faults, key)
		return f
	}
	raw, err := os.ReadFile(filepath.Join(d.dir, "faults", key))
	if err != nil {
		return ""
	}
	_ = os.Remove(filepath.Join(d.dir, "faults", key))
	return string(raw)
}

// InstallFault makes the next lease call of the kind at the occurrence
// answer the fault ("unknown" or "lost"); tests of the lease supervisor
// use it, in process or through the faults directory of another process.
func (d *Double) InstallFault(kind string, occurrence uint64, fault string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.faults[kind+":"+strconv.FormatUint(occurrence, 10)] = fault
}

func held(rec *leaseRecord) activities.LeaseResult {
	exp := rec.ExpiresAt
	return activities.LeaseResult{State: "held", LeaseID: rec.LeaseID, Fence: strconv.FormatUint(rec.Fence, 10), ExpiresAt: &exp}
}

// Acquire grants the subject's lease to the owner when no other owner
// holds an unexpired one; the same owner and occurrence re-acquire the
// same lease (the reentry of a lost receipt).
func (d *Double) Acquire(ctx context.Context, in activities.LeaseInput) (activities.LeaseResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	switch d.fault("acquire", in.Occurrence) {
	case "unknown":
		return activities.LeaseResult{State: "unknown", Reason: "FAULT_INJECTED"}, nil
	case "lost":
		return activities.LeaseResult{State: "lost", Reason: "FAULT_INJECTED"}, nil
	}
	now := d.now()
	rec, err := d.readLease(in.Subject)
	if err != nil {
		return activities.LeaseResult{}, err
	}
	if rec != nil && !rec.Released && rec.ExpiresAt.After(now) && rec.Owner != in.Owner {
		return activities.LeaseResult{State: "lost", Reason: "HELD_BY_ANOTHER_OWNER"}, nil
	}
	if rec != nil && rec.Owner == in.Owner && !rec.Released && rec.ExpiresAt.After(now) {
		return held(rec), nil
	}
	fence := uint64(1)
	if rec != nil {
		fence = rec.Fence + 1
	}
	rec = &leaseRecord{LeaseID: fmt.Sprintf("lease_%x", sha256.Sum256([]byte(in.Subject+"/"+strconv.FormatUint(fence, 10))))[:24], Subject: in.Subject, Owner: in.Owner, Fence: fence, ExpiresAt: now.Add(in.TTL), Renewals: map[string]time.Time{}}
	if err := d.writeLease(rec); err != nil {
		return activities.LeaseResult{}, err
	}
	return held(rec), nil
}

// Renew extends the lease of the stated identity and fence; a lease held
// by another fence, released or expired is a confirmed loss. The same
// occurrence answers the expiry it produced.
func (d *Double) Renew(ctx context.Context, in activities.LeaseInput) (activities.LeaseResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	switch d.fault("renew", in.Occurrence) {
	case "unknown":
		return activities.LeaseResult{State: "unknown", Reason: "FAULT_INJECTED"}, nil
	case "lost":
		rec, err := d.readLease(in.Subject)
		if err == nil && rec != nil {
			rec.Released = true
			_ = d.writeLease(rec)
		}
		return activities.LeaseResult{State: "lost", Reason: "FAULT_INJECTED"}, nil
	}
	now := d.now()
	rec, err := d.readLease(in.Subject)
	if err != nil {
		return activities.LeaseResult{}, err
	}
	if rec == nil || rec.Released || rec.LeaseID != in.LeaseID || strconv.FormatUint(rec.Fence, 10) != in.Fence {
		return activities.LeaseResult{State: "lost", Reason: "LEASE_NOT_HELD"}, nil
	}
	key := strconv.FormatUint(in.Occurrence, 10)
	if at, ok := rec.Renewals[key]; ok {
		exp := at
		return activities.LeaseResult{State: "held", LeaseID: rec.LeaseID, Fence: in.Fence, ExpiresAt: &exp}, nil
	}
	if !rec.ExpiresAt.After(now) {
		return activities.LeaseResult{State: "lost", Reason: "LEASE_EXPIRED"}, nil
	}
	rec.ExpiresAt = now.Add(in.TTL)
	rec.Renewals[key] = rec.ExpiresAt
	if err := d.writeLease(rec); err != nil {
		return activities.LeaseResult{}, err
	}
	return held(rec), nil
}

// Query answers the current state of the lease identity without changing it.
func (d *Double) Query(ctx context.Context, in activities.LeaseInput) (activities.LeaseResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fault("query", in.Occurrence) == "unknown" {
		return activities.LeaseResult{State: "unknown", Reason: "FAULT_INJECTED"}, nil
	}
	rec, err := d.readLease(in.Subject)
	if err != nil {
		return activities.LeaseResult{}, err
	}
	if rec == nil || rec.Released || rec.LeaseID != in.LeaseID || !rec.ExpiresAt.After(d.now()) {
		return activities.LeaseResult{State: "lost", Reason: "LEASE_NOT_HELD"}, nil
	}
	return held(rec), nil
}

// Release ends the lease of the identity; idempotent.
func (d *Double) Release(ctx context.Context, in activities.LeaseInput) (activities.LeaseResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	rec, err := d.readLease(in.Subject)
	if err != nil {
		return activities.LeaseResult{}, err
	}
	if rec == nil || rec.LeaseID != in.LeaseID {
		return activities.LeaseResult{State: "lost", Reason: "LEASE_NOT_HELD"}, nil
	}
	rec.Released = true
	if err := d.writeLease(rec); err != nil {
		return activities.LeaseResult{}, err
	}
	return activities.LeaseResult{State: "lost", LeaseID: rec.LeaseID, Fence: strconv.FormatUint(rec.Fence, 10), Reason: "RELEASED"}, nil
}

// ---- SourcePort ----

// CheckScope allows every source scope except a subject the double was
// told to refuse (a file named after the subject digest under the
// registrations directory with the content "denied"), so the rejected
// scope path is testable without a real authority.
func (d *Double) CheckScope(ctx context.Context, in activities.CheckSourceScopeInput) (activities.ScopeDecision, error) {
	raw, err := os.ReadFile(filepath.Join(d.dir, "registrations", "scope-"+safe(in.SubjectDigest)))
	if err == nil && string(raw) == "denied" {
		return activities.ScopeDecision{Allowed: false, ReasonCode: "SCOPE_DENIED", Revision: "1"}, nil
	}
	return activities.ScopeDecision{Allowed: true, Revision: "1"}, nil
}

// DenyScope makes the subject's scope refused (tests).
func (d *Double) DenyScope(subjectDigest string) error {
	return os.WriteFile(filepath.Join(d.dir, "registrations", "scope-"+safe(subjectDigest)), []byte("denied"), 0o600)
}

func safe(s string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(s))) }

type registration struct {
	EffectID    string `json:"effectId"`
	CandidateID string `json:"candidateId"`
	Subject     string `json:"subject"`
	Digest      string `json:"digest"`
	Revision    string `json:"revision"`
}

// RegisterCandidate records the registration under the effect identity:
// the same effect id answers the original registration (the upstream's
// idempotency by our effect id), never a second candidate.
func (d *Double) RegisterCandidate(ctx context.Context, effectID string, in activities.RegisterCandidateInput) (activities.CandidateRef, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fault("register", in.Occurrence) == "unknown" {
		// The registration is recorded (it happened upstream) but no answer reaches the caller.
		rec := registration{EffectID: effectID, CandidateID: "cand_" + safe(effectID)[:16], Subject: in.Subject, Digest: in.Source.Digest, Revision: "1"}
		raw, _ := json.Marshal(rec)
		_ = os.WriteFile(filepath.Join(d.dir, "registrations", safe(effectID)), raw, 0o600)
		return activities.CandidateRef{}, errors.New("registration answer lost (fault injected)")
	}
	path := filepath.Join(d.dir, "registrations", safe(effectID))
	if raw, err := os.ReadFile(path); err == nil {
		var rec registration
		if err := json.Unmarshal(raw, &rec); err == nil {
			return activities.CandidateRef{EffectID: effectID, State: "succeeded", CandidateID: rec.CandidateID, Revision: rec.Revision}, nil
		}
	}
	rec := registration{EffectID: effectID, CandidateID: "cand_" + safe(effectID)[:16], Subject: in.Subject, Digest: in.Source.Digest, Revision: "1"}
	raw, err := json.Marshal(rec)
	if err != nil {
		return activities.CandidateRef{}, err
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return activities.CandidateRef{}, err
	}
	return activities.CandidateRef{EffectID: effectID, State: "succeeded", CandidateID: rec.CandidateID, Revision: rec.Revision}, nil
}

// QueryRegistration answers what the double recorded under the effect id.
func (d *Double) QueryRegistration(ctx context.Context, effectID string) (activities.CandidateRef, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	raw, err := os.ReadFile(filepath.Join(d.dir, "registrations", safe(effectID)))
	if errors.Is(err, os.ErrNotExist) {
		return activities.CandidateRef{}, false, nil
	}
	if err != nil {
		return activities.CandidateRef{}, false, err
	}
	var rec registration
	if err := json.Unmarshal(raw, &rec); err != nil {
		return activities.CandidateRef{}, false, err
	}
	return activities.CandidateRef{EffectID: effectID, State: "succeeded", CandidateID: rec.CandidateID, Revision: rec.Revision}, true, nil
}

// ---- SourcePort: conditional saves (P20) ----

// lineage is the double's record of one component source lineage: its
// current revision (the registered candidate is revision 1) and every save
// under its effect identity.
type lineage struct {
	Revision uint64                                 `json:"revision"`
	Digest   string                                 `json:"digest"`
	Saves    map[string]activities.SaveSourceResult `json:"saves"`
	// Revisions are the archive digests of the saved revisions (P21: the
	// exact-revision read a release binds its bytes to).
	Revisions map[string]string `json:"revisions,omitempty"`
}

func (d *Double) lineagePath(subject string) string {
	return filepath.Join(d.dir, "sources", safe(subject)+".json")
}

func (d *Double) readLineage(subject string) (*lineage, error) {
	raw, err := os.ReadFile(d.lineagePath(subject))
	if errors.Is(err, os.ErrNotExist) {
		return &lineage{Revision: 1, Saves: map[string]activities.SaveSourceResult{}}, nil
	}
	if err != nil {
		return nil, err
	}
	var l lineage
	if err := json.Unmarshal(raw, &l); err != nil {
		return nil, err
	}
	if l.Saves == nil {
		l.Saves = map[string]activities.SaveSourceResult{}
	}
	return &l, nil
}

func (d *Double) writeLineage(subject string, l *lineage) error {
	raw, err := json.Marshal(l)
	if err != nil {
		return err
	}
	tmp := d.lineagePath(subject) + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, d.lineagePath(subject))
}

// SaveRevision saves under the expected revision: the base must be the
// lineage's current revision, else a conflict names the current one; the
// same effect id answers its original outcome. The "save" fault "unknown"
// records the save and loses the answer.
func (d *Double) SaveRevision(ctx context.Context, effectID string, in activities.SaveSourceInput) (activities.SaveSourceResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	subject := "source:" + in.Lineage
	l, err := d.readLineage(subject)
	if err != nil {
		return activities.SaveSourceResult{}, err
	}
	if res, ok := l.Saves[effectID]; ok {
		return res, nil
	}
	current := strconv.FormatUint(l.Revision, 10)
	var res activities.SaveSourceResult
	if in.BaseRevision != current {
		res = activities.SaveSourceResult{EffectID: effectID, State: "conflict", CurrentRevision: current}
	} else {
		l.Revision++
		l.Digest = in.Source.Digest
		if l.Revisions == nil {
			l.Revisions = map[string]string{}
		}
		l.Revisions[strconv.FormatUint(l.Revision, 10)] = in.Source.Digest
		res = activities.SaveSourceResult{EffectID: effectID, State: "saved", Revision: strconv.FormatUint(l.Revision, 10)}
	}
	l.Saves[effectID] = res
	if err := d.writeLineage(subject, l); err != nil {
		return activities.SaveSourceResult{}, err
	}
	if d.fault("save", in.Occurrence) == "unknown" {
		return activities.SaveSourceResult{}, errors.New("save answer lost (fault injected)")
	}
	return res, nil
}

// QuerySave answers the save recorded under the effect id.
func (d *Double) QuerySave(ctx context.Context, effectID string) (activities.SaveSourceResult, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	entries, err := os.ReadDir(filepath.Join(d.dir, "sources"))
	if err != nil {
		return activities.SaveSourceResult{}, false, err
	}
	for _, e := range entries {
		raw, err := os.ReadFile(filepath.Join(d.dir, "sources", e.Name()))
		if err != nil || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		var l lineage
		if json.Unmarshal(raw, &l) == nil {
			if res, ok := l.Saves[effectID]; ok {
				return res, true, nil
			}
		}
	}
	return activities.SaveSourceResult{}, false, nil
}

// CurrentRevision answers the lineage's current revision.
func (d *Double) CurrentRevision(ctx context.Context, subject string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	l, err := d.readLineage(subject)
	if err != nil {
		return "", err
	}
	return strconv.FormatUint(l.Revision, 10), nil
}

// SetRevision moves a lineage to a revision (tests: a concurrent save by
// another author).
func (d *Double) SetRevision(subject string, revision uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	l, err := d.readLineage(subject)
	if err != nil {
		return err
	}
	l.Revision = revision
	return d.writeLineage(subject, l)
}

// ---- Knowledge content digests ----

// ContentDigest is the DEVELOPMENT_ONLY content digest of a source
// revision: a deterministic digest of the reference itself, so a brief
// freezes an exact and reproducible binding; it establishes nothing about
// real source content (P15/P16).
func (d *Double) ContentDigest(ctx context.Context, tenantID string, ref activities.SourceReference) (activities.ContentDigest, error) {
	if ref.SourceID == "" {
		return activities.ContentDigest{}, errors.New("a source reference needs a source id")
	}
	return activities.ContentDigest{SourceID: ref.SourceID, Revision: ref.Revision, Digest: fmt.Sprintf("sha256:%x", sha256.Sum256([]byte("DEVELOPMENT_ONLY:"+tenantID+"/"+ref.SourceID+"@"+ref.Revision)))}, nil
}
