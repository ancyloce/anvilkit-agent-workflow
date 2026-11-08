package pagix

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// ClientOptions place the authenticated Pagix client (P2.2).
type ClientOptions struct {
	// BaseURL is the upstream origin and base path (https outside
	// development; the declaration's paths are appended).
	BaseURL string
	// Timeout bounds one exchange.
	Timeout time.Duration
	// CAFile verifies the upstream (empty: the system roots, which include
	// the platform trust bundle through SSL_CERT_DIR).
	CAFile     string
	ServerName string
	// CertFile/KeyFile present the workload certificate (mTLS); TokenFile
	// is an OAuth client-credentials token the platform keeps current in
	// the CSI-mounted file. At least one is required.
	CertFile, KeyFile, TokenFile string
	// HTTPClient overrides the transport (tests).
	HTTPClient *http.Client
}

// Client is the declaration-driven Pagix adapter of the LeasePort,
// SourcePort and ReleasePort (DD-06 §3, P2.2). Every effect call carries
// the effect identity as the idempotency key and is sent once: an answer
// that does not arrive is an error the Activity turns into an
// original-identity query, never a resend. Conditional writes carry the
// expected revision in the declared condition header. Redirects are never
// followed and every body is bounded.
type Client struct {
	decl  *Declaration
	base  *url.URL
	http  *http.Client
	token string
}

// NewClient validates the placement against the declaration.
func NewClient(decl *Declaration, o ClientOptions) (*Client, error) {
	base, err := url.Parse(o.BaseURL)
	if err != nil || base.Host == "" || base.User != nil || base.RawQuery != "" || (base.Scheme != "https" && base.Scheme != "http") {
		return nil, errors.New("pagix: base url must be an absolute http(s) URL without credentials or query")
	}
	if o.CertFile == "" && o.TokenFile == "" && o.HTTPClient == nil {
		return nil, errors.New("pagix: the client presents a workload certificate or a token file")
	}
	if o.Timeout <= 0 {
		o.Timeout = 30 * time.Second
	}
	c := o.HTTPClient
	if c == nil {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: o.ServerName}
		if o.CAFile != "" {
			pem, err := os.ReadFile(o.CAFile)
			if err != nil {
				return nil, fmt.Errorf("pagix: ca file: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, errors.New("pagix: ca file holds no certificate")
			}
			tlsCfg.RootCAs = pool
		}
		if o.CertFile != "" {
			certFile, keyFile := o.CertFile, o.KeyFile
			tlsCfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
				pair, err := tls.LoadX509KeyPair(certFile, keyFile)
				return &pair, err
			}
		}
		c = &http.Client{Timeout: o.Timeout, Transport: &http.Transport{TLSClientConfig: tlsCfg, Proxy: nil, MaxIdleConnsPerHost: 8}}
	}
	copy := *c
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{decl: decl, base: base, http: &copy, token: o.TokenFile}, nil
}

// errNoRecord is a query of an original identity the upstream has no
// record of (404 on a query operation).
var errNoRecord = errors.New("no record")

// Class is what a failed exchange says about its effect (DD-06 §2): invalid
// input, current denial, revision conflict, explicit business rejection,
// capacity failure, proven not-sent, or sent-but-unknown.
type Class string

const (
	ClassInvalid   Class = "invalid"
	ClassDenied    Class = "denied"
	ClassConflict  Class = "conflict"
	ClassRejection Class = "rejection"
	ClassCapacity  Class = "capacity"
	ClassNotSent   Class = "not-sent"
	ClassUnknown   Class = "unknown"
)

// ExchangeError is a failed exchange with its class. Status is the
// upstream's answer, 0 when none arrived. Proven not-sent is only a request
// that never left this process or a status the declaration verifies as
// not-sent; an HTTP 500, a timeout or a lost connection is unknown. The
// class informs reconciliation and reporting: the release Activities still
// record every failed send UNKNOWN and never resend it (a replacement needs
// trusted not-sent evidence through Control, RB-S02).
type ExchangeError struct {
	Op     string
	Status int
	Class  Class
	Err    error
}

func (e *ExchangeError) Error() string {
	return fmt.Sprintf("pagix %s [class %s]: %v", e.Op, e.Class, e.Err)
}

func (e *ExchangeError) Unwrap() error { return e.Err }

// ClassOf is the class of a failed exchange: unknown for any error that
// carries none, since nothing proves it was not sent; empty for nil.
func ClassOf(err error) Class {
	if err == nil {
		return ""
	}
	var e *ExchangeError
	if errors.As(err, &e) {
		return e.Class
	}
	return ClassUnknown
}

func failed(op string, status int, class Class, err error) *ExchangeError {
	return &ExchangeError{Op: op, Status: status, Class: class, Err: err}
}

// classify reads an answered status: a declared not-sent status counts only
// when the declaration verifies it, declared conflict and rejection statuses
// before the generic ones.
func (c *Client) classify(op string, status int) Class {
	o := c.decl.Operations[op]
	conflicts := o.ConflictStatuses
	if len(conflicts) == 0 {
		conflicts = []int{http.StatusConflict, http.StatusPreconditionFailed}
	}
	switch {
	case slices.Contains(o.NotSentStatuses, status) && o.verifies(CapNotSent):
		return ClassNotSent
	case slices.Contains(conflicts, status):
		return ClassConflict
	case slices.Contains(o.RejectionStatuses, status):
		return ClassRejection
	case status == http.StatusBadRequest, status == http.StatusUnprocessableEntity, status == http.StatusNotFound:
		return ClassInvalid
	case status == http.StatusUnauthorized, status == http.StatusForbidden:
		return ClassDenied
	case status == http.StatusTooManyRequests, status == http.StatusServiceUnavailable:
		return ClassCapacity
	}
	return ClassUnknown
}

// queryOps are the reads whose 404 means "no record" rather than an error.
var queryOps = map[string]bool{
	OpLeaseQuery: true, OpSourceQueryReg: true, OpSourceQuerySave: true, OpRevisionDigest: true,
	OpReviewQuery: true, OpPublicationQuery: true, OpActivationQuery: true,
}

// exchange sends one declared request and decodes a 2xx (or, where the
// operation answers a conflict as a result, a 409) body into out.
func (c *Client) exchange(ctx context.Context, op string, vars map[string]string, idempotencyKey, condition string, in, out any, conflictIsResult bool) error {
	// Until c.http.Do the request has not left this process: proven not-sent.
	method, target, err := c.decl.target(c.base, op, vars)
	if err != nil {
		return activities.Refused("DEPENDENCY_UNAVAILABLE", failed(op, 0, ClassNotSent, err))
	}
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return activities.Refused("INVALID_ARGUMENT", failed(op, 0, ClassNotSent, err))
		}
		if int64(len(raw)) > c.decl.MaxBodyBytes {
			return activities.Refused("INVALID_ARGUMENT", failed(op, 0, ClassNotSent, errors.New("the request exceeds the declared body bound")))
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return activities.Refused("INVALID_ARGUMENT", failed(op, 0, ClassNotSent, err))
	}
	// A written request is never sent again by the transport (RB-A02):
	// net/http replays a request that carries an Idempotency-Key header and a
	// re-readable body when a reused connection drops before the answer, a
	// second physical send that the upstream's unverified idempotency may not
	// absorb. Without GetBody the lost answer is an error instead: unknown,
	// queried under the original identity. Only GET, a safe read, stays
	// replayable.
	if method != http.MethodGet {
		req.GetBody = nil
		if req.Body == nil {
			req.Body = io.NopCloser(bytes.NewReader(nil))
		}
	}
	req.Header.Set("Accept", "application/json")
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		req.Header.Set(c.decl.IdempotencyHeader, idempotencyKey)
	}
	if condition != "" {
		req.Header.Set(c.decl.ConditionHeader, strconv.Quote(condition))
	}
	if c.token != "" {
		tok, err := os.ReadFile(c.token)
		if err != nil {
			return failed(op, 0, ClassNotSent, errors.New("token file unreadable"))
		}
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(tok)))
	}
	resp, err := c.http.Do(req)
	if err != nil {
		// Not proof that nothing happened (DD-06 §2): the caller queries the
		// original identity.
		return failed(op, 0, ClassUnknown, fmt.Errorf("no answer: %w", err))
	}
	defer resp.Body.Close()
	s := resp.StatusCode
	raw, err := io.ReadAll(io.LimitReader(resp.Body, c.decl.MaxBodyBytes+1))
	if err != nil {
		return failed(op, s, ClassUnknown, fmt.Errorf("answer unreadable: %w", err))
	}
	if int64(len(raw)) > c.decl.MaxBodyBytes {
		return failed(op, s, ClassUnknown, errors.New("answer exceeds the declared body bound"))
	}
	if s >= 200 && s < 300 || s == http.StatusConflict && conflictIsResult {
		if out == nil {
			return nil
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(out); err != nil {
			return failed(op, s, ClassUnknown, fmt.Errorf("answer is not the port contract: %w", err))
		}
		return nil
	}
	if s == http.StatusNotFound && queryOps[op] {
		return errNoRecord
	}
	// The public codes and Temporal's retry stay what each status had before
	// the classes (RB-A02); a declared status refines only the class.
	class := c.classify(op, s)
	switch {
	case class == ClassConflict:
		return activities.Refused("REVISION_CONFLICT", failed(op, s, class, fmt.Errorf("conflict (%d)", s)))
	case class == ClassRejection:
		return activities.Refused("EFFECT_DENIED", failed(op, s, class, fmt.Errorf("rejected by the upstream (%d)", s)))
	case s == http.StatusNotFound:
		return activities.Refused("NOT_FOUND", failed(op, s, class, errors.New("not found")))
	case class == ClassInvalid:
		return activities.Refused("INVALID_ARGUMENT", failed(op, s, class, fmt.Errorf("refused the request (%d)", s)))
	case class == ClassDenied:
		return activities.Refused("FORBIDDEN", failed(op, s, class, fmt.Errorf("denied the caller (%d)", s)))
	case s >= 300 && s < 400:
		return activities.Refused("DEPENDENCY_UNAVAILABLE", failed(op, s, class, fmt.Errorf("answered a redirect (%d), never followed", s)))
	}
	// Capacity, a verified not-sent answer and every other status stay
	// retryable: a query is asked again, a send is recorded UNKNOWN.
	return failed(op, s, class, fmt.Errorf("answered %d", s))
}

// query is an original-identity query: known false when the upstream has
// no record.
func (c *Client) query(ctx context.Context, op string, vars map[string]string, out any) (bool, error) {
	err := c.exchange(ctx, op, vars, "", "", nil, out, false)
	if errors.Is(err, errNoRecord) {
		return false, nil
	}
	return err == nil, err
}

func occ(n uint64) string { return strconv.FormatUint(n, 10) }

// LeasePort.

func (c *Client) Acquire(ctx context.Context, in activities.LeaseInput) (out activities.LeaseResult, err error) {
	err = c.exchange(ctx, OpLeaseAcquire, map[string]string{"subject": in.Subject, "occurrence": occ(in.Occurrence)}, in.Subject+"#acquire#"+occ(in.Occurrence), "", in, &out, false)
	return out, err
}

func (c *Client) Renew(ctx context.Context, in activities.LeaseInput) (out activities.LeaseResult, err error) {
	err = c.exchange(ctx, OpLeaseRenew, map[string]string{"subject": in.Subject, "occurrence": occ(in.Occurrence)}, in.LeaseID+"#renew#"+occ(in.Occurrence), in.Fence, in, &out, false)
	return out, err
}

func (c *Client) Query(ctx context.Context, in activities.LeaseInput) (out activities.LeaseResult, err error) {
	// The lease identity and fence travel in the body: a query names the
	// lease it asks about, not only the subject.
	err = c.exchange(ctx, OpLeaseQuery, map[string]string{"subject": in.Subject, "occurrence": occ(in.Occurrence)}, "", "", in, &out, false)
	if errors.Is(err, errNoRecord) {
		return activities.LeaseResult{State: "lost", Reason: "no lease"}, nil
	}
	return out, err
}

func (c *Client) Release(ctx context.Context, in activities.LeaseInput) (out activities.LeaseResult, err error) {
	err = c.exchange(ctx, OpLeaseRelease, map[string]string{"subject": in.Subject, "occurrence": occ(in.Occurrence)}, in.LeaseID+"#release#"+occ(in.Occurrence), in.Fence, in, &out, false)
	return out, err
}

// SourcePort.

func (c *Client) CheckScope(ctx context.Context, in activities.CheckSourceScopeInput) (out activities.ScopeDecision, err error) {
	err = c.exchange(ctx, OpSourceCheckScope, map[string]string{"subject": in.SubjectDigest}, "", "", in, &out, false)
	return out, err
}

func (c *Client) RegisterCandidate(ctx context.Context, effectID string, in activities.RegisterCandidateInput) (out activities.CandidateRef, err error) {
	err = c.exchange(ctx, OpSourceRegister, map[string]string{"subject": in.Subject, "effectId": effectID}, effectID, in.Lease.Fence, in, &out, false)
	return out, err
}

func (c *Client) QueryRegistration(ctx context.Context, effectID string) (out activities.CandidateRef, known bool, err error) {
	known, err = c.query(ctx, OpSourceQueryReg, map[string]string{"effectId": effectID}, &out)
	return out, known, err
}

func (c *Client) SaveRevision(ctx context.Context, effectID string, in activities.SaveSourceInput) (out activities.SaveSourceResult, err error) {
	err = c.exchange(ctx, OpSourceSave, map[string]string{"lineage": in.Lineage, "effectId": effectID}, effectID, in.BaseRevision, in, &out, true)
	return out, err
}

func (c *Client) QuerySave(ctx context.Context, effectID string) (out activities.SaveSourceResult, known bool, err error) {
	known, err = c.query(ctx, OpSourceQuerySave, map[string]string{"effectId": effectID}, &out)
	return out, known, err
}

func (c *Client) CurrentRevision(ctx context.Context, subject string) (string, error) {
	var out struct {
		Revision string `json:"revision"`
	}
	err := c.exchange(ctx, OpSourceCurrent, map[string]string{"subject": subject, "lineage": subject}, "", "", nil, &out, false)
	return out.Revision, err
}

// ReleasePort.

func (c *Client) RevisionDigest(ctx context.Context, lineage, revision string) (string, bool, error) {
	var out struct {
		Digest string `json:"digest"`
	}
	known, err := c.query(ctx, OpRevisionDigest, map[string]string{"lineage": lineage, "revision": revision}, &out)
	return out.Digest, known && out.Digest != "", err
}

func (c *Client) RegisterReview(ctx context.Context, effectID string, in activities.RegisterReviewInput) (out activities.ReviewRef, err error) {
	err = c.exchange(ctx, OpReviewRegister, map[string]string{"lineage": in.Lineage, "effectId": effectID}, effectID, "", in, &out, false)
	return out, err
}

func (c *Client) QueryReview(ctx context.Context, effectID string) (out activities.ReviewRef, known bool, err error) {
	known, err = c.query(ctx, OpReviewQuery, map[string]string{"effectId": effectID}, &out)
	return out, known, err
}

func (c *Client) ReviewDecision(ctx context.Context, releaseID string) (out activities.ReviewDecision, err error) {
	err = c.exchange(ctx, OpReviewDecision, map[string]string{"releaseId": releaseID}, "", "", nil, &out, false)
	return out, err
}

func (c *Client) Publish(ctx context.Context, effectID string, in activities.PublishInput) (out activities.PublishResult, err error) {
	err = c.exchange(ctx, OpPublish, map[string]string{"target": in.Target, "releaseId": in.ReleaseID, "effectId": effectID}, effectID, "", in, &out, false)
	return out, err
}

func (c *Client) QueryPublication(ctx context.Context, effectID string) (out activities.PublishResult, known bool, err error) {
	known, err = c.query(ctx, OpPublicationQuery, map[string]string{"effectId": effectID}, &out)
	return out, known, err
}

func (c *Client) FetchPublished(ctx context.Context, target, destination, releaseID, path string) ([]byte, error) {
	var out struct {
		Bytes []byte `json:"bytes"`
	}
	err := c.exchange(ctx, OpPublicationFetch, map[string]string{"target": target, "destination": destination, "releaseId": releaseID, "path": path}, "", "", nil, &out, false)
	return out.Bytes, err
}

func (c *Client) Activate(ctx context.Context, effectID string, in activities.ActivateInput) (out activities.ActivationResult, err error) {
	err = c.exchange(ctx, OpActivate, map[string]string{"effectId": effectID, "releaseId": in.Lock.ReleaseID}, effectID, in.ExpectedCatalogRevision, in, &out, true)
	return out, err
}

func (c *Client) QueryActivation(ctx context.Context, effectID string) (out activities.ActivationResult, known bool, err error) {
	known, err = c.query(ctx, OpActivationQuery, map[string]string{"effectId": effectID}, &out)
	return out, known, err
}

func (c *Client) CatalogRevision(ctx context.Context) (string, error) {
	var out struct {
		Revision string `json:"revision"`
	}
	err := c.exchange(ctx, OpCatalogRevision, nil, "", "", nil, &out, false)
	return out.Revision, err
}
