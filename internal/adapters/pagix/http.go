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

// queryOps are the reads whose 404 means "no record" rather than an error.
var queryOps = map[string]bool{
	OpLeaseQuery: true, OpSourceQueryReg: true, OpSourceQuerySave: true, OpRevisionDigest: true,
	OpReviewQuery: true, OpPublicationQuery: true, OpActivationQuery: true,
}

// exchange sends one declared request and decodes a 2xx (or, where the
// operation answers a conflict as a result, a 409) body into out.
func (c *Client) exchange(ctx context.Context, op string, vars map[string]string, idempotencyKey, condition string, in, out any, conflictIsResult bool) error {
	method, target, err := c.decl.target(c.base, op, vars)
	if err != nil {
		return activities.Refused("DEPENDENCY_UNAVAILABLE", err)
	}
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return activities.Refused("INVALID_ARGUMENT", err)
		}
		if int64(len(raw)) > c.decl.MaxBodyBytes {
			return activities.Refused("INVALID_ARGUMENT", fmt.Errorf("%s request exceeds the declared body bound", op))
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return activities.Refused("INVALID_ARGUMENT", err)
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
			return errors.New("pagix: token file unreadable")
		}
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(tok)))
	}
	resp, err := c.http.Do(req)
	if err != nil {
		// Not proof that nothing happened (DD-06 §2): the caller queries the
		// original identity.
		return fmt.Errorf("pagix %s: no answer: %w", op, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, c.decl.MaxBodyBytes+1))
	if err != nil {
		return fmt.Errorf("pagix %s: answer unreadable: %w", op, err)
	}
	if int64(len(raw)) > c.decl.MaxBodyBytes {
		return fmt.Errorf("pagix %s: answer exceeds the declared body bound", op)
	}
	switch s := resp.StatusCode; {
	case s >= 200 && s < 300, s == http.StatusConflict && conflictIsResult:
		if out == nil {
			return nil
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(out); err != nil {
			return fmt.Errorf("pagix %s: answer is not the port contract: %w", op, err)
		}
		return nil
	case s == http.StatusNotFound && queryOps[op]:
		return errNoRecord
	case s == http.StatusBadRequest, s == http.StatusUnprocessableEntity:
		return activities.Refused("INVALID_ARGUMENT", fmt.Errorf("pagix %s refused the request (%d)", op, s))
	case s == http.StatusUnauthorized, s == http.StatusForbidden:
		return activities.Refused("FORBIDDEN", fmt.Errorf("pagix %s denied the caller (%d)", op, s))
	case s == http.StatusNotFound:
		return activities.Refused("NOT_FOUND", fmt.Errorf("pagix %s: not found", op))
	case s == http.StatusConflict, s == http.StatusPreconditionFailed:
		return activities.Refused("REVISION_CONFLICT", fmt.Errorf("pagix %s: conflict (%d)", op, s))
	case s >= 300 && s < 400:
		return activities.Refused("DEPENDENCY_UNAVAILABLE", fmt.Errorf("pagix %s answered a redirect (%d), never followed", op, s))
	default:
		return fmt.Errorf("pagix %s answered %d", op, s)
	}
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
