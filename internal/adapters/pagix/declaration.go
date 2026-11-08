package pagix

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"

	"sigs.k8s.io/yaml"
)

// Declaration is the ENV-07 adapter declaration of the Pagix ports (DD-06
// §2, P2.2): the integration owner records the upstream's actual API
// version, the method and path of every port operation, the auth issuer
// and audience, the idempotency and conditional-write headers and the body
// bound, and, per DD-06 §2 (RB-A02), each operation's request and response
// schema, conflict, rejection and not-sent statuses and idempotency scope
// and retention, the receipt signatures and digests, pagination, rate and
// time bounds and the maximum exposure. A capability counts as proven only
// when the declaration lists it under verified with its evidence; every
// other one is UNVERIFIED (Unverified), and the adapter treats it as
// unproven: a declared not-sent status classifies an answer as proven
// not-sent only when verified. Nothing here is invented by this service: an operation the
// declaration omits is not supported and its port call refuses (a query
// without upstream support leaves the original UNKNOWN for controlled
// reconciliation, never a resend). The request and response bodies are the
// port types' JSON (the port contract); an upstream that differs is
// adapted here, and the conformance suite (tests/conformance/pagix) is
// what proves a declaration against an environment.
type Declaration struct {
	SchemaVersion int    `json:"schemaVersion"`
	APIVersion    string `json:"apiVersion"`
	// Auth is recorded for review; the transport credential is the
	// Workflow's configuration (workload mTLS or a token file).
	Auth struct {
		Issuer   string `json:"issuer"`
		Audience string `json:"audience"`
	} `json:"auth"`
	IdempotencyHeader string               `json:"idempotencyHeader"`
	ConditionHeader   string               `json:"conditionHeader"`
	MaxBodyBytes      int64                `json:"maxBodyBytes"`
	Operations        map[string]Operation `json:"operations"`
	// Receipts: how the upstream signs and digests a publication or
	// activation receipt (the scheme names as the upstream states them).
	Receipts *Receipts `json:"receipts,omitempty"`
	// Pagination: the upstream's paging scheme for listing reads.
	Pagination string `json:"pagination,omitempty"`
	// Rate: the upstream's declared request rate and per-request time bound.
	Rate *Rate `json:"rate,omitempty"`
	// MaxExposure: what one mutation can commit at most, as the upstream
	// states it (amount and unit).
	MaxExposure string `json:"maxExposure,omitempty"`
	// Verified lists the declaration-wide capabilities proven (Capabilities),
	// Evidence where the proof is recorded.
	Verified []string `json:"verified,omitempty"`
	Evidence string   `json:"evidence,omitempty"`
}

// Operation is one declared HTTP exchange.
type Operation struct {
	Method string `json:"method"`
	Path   string `json:"path"`
	// RequestSchema and ResponseSchema name the upstream's schemas of the
	// exchange (a JSON Pointer into its OpenAPI document).
	RequestSchema  string `json:"requestSchema,omitempty"`
	ResponseSchema string `json:"responseSchema,omitempty"`
	// ConflictStatuses answer a conditional-write conflict (409 and 412 when
	// none are declared); RejectionStatuses an explicit business rejection;
	// NotSentStatuses an answer the upstream gives only when it did not act.
	ConflictStatuses  []int `json:"conflictStatuses,omitempty"`
	RejectionStatuses []int `json:"rejectionStatuses,omitempty"`
	NotSentStatuses   []int `json:"notSentStatuses,omitempty"`
	// Idempotency: the scope and retention of the idempotency key.
	Idempotency *Idempotency `json:"idempotency,omitempty"`
	// Verified lists this operation's proven capabilities
	// (OperationCapabilities), Evidence where the proof is recorded.
	Verified []string `json:"verified,omitempty"`
	Evidence string   `json:"evidence,omitempty"`
}

// Idempotency is an operation's idempotency-key semantics.
type Idempotency struct {
	Scope            string `json:"scope"`
	RetentionSeconds int64  `json:"retentionSeconds"`
}

// Receipts is the receipt signature and digest scheme.
type Receipts struct {
	Signature string `json:"signature"`
	Digest    string `json:"digest"`
}

// Rate is the upstream's declared request rate and time bound.
type Rate struct {
	RequestsPerMinute int `json:"requestsPerMinute"`
	TimeoutSeconds    int `json:"timeoutSeconds"`
}

// The capabilities a declaration can prove (DD-06 §2): per operation, and
// declaration-wide. A capability is verified only with its record present
// and evidence named; one successful example is not a duplicate,
// concurrency or recovery proof.
const (
	CapSchema        = "schema"
	CapConflict      = "conflict-statuses"
	CapRejection     = "rejection-statuses"
	CapNotSent       = "not-sent"
	CapIdempotency   = "idempotency"
	CapQueryIdentity = "query-by-identity"
	CapAuth          = "auth"
	CapReceipts      = "receipts"
	CapPagination    = "pagination"
	CapRate          = "rate"
	CapExposure      = "exposure"
)

// OperationCapabilities and Capabilities are the provable capabilities in
// a stable order.
var (
	OperationCapabilities = []string{CapSchema, CapConflict, CapRejection, CapNotSent, CapIdempotency, CapQueryIdentity}
	Capabilities          = []string{CapAuth, CapReceipts, CapPagination, CapRate, CapExposure}
)

// The port operations a declaration may name.
const (
	OpLeaseAcquire       = "lease.acquire"
	OpLeaseRenew         = "lease.renew"
	OpLeaseQuery         = "lease.query"
	OpLeaseRelease       = "lease.release"
	OpSourceCheckScope   = "source.check-scope"
	OpSourceRegister     = "source.register-candidate"
	OpSourceQueryReg     = "source.query-registration"
	OpSourceSave         = "source.save-revision"
	OpSourceQuerySave    = "source.query-save"
	OpSourceCurrent      = "source.current-revision"
	OpRevisionDigest     = "source.revision-digest"
	OpReviewRegister     = "review.register"
	OpReviewQuery        = "review.query"
	OpReviewDecision     = "review.decision"
	OpPublish            = "publication.publish"
	OpPublicationQuery   = "publication.query"
	OpPublicationFetch   = "publication.fetch"
	OpActivate           = "activation.activate"
	OpActivationQuery    = "activation.query"
	OpCatalogRevision    = "catalog.revision"
	defaultMaxBodyBytes  = 32 << 20
	defaultIdempotencyHd = "Idempotency-Key"
	defaultConditionHd   = "If-Match"
)

// Operations are every operation name in a stable order.
var Operations = []string{
	OpLeaseAcquire, OpLeaseRenew, OpLeaseQuery, OpLeaseRelease,
	OpSourceCheckScope, OpSourceRegister, OpSourceQueryReg, OpSourceSave, OpSourceQuerySave, OpSourceCurrent, OpRevisionDigest,
	OpReviewRegister, OpReviewQuery, OpReviewDecision, OpPublish, OpPublicationQuery, OpPublicationFetch,
	OpActivate, OpActivationQuery, OpCatalogRevision,
}

var (
	pathVar    = regexp.MustCompile(`\{([a-zA-Z]+)\}`)
	knownVars  = map[string]bool{"subject": true, "effectId": true, "lineage": true, "revision": true, "releaseId": true, "target": true, "destination": true, "path": true, "occurrence": true}
	headerName = regexp.MustCompile(`^[A-Za-z0-9-]{1,64}$`)
)

// LoadDeclaration reads and validates a declaration file; unknown keys,
// an unknown operation, a method outside GET/POST/PUT/PATCH, a path that
// is not absolute or names an unknown variable reject it.
func LoadDeclaration(path string) (*Declaration, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("pagix declaration: %w", err)
	}
	return ParseDeclaration(raw)
}

func ParseDeclaration(raw []byte) (*Declaration, error) {
	var d Declaration
	if err := yaml.UnmarshalStrict(raw, &d); err != nil {
		return nil, fmt.Errorf("pagix declaration: %w", err)
	}
	if d.SchemaVersion != 1 || d.APIVersion == "" {
		return nil, fmt.Errorf("pagix declaration: needs schemaVersion 1 and the upstream apiVersion")
	}
	if d.IdempotencyHeader == "" {
		d.IdempotencyHeader = defaultIdempotencyHd
	}
	if d.ConditionHeader == "" {
		d.ConditionHeader = defaultConditionHd
	}
	if !headerName.MatchString(d.IdempotencyHeader) || !headerName.MatchString(d.ConditionHeader) {
		return nil, fmt.Errorf("pagix declaration: header names must be HTTP tokens")
	}
	if d.MaxBodyBytes <= 0 {
		d.MaxBodyBytes = defaultMaxBodyBytes
	}
	known := map[string]bool{}
	for _, op := range Operations {
		known[op] = true
	}
	var problems []string
	for name, op := range d.Operations {
		if !known[name] {
			problems = append(problems, "unknown operation "+name)
			continue
		}
		switch op.Method {
		case "GET", "POST", "PUT", "PATCH":
		default:
			problems = append(problems, fmt.Sprintf("%s: method %q", name, op.Method))
		}
		if !strings.HasPrefix(op.Path, "/") || strings.Contains(op.Path, "..") {
			problems = append(problems, fmt.Sprintf("%s: path %q is not absolute", name, op.Path))
		}
		for _, m := range pathVar.FindAllStringSubmatch(op.Path, -1) {
			if !knownVars[m[1]] {
				problems = append(problems, fmt.Sprintf("%s: path variable {%s}", name, m[1]))
			}
		}
	}
	for name, op := range d.Operations {
		if known[name] {
			problems = append(problems, op.problems(name)...)
		}
	}
	problems = append(problems, d.problems()...)
	if len(problems) > 0 {
		sort.Strings(problems)
		return nil, fmt.Errorf("pagix declaration: %s", strings.Join(problems, "; "))
	}
	return &d, nil
}

// recorded reports whether the operation records what capability claims.
func (o Operation) recorded(capability string) bool {
	switch capability {
	case CapSchema:
		return o.RequestSchema != "" || o.ResponseSchema != ""
	case CapConflict:
		return len(o.ConflictStatuses) > 0
	case CapRejection:
		return len(o.RejectionStatuses) > 0
	case CapNotSent:
		return len(o.NotSentStatuses) > 0
	case CapIdempotency:
		return o.Idempotency != nil
	case CapQueryIdentity:
		return true
	}
	return false
}

// verifies reports whether the operation proves capability.
func (o Operation) verifies(capability string) bool {
	return slices.Contains(o.Verified, capability) && o.recorded(capability) && o.Evidence != ""
}

func (o Operation) problems(name string) []string {
	var out []string
	seen := map[int]string{}
	for class, statuses := range map[string][]int{"conflictStatuses": o.ConflictStatuses, "rejectionStatuses": o.RejectionStatuses, "notSentStatuses": o.NotSentStatuses} {
		for _, s := range statuses {
			switch {
			case s < 400 || s > 599:
				out = append(out, fmt.Sprintf("%s: %s names %d, not a 4xx or 5xx status", name, class, s))
			case class != "notSentStatuses" && s >= 500:
				out = append(out, fmt.Sprintf("%s: %s names %d, not a 4xx status", name, class, s))
			case seen[s] != "":
				out = append(out, fmt.Sprintf("%s: status %d is in both %s and %s", name, s, seen[s], class))
			}
			if seen[s] == "" {
				seen[s] = class
			}
		}
	}
	if i := o.Idempotency; i != nil && (i.Scope == "" || i.RetentionSeconds <= 0) {
		out = append(out, name+": idempotency needs its scope and a positive retentionSeconds")
	}
	for _, c := range o.Verified {
		switch {
		case !slices.Contains(OperationCapabilities, c):
			out = append(out, fmt.Sprintf("%s: verified names unknown capability %q", name, c))
		case !o.recorded(c):
			out = append(out, fmt.Sprintf("%s: verified %s without its record", name, c))
		}
	}
	if len(o.Verified) > 0 && o.Evidence == "" {
		out = append(out, name+": verified capabilities need their evidence")
	}
	return out
}

func (d *Declaration) recorded(capability string) bool {
	switch capability {
	case CapAuth:
		return d.Auth.Issuer != "" && d.Auth.Audience != ""
	case CapReceipts:
		return d.Receipts != nil
	case CapPagination:
		return d.Pagination != ""
	case CapRate:
		return d.Rate != nil
	case CapExposure:
		return d.MaxExposure != ""
	}
	return false
}

func (d *Declaration) problems() []string {
	var out []string
	if r := d.Receipts; r != nil && (r.Signature == "" || r.Digest == "") {
		out = append(out, "receipts need their signature and digest scheme")
	}
	if r := d.Rate; r != nil && (r.RequestsPerMinute <= 0 || r.TimeoutSeconds <= 0) {
		out = append(out, "rate needs a positive requestsPerMinute and timeoutSeconds")
	}
	for _, c := range d.Verified {
		switch {
		case !slices.Contains(Capabilities, c):
			out = append(out, fmt.Sprintf("verified names unknown capability %q", c))
		case !d.recorded(c):
			out = append(out, fmt.Sprintf("verified %s without its record", c))
		}
	}
	if len(d.Verified) > 0 && d.Evidence == "" {
		out = append(out, "verified capabilities need their evidence")
	}
	return out
}

// Unverified names every capability the declaration does not prove: the
// declaration-wide ones, then each declared operation's ("<operation>:
// <capability>"), in a stable order. An empty list is a fully proven
// declaration.
func (d *Declaration) Unverified() []string {
	var out []string
	for _, c := range Capabilities {
		if !(slices.Contains(d.Verified, c) && d.recorded(c) && d.Evidence != "") {
			out = append(out, c)
		}
	}
	for _, name := range Operations {
		op, ok := d.Operations[name]
		if !ok {
			continue
		}
		for _, c := range OperationCapabilities {
			if !op.verifies(c) {
				out = append(out, name+": "+c)
			}
		}
	}
	return out
}

// Missing names the operations the declaration does not support.
func (d *Declaration) Missing() []string {
	var out []string
	for _, op := range Operations {
		if _, ok := d.Operations[op]; !ok {
			out = append(out, op)
		}
	}
	return out
}

// target expands an operation's path with escaped variables.
func (d *Declaration) target(base *url.URL, op string, vars map[string]string) (string, string, error) {
	o, ok := d.Operations[op]
	if !ok {
		return "", "", fmt.Errorf("operation %s is not declared by the Pagix adapter declaration (%s)", op, d.APIVersion)
	}
	var missing string
	path := pathVar.ReplaceAllStringFunc(o.Path, func(m string) string {
		name := m[1 : len(m)-1]
		v, ok := vars[name]
		if !ok || v == "" {
			missing = name
		}
		return url.PathEscape(v)
	})
	if missing != "" {
		return "", "", fmt.Errorf("operation %s needs {%s}", op, missing)
	}
	return o.Method, strings.TrimSuffix(base.String(), "/") + path, nil
}
