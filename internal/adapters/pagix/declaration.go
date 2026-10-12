package pagix

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"

	"sigs.k8s.io/yaml"
)

// Declaration is the ENV-07 adapter declaration of the Pagix ports (DD-06
// §2, P2.2): the integration owner records the upstream's actual API
// version, the method and path of every port operation, the auth issuer
// and audience, the idempotency and conditional-write headers and the body
// bound. Nothing here is invented by this service: an operation the
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
}

// Operation is one declared HTTP exchange.
type Operation struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

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
	if len(problems) > 0 {
		sort.Strings(problems)
		return nil, fmt.Errorf("pagix declaration: %s", strings.Join(problems, "; "))
	}
	return &d, nil
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
