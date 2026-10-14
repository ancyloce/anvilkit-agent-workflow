// Package pagix is the conformance suite of the Pagix port contract (P2.2,
// DD-06): the same cases run against the DEVELOPMENT_ONLY double in
// process, against the declaration-driven HTTP adapter talking to the
// reference endpoint below (the double served through a declaration), and,
// when ANVILKIT_PAGIX_URL and ANVILKIT_PAGIX_DECLARATION name an ENV-07
// environment, against the real upstream. A run without those inputs is
// reported UNEXECUTED with the missing item named, never as a pass.
package pagix

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
	pagixadapter "github.com/ancyloce/anvilkit-agent-workflow/internal/adapters/pagix"
)

// Reference serves a Double under a declaration: the conformance target of
// the adapter when no upstream exists. It records the idempotency keys and
// conditions it saw so the suite can assert what went on the wire.
type Reference struct {
	double *pagixadapter.Double
	routes []route
	Seen   []Exchange
}

// Exchange is one request the reference endpoint answered.
type Exchange struct {
	Operation      string
	IdempotencyKey string
	Condition      string
}

type route struct {
	op     string
	method string
	re     *regexp.Regexp
	names  []string
}

var varPattern = regexp.MustCompile(`\{([a-zA-Z]+)\}`)

// NewReference compiles the declaration's routes.
func NewReference(double *pagixadapter.Double, decl *pagixadapter.Declaration) *Reference {
	r := &Reference{double: double}
	for name, op := range decl.Operations {
		var names []string
		var b strings.Builder
		b.WriteString("^")
		last := 0
		for _, m := range varPattern.FindAllStringSubmatchIndex(op.Path, -1) {
			b.WriteString(regexp.QuoteMeta(op.Path[last:m[0]]))
			b.WriteString("([^/]+)")
			names = append(names, op.Path[m[2]:m[3]])
			last = m[1]
		}
		b.WriteString(regexp.QuoteMeta(op.Path[last:]) + "$")
		r.routes = append(r.routes, route{op: name, method: op.Method, re: regexp.MustCompile(b.String()), names: names})
	}
	return r
}

func (r *Reference) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	path := req.URL.EscapedPath()
	for _, rt := range r.routes {
		m := rt.re.FindStringSubmatch(path)
		if m == nil || req.Method != rt.method {
			continue
		}
		vars := map[string]string{}
		for i, n := range rt.names {
			v, err := url.PathUnescape(m[i+1])
			if err != nil {
				http.Error(w, "bad path", http.StatusBadRequest)
				return
			}
			vars[n] = v
		}
		cond, _ := strconv.Unquote(req.Header.Get("If-Match"))
		r.Seen = append(r.Seen, Exchange{Operation: rt.op, IdempotencyKey: req.Header.Get("Idempotency-Key"), Condition: cond})
		status, body, err := r.serve(req, rt.op, vars)
		if err != nil {
			status = http.StatusInternalServerError
			if code := activities.RefusalCode(err); code == "REVISION_CONFLICT" {
				status = http.StatusConflict
			}
			body = map[string]string{"error": err.Error()}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
		return
	}
	http.NotFound(w, req)
}

func decode(req *http.Request, v any) error {
	dec := json.NewDecoder(req.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

var errUndecodable = errors.New("request is not the port contract")

func (r *Reference) serve(req *http.Request, op string, vars map[string]string) (int, any, error) {
	ctx, d := req.Context(), r.double
	found := func(v any, known bool, err error) (int, any, error) {
		switch {
		case err != nil:
			return 0, nil, err
		case !known:
			return http.StatusNotFound, map[string]string{"error": "no record"}, nil
		}
		return http.StatusOK, v, nil
	}
	switch op {
	case pagixadapter.OpLeaseAcquire, pagixadapter.OpLeaseRenew, pagixadapter.OpLeaseRelease:
		var in activities.LeaseInput
		if decode(req, &in) != nil {
			return http.StatusBadRequest, nil, errUndecodable
		}
		var out activities.LeaseResult
		var err error
		switch op {
		case pagixadapter.OpLeaseAcquire:
			out, err = d.Acquire(ctx, in)
		case pagixadapter.OpLeaseRenew:
			out, err = d.Renew(ctx, in)
		default:
			out, err = d.Release(ctx, in)
		}
		return http.StatusOK, out, err
	case pagixadapter.OpLeaseQuery:
		var in activities.LeaseInput
		if decode(req, &in) != nil {
			return http.StatusBadRequest, nil, errUndecodable
		}
		out, err := d.Query(ctx, in)
		return http.StatusOK, out, err
	case pagixadapter.OpSourceCheckScope:
		var in activities.CheckSourceScopeInput
		if decode(req, &in) != nil {
			return http.StatusBadRequest, nil, errUndecodable
		}
		out, err := d.CheckScope(ctx, in)
		return http.StatusOK, out, err
	case pagixadapter.OpSourceRegister:
		var in activities.RegisterCandidateInput
		if decode(req, &in) != nil {
			return http.StatusBadRequest, nil, errUndecodable
		}
		out, err := d.RegisterCandidate(ctx, vars["effectId"], in)
		return http.StatusOK, out, err
	case pagixadapter.OpSourceQueryReg:
		return found(d.QueryRegistration(ctx, vars["effectId"]))
	case pagixadapter.OpSourceSave:
		var in activities.SaveSourceInput
		if decode(req, &in) != nil {
			return http.StatusBadRequest, nil, errUndecodable
		}
		out, err := d.SaveRevision(ctx, vars["effectId"], in)
		if err == nil && out.State == "conflict" {
			return http.StatusConflict, out, nil
		}
		return http.StatusOK, out, err
	case pagixadapter.OpSourceQuerySave:
		return found(d.QuerySave(ctx, vars["effectId"]))
	case pagixadapter.OpSourceCurrent:
		rev, err := d.CurrentRevision(ctx, vars["lineage"])
		return http.StatusOK, map[string]string{"revision": rev}, err
	case pagixadapter.OpRevisionDigest:
		digest, known, err := d.RevisionDigest(ctx, vars["lineage"], vars["revision"])
		return found(map[string]string{"digest": digest}, known, err)
	case pagixadapter.OpReviewRegister:
		var in activities.RegisterReviewInput
		if decode(req, &in) != nil {
			return http.StatusBadRequest, nil, errUndecodable
		}
		out, err := d.RegisterReview(ctx, vars["effectId"], in)
		return http.StatusOK, out, err
	case pagixadapter.OpReviewQuery:
		return found(d.QueryReview(ctx, vars["effectId"]))
	case pagixadapter.OpReviewDecision:
		out, err := d.ReviewDecision(ctx, vars["releaseId"])
		return http.StatusOK, out, err
	case pagixadapter.OpPublish:
		var in activities.PublishInput
		if decode(req, &in) != nil {
			return http.StatusBadRequest, nil, errUndecodable
		}
		out, err := d.Publish(ctx, vars["effectId"], in)
		return http.StatusOK, out, err
	case pagixadapter.OpPublicationQuery:
		return found(d.QueryPublication(ctx, vars["effectId"]))
	case pagixadapter.OpPublicationFetch:
		b, err := d.FetchPublished(ctx, vars["target"], vars["destination"], vars["releaseId"], vars["path"])
		return http.StatusOK, map[string][]byte{"bytes": b}, err
	case pagixadapter.OpActivate:
		var in activities.ActivateInput
		if decode(req, &in) != nil {
			return http.StatusBadRequest, nil, errUndecodable
		}
		out, err := d.Activate(ctx, vars["effectId"], in)
		if err == nil && out.State == "conflict" {
			return http.StatusConflict, out, nil
		}
		return http.StatusOK, out, err
	case pagixadapter.OpActivationQuery:
		return found(d.QueryActivation(ctx, vars["effectId"]))
	case pagixadapter.OpCatalogRevision:
		rev, err := d.CatalogRevision(ctx)
		return http.StatusOK, map[string]string{"revision": rev}, err
	}
	return http.StatusNotFound, nil, fmt.Errorf("operation %s", op)
}
