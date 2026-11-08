package pagix

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// TestExchangeErrorTaxonomy (RB-A02, DD-06 §2): a failed exchange is
// classified invalid, denied, conflict, rejection, capacity, not-sent or
// unknown. Proven not-sent is only a request that never left the process or
// a status the declaration verifies as not-sent; an HTTP 500, a redirect or
// a lost connection is unknown. The public codes and retryability stay what
// each status had before the classes.
func TestExchangeErrorTaxonomy(t *testing.T) {
	var status atomic.Int32
	var reached atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached.Add(1)
		s := int(status.Load())
		switch {
		case s == 0: // the connection drops without an answer
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
		case s == 200:
			_, _ = w.Write([]byte(`{"revision":"1"}`))
		case s == 299: // a success that is not the port contract
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"unexpected":true}`))
		default:
			if s >= 300 && s < 400 {
				w.Header().Set("Location", "https://elsewhere.example/")
			}
			w.WriteHeader(s)
		}
	}))
	t.Cleanup(srv.Close)
	decl, err := ParseDeclaration([]byte(proven))
	require.NoError(t, err)
	client, err := NewClient(decl, ClientOptions{BaseURL: srv.URL, HTTPClient: srv.Client()})
	require.NoError(t, err)
	vars := map[string]string{"effectId": "eff_1"}
	call := func(op string, s int, in any) error {
		status.Store(int32(s))
		var out struct {
			Revision string `json:"revision"`
		}
		return client.exchange(context.Background(), op, vars, "eff_1", "", in, &out, false)
	}

	for _, tc := range []struct {
		name   string
		op     string
		status int
		class  Class
		code   string // the refusal code; "" for a retryable error
	}{
		{"400 is invalid input", OpCatalogRevision, 400, ClassInvalid, "INVALID_ARGUMENT"},
		{"422 is invalid input", OpCatalogRevision, 422, ClassInvalid, "INVALID_ARGUMENT"},
		{"404 outside a query is invalid input", OpCatalogRevision, 404, ClassInvalid, "NOT_FOUND"},
		{"401 is a current denial", OpCatalogRevision, 401, ClassDenied, "FORBIDDEN"},
		{"403 is a current denial", OpCatalogRevision, 403, ClassDenied, "FORBIDDEN"},
		{"409 is a revision conflict", OpCatalogRevision, 409, ClassConflict, "REVISION_CONFLICT"},
		{"412 is a revision conflict", OpCatalogRevision, 412, ClassConflict, "REVISION_CONFLICT"},
		{"a declared conflict status", OpPublish, 423, ClassConflict, "REVISION_CONFLICT"},
		{"a declared rejection status is an explicit business rejection", OpPublish, 451, ClassRejection, "EFFECT_DENIED"},
		{"429 is a capacity failure, retried", OpCatalogRevision, 429, ClassCapacity, ""},
		{"503 is a capacity failure, retried", OpCatalogRevision, 503, ClassCapacity, ""},
		{"a not-sent status the declaration verifies is proven not-sent", OpPublish, 429, ClassNotSent, ""},
		{"a not-sent status it does not verify stays a capacity failure", OpActivate, 429, ClassCapacity, ""},
		{"500 is not proof of no effect", OpPublish, 500, ClassUnknown, ""},
		{"a redirect is unknown and never followed", OpPublish, 307, ClassUnknown, "DEPENDENCY_UNAVAILABLE"},
		{"a dropped connection is unknown, and the transport never sends it again", OpPublish, 0, ClassUnknown, ""},
		{"a success that is not the port contract is unknown", OpPublish, 299, ClassUnknown, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := reached.Load()
			err := call(tc.op, tc.status, nil)
			require.Error(t, err)
			require.Equal(t, tc.class, ClassOf(err), "%v", err)
			require.Equal(t, tc.code, activities.RefusalCode(err), "%v", err)
			require.Contains(t, err.Error(), "[class "+string(tc.class)+"]", "the class travels in the message across the Activity boundary")
			require.Equal(t, before+1, reached.Load(), "the request was sent once")
		})
	}

	t.Run("a mutation whose reused connection drops reaches the upstream once", func(t *testing.T) {
		// A first answered exchange leaves a keep-alive connection, which the
		// drop then closes under the written request: net/http would replay a
		// request with an Idempotency-Key header and a re-readable body.
		require.NoError(t, call(OpCatalogRevision, 200, nil))
		before := reached.Load()
		err := call(OpPublish, 0, map[string]string{"target": "npm"})
		require.Equal(t, ClassUnknown, ClassOf(err), "%v", err)
		require.Equal(t, before+1, reached.Load(), "one physical send for one permit")
	})

	t.Run("an undeclared operation is proven not-sent: nothing reaches the upstream", func(t *testing.T) {
		before := reached.Load()
		err := call(OpLeaseAcquire, 200, nil)
		require.Equal(t, ClassNotSent, ClassOf(err))
		require.Equal(t, "DEPENDENCY_UNAVAILABLE", activities.RefusalCode(err))
		require.Equal(t, before, reached.Load())
	})

	t.Run("a request over the declared body bound is proven not-sent", func(t *testing.T) {
		small := *decl
		small.MaxBodyBytes = 8
		c, err := NewClient(&small, ClientOptions{BaseURL: srv.URL, HTTPClient: srv.Client()})
		require.NoError(t, err)
		before := reached.Load()
		err = c.exchange(context.Background(), OpPublish, vars, "eff_1", "", map[string]string{"bytes": strings.Repeat("x", 64)}, nil, false)
		require.Equal(t, ClassNotSent, ClassOf(err))
		require.Equal(t, "INVALID_ARGUMENT", activities.RefusalCode(err))
		require.Equal(t, before, reached.Load())
	})

	t.Run("a success has no class; any other error is unknown", func(t *testing.T) {
		require.NoError(t, call(OpCatalogRevision, 200, nil))
		require.Equal(t, Class(""), ClassOf(nil))
		require.Equal(t, ClassUnknown, ClassOf(context.DeadlineExceeded))
	})
}
