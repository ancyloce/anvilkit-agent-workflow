package artifacts

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
)

// forbidden are the substrings a transfer failure must never carry (P1.6
// AC4): the signing parameters, the bucket and the object key.
var forbidden = []string{"X-Amz-", "Signature=", "anvilkit-artifacts", "tenant_a/op_red", "xfer_red", "AKIAEXAMPLE"}

func presigned(base string) string {
	return base + "/anvilkit-artifacts/artifacts/tenant_a/op_red/xfer_red?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAEXAMPLE%2F20261011%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20261011T000000Z&X-Amz-Expires=300&X-Amz-SignedHeaders=host&X-Amz-Signature=0123456789abcdef"
}

// TestTransferFailuresWithholdTheCapability is P1.6 AC4 at the source: a
// forced transport failure of an upload or a download states its kind and
// never the capability URL.
func TestTransferFailuresWithholdTheCapability(t *testing.T) {
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	refusedBase := "http://" + closed.Addr().String()
	require.NoError(t, closed.Close())
	reset := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	defer reset.Close()
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer hang.Close()
	c := &Client{http: &http.Client{Timeout: 300 * time.Millisecond}}
	for name, url := range map[string]string{"refused": presigned(refusedBase), "reset": presigned(reset.URL), "timeout": presigned(hang.URL), "malformed": "http://[::1" + presigned("")} {
		_, perr := c.put(context.Background(), &controlv1.TransferCapability{Method: "PUT", Url: url}, []byte("x"))
		_, gerr := c.get(context.Background(), &controlv1.TransferCapability{Method: "GET", Url: url}, 1)
		for _, err := range []error{perr, gerr} {
			require.Error(t, err, name)
			for _, f := range forbidden {
				require.NotContains(t, err.Error(), f, "%s: %v", name, err)
			}
		}
		t.Logf("%s: %v | %v", name, perr, gerr)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = c.put(ctx, &controlv1.TransferCapability{Method: "PUT", Url: presigned(hang.URL)}, []byte("x"))
	require.ErrorIs(t, err, context.Canceled, "a cancellation stays recognizable")
	require.False(t, strings.Contains(err.Error(), "X-Amz-"))
}
