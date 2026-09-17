package modelproxy_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/adapters/modelproxy"
)

// fixtures are the cross-runtime vectors of the contract; the Go side must
// read the same frames the Proxy (TypeScript) produces.
func fixtureFrames(t *testing.T) map[string]json.RawMessage {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "..", "..", "contracts", "openapi", "model-proxy.fixtures.json"))
	if err != nil {
		t.Skip("contracts/openapi/model-proxy.fixtures.json not beside this repository")
	}
	var doc struct {
		Cases []struct {
			Name     string          `json:"name"`
			Schema   string          `json:"schema"`
			Valid    bool            `json:"valid"`
			Instance json.RawMessage `json:"instance"`
		} `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &doc))
	out := map[string]json.RawMessage{}
	for _, c := range doc.Cases {
		if c.Schema == "StreamFrame" && c.Valid {
			out[c.Name] = c.Instance
		}
	}
	return out
}

// fakeProxy serves the frozen contract: a scripted SSE answer, the record, and the cancel.
type fakeProxy struct {
	srv      *httptest.Server
	posts    atomic.Int32
	auth     atomic.Value
	frames   []string
	status   int
	envelope string
	record   string
}

func newFakeProxy(t *testing.T) *fakeProxy {
	f := &fakeProxy{status: 200}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.auth.Store(r.Header.Get("Authorization"))
		switch {
		case r.Method == "POST" && r.URL.Path == "/api/v1/model-calls":
			f.posts.Add(1)
			if f.status != 200 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(f.status)
				_, _ = w.Write([]byte(f.envelope))
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(200)
			for i, fr := range f.frames {
				fmt.Fprintf(w, "id: %d\ndata: %s\n\n", i, fr)
			}
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/v1/model-calls/"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			_, _ = w.Write([]byte(f.record))
		case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/cancellations"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(202)
			_, _ = w.Write([]byte(f.record))
		case r.URL.Path == "/elsewhere":
			w.WriteHeader(200)
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func input(callID string) activities.ModelCallInput {
	return activities.ModelCallInput{
		CallID: callID, TenantID: "tenant_a", OperationID: "op_01J9ABCDEF", AttemptID: "att_01J9ABCDEF", InstanceID: "inst_01J9ABCDEF", ExecutionEpoch: 1,
		RouteID:         "route_planner_default",
		Messages:        []activities.ModelMessage{{Role: "system", Content: "You are the planner."}, {Role: "user", Content: "Plan the hero component."}},
		Tools:           []activities.ModelTool{{Name: "read_brief", Description: "Reads the frozen brief.", InputSchemaDigest: "sha256:0dc7fa9db7237a2b5c96f70f59bb00f73bb86a0ca5554e91c312f9ada26e18b3"}},
		MaxOutputTokens: 4096, MaxExposure: activities.Money{Currency: "USD", Amount: "250000"}, Deadline: time.Now().Add(time.Minute),
	}
}

func TestCallModelReadsTheContractFrames(t *testing.T) {
	frames := fixtureFrames(t)
	f := newFakeProxy(t)
	f.frames = []string{
		string(frames["admitted frame"]),
		`{"callId":"call_01J9ABCDEF","sequence":"1","type":"text","text":"Hello"}`,
		`{"callId":"call_01J9ABCDEF","sequence":"2","type":"text","text":" world"}`,
		strings.Replace(string(frames["tool call frame with arguments digest"]), `"sequence": "7"`, `"sequence": "3"`, 1),
		strings.Replace(string(frames["done frame with native usage counters as decimal strings"]), `"sequence": "12"`, `"sequence": "4"`, 1),
	}
	f.record = `{"callId":"call_01J9ABCDEF","routeId":"route_planner_default","state":"succeeded","dispatchId":"dsp_01J9ABCDEF","usage":{"inputUnits":"1200","outputUnits":"340","reasoningUnits":"0","cachedInputUnits":"0"},"nativeReference":"chatcmpl-1","createdAt":"2026-09-14T12:00:00Z","updatedAt":"2026-09-14T12:00:09.5Z"}`
	c, err := modelproxy.New(modelproxy.Options{BaseURL: f.srv.URL, Token: "workflow-token", Timeout: 10 * time.Second})
	require.NoError(t, err)
	beats := 0
	res, err := c.CallModel(t.Context(), input("call_01J9ABCDEF"), func() { beats++ })
	require.NoError(t, err)
	require.Equal(t, "succeeded", res.State)
	require.Equal(t, "Hello world", res.Text)
	require.Equal(t, 5, res.Frames)
	require.Equal(t, beats, res.Frames, "one heartbeat per frame")
	require.Len(t, res.ToolCalls, 1)
	require.Equal(t, activities.ModelToolCall{ToolCallID: "tc_1", Name: "read_brief", Arguments: "{}", ArgumentsDigest: "sha256:0dc7fa9db7237a2b5c96f70f59bb00f73bb86a0ca5554e91c312f9ada26e18b3"}, res.ToolCalls[0])
	require.Equal(t, &activities.ModelUsage{InputUnits: "1200", OutputUnits: "340", ReasoningUnits: "0", CachedInputUnits: "0"}, res.Usage)
	require.Equal(t, "dsp_01J9ABCDEF", res.DispatchID)
	require.Equal(t, "chatcmpl-1", res.NativeReference)
	require.Equal(t, "Bearer workflow-token", f.auth.Load(), "the worker presents its own identity")
	require.Equal(t, int32(1), f.posts.Load())

	got, err := c.GetModelCall(t.Context(), "call_01J9ABCDEF")
	require.NoError(t, err)
	require.Equal(t, "succeeded", got.State)
	require.Equal(t, int32(1), f.posts.Load(), "a query never opens a call")
	canceled, err := c.CancelModelCall(t.Context(), "call_01J9ABCDEF")
	require.NoError(t, err)
	require.Equal(t, "dsp_01J9ABCDEF", canceled.DispatchID)
}

func TestCallModelRefusesFramesOutsideTheContract(t *testing.T) {
	f := newFakeProxy(t)
	c, err := modelproxy.New(modelproxy.Options{BaseURL: f.srv.URL, Token: "t", Timeout: 10 * time.Second})
	require.NoError(t, err)
	for name, frames := range map[string][]string{
		"unknown member":     {`{"callId":"c","sequence":"0","type":"admitted","raw":{"provider":"body"}}`},
		"another call":       {`{"callId":"other","sequence":"0","type":"admitted"}`},
		"sequence gap":       {`{"callId":"c","sequence":"0","type":"admitted"}`, `{"callId":"c","sequence":"2","type":"done","outcome":"succeeded"}`},
		"non-canonical seq":  {`{"callId":"c","sequence":"00","type":"admitted"}`},
		"unknown frame type": {`{"callId":"c","sequence":"0","type":"retry"}`},
	} {
		f.frames = frames
		_, err := c.CallModel(t.Context(), input("c"), func() {})
		require.Equal(t, "INVALID_ARGUMENT", activities.RefusalCode(err), name)
	}
	f.frames = []string{`{"callId":"c","sequence":"0","type":"admitted"}`, `{"callId":"c","sequence":"1","type":"text","text":"partial"}`}
	_, err = c.CallModel(t.Context(), input("c"), func() {})
	require.Error(t, err)
	require.Empty(t, activities.RefusalCode(err), "a stream without its final frame is retryable: the same call id reenters")
	require.ErrorContains(t, err, "without a final frame")
}

func TestCallModelMapsRefusalsAndRetryableAnswers(t *testing.T) {
	f := newFakeProxy(t)
	c, err := modelproxy.New(modelproxy.Options{BaseURL: f.srv.URL, Token: "t", Timeout: 10 * time.Second})
	require.NoError(t, err)
	f.status, f.envelope = 403, `{"error":{"code":"STALE_EXECUTION","message":"the call deadline has passed","requestId":"req_1","retryable":false}}`
	_, err = c.CallModel(t.Context(), input("c"), func() {})
	require.Equal(t, "STALE_EXECUTION", activities.RefusalCode(err))
	f.status, f.envelope = 409, `{"error":{"code":"IDEMPOTENCY_CONFLICT","message":"other content","requestId":"req_2","retryable":false}}`
	_, err = c.CallModel(t.Context(), input("c"), func() {})
	require.Equal(t, "IDEMPOTENCY_CONFLICT", activities.RefusalCode(err))
	f.status, f.envelope = 503, `{"error":{"code":"DEPENDENCY_UNAVAILABLE","message":"Control did not answer","requestId":"req_3","retryable":true}}`
	_, err = c.CallModel(t.Context(), input("c"), func() {})
	require.Error(t, err)
	require.Empty(t, activities.RefusalCode(err), "a retryable answer is retried under the same call id")
	f.status, f.envelope = 500, `not json`
	_, err = c.CallModel(t.Context(), input("c"), func() {})
	require.ErrorContains(t, err, "without an error envelope")
}

func TestClientOptionsAndUnavailablePort(t *testing.T) {
	_, err := modelproxy.New(modelproxy.Options{BaseURL: "http://x", Token: "t", TLS: &modelproxy.TLSFiles{}})
	require.ErrorContains(t, err, "exactly one of")
	_, err = modelproxy.New(modelproxy.Options{BaseURL: "http://x"})
	require.ErrorContains(t, err, "exactly one of")
	_, err = modelproxy.Unavailable().CallModel(t.Context(), input("c"), func() {})
	require.Equal(t, "DEPENDENCY_UNAVAILABLE", activities.RefusalCode(err))
	// A redirect from the Proxy is never followed: the call reaches the
	// configured placement or fails.
	redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/elsewhere", http.StatusTemporaryRedirect)
	}))
	defer redirecting.Close()
	c, err := modelproxy.New(modelproxy.Options{BaseURL: redirecting.URL, Token: "t", Timeout: 5 * time.Second})
	require.NoError(t, err)
	_, err = c.CallModel(t.Context(), input("c"), func() {})
	require.ErrorContains(t, err, "307")
}
