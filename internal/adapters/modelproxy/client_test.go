package modelproxy_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tmaxmax/go-sse"

	"github.com/ancyloce/anvilkit-agent-contracts/go/modelproxyapi"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/adapters/modelproxy"
)

// The positive StreamFrame vectors of the contract's cross-runtime fixture
// file (openapi/model-proxy.fixtures.json of the contracts repository,
// checked there against every runtime), as the Proxy writes them. They are
// read here through the client's strict decoder against the contract the
// pinned module embeds, so a vector this copy and the contract disagree on
// fails here rather than being skipped.
var fixtureFrames = map[string]string{
	"admitted frame":                                           `{"callId":"call_01J9ABCDEF","sequence":"0","type":"admitted"}`,
	"tool call frame with arguments digest":                    `{"callId":"call_01J9ABCDEF","sequence":"7","type":"tool_call","toolCall":{"toolCallId":"tc_1","name":"read_brief","argumentsDigest":"sha256:0dc7fa9db7237a2b5c96f70f59bb00f73bb86a0ca5554e91c312f9ada26e18b3","arguments":"{}"}}`,
	"done frame with native usage counters as decimal strings": `{"callId":"call_01J9ABCDEF","sequence":"12","type":"done","outcome":"succeeded","usage":{"inputUnits":"1200","outputUnits":"340","reasoningUnits":"0","cachedInputUnits":"0"}}`,
}

// fakeProxy serves the frozen contract: a scripted SSE answer (go-sse
// encodes the events; raw bytes stand in only for the negative shapes), the
// record, and the cancel.
type fakeProxy struct {
	srv      *httptest.Server
	posts    atomic.Int32
	auth     atomic.Value
	body     atomic.Value
	frames   []string
	status   int
	envelope string
	record   string
	// raw, when set, is written instead of the encoded frames (protocol shapes the encoder does not produce).
	raw string
	// fragment, when positive, writes the answer in pieces of that many bytes, flushed one by one.
	fragment int
	// cutBody, when set, declares a longer Content-Length than the envelope or
	// record written, so the connection closes before the body is complete.
	cutBody bool
}

// answer writes a non-stream body (an envelope or a record), cut short when cutBody is set.
func (f *fakeProxy) answer(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	if f.cutBody {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)+64))
	}
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

func newFakeProxy(t *testing.T) *fakeProxy {
	f := &fakeProxy{status: 200}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.auth.Store(r.Header.Get("Authorization"))
		switch {
		case r.Method == "POST" && r.URL.Path == "/api/v1/model-calls":
			f.posts.Add(1)
			raw, _ := readAll(r)
			f.body.Store(raw)
			if f.status != 200 {
				f.answer(w, f.status, f.envelope)
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(200)
			var out bytes.Buffer
			if f.raw != "" {
				out.WriteString(f.raw)
			} else {
				for i, fr := range f.frames {
					m := &sse.Message{ID: sse.ID(strconv.Itoa(i))}
					m.AppendData(fr)
					_, _ = m.WriteTo(&out)
				}
			}
			if f.fragment > 0 {
				b := out.Bytes()
				for i := 0; i < len(b); i += f.fragment {
					end := min(i+f.fragment, len(b))
					_, _ = w.Write(b[i:end])
					w.(http.Flusher).Flush()
				}
				return
			}
			_, _ = w.Write(out.Bytes())
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/v1/model-calls/"):
			if f.status != 200 {
				f.answer(w, f.status, f.envelope)
				return
			}
			f.answer(w, 200, f.record)
		case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/cancellations"):
			if f.status != 200 {
				f.answer(w, f.status, f.envelope)
				return
			}
			f.answer(w, 202, f.record)
		case r.URL.Path == "/elsewhere":
			w.WriteHeader(200)
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func readAll(r *http.Request) (string, error) {
	var b bytes.Buffer
	_, err := b.ReadFrom(r.Body)
	return b.String(), err
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
	frames := fixtureFrames
	f := newFakeProxy(t)
	f.frames = []string{
		frames["admitted frame"],
		`{"callId":"call_01J9ABCDEF","sequence":"1","type":"text","text":"Hello"}`,
		`{"callId":"call_01J9ABCDEF","sequence":"2","type":"text","text":" world"}`,
		strings.Replace(frames["tool call frame with arguments digest"], `"sequence":"7"`, `"sequence":"3"`, 1),
		strings.Replace(frames["done frame with native usage counters as decimal strings"], `"sequence":"12"`, `"sequence":"4"`, 1),
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

func TestCallModelReadsTheProtocolShapes(t *testing.T) {
	// The same frames however the wire delivers them: one data line, several
	// data lines (joined by newlines, whitespace to the JSON decoder), CRLF
	// terminators with keepalive comments in between, and pieces that cut
	// lines and multibyte sequences. The decoded frames agree.
	frames := []string{
		`{"callId":"c","sequence":"0","type":"admitted"}`,
		`{"callId":"c","sequence":"1","type":"text","text":"héllo 🎉 日本"}`,
		`{"callId":"c","sequence":"2","type":"done","outcome":"succeeded","usage":{"inputUnits":"3","outputUnits":"1","reasoningUnits":"0","cachedInputUnits":"0"}}`,
	}
	f := newFakeProxy(t)
	f.record = `{"callId":"c","routeId":"r","state":"succeeded","dispatchId":"d","createdAt":"2026-09-14T12:00:00Z","updatedAt":"2026-09-14T12:00:09.5Z"}`
	c, err := modelproxy.New(modelproxy.Options{BaseURL: f.srv.URL, Token: "t", Timeout: 10 * time.Second})
	require.NoError(t, err)
	f.frames = frames
	single, err := c.CallModel(t.Context(), input("c"), func() {})
	require.NoError(t, err)
	require.Equal(t, "succeeded", single.State)
	require.Equal(t, "héllo 🎉 日本", single.Text)
	require.Equal(t, 3, single.Frames)
	f.frames, f.raw = nil, "id: 0\ndata: "+frames[0]+"\n\n"+
		"id: 1\ndata: {\"callId\":\"c\",\"sequence\":\"1\",\ndata: \"type\":\"text\",\ndata: \"text\":\"héllo 🎉 日本\"}\n\n"+
		"id: 2\ndata: "+frames[2]+"\n\n"
	multiline, err := c.CallModel(t.Context(), input("c"), func() {})
	require.NoError(t, err)
	require.Equal(t, single, multiline)
	f.raw = ": keepalive\r\n\r\nid: 0\r\ndata: " + frames[0] + "\r\n\r\n: keepalive\r\n\r\nid: 1\r\ndata: " + frames[1] + "\r\n\r\nid: 2\r\ndata: " + frames[2] + "\r\n\r\n"
	crlf, err := c.CallModel(t.Context(), input("c"), func() {})
	require.NoError(t, err)
	require.Equal(t, single, crlf)
	f.raw, f.frames, f.fragment = "", frames, 7
	fragmented, err := c.CallModel(t.Context(), input("c"), func() {})
	require.NoError(t, err)
	require.Equal(t, single, fragmented)
	f.fragment = 0
	// An event over the frame bound is refused, never read partially.
	bounded, err := modelproxy.New(modelproxy.Options{BaseURL: f.srv.URL, Token: "t", Timeout: 10 * time.Second, MaxFrameBytes: 64})
	require.NoError(t, err)
	f.frames = []string{frames[0], `{"callId":"c","sequence":"1","type":"text","text":"` + strings.Repeat("x", 100) + `"}`, frames[2]}
	_, err = bounded.CallModel(t.Context(), input("c"), func() {})
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds the bound")
	// The final frame is the last one read: frames after it are not consumed.
	f.frames = append(append([]string{}, frames...), `{"callId":"c","sequence":"3","type":"text","text":"late"}`)
	after, err := c.CallModel(t.Context(), input("c"), func() {})
	require.NoError(t, err)
	require.Equal(t, 3, after.Frames)
}

func TestRequestCarriesTheToolRoundTrip(t *testing.T) {
	f := newFakeProxy(t)
	f.frames = []string{`{"callId":"c","sequence":"0","type":"admitted"}`, `{"callId":"c","sequence":"1","type":"done","outcome":"succeeded","usage":{"inputUnits":"3","outputUnits":"1","reasoningUnits":"0","cachedInputUnits":"0"}}`}
	f.record = `{"callId":"c","routeId":"r","state":"succeeded","dispatchId":"d","createdAt":"2026-09-14T12:00:00Z","updatedAt":"2026-09-14T12:00:09.5Z"}`
	c, err := modelproxy.New(modelproxy.Options{BaseURL: f.srv.URL, Token: "t", Timeout: 10 * time.Second})
	require.NoError(t, err)
	in := input("c")
	in.Messages = append(in.Messages,
		activities.ModelMessage{Role: "assistant", Content: "", ToolCalls: []activities.ModelToolCall{{ToolCallID: "tc_1", Name: "read_brief", Arguments: `{"section":"hero"}`, ArgumentsDigest: "sha256:0dc7fa9db7237a2b5c96f70f59bb00f73bb86a0ca5554e91c312f9ada26e18b3"}}},
		activities.ModelMessage{Role: "tool", ToolCallID: "tc_1", Content: "The brief: one hero."},
	)
	_, err = c.CallModel(t.Context(), in, func() {})
	require.NoError(t, err)
	var sent modelproxyapi.ModelCallRequest
	dec := json.NewDecoder(strings.NewReader(f.body.Load().(string)))
	dec.DisallowUnknownFields()
	require.NoError(t, dec.Decode(&sent), "the request is the contract's, tool calls included")
	require.Len(t, sent.Messages, 4)
	require.NotNil(t, sent.Messages[2].ToolCalls)
	require.Equal(t, []modelproxyapi.MessageToolCall{{ToolCallId: "tc_1", Name: "read_brief", Arguments: `{"section":"hero"}`}}, *sent.Messages[2].ToolCalls)
	require.Nil(t, sent.Messages[2].ToolCallId)
	require.NotNil(t, sent.Messages[3].ToolCallId)
	require.Equal(t, "tc_1", string(*sent.Messages[3].ToolCallId))
	require.Nil(t, sent.Messages[3].ToolCalls)
	require.NotContains(t, f.body.Load().(string), "ArgumentsDigest", "the digest is the frame's, not part of the replay")
}

func TestCallModelRefusesFramesOutsideTheContract(t *testing.T) {
	f := newFakeProxy(t)
	c, err := modelproxy.New(modelproxy.Options{BaseURL: f.srv.URL, Token: "t", Timeout: 10 * time.Second})
	require.NoError(t, err)
	admitted := `{"callId":"c","sequence":"0","type":"admitted"}`
	for name, frames := range map[string][]string{
		"unknown member":     {`{"callId":"c","sequence":"0","type":"admitted","raw":{"provider":"body"}}`},
		"another call":       {`{"callId":"other","sequence":"0","type":"admitted"}`},
		"sequence gap":       {admitted, `{"callId":"c","sequence":"2","type":"done","outcome":"succeeded"}`},
		"non-canonical seq":  {`{"callId":"c","sequence":"00","type":"admitted"}`},
		"unknown frame type": {`{"callId":"c","sequence":"0","type":"retry"}`},
		// The JSON contract: one document, every member once, the schema's
		// types, enums and patterns — a duplicate member with an equal value
		// included, and a counter the Sequence pattern excludes.
		"duplicate member":              {`{"callId":"c","sequence":"0","type":"admitted","type":"admitted"}`},
		"duplicate member, other value": {admitted, `{"callId":"c","sequence":"1","type":"text","text":"a","text":"b"}`},
		"trailing second document":      {admitted + ` {"callId":"c","sequence":"1","type":"done","outcome":"succeeded"}`},
		"trailing garbage":              {admitted + `x`},
		"negative usage counter":        {admitted, `{"callId":"c","sequence":"1","type":"done","outcome":"succeeded","usage":{"inputUnits":"-1","outputUnits":"1","reasoningUnits":"0","cachedInputUnits":"0"}}`},
		"usage counter as a number":     {admitted, `{"callId":"c","sequence":"1","type":"usage","usage":{"inputUnits":1,"outputUnits":"1","reasoningUnits":"0","cachedInputUnits":"0"}}`},
		"usage without a counter":       {admitted, `{"callId":"c","sequence":"1","type":"usage","usage":{"inputUnits":"1","outputUnits":"1","reasoningUnits":"0"}}`},
		"sequence as a number":          {`{"callId":"c","sequence":0,"type":"admitted"}`},
		"outcome outside the enum":      {admitted, `{"callId":"c","sequence":"1","type":"done","outcome":"maybe"}`},
		"text over the contract bound":  {admitted, `{"callId":"c","sequence":"1","type":"text","text":"` + strings.Repeat("x", 65537) + `"}`},
		"tool call without its digest":  {admitted, `{"callId":"c","sequence":"1","type":"tool_call","toolCall":{"toolCallId":"tc_1","name":"read_brief","arguments":"{}"}}`},
		"tool call member unknown":      {admitted, `{"callId":"c","sequence":"1","type":"tool_call","toolCall":{"toolCallId":"tc_1","name":"read_brief","argumentsDigest":"sha256:0dc7fa9db7237a2b5c96f70f59bb00f73bb86a0ca5554e91c312f9ada26e18b3","function":{}}}`},
		"invalid UTF-8 in text":         {admitted, "{\"callId\":\"c\",\"sequence\":\"1\",\"type\":\"text\",\"text\":\"\xff\"}"},
	} {
		f.frames = frames
		_, err := c.CallModel(t.Context(), input("c"), func() {})
		require.Equal(t, "INVALID_ARGUMENT", activities.RefusalCode(err), name)
		require.NotContains(t, err.Error(), "read_brief", "%s: the refusal names members and reasons, not values", name)
	}
	f.frames = []string{`{"callId":"c","sequence":"0","type":"admitted"}`, `{"callId":"c","sequence":"1","type":"text","text":"partial"}`}
	_, err = c.CallModel(t.Context(), input("c"), func() {})
	require.Error(t, err)
	require.Empty(t, activities.RefusalCode(err), "a stream without its final frame is retryable: the same call id reenters")
	require.ErrorContains(t, err, "without a final frame")
}

func TestQueryAndCancelReadTheRecordStrictly(t *testing.T) {
	f := newFakeProxy(t)
	c, err := modelproxy.New(modelproxy.Options{BaseURL: f.srv.URL, Token: "t", Timeout: 10 * time.Second})
	require.NoError(t, err)
	valid := `{"callId":"c","routeId":"r","state":"succeeded","dispatchId":"d","usage":{"inputUnits":"3","outputUnits":"1","reasoningUnits":"0","cachedInputUnits":"0"},"nativeReference":"chatcmpl-1","errorCode":"","createdAt":"2026-09-14T12:00:00Z","updatedAt":"2026-09-14T12:00:09.5Z"}`
	f.record = valid
	got, err := c.GetModelCall(t.Context(), "c")
	require.NoError(t, err)
	require.Equal(t, activities.ModelCallResult{CallID: "c", DispatchID: "d", State: "succeeded", NativeReference: "chatcmpl-1", Usage: &activities.ModelUsage{InputUnits: "3", OutputUnits: "1", ReasoningUnits: "0", CachedInputUnits: "0"}}, got)
	canceled, err := c.CancelModelCall(t.Context(), "c")
	require.NoError(t, err)
	require.Equal(t, got, canceled)
	for name, record := range map[string]string{
		"duplicate member":              `{"callId":"c","callId":"c","routeId":"r","state":"succeeded","dispatchId":"d","createdAt":"2026-09-14T12:00:00Z","updatedAt":"2026-09-14T12:00:09.5Z"}`,
		"trailing second document":      valid + valid,
		"unknown member":                `{"callId":"c","routeId":"r","state":"succeeded","dispatchId":"d","createdAt":"2026-09-14T12:00:00Z","updatedAt":"2026-09-14T12:00:09.5Z","frames":[]}`,
		"state outside the enum":        `{"callId":"c","routeId":"r","state":"streaming","dispatchId":"d","createdAt":"2026-09-14T12:00:00Z","updatedAt":"2026-09-14T12:00:09.5Z"}`,
		"negative usage counter":        `{"callId":"c","routeId":"r","state":"succeeded","dispatchId":"d","usage":{"inputUnits":"-1","outputUnits":"1","reasoningUnits":"0","cachedInputUnits":"0"},"createdAt":"2026-09-14T12:00:00Z","updatedAt":"2026-09-14T12:00:09.5Z"}`,
		"missing required member":       `{"callId":"c","routeId":"r","state":"succeeded","createdAt":"2026-09-14T12:00:00Z","updatedAt":"2026-09-14T12:00:09.5Z"}`,
		"timestamp outside the pattern": `{"callId":"c","routeId":"r","state":"succeeded","dispatchId":"d","createdAt":"2026-09-14 12:00:00","updatedAt":"2026-09-14T12:00:09.5Z"}`,
		"dispatch id as a number":       `{"callId":"c","routeId":"r","state":"succeeded","dispatchId":7,"createdAt":"2026-09-14T12:00:00Z","updatedAt":"2026-09-14T12:00:09.5Z"}`,
	} {
		f.record = record
		_, err := c.GetModelCall(t.Context(), "c")
		require.Equal(t, "INVALID_ARGUMENT", activities.RefusalCode(err), "query: %s", name)
		_, err = c.CancelModelCall(t.Context(), "c")
		require.Equal(t, "INVALID_ARGUMENT", activities.RefusalCode(err), "cancel: %s", name)
	}
	require.Equal(t, int32(0), f.posts.Load(), "queries and cancels never open a call")
}

func TestAnswersAreReadWithinTheirBounds(t *testing.T) {
	f := newFakeProxy(t)
	c, err := modelproxy.New(modelproxy.Options{BaseURL: f.srv.URL, Token: "t", Timeout: 10 * time.Second})
	require.NoError(t, err)
	envelope := `{"error":{"code":"STALE_EXECUTION","message":"the call deadline has passed","requestId":"req_1","retryable":false}}`
	record := `{"callId":"c","routeId":"r","state":"succeeded","dispatchId":"d","createdAt":"2026-09-14T12:00:00Z","updatedAt":"2026-09-14T12:00:09.5Z"}`
	pad := func(doc string, size int) string { return doc + strings.Repeat(" ", size-len(doc)) }
	// An envelope within its 64 KiB bound is the refusal it states; one byte
	// over, or a body the connection cut short, is not an envelope at all —
	// an ordinary error, never a denial of the call.
	f.status, f.envelope = 403, pad(envelope, 64<<10)
	_, err = c.CallModel(t.Context(), input("c"), func() {})
	require.Equal(t, "STALE_EXECUTION", activities.RefusalCode(err))
	f.envelope = pad(envelope, 64<<10+1)
	_, err = c.CallModel(t.Context(), input("c"), func() {})
	require.Error(t, err)
	require.Empty(t, activities.RefusalCode(err), "an oversized answer decides nothing")
	require.ErrorContains(t, err, "exceeds the bound")
	f.envelope, f.cutBody = envelope, true
	_, err = c.CallModel(t.Context(), input("c"), func() {})
	require.Error(t, err)
	require.Empty(t, activities.RefusalCode(err), "a cut answer decides nothing")
	require.ErrorContains(t, err, "reading the response body")
	_, err = c.GetModelCall(t.Context(), "c")
	require.Empty(t, activities.RefusalCode(err))
	require.ErrorContains(t, err, "reading the response body")
	f.cutBody = false
	f.envelope = pad(envelope, 64<<10+1)
	_, err = c.CancelModelCall(t.Context(), "c")
	require.Empty(t, activities.RefusalCode(err))
	require.ErrorContains(t, err, "exceeds the bound")
	// A record within its 1 MiB bound is read; over it, it is outside the
	// contract; cut short, the query is an ordinary error and is repeated.
	f.status, f.record = 200, pad(record, 1<<20)
	got, err := c.GetModelCall(t.Context(), "c")
	require.NoError(t, err)
	require.Equal(t, "succeeded", got.State)
	canceled, err := c.CancelModelCall(t.Context(), "c")
	require.NoError(t, err)
	require.Equal(t, "d", canceled.DispatchID)
	f.record = pad(record, 1<<20+1)
	_, err = c.GetModelCall(t.Context(), "c")
	require.Equal(t, "INVALID_ARGUMENT", activities.RefusalCode(err))
	require.ErrorContains(t, err, "exceeds the bound")
	_, err = c.CancelModelCall(t.Context(), "c")
	require.Equal(t, "INVALID_ARGUMENT", activities.RefusalCode(err))
	f.record = record + " " + record
	_, err = c.GetModelCall(t.Context(), "c")
	require.Equal(t, "INVALID_ARGUMENT", activities.RefusalCode(err), "a second document is refused, not read up to the first")
	f.record, f.cutBody = record, true
	_, err = c.GetModelCall(t.Context(), "c")
	require.Error(t, err)
	require.Empty(t, activities.RefusalCode(err))
	require.ErrorContains(t, err, "reading the response body")
	_, err = c.CancelModelCall(t.Context(), "c")
	require.Empty(t, activities.RefusalCode(err))
	require.ErrorContains(t, err, "reading the response body")
	f.cutBody = false
	// After the stream, the record query that fails to read leaves the result without the dispatch identity, never fails the call.
	f.frames = []string{`{"callId":"c","sequence":"0","type":"admitted"}`, `{"callId":"c","sequence":"1","type":"done","outcome":"succeeded","usage":{"inputUnits":"3","outputUnits":"1","reasoningUnits":"0","cachedInputUnits":"0"}}`}
	f.record = pad(record, 1<<20+1)
	res, err := c.CallModel(t.Context(), input("c"), func() {})
	require.NoError(t, err)
	require.Equal(t, "succeeded", res.State)
	require.Empty(t, res.DispatchID)
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
	for name, envelope := range map[string]string{
		"not json":                 `not json`,
		"duplicate member":         `{"error":{"code":"FORBIDDEN","code":"FORBIDDEN","message":"m","requestId":"req_4","retryable":false}}`,
		"trailing second document": `{"error":{"code":"FORBIDDEN","message":"m","requestId":"req_4","retryable":false}}{}`,
		"code outside the enum":    `{"error":{"code":"TEAPOT","message":"m","requestId":"req_4","retryable":false}}`,
		"unknown member":           `{"error":{"code":"FORBIDDEN","message":"m","requestId":"req_4","retryable":false,"hint":"x"}}`,
		"retryable as a string":    `{"error":{"code":"FORBIDDEN","message":"m","requestId":"req_4","retryable":"no"}}`,
	} {
		f.status, f.envelope = 500, envelope
		_, err = c.CallModel(t.Context(), input("c"), func() {})
		require.ErrorContains(t, err, "without an error envelope", name)
		require.Empty(t, activities.RefusalCode(err), "%s: an answer outside the contract is not a refusal of the call", name)
	}
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
