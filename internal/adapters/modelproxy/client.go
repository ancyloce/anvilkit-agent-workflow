// Package modelproxy is the Workflow's client of the Model Proxy transport
// (openapi/model-proxy.yaml through the generated modelproxyapi client):
// one POST opens a call and streams its frames, a GET queries the original
// call, a POST cancels it. The client holds no provider key; it presents the
// Workflow's identity to the Proxy (a DEVELOPMENT_ONLY bearer token from the
// environment, or the workload certificate) and forwards nothing else. Every
// frame is decoded strictly and checked against the call and the sequence.
package modelproxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ancyloce/anvilkit-agent-contracts/go/modelproxyapi"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// Options of the client.
type Options struct {
	// BaseURL of the Proxy (scheme, host, port; the /api/v1 prefix is the contract's).
	BaseURL string
	// Token is the DEVELOPMENT_ONLY bearer identity; empty with TLS set.
	Token string
	// TLS names the workload certificate files of the mtls identity.
	TLS *TLSFiles
	// Timeout bounds one request when the call's deadline does not.
	Timeout time.Duration
	// MaxFrames bounds the frames one call may deliver (the route profile's bound or the contract's).
	MaxFrames int
}

// TLSFiles is the workload identity material.
type TLSFiles struct{ CertFile, KeyFile, CAFile, ServerName string }

// Client implements activities.ModelCaller.
type Client struct {
	api       *modelproxyapi.Client
	timeout   time.Duration
	maxFrames int
}

// unavailable marks the port as not configured: every call answers
// DEPENDENCY_UNAVAILABLE without any network.
type unavailable struct{}

func (unavailable) CallModel(context.Context, activities.ModelCallInput, func()) (activities.ModelCallResult, error) {
	return activities.ModelCallResult{}, activities.Refused("DEPENDENCY_UNAVAILABLE", errors.New("model proxy address not configured"))
}

func (unavailable) GetModelCall(context.Context, string) (activities.ModelCallResult, error) {
	return activities.ModelCallResult{}, activities.Refused("DEPENDENCY_UNAVAILABLE", errors.New("model proxy address not configured"))
}

func (unavailable) CancelModelCall(context.Context, string) (activities.ModelCallResult, error) {
	return activities.ModelCallResult{}, activities.Refused("DEPENDENCY_UNAVAILABLE", errors.New("model proxy address not configured"))
}

// Unavailable is the port of a deployment without a Proxy placement.
func Unavailable() activities.ModelCaller { return unavailable{} }

// New builds the client. A token and TLS material are mutually exclusive;
// neither is ever logged.
func New(o Options) (*Client, error) {
	if o.BaseURL == "" {
		return nil, errors.New("model proxy: base url is required")
	}
	if (o.Token == "") == (o.TLS == nil) {
		return nil, errors.New("model proxy: exactly one of a bearer token (development) or the mtls identity is required")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableCompression = true
	if o.TLS != nil {
		cert, err := tls.LoadX509KeyPair(o.TLS.CertFile, o.TLS.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("model proxy: client certificate: %w", err)
		}
		ca, err := os.ReadFile(o.TLS.CAFile)
		if err != nil {
			return nil, fmt.Errorf("model proxy: ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return nil, errors.New("model proxy: ca file holds no certificate")
		}
		transport.TLSClientConfig = &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool, ServerName: o.TLS.ServerName, MinVersion: tls.VersionTLS13}
	}
	// No redirect is followed: a call goes to the configured Proxy and nowhere else.
	httpClient := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	editors := []modelproxyapi.ClientOption{modelproxyapi.WithHTTPClient(httpClient)}
	if o.Token != "" {
		token := o.Token
		editors = append(editors, modelproxyapi.WithRequestEditorFn(func(_ context.Context, req *http.Request) error {
			req.Header.Set("Authorization", "Bearer "+token)
			return nil
		}))
	}
	api, err := modelproxyapi.NewClient(strings.TrimRight(o.BaseURL, "/")+"/api/v1", editors...)
	if err != nil {
		return nil, err
	}
	timeout := o.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	maxFrames := o.MaxFrames
	if maxFrames <= 0 {
		maxFrames = 1 << 20
	}
	return &Client{api: api, timeout: timeout, maxFrames: maxFrames}, nil
}

// requestOf maps the typed input to the contract request; the request
// digest binds the whole typed input so a changed input under the same call
// id is refused by the Proxy and by Control.
func requestOf(in activities.ModelCallInput) (modelproxyapi.ModelCallRequest, error) {
	raw, err := json.Marshal(in)
	if err != nil {
		return modelproxyapi.ModelCallRequest{}, err
	}
	req := modelproxyapi.ModelCallRequest{
		CallId: in.CallID,
		Binding: modelproxyapi.ExecutionBinding{
			TenantId: in.TenantID, OperationId: in.OperationID, AttemptId: in.AttemptID, ExecutionEpoch: strconv.FormatUint(in.ExecutionEpoch, 10),
		},
		RouteId:         in.RouteID,
		RequestDigest:   fmt.Sprintf("sha256:%x", sha256.Sum256(raw)),
		MaxOutputTokens: in.MaxOutputTokens,
		MaxExposure:     modelproxyapi.Money{Currency: in.MaxExposure.Currency, Amount: in.MaxExposure.Amount},
		// The transport's deadline is stated at millisecond precision: the
		// admission Control records carries exactly this instant, and a retry
		// of the Activity repeats it exactly.
		Deadline: in.Deadline.UTC().Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000Z"),
	}
	if in.InstanceID != "" {
		req.Binding.InstanceId = &in.InstanceID
	}
	for _, m := range in.Messages {
		msg := modelproxyapi.Message{Role: modelproxyapi.MessageRole(m.Role), Content: m.Content}
		if m.ToolCallID != "" {
			id := m.ToolCallID
			msg.ToolCallId = &id
		}
		req.Messages = append(req.Messages, msg)
	}
	if len(in.Tools) > 0 {
		tools := make([]modelproxyapi.ToolDefinition, 0, len(in.Tools))
		for _, t := range in.Tools {
			td := modelproxyapi.ToolDefinition{Name: t.Name, InputSchemaDigest: t.InputSchemaDigest}
			if t.Description != "" {
				d := t.Description
				td.Description = &d
			}
			tools = append(tools, td)
		}
		req.Tools = &tools
	}
	if in.SupersedesCallID != "" {
		s := in.SupersedesCallID
		req.SupersedesCallId = &s
	}
	if in.EvidenceRef != "" {
		e := in.EvidenceRef
		req.EvidenceRef = &e
	}
	return req, nil
}

// refusal maps an error envelope to the Activity's errors: a non-retryable
// refusal for the Proxy's precondition codes, a retryable error for a
// retryable answer (nothing was sent; the same call id reenters later).
func refusal(status int, body []byte) error {
	var env modelproxyapi.ErrorEnvelope
	if err := json.Unmarshal(body, &env); err != nil || env.Error.Code == "" {
		return fmt.Errorf("model proxy answered %d without an error envelope", status)
	}
	err := fmt.Errorf("%s: %s (request %s)", env.Error.Code, env.Error.Message, env.Error.RequestId)
	if env.Error.Retryable {
		return err
	}
	return activities.Refused(string(env.Error.Code), err)
}

// CallModel opens the call and consumes its frames. A frame outside the
// contract, out of sequence or for another call ends the Activity with a
// refusal (the call is queried, never repeated).
func (c *Client) CallModel(ctx context.Context, in activities.ModelCallInput, heartbeat func()) (activities.ModelCallResult, error) {
	req, err := requestOf(in)
	if err != nil {
		return activities.ModelCallResult{}, activities.Refused("INVALID_ARGUMENT", err)
	}
	bound := c.timeout
	if until := time.Until(in.Deadline); until > 0 && until+30*time.Second > bound {
		bound = until + 30*time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	resp, err := c.api.CreateModelCall(ctx, req)
	if err != nil {
		return activities.ModelCallResult{}, fmt.Errorf("model proxy: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return activities.ModelCallResult{}, refusal(resp.StatusCode, body)
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		return activities.ModelCallResult{}, fmt.Errorf("model proxy answered %q, not an event stream", resp.Header.Get("Content-Type"))
	}
	out := activities.ModelCallResult{CallID: in.CallID}
	var text strings.Builder
	next := uint64(0)
	final := false
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)
	var data []byte
	handle := func(payload []byte) error {
		var f modelproxyapi.StreamFrame
		dec := json.NewDecoder(bytes.NewReader(payload))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&f); err != nil {
			return activities.Refused("INVALID_ARGUMENT", fmt.Errorf("frame outside the contract: %w", err))
		}
		if f.CallId != in.CallID {
			return activities.Refused("INVALID_ARGUMENT", fmt.Errorf("frame of call %s on call %s", f.CallId, in.CallID))
		}
		seq, err := strconv.ParseUint(f.Sequence, 10, 64)
		if err != nil || seq != next || strconv.FormatUint(seq, 10) != f.Sequence {
			return activities.Refused("INVALID_ARGUMENT", fmt.Errorf("frame sequence %q, expected %d", f.Sequence, next))
		}
		next++
		out.Frames++
		if out.Frames > c.maxFrames {
			return activities.Refused("INVALID_ARGUMENT", fmt.Errorf("more than %d frames", c.maxFrames))
		}
		switch f.Type {
		case modelproxyapi.StreamFrameTypeAdmitted:
		case modelproxyapi.StreamFrameTypeText:
			if f.Text != nil {
				text.WriteString(*f.Text)
			}
		case modelproxyapi.StreamFrameTypeToolCall:
			if f.ToolCall != nil {
				tc := activities.ModelToolCall{ToolCallID: f.ToolCall.ToolCallId, Name: f.ToolCall.Name, ArgumentsDigest: f.ToolCall.ArgumentsDigest}
				if f.ToolCall.Arguments != nil {
					tc.Arguments = *f.ToolCall.Arguments
				}
				out.ToolCalls = append(out.ToolCalls, tc)
			}
		case modelproxyapi.StreamFrameTypeUsage:
			out.Usage = usageOf(f.Usage)
		case modelproxyapi.StreamFrameTypeDone, modelproxyapi.StreamFrameTypeError:
			final = true
			if f.Outcome != nil {
				out.State = string(*f.Outcome)
			} else {
				out.State = "unknown"
			}
			if f.Usage != nil {
				out.Usage = usageOf(f.Usage)
			}
			if f.ErrorCode != nil {
				out.ErrorCode = *f.ErrorCode
			}
		default:
			return activities.Refused("INVALID_ARGUMENT", fmt.Errorf("frame type %q", f.Type))
		}
		heartbeat()
		return nil
	}
	for scanner.Scan() {
		line := scanner.Bytes()
		switch {
		case len(line) == 0:
			if len(data) > 0 {
				if err := handle(data); err != nil {
					return out, err
				}
				data = data[:0]
			}
		case bytes.HasPrefix(line, []byte("data:")):
			data = append(data, bytes.TrimSpace(line[5:])...)
		}
		if final {
			break
		}
	}
	if !final && len(data) > 0 {
		if err := handle(data); err != nil {
			return out, err
		}
	}
	if err := scanner.Err(); err != nil && !final {
		// The stream ended without its final frame: the call is not over or
		// the connection was lost; a retry reenters the same call id.
		return out, fmt.Errorf("model proxy stream interrupted after %d frames: %w", out.Frames, err)
	}
	if !final {
		return out, fmt.Errorf("model proxy stream ended after %d frames without a final frame", out.Frames)
	}
	out.Text = text.String()
	// The record carries the dispatch identity the frames do not.
	if rec, err := c.GetModelCall(ctx, in.CallID); err == nil {
		out.DispatchID, out.NativeReference = rec.DispatchID, rec.NativeReference
		if out.ErrorCode == "" {
			out.ErrorCode = rec.ErrorCode
		}
	}
	return out, nil
}

func usageOf(u *modelproxyapi.Usage) *activities.ModelUsage {
	if u == nil {
		return nil
	}
	return &activities.ModelUsage{InputUnits: u.InputUnits, OutputUnits: u.OutputUnits, ReasoningUnits: u.ReasoningUnits, CachedInputUnits: u.CachedInputUnits}
}

func resultOf(m *modelproxyapi.ModelCall) activities.ModelCallResult {
	out := activities.ModelCallResult{CallID: m.CallId, DispatchID: m.DispatchId, State: string(m.State), Usage: usageOf(m.Usage)}
	if m.NativeReference != nil {
		out.NativeReference = *m.NativeReference
	}
	if m.ErrorCode != nil {
		out.ErrorCode = *m.ErrorCode
	}
	return out
}

// GetModelCall queries the original call; it never sends.
func (c *Client) GetModelCall(ctx context.Context, callID string) (activities.ModelCallResult, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	resp, err := c.api.GetModelCall(ctx, callID)
	if err != nil {
		return activities.ModelCallResult{}, fmt.Errorf("model proxy: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return activities.ModelCallResult{}, refusal(resp.StatusCode, body)
	}
	var m modelproxyapi.ModelCall
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return activities.ModelCallResult{}, activities.Refused("INVALID_ARGUMENT", fmt.Errorf("call record outside the contract: %w", err))
	}
	return resultOf(&m), nil
}

// CancelModelCall records the cancel intent; incurred usage stays.
func (c *Client) CancelModelCall(ctx context.Context, callID string) (activities.ModelCallResult, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	resp, err := c.api.CancelModelCall(ctx, callID)
	if err != nil {
		return activities.ModelCallResult{}, fmt.Errorf("model proxy: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusAccepted {
		return activities.ModelCallResult{}, refusal(resp.StatusCode, body)
	}
	var m modelproxyapi.ModelCall
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return activities.ModelCallResult{}, activities.Refused("INVALID_ARGUMENT", fmt.Errorf("call record outside the contract: %w", err))
	}
	return resultOf(&m), nil
}
