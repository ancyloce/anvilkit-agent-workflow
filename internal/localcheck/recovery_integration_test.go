//go:build temporalintegration

package localcheck

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/logging"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
)

// TestBoundaryProxy is a parent-driven test process, not a runtime feature.
// It either withholds a successful start reply or blocks Activity completion
// before forwarding. Test witnesses contain identities only, never task tokens.
func TestBoundaryProxy(t *testing.T) {
	directory := os.Getenv("ANVILKIT_WORKFLOW_TEST_PROXY_DIRECTORY")
	if directory == "" {
		t.Skip("the integrated parent driver selects the boundary proxy")
	}
	mode := requiredSetting(t, "ANVILKIT_WORKFLOW_TEST_PROXY_MODE")
	if mode != "lost-start-reply" && mode != "hold-activity-completion" && mode != "missing-history" {
		t.Fatal("invalid boundary fault")
	}
	ca, err := os.ReadFile(requiredSetting(t, "ANVILKIT_WORKFLOW_TEMPORAL_TLS_CA"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid CA")
	}
	serverCert, err := tls.LoadX509KeyPair(requiredSetting(t, "ANVILKIT_WORKFLOW_TEST_PROXY_CERT"), requiredSetting(t, "ANVILKIT_WORKFLOW_TEST_PROXY_KEY"))
	if err != nil {
		t.Fatal("invalid proxy certificate")
	}
	upstreamCert, err := tls.LoadX509KeyPair(requiredSetting(t, "ANVILKIT_WORKFLOW_TEST_CLIENT_CERT"), requiredSetting(t, "ANVILKIT_WORKFLOW_TEST_CLIENT_KEY"))
	if err != nil {
		t.Fatal("invalid upstream certificate")
	}
	transport := &http.Transport{ForceAttemptHTTP2: true, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, ServerName: "temporal.local", RootCAs: roots, Certificates: []tls.Certificate{upstreamCert}}}
	defer transport.CloseIdleConnections()
	var count atomic.Int32
	witness := func(method string) {
		n := count.Add(1)
		raw, _ := json.Marshal(map[string]any{"method": method, "sequence": n, "observedAt": time.Now().UTC()})
		if err := os.WriteFile(filepath.Join(directory, "witness-"+strconv.Itoa(int(n))+".json"), raw, 0600); err != nil {
			t.Error(err)
		}
	}
	server := &http.Server{TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}, ReadHeaderTimeout: 5 * time.Second}
	server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if mode == "missing-history" && r.URL.Path == "/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory" {
			if _, err := io.Copy(io.Discard, r.Body); err != nil {
				return
			}
			witness(r.URL.Path)
			w.Header().Set("Content-Type", "application/grpc+proto")
			w.Header().Set("Grpc-Status", "5")
			w.Header().Set("Grpc-Message", "test retained history unavailable")
			w.WriteHeader(http.StatusOK)
			return
		}
		if mode == "hold-activity-completion" && r.URL.Path == "/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted" {
			// Read the complete request before recording the boundary witness.
			if _, err := io.Copy(io.Discard, r.Body); err != nil {
				return
			}
			witness(r.URL.Path)
			<-r.Context().Done()
			return
		}
		request := r.Clone(r.Context())
		request.RequestURI = ""
		request.URL.Scheme = "https"
		request.URL.Host = requiredSetting(t, "ANVILKIT_WORKFLOW_TEMPORAL_ENDPOINT")
		request.Host = request.URL.Host
		response, err := transport.RoundTrip(request)
		if err != nil {
			http.Error(w, "test upstream unavailable", http.StatusBadGateway)
			return
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return
		}
		if mode == "lost-start-reply" && r.URL.Path == "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution" && len(body) >= 5 && int(binary.BigEndian.Uint32(body[1:5])) == len(body)-5 {
			payload := body[5:]
			if body[0] == 1 && response.Header.Get("Grpc-Encoding") == "gzip" {
				reader, err := gzip.NewReader(bytes.NewReader(payload))
				if err != nil {
					t.Error("invalid compressed test response")
					return
				}
				payload, err = io.ReadAll(reader)
				_ = reader.Close()
				if err != nil {
					t.Error("incomplete compressed test response")
					return
				}
			}
			var result workflowservice.StartWorkflowExecutionResponse
			if proto.Unmarshal(payload, &result) == nil && result.GetRunId() != "" {
				witness(r.URL.Path)
				<-r.Context().Done()
				return
			}
		}
		for key, values := range response.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		for key := range response.Trailer {
			w.Header().Add("Trailer", key)
		}
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(body)
		for key, values := range response.Trailer {
			w.Header()[http.TrailerPrefix+key] = values
		}
	})
	listener, err := net.Listen("tcp", requiredSetting(t, "ANVILKIT_WORKFLOW_TEST_PROXY_LISTEN"))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.ServeTLS(listener, "", "") }()
	if err := os.WriteFile(filepath.Join(directory, "ready"), []byte("ready"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	_ = server.Close()
	<-done
}

// TestRetainedLocalHistories replays the exact histories produced through the
// three-service boundary. The parent compares SQL result/event counts around
// this process; this code has no database credentials or database dependency.
func TestRetainedLocalHistories(t *testing.T) {
	path := os.Getenv("ANVILKIT_WORKFLOW_TEST_HISTORY_INDEX")
	if path == "" {
		t.Skip("the integrated parent driver supplies retained executions")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var executions []struct {
		WorkflowID, RunID string
	}
	if err := json.Unmarshal(raw, &executions); err != nil || len(executions) == 0 {
		t.Fatal("missing execution identities", err)
	}
	c := temporalClient(t)
	for _, execution := range executions {
		t.Run(execution.WorkflowID, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			run := c.GetWorkflow(ctx, execution.WorkflowID, execution.RunID)
			h := history(t, ctx, c, run)
			var output bytes.Buffer
			replayer := worker.NewWorkflowReplayer()
			replayer.RegisterWorkflow(LocalCheckWorkflow)
			if err := replayer.ReplayWorkflowHistoryWithOptions(logging.New(&output, "replay", "fixture-replay", "local"), h, worker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: execution.WorkflowID, RunID: execution.RunID}}); err != nil {
				t.Fatal(err)
			}
			if output.Len() != 0 {
				t.Fatal("replay emitted a duplicate execution record")
			}
		})
	}
}

// TestOriginalTerminalEvidence witnesses Temporal's recorded completion while
// Control is stopped and retains its original history for replay.
func TestOriginalTerminalEvidence(t *testing.T) {
	raw := os.Getenv("ANVILKIT_WORKFLOW_TEST_EXECUTION")
	if raw == "" {
		t.Skip("the integrated driver selects an original execution")
	}
	var execution struct {
		WorkflowID, RunID string
	}
	if json.Unmarshal([]byte(raw), &execution) != nil || execution.WorkflowID == "" || execution.RunID == "" {
		t.Fatal("invalid test execution identity")
	}
	c := temporalClient(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	run := c.GetWorkflow(ctx, execution.WorkflowID, execution.RunID)
	if err := run.Get(ctx, nil); err != nil {
		t.Fatal("original execution did not complete successfully", err)
	}
	h := history(t, ctx, c, run)
	if h.Events[len(h.Events)-1].GetWorkflowExecutionCompletedEventAttributes() == nil {
		t.Fatal("no recorded completion")
	}
}
