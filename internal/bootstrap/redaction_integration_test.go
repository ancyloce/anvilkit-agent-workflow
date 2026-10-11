//go:build integration

package bootstrap

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
	tlog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/redact"
)

// TestRedactionAtTheWorkerBoundary is P1.6 AC4 at the boundary: an
// Activity whose error carries a presigned URL (a source that forgot to
// withhold it) fails against the development foundation's Temporal with
// the worker's production redaction in place. The ActivityTaskFailed and
// WorkflowExecutionFailed events read back from history, every record the
// worker logged and every exported span hold no signing parameter, bucket
// or object key.
func TestRedactionAtTheWorkerBoundary(t *testing.T) {
	address := os.Getenv("ANVILKIT_DEV_TEMPORAL_ADDRESS")
	if address == "" {
		t.Skip("ANVILKIT_DEV_TEMPORAL_ADDRESS is not set: the development foundation's Temporal is required")
	}
	forbidden := []string{"X-Amz-", "Signature=", "anvilkit-artifacts", "tenant_a/op_red", "xfer_red", "AKIAEXAMPLE"}
	var suffix [6]byte
	_, err := rand.Read(suffix[:])
	require.NoError(t, err)
	queue := "anvilkit-workflow-redaction-" + hex.EncodeToString(suffix[:])

	var logs syncBuffer
	logger := slog.New(redact.NewHandler(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	spans := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(redact.NewExporter(spans)))
	defer func() { _ = provider.Shutdown(context.Background()) }()
	tracing, err := temporalotel.NewTracingInterceptor(temporalotel.TracerOptions{Tracer: provider.Tracer(serviceName)})
	require.NoError(t, err)

	c, err := client.Dial(client.Options{HostPort: address, Namespace: "anvilkit", Identity: "anvilkit-agent-workflow-redaction-test",
		Interceptors: []interceptor.ClientInterceptor{tracing},
		Logger:       tlog.NewStructuredLogger(logger), FailureConverter: redact.NewFailureConverter(nil)})
	require.NoError(t, err)
	defer c.Close()

	closed, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	capability := "http://" + closed.Addr().String() + "/anvilkit-artifacts/artifacts/tenant_a/op_red/xfer_red?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAEXAMPLE%2F20261011%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=0123456789abcdef"
	require.NoError(t, closed.Close())
	var raw string
	upload := func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, capability, strings.NewReader("x"))
		if err != nil {
			return err
		}
		_, err = http.DefaultClient.Do(req)
		raw = fmt.Errorf("upload: %w", err).Error()
		return fmt.Errorf("upload: %w", err) // the unredacted source
	}
	w := worker.New(c, queue, worker.Options{})
	w.RegisterActivityWithOptions(upload, activity.RegisterOptions{Name: "ForcedUpload"})
	w.RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1}})
		return workflow.ExecuteActivity(ctx, "ForcedUpload").Get(ctx, nil)
	}, workflow.RegisterOptions{Name: "RedactionProbe"})
	require.NoError(t, w.Start())
	defer w.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "redaction-" + hex.EncodeToString(suffix[:]), TaskQueue: queue}, "RedactionProbe")
	require.NoError(t, err)
	require.Error(t, run.Get(ctx, nil))
	require.Contains(t, raw, "X-Amz-Signature", "the Activity's own error carried the capability")

	var failed []string
	it := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for it.HasNext() {
		ev, err := it.Next()
		require.NoError(t, err)
		switch ev.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
			b, err := protojson.Marshal(ev)
			require.NoError(t, err)
			failed = append(failed, string(b))
		}
	}
	require.Len(t, failed, 2, "the Activity and the run failed")
	require.NoError(t, provider.ForceFlush(ctx))
	stubs := spans.GetSpans()
	errored := 0
	var spanText strings.Builder
	for _, s := range stubs {
		if s.Status.Code.String() == "Error" {
			errored++
		}
		spanText.WriteString(s.Status.Description + "\n")
		for _, ev := range s.Events {
			for _, a := range ev.Attributes {
				spanText.WriteString(a.Value.Emit() + "\n")
			}
		}
		for _, a := range s.Attributes {
			spanText.WriteString(a.Value.Emit() + "\n")
		}
	}
	require.NotZero(t, errored, "the failure reached a span")
	logged := logs.String()
	require.Contains(t, logged, "redacted", "the worker logged the failure")
	for _, f := range forbidden {
		for _, ev := range failed {
			require.NotContains(t, ev, f, "history event")
		}
		require.NotContains(t, logged, f, "worker logs")
		require.NotContains(t, spanText.String(), f, "span status and events")
	}
	t.Logf("P1.6 AC4: history %d failure events, %d log bytes, %d spans (%d with error status): no signing parameter, bucket or key", len(failed), len(logged), len(stubs), errored)
	t.Logf("ActivityTaskFailed: %s", failed[0])
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
