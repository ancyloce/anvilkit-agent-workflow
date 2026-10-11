package bootstrap

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type traceCollector struct {
	coltracepb.UnimplementedTraceServiceServer
	spans atomic.Int32
}

func (c *traceCollector) Export(_ context.Context, r *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	for _, rs := range r.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			c.spans.Add(int32(len(ss.GetSpans())))
		}
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

// The exporter dials a development collector in plaintext: given only as a
// dial option, its transport lost to the exporter's TLS default and no span
// reached the collector (P0 close-out, qualification telemetry).
func TestOTLPExporterUsesTheConfiguredTransport(t *testing.T) {
	ctx := context.Background()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	c := &traceCollector{}
	coltracepb.RegisterTraceServiceServer(srv, c)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(lis.Addr().String()), otlpCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	_, span := provider.Tracer("otlp-test").Start(ctx, "span")
	span.End()
	require.NoError(t, provider.Shutdown(ctx))
	require.Equal(t, int32(1), c.spans.Load())
}
