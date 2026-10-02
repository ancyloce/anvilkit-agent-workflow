package bootstrap

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
	"go.uber.org/fx"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/config"
)

const serviceName = "anvilkit-agent-workflow"

// telemetry is the worker's redacted signals. The Temporal SDK's own
// tracing interceptor names spans after Workflow and Activity types and
// tags them with their identifiers only (never inputs, results or
// payloads) and is replay-safe; the Control client's gRPC stats handler
// continues the trace. The SDK metrics and the process metrics are served on
// the health listener's /metrics.
type telemetry struct {
	interceptors []interceptor.ClientInterceptor
	metrics      client.MetricsHandler
	registry     *prometheus.Registry
}

func newTelemetry(lc fx.Lifecycle, cfg config.Config) (*telemetry, error) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	exporter, err := otelprom.New(otelprom.WithRegisterer(reg))
	if err != nil {
		return nil, err
	}
	meters := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	t := &telemetry{registry: reg, metrics: temporalotel.NewMetricsHandler(temporalotel.MetricsHandlerOptions{Meter: meters.Meter("temporal-sdk-go")})}
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if cfg.Telemetry.OTLPEndpoint != "" {
		spans, err := otlptracegrpc.New(context.Background(), otlptracegrpc.WithEndpoint(cfg.Telemetry.OTLPEndpoint), otlptracegrpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		provider := sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(spans),
			sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName(serviceName))),
			sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.Telemetry.SampleRatio))),
		)
		otel.SetTracerProvider(provider)
		tracing, err := temporalotel.NewTracingInterceptor(temporalotel.TracerOptions{Tracer: provider.Tracer(serviceName)})
		if err != nil {
			return nil, err
		}
		t.interceptors = append(t.interceptors, tracing)
		lc.Append(fx.Hook{OnStop: func(ctx context.Context) error {
			flush, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			return provider.Shutdown(flush)
		}})
	}
	lc.Append(fx.Hook{OnStop: func(ctx context.Context) error { return meters.Shutdown(ctx) }})
	return t, nil
}
