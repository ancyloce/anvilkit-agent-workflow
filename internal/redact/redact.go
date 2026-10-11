// Package redact removes capability material from what the worker emits
// (P1.6): presigned URLs (their host names a bucket, their path an object
// key, their query a signature), signing parameters and bearer tokens are
// replaced in failure messages, log records and span status before they
// leave the process. Callers redact at the source as well; this is the
// boundary that holds when a source forgot.
package redact

import (
	"context"
	"log/slog"
	"regexp"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

// Marker replaces what was removed.
const Marker = "[redacted]"

var (
	urlRe    = regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.-]*://[^\s"'<>]+`)
	paramRe  = regexp.MustCompile(`(?i)\b(X-Amz-[A-Za-z0-9-]+|Signature|AWSAccessKeyId|Expires|access_token|refresh_token|token|sig|se|sp|sv|sr)=[^&\s"'<>]+`)
	bearerRe = regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}`)
)

// String returns s with every URL, signing parameter and bearer token
// replaced by the marker.
func String(s string) string {
	s = urlRe.ReplaceAllString(s, "<url "+Marker+">")
	s = paramRe.ReplaceAllString(s, "$1="+Marker)
	s = bearerRe.ReplaceAllString(s, "$1 "+Marker)
	return s
}

// Handler is a slog.Handler that redacts the message and every string,
// error and group attribute before the next handler sees the record.
type Handler struct{ next slog.Handler }

func NewHandler(next slog.Handler) *Handler { return &Handler{next: next} }

func (h *Handler) Enabled(ctx context.Context, l slog.Level) bool { return h.next.Enabled(ctx, l) }

func (h *Handler) Handle(ctx context.Context, r slog.Record) error {
	out := slog.NewRecord(r.Time, r.Level, String(r.Message), r.PC)
	r.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(attr(a))
		return true
	})
	return h.next.Handle(ctx, out)
}

func (h *Handler) WithAttrs(as []slog.Attr) slog.Handler {
	red := make([]slog.Attr, len(as))
	for i, a := range as {
		red[i] = attr(a)
	}
	return &Handler{next: h.next.WithAttrs(red)}
}

func (h *Handler) WithGroup(name string) slog.Handler { return &Handler{next: h.next.WithGroup(name)} }

func attr(a slog.Attr) slog.Attr {
	v := a.Value.Resolve()
	switch v.Kind() {
	case slog.KindString:
		return slog.String(a.Key, String(v.String()))
	case slog.KindGroup:
		g := v.Group()
		red := make([]any, len(g))
		for i, x := range g {
			red[i] = attr(x)
		}
		return slog.Group(a.Key, red...)
	case slog.KindAny:
		if err, ok := v.Any().(error); ok && err != nil {
			return slog.String(a.Key, String(err.Error()))
		}
		if s, ok := v.Any().(interface{ String() string }); ok {
			return slog.String(a.Key, String(s.String()))
		}
		return slog.String(a.Key, String(v.String()))
	}
	return slog.Attr{Key: a.Key, Value: v}
}

// FailureConverter redacts the message and stack trace of every failure,
// its causes included, before it is written to history (ActivityTaskFailed,
// WorkflowExecutionFailed); the conversion back is the default one.
type FailureConverter struct{ next converter.FailureConverter }

func NewFailureConverter(next converter.FailureConverter) *FailureConverter {
	if next == nil {
		next = temporal.GetDefaultFailureConverter()
	}
	return &FailureConverter{next: next}
}

func (c *FailureConverter) ErrorToFailure(err error) *failurepb.Failure {
	f := c.next.ErrorToFailure(err)
	for x := f; x != nil; x = x.GetCause() {
		x.Message = String(x.GetMessage())
		x.StackTrace = String(x.GetStackTrace())
	}
	return f
}

func (c *FailureConverter) FailureToError(f *failurepb.Failure) error {
	return c.next.FailureToError(f)
}

// Exporter redacts span status descriptions and event attributes (an
// exception message carries the error text) before the next exporter.
type Exporter struct{ next sdktrace.SpanExporter }

func NewExporter(next sdktrace.SpanExporter) *Exporter { return &Exporter{next: next} }

func (e *Exporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	out := make([]sdktrace.ReadOnlySpan, len(spans))
	for i, s := range spans {
		out[i] = redactedSpan{s}
	}
	return e.next.ExportSpans(ctx, out)
}

func (e *Exporter) Shutdown(ctx context.Context) error { return e.next.Shutdown(ctx) }

type redactedSpan struct{ sdktrace.ReadOnlySpan }

func (s redactedSpan) Status() sdktrace.Status {
	st := s.ReadOnlySpan.Status()
	st.Description = String(st.Description)
	return st
}

func (s redactedSpan) Events() []sdktrace.Event {
	evs := s.ReadOnlySpan.Events()
	out := make([]sdktrace.Event, len(evs))
	for i, ev := range evs {
		ev.Attributes = attrs(ev.Attributes)
		out[i] = ev
	}
	return out
}

func (s redactedSpan) Attributes() []attribute.KeyValue { return attrs(s.ReadOnlySpan.Attributes()) }

func attrs(kvs []attribute.KeyValue) []attribute.KeyValue {
	out := make([]attribute.KeyValue, len(kvs))
	for i, kv := range kvs {
		if kv.Value.Type() == attribute.STRING {
			kv = attribute.String(string(kv.Key), String(kv.Value.AsString()))
		}
		out[i] = kv
	}
	return out
}
