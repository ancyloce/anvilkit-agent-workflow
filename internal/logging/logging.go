// Package logging maps the SDK logger onto the retained, content-free log contract.
package logging

import (
	"context"
	"io"
	"log/slog"
	"strings"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/contracts"
)

// Logger also satisfies the Temporal log.Logger interface. SDK diagnostic
// messages and error values may contain payloads, so only named fields survive.
type Logger struct{ logger *slog.Logger }

func New(output io.Writer, version, instance, environment string) *Logger {
	handler := slog.NewJSONHandler(output, &slog.HandlerOptions{ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
		if len(groups) == 0 {
			switch attr.Key {
			case slog.TimeKey:
				return slog.String("timestamp", attr.Value.Time().UTC().Format("2006-01-02T15:04:05.000Z"))
			case slog.LevelKey:
				return slog.String("severity", attr.Value.String())
			case slog.MessageKey:
				return slog.String("eventName", attr.Value.String())
			}
		}
		return attr
	}})
	return &Logger{slog.New(handler).With("schemaVersion", 1, "origin", "service", "service.name", "anvilkit-agent-workflow",
		"service.version", version, "service.instance.id", instance, "environment", environment)}
}

func (l *Logger) Debug(message string, fields ...any) { l.write(slog.LevelDebug, message, fields) }
func (l *Logger) Info(message string, fields ...any)  { l.write(slog.LevelInfo, message, fields) }
func (l *Logger) Warn(message string, fields ...any)  { l.write(slog.LevelWarn, message, fields) }
func (l *Logger) Error(message string, fields ...any) { l.write(slog.LevelError, message, fields) }

func (l *Logger) write(level slog.Level, event string, fields []any) {
	switch event {
	case "workflow.started", "workflow.completed", "activity.started", "activity.completed", "service.started", "service.stopping", "service.drained", "health.transition":
	default:
		// Preserve SDK warnings/errors as bounded diagnostics without retaining
		// their free-form message, error, headers, stack or input/result values.
		if level < slog.LevelWarn {
			return
		}
		event = "health.transition"
	}
	values := map[string]any{}
	for index := 0; index+1 < len(fields); index += 2 {
		key, ok := fields[index].(string)
		if !ok {
			continue
		}
		switch key {
		case "WorkflowID":
			key = "temporal.workflowId"
		case "RunID":
			key = "temporal.runId"
		}
		switch key {
		case "operationId", "temporal.workflowId", "temporal.runId", "temporal.activityType", "activityAttempt", "trace.source", "outcome", "durationMs", "profileRef":
			values[key] = fields[index+1]
		}
	}
	if id, ok := values["temporal.workflowId"].(string); ok && strings.HasPrefix(id, contracts.LocalCheckWorkflowIDPrefix) {
		values["operationId"] = strings.TrimPrefix(id, contracts.LocalCheckWorkflowIDPrefix)
	}
	attrs := make([]slog.Attr, 0, len(values)+2)
	for _, key := range []string{"operationId", "temporal.workflowId", "temporal.runId", "temporal.activityType", "activityAttempt", "trace.source", "outcome", "durationMs"} {
		if value, ok := values[key]; ok {
			attrs = append(attrs, slog.Any(key, value))
		}
	}
	if values["profileRef"] == contracts.LocalCheckProfileRef {
		attrs = append(attrs, slog.Group("attributes", "profileRef", contracts.LocalCheckProfileRef))
	}
	if outcome, ok := values["outcome"].(string); ok && outcome != "ok" {
		kind, code := "internal", "LOCAL_CHECK_FAILED"
		if outcome == "canceled" {
			kind, code = "canceled", "CANCELED"
		}
		attrs = append(attrs, slog.String("error.type", kind), slog.String("error.code", code))
	}
	l.logger.LogAttrs(context.Background(), level, event, attrs...)
}
