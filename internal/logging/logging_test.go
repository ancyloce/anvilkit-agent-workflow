package logging

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/contracts"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestRecordsConformAndExcludeSDKPayloads(t *testing.T) {
	var output bytes.Buffer
	logger := New(&output, "test", "worker-1", "local")
	fields := []any{"WorkflowID", "local-check:op-1", "RunID", "actual-run-1", "temporal.activityType", "ComputeLocalCheck", "activityAttempt", 1, "profileRef", "local-check-v1"}
	logger.Info("activity.started", fields...)
	logger.Info("activity.completed", append(fields, "outcome", "ok", "durationMs", 1)...)
	logger.Info("workflow.started", "WorkflowID", "local-check:op-1", "RunID", "actual-run-1", "trace.source", "new")
	logger.Info("workflow.completed", "WorkflowID", "local-check:op-1", "RunID", "actual-run-1", "outcome", "canceled")
	logger.Warn("private-payload-in-SDK-message", "WorkflowID", "local-check:op-1", "Error", "private-payload-in-SDK-error", "fixtureText", "private-fixture-text")
	if strings.Contains(output.String(), "private-") {
		t.Fatal("SDK data escaped the diagnostic field allowlist")
	}
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	for _, name := range []string{"common-v1.schema.json", "log-record-v1.schema.json"} {
		raw, err := contracts.Retained.ReadFile("retained/" + name)
		if err != nil {
			t.Fatal(err)
		}
		var document map[string]any
		if err := json.Unmarshal(raw, &document); err != nil {
			t.Fatal(err)
		}
		if err := compiler.AddResource(document["$id"].(string), document); err != nil {
			t.Fatal(err)
		}
	}
	schema, err := compiler.Compile("urn:anvilkit:log-record:v1")
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte("\n"))
	if len(lines) != 5 {
		t.Fatalf("unexpected record count %d", len(lines))
	}
	for _, line := range lines {
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		if err := schema.Validate(record); err != nil {
			t.Fatal(err)
		}
		if record["operationId"] != "op-1" {
			t.Fatal("operation correlation lost")
		}
	}
}
