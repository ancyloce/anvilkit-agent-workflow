//go:build historyexport

package workflows_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/encoding/protojson"
)

// TestExportHistories records the replay evidence of P1.7 from the
// development foundation's Temporal (ANVILKIT_DEV_TEMPORAL_ADDRESS): per
// Workflow type, runs (closed, or abandoned by a stopped worker) recorded
// before the P1.6 version markers
// (ANVILKIT_HISTORY_BEFORE, RFC 3339) and after them
// (ANVILKIT_HISTORY_AFTER), written to testdata/histories as the JSON the
// replayer reads. A history whose payloads carry a URL, a signing
// parameter or a bearer token is skipped, never written.
//
//	go test -tags historyexport -run TestExportHistories ./internal/workflows/
func TestExportHistories(t *testing.T) {
	address := os.Getenv("ANVILKIT_DEV_TEMPORAL_ADDRESS")
	before, after := os.Getenv("ANVILKIT_HISTORY_BEFORE"), os.Getenv("ANVILKIT_HISTORY_AFTER")
	if address == "" || before == "" || after == "" {
		t.Skip("ANVILKIT_DEV_TEMPORAL_ADDRESS, ANVILKIT_HISTORY_BEFORE and ANVILKIT_HISTORY_AFTER are required")
	}
	c, err := client.Dial(client.Options{HostPort: address, Namespace: "anvilkit"})
	require.NoError(t, err)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	secret := regexp.MustCompile(`(?i)(X-Amz-|Signature=|https?://[^"\s]*\?|bearer\s+[a-z0-9._-]{8,}|"token")`)
	perKind := 3
	// excluded are histories recorded by Workflow code that was never
	// committed (see testdata/histories/README.md).
	excluded := map[string]bool{"Release-before-zadph65k546jssh4gbjrehaq.json": true}
	for _, typ := range []string{"LocalCheckWorkflow", "PreparationWorkflow", "GenerationWorkflow", "PreviewBuildWorkflow", "ReleaseWorkflow", "RecoveryWorkflow"} {
		for _, w := range []struct{ label, query string }{
			{"before", fmt.Sprintf("StartTime < '%s'", before)},
			{"after", fmt.Sprintf("StartTime >= '%s'", after)},
		} {
			resp, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{Namespace: "anvilkit", PageSize: 1000,
				Query: fmt.Sprintf("WorkflowType = '%s' AND %s", typ, w.query)})
			require.NoError(t, err)
			// Fetch up to 80 candidates and keep the longest histories: a run
			// abandoned by its worker replays as far as it got, which is
			// evidence too; a run no worker ever executed (fewer than three
			// events) replays nothing and is skipped.
			type candidate struct {
				exec   *workflowpb.WorkflowExecutionInfo
				events int
				raw    []byte
			}
			var found []candidate
			for i, e := range resp.Executions {
				if i >= 80 {
					break
				}
				h := &historypb.History{}
				it := c.GetWorkflowHistory(ctx, e.GetExecution().GetWorkflowId(), e.GetExecution().GetRunId(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
				for it.HasNext() {
					ev, err := it.Next()
					require.NoError(t, err)
					h.Events = append(h.Events, ev)
				}
				if len(h.Events) < 3 {
					continue
				}
				raw, err := protojson.MarshalOptions{Indent: "  "}.Marshal(h)
				require.NoError(t, err)
				if secret.Match(raw) {
					t.Logf("skipped %s %s: its payloads carry capability material", typ, e.GetExecution().GetWorkflowId())
					continue
				}
				found = append(found, candidate{e, len(h.Events), raw})
			}
			slices.SortFunc(found, func(a, b candidate) int { return b.events - a.events })
			written := 0
			for _, f := range found {
				if written >= perKind {
					break
				}
				e := f.exec
				name := fmt.Sprintf("%s-%s-%s.json", strings.TrimSuffix(typ, "Workflow"), w.label, strings.ToLower(strings.TrimPrefix(e.GetExecution().GetWorkflowId(), "op_")))
				if excluded[name] {
					continue
				}
				require.NoError(t, os.WriteFile(filepath.Join("testdata", "histories", name), f.raw, 0o644))
				written++
				t.Logf("%s %-6s %s %s %d events started %s", typ, w.label, e.GetExecution().GetWorkflowId(), e.GetStatus().String(), f.events, e.GetStartTime().AsTime().Format(time.RFC3339))
			}
			if written == 0 {
				t.Logf("%s %-6s: no closed run in the window", typ, w.label)
			}
		}
	}
}
