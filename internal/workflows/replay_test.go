package workflows_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/workflows"
)

// TestReplay is P1.7's replay evidence: every history recorded under
// testdata/histories (runs of each Workflow type from before the P1.6
// version markers and after them, exported from the development
// foundation by TestExportHistories) replays on the current code without
// a non-determinism error. A change to a Workflow that is not guarded by
// workflow.GetVersion fails here before it reaches a worker.
func TestReplay(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "histories", "*.json"))
	require.NoError(t, err)
	require.NotEmpty(t, files, "recorded histories")
	r := worker.NewWorkflowReplayer()
	// The bounds a run uses are the ones its history recorded (SideEffect);
	// the values given here only have to register the Workflows.
	r.RegisterWorkflowWithOptions(workflows.LocalCheck(queues, bounds), workflow.RegisterOptions{Name: workflows.LocalCheckWorkflowName})
	r.RegisterWorkflowWithOptions(workflows.Preparation(queues, bounds, lifecycleBounds), workflow.RegisterOptions{Name: workflows.PreparationWorkflowName})
	r.RegisterWorkflowWithOptions(workflows.Generation(queues, bounds, lifecycleBounds), workflow.RegisterOptions{Name: workflows.GenerationWorkflowName})
	r.RegisterWorkflowWithOptions(workflows.PreviewBuild(queues, bounds, lifecycleBounds), workflow.RegisterOptions{Name: workflows.PreviewWorkflowName})
	r.RegisterWorkflowWithOptions(workflows.Release(queues, bounds, lifecycleBounds), workflow.RegisterOptions{Name: workflows.ReleaseWorkflowName})
	r.RegisterWorkflowWithOptions(workflows.Recovery(queues, bounds), workflow.RegisterOptions{Name: workflows.RecoveryWorkflowName})
	for _, f := range files {
		t.Run(strings.TrimSuffix(filepath.Base(f), ".json"), func(t *testing.T) {
			require.NoError(t, r.ReplayWorkflowHistoryFromJSONFile(nil, f))
		})
	}
}
