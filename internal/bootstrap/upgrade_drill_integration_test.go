//go:build integration

package bootstrap

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	tlog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// TestUpgradeDrill is the P1.7 upgrade drill (AC2) on the development
// foundation's Temporal: a worker deployment whose build A holds ten
// in-flight pinned runs while build B, whose Workflow code is incompatible
// with A's histories (it schedules an extra Activity first), becomes the
// current version. The ten runs continue and complete on build A, new runs
// land on build B, and neither worker reports a non-determinism error.
func TestUpgradeDrill(t *testing.T) {
	address := os.Getenv("ANVILKIT_DEV_TEMPORAL_ADDRESS")
	if address == "" {
		t.Skip("ANVILKIT_DEV_TEMPORAL_ADDRESS is not set: the development foundation's Temporal is required")
	}
	var suffix [4]byte
	_, _ = rand.Read(suffix[:])
	tag := hex.EncodeToString(suffix[:])
	deployment, queue := "anvilkit-drill-"+tag, "anvilkit-drill-"+tag
	var logs syncBuffer
	logger := tlog.NewStructuredLogger(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	c, err := client.Dial(client.Options{HostPort: address, Namespace: "anvilkit", Identity: "anvilkit-upgrade-drill", Logger: logger})
	require.NoError(t, err)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	newWorker := func(build string, wf any) worker.Worker {
		w := worker.New(c, queue, worker.Options{DeploymentOptions: worker.DeploymentOptions{
			UseVersioning: true, Version: worker.WorkerDeploymentVersion{DeploymentName: deployment, BuildID: build},
			DefaultVersioningBehavior: workflow.VersioningBehaviorPinned,
		}})
		w.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "DrillWorkflow"})
		w.RegisterActivityWithOptions(func(context.Context) (string, error) { return build, nil }, activity.RegisterOptions{Name: "Step"})
		w.RegisterActivityWithOptions(func(context.Context) error { return nil }, activity.RegisterOptions{Name: "Prepare"})
		require.NoError(t, w.Start())
		return w
	}
	step := func(ctx workflow.Context) (string, error) {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second})
		_ = workflow.GetSignalChannel(ctx, "go").Receive(ctx, nil)
		var build string
		err := workflow.ExecuteActivity(ctx, "Step").Get(ctx, &build)
		return "ran on " + build, err
	}
	buildA := func(ctx workflow.Context) (string, error) { return step(ctx) }
	buildB := func(ctx workflow.Context) (string, error) {
		// Incompatible with A's histories: an Activity before the wait.
		if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second}), "Prepare").Get(ctx, nil); err != nil {
			return "", err
		}
		return step(ctx)
	}
	setCurrent := func(build string) {
		t.Helper()
		h := c.WorkerDeploymentClient().GetHandle(deployment)
		require.Eventually(t, func() bool {
			_, err := h.SetCurrentVersion(ctx, client.WorkerDeploymentSetCurrentVersionOptions{BuildID: build})
			if err != nil {
				t.Logf("set current %s: %v", build, err)
			}
			return err == nil
		}, time.Minute, time.Second, "build %s becomes the current version", build)
	}

	wA := newWorker("A", buildA)
	defer wA.Stop()
	setCurrent("A")
	var inflight []client.WorkflowRun
	for i := range 10 {
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: fmt.Sprintf("drill-%s-a%02d", tag, i), TaskQueue: queue}, "DrillWorkflow")
		require.NoError(t, err)
		inflight = append(inflight, run)
	}
	// Each in-flight run has taken its first task on build A.
	require.Eventually(t, func() bool {
		for _, r := range inflight {
			d, err := c.DescribeWorkflowExecution(ctx, r.GetID(), "")
			if err != nil || d.GetWorkflowExecutionInfo().GetVersioningInfo().GetDeploymentVersion().GetBuildId() != "A" {
				return false
			}
		}
		return true
	}, time.Minute, 500*time.Millisecond, "the in-flight runs are pinned to build A")

	wB := newWorker("B", buildB)
	defer wB.Stop()
	setCurrent("B")
	var fresh []client.WorkflowRun
	for i := range 5 {
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: fmt.Sprintf("drill-%s-b%02d", tag, i), TaskQueue: queue}, "DrillWorkflow")
		require.NoError(t, err)
		fresh = append(fresh, run)
	}
	for _, r := range append(append([]client.WorkflowRun{}, inflight...), fresh...) {
		require.NoError(t, c.SignalWorkflow(ctx, r.GetID(), "", "go", nil))
	}
	var record strings.Builder
	for _, r := range inflight {
		var out string
		require.NoError(t, r.Get(ctx, &out))
		require.Equal(t, "ran on A", out, "an in-flight run continues on the build it started on")
		fmt.Fprintf(&record, "%s %s\n", r.GetID(), out)
	}
	for _, r := range fresh {
		var out string
		require.NoError(t, r.Get(ctx, &out))
		require.Equal(t, "ran on B", out, "a new run lands on the current build")
		fmt.Fprintf(&record, "%s %s\n", r.GetID(), out)
	}
	require.NotContains(t, strings.ToLower(logs.String()), "nondetermin", "no non-determinism error in the worker logs")
	t.Logf("P1.7 AC2 upgrade drill (deployment %s): 10 in-flight runs completed on build A, 5 new runs on build B, no non-determinism error\n%s", deployment, record.String())
}

var _ = bytes.Buffer{}
