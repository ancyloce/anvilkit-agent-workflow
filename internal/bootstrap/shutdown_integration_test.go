//go:build integration

package bootstrap

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// TestBoundedShutdownWithWorkOnBothQueues runs the real pollers against the
// development foundation's Temporal (ANVILKIT_DEV_TEMPORAL_ADDRESS from
// .local/dev/env.sh) on two Task Queues of its own, holds one Activity in
// flight on each and stops the worker: the stop withdraws readiness first,
// ends within the drain bound plus the close allowance although neither
// Activity finishes on its own (a sequential drain of the two queues would
// take twice the bound), and abandons both Activities to Temporal with the
// drain cause. Their retries are Temporal's existing semantics and are not
// exercised here.
func TestBoundedShutdownWithWorkOnBothQueues(t *testing.T) {
	address := os.Getenv("ANVILKIT_DEV_TEMPORAL_ADDRESS")
	if address == "" {
		t.Skip("ANVILKIT_DEV_TEMPORAL_ADDRESS is not set: the development foundation's Temporal is required")
	}
	var suffix [6]byte
	_, err := rand.Read(suffix[:])
	require.NoError(t, err)
	businessQueue := "anvilkit-workflow-lifecycle-" + hex.EncodeToString(suffix[:])
	controlQueue := businessQueue + "-control"
	const drain = 3 * time.Second

	c, err := client.Dial(client.Options{HostPort: address, Namespace: "anvilkit", Identity: "anvilkit-agent-workflow-lifecycle-test"})
	require.NoError(t, err)
	cleanup, err := client.Dial(client.Options{HostPort: address, Namespace: "anvilkit", Identity: "anvilkit-agent-workflow-lifecycle-cleanup"})
	require.NoError(t, err)
	defer cleanup.Close()

	w := newLifecycle(drain, newHealth("127.0.0.1:0"), nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.business = &queueWorker{name: businessQueue}
	w.control = &queueWorker{name: controlQueue}
	w.business.Worker = worker.New(c, businessQueue, w.options("anvilkit-agent-workflow-lifecycle-test", w.business))
	w.control.Worker = worker.New(c, controlQueue, w.options("anvilkit-agent-workflow-lifecycle-test", w.control))
	w.closers = []func() error{func() error { c.Close(); return nil }}

	started := make(chan string, 2)
	ended := make(chan error, 2)
	hold := func(ctx context.Context, queue string) error {
		started <- queue
		for {
			select {
			case <-ctx.Done():
				ended <- context.Cause(ctx)
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
				activity.RecordHeartbeat(ctx)
			}
		}
	}
	w.business.RegisterActivityWithOptions(hold, activity.RegisterOptions{Name: "LifecycleHoldBusiness"})
	w.control.RegisterActivityWithOptions(hold, activity.RegisterOptions{Name: "LifecycleHoldControl"})
	w.business.RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		opts := workflow.ActivityOptions{StartToCloseTimeout: 2 * time.Minute, HeartbeatTimeout: 20 * time.Second, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1}}
		opts.TaskQueue = businessQueue
		business := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, opts), "LifecycleHoldBusiness", businessQueue)
		opts.TaskQueue = controlQueue
		control := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, opts), "LifecycleHoldControl", controlQueue)
		if err := business.Get(ctx, nil); err != nil {
			return err
		}
		return control.Get(ctx, nil)
	}, workflow.RegisterOptions{Name: "LifecycleHold"})

	addr, err := w.start()
	require.NoError(t, err)
	readyStatus := func() int {
		resp, err := http.Get("http://" + addr.String() + "/readyz")
		if err != nil {
			return 0
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	require.Equal(t, http.StatusOK, readyStatus())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "lifecycle-hold-" + hex.EncodeToString(suffix[:]), TaskQueue: businessQueue, WorkflowExecutionTimeout: 5 * time.Minute}, "LifecycleHold")
	require.NoError(t, err)
	defer func() {
		_ = cleanup.TerminateWorkflow(context.Background(), run.GetID(), run.GetRunID(), "lifecycle test cleanup")
	}()
	queues := map[string]bool{}
	for len(queues) < 2 {
		select {
		case q := <-started:
			queues[q] = true
		case <-time.After(time.Minute):
			t.Fatalf("both Activities must be in flight; started on %v", queues)
		}
	}

	// The stop: readiness gone at once, the drain bound shared by both
	// queues, the clients and the listener closed within the allowance.
	probed := make(chan int, 1)
	go func() {
		time.Sleep(drain / 3)
		probed <- readyStatus()
	}()
	stopStarted := time.Now()
	require.NoError(t, w.stop(context.Background()))
	elapsed := time.Since(stopStarted)
	require.Equal(t, http.StatusServiceUnavailable, <-probed, "a draining worker is not ready")
	require.GreaterOrEqual(t, elapsed, drain, "the in-flight Activities were held until the bound")
	require.Less(t, elapsed, 2*drain, "the stop exceeded one drain bound: %s (sequential drains take two)", elapsed)
	require.Less(t, elapsed, drain+closeAllowance)
	for i := 0; i < 2; i++ {
		select {
		case cause := <-ended:
			require.ErrorIs(t, cause, errDrainBoundElapsed)
		case <-time.After(5 * time.Second):
			t.Fatal("an Activity was not abandoned with the drain cause")
		}
	}
	require.Equal(t, 0, readyStatus(), "the listener is closed")

	// Temporal saw the abandonment: the failures were reported within the
	// grace (no attempt is left pending on the stopped worker), so the
	// retries begin at once rather than at the heartbeat bound.
	require.Eventually(t, func() bool {
		described, err := cleanup.DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
		return err == nil && len(described.GetPendingActivities()) == 0
	}, 5*time.Second, 200*time.Millisecond, "the abandoned Activities were not reported to Temporal")
}
