//go:build temporalintegration

package localcheck

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/contracts"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/logging"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

func requiredSetting(t *testing.T, key string) string {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		t.Fatalf("UNEXECUTED: real Temporal test requires %s", key)
	}
	return value
}

func temporalClient(t *testing.T) client.Client {
	t.Helper()
	ca, err := os.ReadFile(requiredSetting(t, "ANVILKIT_WORKFLOW_TEMPORAL_TLS_CA"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid test CA")
	}
	certificate, err := tls.LoadX509KeyPair(requiredSetting(t, "ANVILKIT_WORKFLOW_TEST_CLIENT_CERT"), requiredSetting(t, "ANVILKIT_WORKFLOW_TEST_CLIENT_KEY"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := client.DialContext(ctx, client.Options{
		HostPort: requiredSetting(t, "ANVILKIT_WORKFLOW_TEMPORAL_ENDPOINT"), Namespace: requiredSetting(t, "ANVILKIT_WORKFLOW_TEMPORAL_NAMESPACE"),
		Logger: logging.New(io.Discard, "test", "starter", "local"),
		ConnectionOptions: client.ConnectionOptions{TLS: &tls.Config{
			MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: requiredSetting(t, "ANVILKIT_WORKFLOW_TEMPORAL_TLS_SERVER_NAME"), Certificates: []tls.Certificate{certificate},
		}},
	})
	if err != nil {
		t.Fatal("real Temporal mTLS connection failed", err)
	}
	t.Cleanup(c.Close)
	return c
}

func startOptions(input contracts.LocalCheckInputV1) client.StartWorkflowOptions {
	return client.StartWorkflowOptions{
		ID: contracts.LocalCheckWorkflowIDPrefix + input.OperationID, TaskQueue: contracts.LocalCheckTaskQueue,
		WorkflowExecutionTimeout:                 contracts.LocalCheckWorkflowExecutionTimeoutSeconds * time.Second,
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowIDConflictPolicy:                 enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}
}

func uniqueInput(input contracts.LocalCheckInputV1) contracts.LocalCheckInputV1 {
	input.OperationID = "op-workflow01-" + rand.Text()
	return input
}

func history(t *testing.T, ctx context.Context, c client.Client, run client.WorkflowRun) *historypb.History {
	t.Helper()
	iterator := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	history := &historypb.History{}
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			t.Fatal(err)
		}
		history.Events = append(history.Events, event)
	}
	raw, err := protojson.MarshalOptions{Indent: "  "}.Marshal(history)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(requiredSetting(t, "ANVILKIT_WORKFLOW_TEST_ARTIFACTS"), run.GetRunID()+".history.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("retained workflowId=%s runId=%s history=%s", run.GetID(), run.GetRunID(), path)
	return history
}

func scheduledCount(history *historypb.History) int {
	count := 0
	for _, event := range history.Events {
		if event.GetActivityTaskScheduledEventAttributes() != nil {
			count++
		}
	}
	return count
}

func TestRealTemporalWorker(t *testing.T) {
	c := temporalClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cluster, err := c.WorkflowService().GetClusterInfo(ctx, &workflowservice.GetClusterInfoRequest{})
	if err != nil || cluster.GetServerVersion() != "1.31.2" {
		t.Fatal("selected persistent server version differs", cluster.GetServerVersion(), err)
	}
	namespace, err := c.WorkflowService().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: os.Getenv("ANVILKIT_WORKFLOW_TEMPORAL_NAMESPACE")})
	if err != nil || namespace.GetConfig().GetWorkflowExecutionRetentionTtl().AsDuration() < 24*time.Hour {
		t.Fatal("namespace does not retain the required history", err)
	}
	inputs, results := fixtures(t)
	// Record cancellation before this run's Worker is started. There is no
	// public fault API, timing guess or candidate work used to delay it.
	canceledInput := uniqueInput(inputs[0])
	canceled, err := c.ExecuteWorkflow(ctx, startOptions(canceledInput), contracts.LocalCheckWorkflowType, canceledInput)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CancelWorkflow(ctx, canceled.GetID(), canceled.GetRunID()); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(requiredSetting(t, "ANVILKIT_WORKFLOW_TEST_ARTIFACTS"), "worker.log")
	output, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(requiredSetting(t, "ANVILKIT_WORKFLOW_TEST_BINARY"))
	// The Worker receives only Temporal connection settings. In particular it
	// receives neither Agent database credentials nor the starter credential.
	for _, variable := range os.Environ() {
		if strings.HasPrefix(variable, "ANVILKIT_WORKFLOW_TEMPORAL_") {
			command.Env = append(command.Env, variable)
		}
	}
	command.Stdout, command.Stderr = output, output
	if err := command.Start(); err != nil {
		output.Close()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		_ = command.Process.Signal(syscall.SIGTERM)
		select {
		case err := <-done:
			if err != nil {
				t.Error("Worker process failed", err)
			}
		case <-time.After(15 * time.Second):
			_ = command.Process.Kill()
			<-done
			t.Error("Worker failed to stop within its drain bound")
		}
		output.Close()
	}
	t.Cleanup(stop)
	if err := canceled.Get(ctx, nil); !temporal.IsCanceledError(err) {
		t.Fatal("the pre-recorded cancellation did not cancel the original execution", err)
	}
	if scheduledCount(history(t, ctx, c, canceled)) != 0 {
		t.Fatal("observed cancellation scheduled computation")
	}
	for index, fixture := range inputs {
		t.Run(fixture.FixtureID, func(t *testing.T) {
			input := uniqueInput(fixture)
			options := startOptions(input)
			run, err := c.ExecuteWorkflow(ctx, options, contracts.LocalCheckWorkflowType, input)
			if err != nil {
				t.Fatal(err)
			}
			var result contracts.LocalCheckWorkflowResultV1
			if err := run.Get(ctx, &result); err != nil {
				t.Fatal(err)
			}
			expected := results[index]
			expected.OperationID = input.OperationID
			if result != expected {
				t.Fatalf("real Worker result differs: %+v", result)
			}
			h := history(t, ctx, c, run)
			start := h.Events[0].GetWorkflowExecutionStartedEventAttributes()
			if start.GetWorkflowExecutionTimeout().AsDuration() != 5*time.Minute || start.GetRetryPolicy() != nil || start.GetTaskQueue().GetName() != contracts.LocalCheckTaskQueue {
				t.Fatal("original execution did not retain the fixed options")
			}
			var retained contracts.LocalCheckInputV1
			if err := converter.GetDefaultDataConverter().FromPayloads(start.GetInput(), &retained); err != nil || retained != input {
				t.Fatal("history input differs from the immutable start", err)
			}
			if scheduledCount(h) != 1 {
				t.Fatal("fixed Workflow did not schedule exactly one Activity")
			}
			for _, event := range h.Events {
				if scheduled := event.GetActivityTaskScheduledEventAttributes(); scheduled != nil {
					rp := scheduled.GetRetryPolicy()
					if scheduled.GetActivityType().GetName() != contracts.LocalCheckActivityType || scheduled.GetTaskQueue().GetName() != contracts.LocalCheckTaskQueue ||
						scheduled.GetStartToCloseTimeout().AsDuration() != 10*time.Second || scheduled.GetScheduleToCloseTimeout().AsDuration() != time.Minute ||
						rp.GetMaximumAttempts() != 3 || rp.GetInitialInterval().AsDuration() != time.Second || rp.GetBackoffCoefficient() != 2 || rp.GetMaximumInterval().AsDuration() != 5*time.Second {
						t.Fatal("recorded Activity bounds differ from the approved profile")
					}
					if err := converter.GetDefaultDataConverter().FromPayloads(scheduled.GetInput(), &retained); err != nil || retained != input {
						t.Fatal("Activity input differs from the original history input", err)
					}
				}
			}
			var replayLogs bytes.Buffer
			replayer := worker.NewWorkflowReplayer()
			replayer.RegisterWorkflowWithOptions(LocalCheckWorkflow, workflow.RegisterOptions{Name: contracts.LocalCheckWorkflowType})
			if err := replayer.ReplayWorkflowHistoryWithOptions(logging.New(&replayLogs, "test", "replayer", "local"), h,
				worker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: run.GetID(), RunID: run.GetRunID()}}); err != nil {
				t.Fatal(err)
			}
			if replayLogs.Len() != 0 {
				t.Fatal("real history replay emitted execution logs")
			}
			_, err = c.ExecuteWorkflow(ctx, options, contracts.LocalCheckWorkflowType, input)
			var duplicate *serviceerror.WorkflowExecutionAlreadyStarted
			if !errors.As(err, &duplicate) || duplicate.RunId != run.GetRunID() {
				t.Fatal("closed-ID replay did not retain/reject the original execution", err)
			}
		})
	}
	// The SDK's unit environment omits Workflow RetryPolicy from GetInfo;
	// verify this profile rejection using actual persisted server metadata.
	wrong := uniqueInput(inputs[0])
	options := startOptions(wrong)
	options.RetryPolicy = &temporal.RetryPolicy{MaximumAttempts: 3}
	run, err := c.ExecuteWorkflow(ctx, options, contracts.LocalCheckWorkflowType, wrong)
	if err != nil {
		t.Fatal(err)
	}
	var application *temporal.ApplicationError
	if err := run.Get(ctx, nil); !errors.As(err, &application) || application.Type() != "InvalidLocalCheckExecution" {
		t.Fatal("real Workflow retry policy was accepted", err)
	}
	h := history(t, ctx, c, run)
	if scheduledCount(h) != 0 || h.Events[len(h.Events)-1].GetWorkflowExecutionFailedEventAttributes().GetNewExecutionRunId() != "" {
		t.Fatal("invalid profile scheduled an Activity or replacement Workflow")
	}
	stop()
	logs, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	counts := make(map[string]int)
	failures := 0
	for _, line := range bytes.Split(bytes.TrimSpace(logs), []byte("\n")) {
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal("Worker emitted a non-JSON log", err)
		}
		counts[fmt.Sprint(record["eventName"])]++
		if record["eventName"] == "workflow.completed" && record["outcome"] == "error" && record["operationId"] == wrong.OperationID {
			failures++
		}
		if strings.HasPrefix(fmt.Sprint(record["eventName"]), "activity.") && (record["operationId"] == nil || record["temporal.runId"] == nil || record["stepExecutionId"] != nil) {
			t.Fatal("actual Activity logs do not use the approved local identity mapping")
		}
	}
	if counts["activity.started"] != 2 || counts["activity.completed"] != 2 || counts["service.drained"] != 1 || failures != 1 {
		t.Fatal("canonical Worker execution/drain summaries differ", counts)
	}
	for _, forbidden := range []string{"fixtureText", "requestDigest", "contentDigest", "PRIVATE KEY", "-----BEGIN", "AnvilKit"} {
		if bytes.Contains(logs, []byte(forbidden)) {
			t.Fatal("Worker logs contain protected input/result or credentials")
		}
	}
}
