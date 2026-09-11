package localcheck

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/contracts"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/logging"
	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

func fixtures(t *testing.T) ([]contracts.LocalCheckInputV1, []contracts.LocalCheckWorkflowResultV1) {
	t.Helper()
	raw, err := contracts.Retained.ReadFile("retained/local-check-v1.fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var data struct {
		Inputs  []contracts.LocalCheckInputV1
		Results []contracts.LocalCheckWorkflowResultV1
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		t.Fatal(err)
	}
	return data.Inputs, data.Results
}

func environment(input contracts.LocalCheckInputV1) *testsuite.TestWorkflowEnvironment {
	suite := testsuite.WorkflowTestSuite{}
	suite.SetLogger(logging.New(io.Discard, "test", "test", "local"))
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{
		ID: contracts.LocalCheckWorkflowIDPrefix + input.OperationID, TaskQueue: contracts.LocalCheckTaskQueue,
		WorkflowExecutionTimeout: contracts.LocalCheckWorkflowExecutionTimeoutSeconds * time.Second,
	})
	env.RegisterActivity(ComputeLocalCheck)
	return env
}

func TestFixedFixtures(t *testing.T) {
	inputs, expected := fixtures(t)
	for index, input := range inputs {
		t.Run(input.FixtureID, func(t *testing.T) {
			env := environment(input)
			env.ExecuteWorkflow(LocalCheckWorkflow, input)
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			var result contracts.LocalCheckWorkflowResultV1
			if err := env.GetWorkflowResult(&result); err != nil || result != expected[index] {
				t.Fatalf("result differs: %+v, %v", result, err)
			}
		})
	}
}

func TestActivityPreservesUTF8WithoutNormalization(t *testing.T) {
	inputs, _ := fixtures(t)
	for _, text := range []string{"é", "e\u0301", "🙂\r\n"} {
		input := inputs[0]
		input.FixtureText = text
		input.ExecutionGeneration, input.RecoveryGeneration = "18446744073709551615", "9007199254740993"
		suite := testsuite.WorkflowTestSuite{}
		suite.SetLogger(logging.New(io.Discard, "test", "test", "local"))
		env := suite.NewTestActivityEnvironment()
		env.RegisterActivity(ComputeLocalCheck)
		value, err := env.ExecuteActivity(ComputeLocalCheck, input)
		if err != nil {
			t.Fatal(err)
		}
		var result contracts.LocalCheckWorkflowResultV1
		if err := value.Get(&result); err != nil {
			t.Fatal(err)
		}
		if result.ByteLength != strconv.Itoa(len([]byte(text))) || result.ContentDigest != fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(text))) ||
			result.ExecutionGeneration != input.ExecutionGeneration || result.RecoveryGeneration != input.RecoveryGeneration {
			t.Fatal("Activity altered bytes or generation counters")
		}
	}
}

func TestActivityRetriesExhaustTheOriginalWorkflow(t *testing.T) {
	inputs, _ := fixtures(t)
	input := inputs[0]
	env := environment(input)
	var attempts []int32
	env.OnActivity(ComputeLocalCheck, mock.Anything, input).Return(func(ctx context.Context, input contracts.LocalCheckInputV1) (contracts.LocalCheckWorkflowResultV1, error) {
		attempts = append(attempts, activity.GetInfo(ctx).Attempt)
		return contracts.LocalCheckWorkflowResultV1{}, errors.New("injected completion failure")
	}).Times(3)
	env.ExecuteWorkflow(LocalCheckWorkflow, input)
	if env.GetWorkflowError() == nil || fmt.Sprint(attempts) != "[1 2 3]" {
		t.Fatalf("expected failure after exactly three attempts: %v, %v", attempts, env.GetWorkflowError())
	}
	env.AssertExpectations(t)
}

func TestObservedCancellationCannotReturnSuccess(t *testing.T) {
	inputs, results := fixtures(t)
	env := environment(inputs[0])
	env.OnActivity(ComputeLocalCheck, mock.Anything, inputs[0]).After(5*time.Second).Return(results[0], nil)
	env.RegisterDelayedCallback(env.CancelWorkflow, time.Second)
	env.ExecuteWorkflow(LocalCheckWorkflow, inputs[0])
	if !temporal.IsCanceledError(env.GetWorkflowError()) {
		t.Fatal("observed cancellation did not cancel the original workflow", env.GetWorkflowError())
	}
	var result contracts.LocalCheckWorkflowResultV1
	if env.GetWorkflowResult(&result) == nil {
		t.Fatal("canceled workflow returned success")
	}
}

func TestWrongExecutionProfileSchedulesNoActivity(t *testing.T) {
	inputs, _ := fixtures(t)
	for _, options := range []client.StartWorkflowOptions{
		{ID: "wrong-id", TaskQueue: contracts.LocalCheckTaskQueue, WorkflowExecutionTimeout: 5 * time.Minute},
		{ID: contracts.LocalCheckWorkflowIDPrefix + inputs[0].OperationID, TaskQueue: contracts.LocalCheckTaskQueue, WorkflowExecutionTimeout: time.Hour},
	} {
		env := environment(inputs[0])
		env.SetStartWorkflowOptions(options)
		started := 0
		env.SetOnActivityStartedListener(func(*activity.Info, context.Context, converter.EncodedValues) { started++ })
		env.ExecuteWorkflow(LocalCheckWorkflow, inputs[0])
		if env.GetWorkflowError() == nil {
			t.Fatal("unsupported execution profile accepted")
		}
		if started != 0 {
			t.Fatal("unsupported profile scheduled an Activity")
		}
	}
}
