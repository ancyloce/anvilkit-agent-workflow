//go:build integration

package modelproxy_test

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/adapters/modelproxy"
)

// TestCallModelActivityChain executes the CallModel Activity (Temporal's
// Activity test environment: the registered function, its heartbeats and
// its typed result) against the Model Proxy the parent's integration
// scenario prepared (delivery.md P11): the Proxy, the real Control it admits
// with and the countable upstream are the parent's; the input is the
// parent's typed ModelCallInput; the results of the call, of its reentry,
// of the query and — when the model asked for a tool — of the next call
// that replays the tool round trip under its own call id are written where
// the parent reads them. The parent asserts the physical receive count and
// Control's records; this side asserts what the Activity handed back.
// Without the parent's inputs it skips.
func TestCallModelActivityChain(t *testing.T) {
	url, token, input, out := os.Getenv("ANVILKIT_INTEGRATION_MODEL_PROXY_URL"), os.Getenv("ANVILKIT_INTEGRATION_MODEL_PROXY_TOKEN"), os.Getenv("ANVILKIT_INTEGRATION_MODEL_PROXY_INPUT"), os.Getenv("ANVILKIT_INTEGRATION_MODEL_PROXY_RESULT")
	if url == "" || token == "" || input == "" || out == "" {
		t.Skip("ANVILKIT_INTEGRATION_MODEL_PROXY_{URL,TOKEN,INPUT,RESULT} not set; the parent's tests/integration prepares them")
	}
	var in activities.ModelCallInput
	require.NoError(t, json.Unmarshal([]byte(input), &in))
	client, err := modelproxy.New(modelproxy.Options{BaseURL: url, Token: token, Timeout: 2 * time.Minute})
	require.NoError(t, err)
	acts := &activities.Activities{Model: client}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivityWithOptions(acts.CallModel, activity.RegisterOptions{Name: activities.NameCallModel})
	env.RegisterActivityWithOptions(acts.GetModelCall, activity.RegisterOptions{Name: activities.NameGetModelCall})
	run := func() activities.ModelCallResult {
		val, err := env.ExecuteActivity(activities.NameCallModel, in)
		require.NoError(t, err)
		var res activities.ModelCallResult
		require.NoError(t, val.Get(&res))
		return res
	}
	first := run()
	require.Equal(t, "succeeded", first.State, "%+v", first)
	require.NotEmpty(t, first.Text)
	require.NotNil(t, first.Usage)
	require.NotEmpty(t, first.DispatchID)
	// A retry of the Activity under the same call id is a reentry: the same
	// frames come back and nothing is sent again (the parent counts).
	again := run()
	require.Equal(t, first, again)
	val, err := env.ExecuteActivity(activities.NameGetModelCall, in.CallID)
	require.NoError(t, err)
	var queried activities.ModelCallResult
	require.NoError(t, val.Get(&queried))
	require.Equal(t, first.DispatchID, queried.DispatchID)
	require.Equal(t, "succeeded", queried.State)
	results := []activities.ModelCallResult{first, again, queried}
	if len(first.ToolCalls) > 0 {
		// The tool round trip: the caller (here the test, never the Proxy)
		// executes the tool and asks the model again under a new call id with
		// the assistant's tool calls and the matching result in the history.
		next := in
		next.CallID = in.CallID + "-2"
		next.Messages = append(append([]activities.ModelMessage{}, in.Messages...),
			activities.ModelMessage{Role: "assistant", Content: first.Text, ToolCalls: first.ToolCalls})
		for _, tc := range first.ToolCalls {
			next.Messages = append(next.Messages, activities.ModelMessage{Role: "tool", ToolCallID: tc.ToolCallID, Content: "tool " + tc.Name + " executed by the caller: " + tc.Arguments})
		}
		val, err := env.ExecuteActivity(activities.NameCallModel, next)
		require.NoError(t, err)
		var second activities.ModelCallResult
		require.NoError(t, val.Get(&second))
		require.Equal(t, "succeeded", second.State, "%+v", second)
		require.NotEqual(t, first.DispatchID, second.DispatchID, "the next request is its own admission")
		results = append(results, second)
	}
	raw, err := json.Marshal(results)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(out, raw, 0o600))
}
