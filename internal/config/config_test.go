package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/config"
)

func write(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

const minimal = "temporal:\n  address: 127.0.0.1:27233\ncontrol:\n  address: 127.0.0.1:9101\n"

var env = []string{"ANVILKIT_WORKFLOW_KUBECONFIG=/run/launcher.kubeconfig", "ANVILKIT_WORKFLOW_LAUNCH_BACKEND=kind-anvilkit-dev", "ANVILKIT_CONTROL_LISTEN=other-service"}

func TestPrecedenceDefaultsFileEnvironment(t *testing.T) {
	c, err := config.LoadFrom(write(t, "temporal:\n  address: 127.0.0.1:27233\n  build_id: file-build\ncontrol:\n  address: 127.0.0.1:9101\nexecution:\n  launch_window: 3m\n"), append(env, "ANVILKIT_WORKFLOW_BUILD_ID=env-build"))
	require.NoError(t, err)
	require.Equal(t, "env-build", c.Temporal.BuildID, "environment overrides the file")
	require.Equal(t, 3*time.Minute, c.Execution.LaunchWindow, "file overrides the default")
	require.Equal(t, 24*time.Hour, c.Execution.Cleanup.ReconcileMaxDuration, "default kept")
	require.Equal(t, "anvilkit-workflow-control", c.Temporal.ControlTaskQueue)
	require.Equal(t, "/run/launcher.kubeconfig", c.Kubernetes.Kubeconfig)
	require.False(t, c.Development.Enabled)
}

func TestCheckedInFileLoads(t *testing.T) {
	c, err := config.LoadFrom(filepath.Join("..", "..", "config.yaml"), env)
	require.NoError(t, err)
	require.Equal(t, 60*time.Second, c.Execution.Cleanup.UnresolvedSettleWindow)
}

func TestUnknownKeysAreRejected(t *testing.T) {
	_, err := config.LoadFrom(write(t, minimal+"execution:\n  cleanup:\n    timeoutt: 1m\n"), env)
	require.ErrorContains(t, err, "timeoutt")
	_, err = config.LoadFrom(write(t, minimal), append(env, "ANVILKIT_WORKFLOW_EXECUTION_LAUNCH_WINDOW=1m"))
	require.ErrorContains(t, err, "ANVILKIT_WORKFLOW_EXECUTION_LAUNCH_WINDOW")
	_, err = config.LoadFrom(write(t, minimal), append(env, "ANVILKIT_WORKFLOW_DEVELOPMENT_ENABLED=true"))
	require.ErrorContains(t, err, "ANVILKIT_WORKFLOW_DEVELOPMENT_ENABLED", "fault injection is never enabled from the environment")
}

func TestRequiredValuesRangesAndCrossFieldRules(t *testing.T) {
	// No kubeconfig selects the in-cluster identity: it is not required.
	c, err := config.LoadFrom(write(t, minimal), env[1:])
	require.NoError(t, err)
	require.Empty(t, c.Kubernetes.Kubeconfig)
	require.Equal(t, "127.0.0.1:9102", c.Health.Listen)
	_, err = config.LoadFrom(write(t, minimal+"health:\n  listen: \"\"\n"), env)
	require.ErrorContains(t, err, "health.listen is required")
	_, err = config.LoadFrom(write(t, "control:\n  address: 127.0.0.1:9101\n"), env)
	require.ErrorContains(t, err, "temporal.address is required")
	_, err = config.LoadFrom(write(t, "temporal:\n  address: x\n  control_task_queue: anvilkit-workflow\ncontrol:\n  address: 127.0.0.1:9101\n"), env)
	require.ErrorContains(t, err, "must differ")
	_, err = config.LoadFrom(write(t, minimal+"execution:\n  control_retry_max_attempts: 0\n"), env)
	require.ErrorContains(t, err, "control_retry_max_attempts")
	_, err = config.LoadFrom(write(t, minimal+"execution:\n  cleanup:\n    unresolved_settle_window: 10s\n"), env)
	require.ErrorContains(t, err, "unresolved_settle_window 10s must be at least", "one observation of an unresolved create lasts at least the create Activity's bound")
	_, err = config.LoadFrom(write(t, minimal+"execution:\n  cleanup:\n    timeout: 30s\n"), env)
	require.ErrorContains(t, err, "cleanup.timeout 30s must exceed")
	_, err = config.LoadFrom(write(t, minimal+"execution:\n  cleanup:\n    reconcile_initial_interval: 5m\n    reconcile_max_interval: 1m\n"), env)
	require.ErrorContains(t, err, "reconcile_initial_interval")
	_, err = config.LoadFrom(write(t, minimal+"execution:\n  cleanup:\n    reconcile_max_duration: 3m\n"), env)
	require.ErrorContains(t, err, "reconcile_max_duration")
	_, err = config.LoadFrom(write(t, minimal+"development:\n  lose_receipt_once: [AcceptResult]\n"), env)
	require.ErrorContains(t, err, "requires development.enabled")
	c, err = config.LoadFrom(write(t, minimal+"development:\n  enabled: true\n  lose_receipt_once: [AcceptResult]\n"), env)
	require.NoError(t, err)
	require.Equal(t, []string{"AcceptResult"}, c.Development.LoseReceiptOnce)
	_, err = config.LoadFrom(write(t, minimal+"development:\n  hold_until_canceled_once: [CreateJob]\n"), env)
	require.ErrorContains(t, err, "hold_until_canceled_once requires development.enabled")
	c, err = config.LoadFrom(write(t, minimal+"development:\n  enabled: true\n  hold_until_canceled_once: [CreateJob]\n"), env)
	require.NoError(t, err)
	require.Equal(t, []string{"CreateJob"}, c.Development.HoldUntilCanceledOnce)
}
