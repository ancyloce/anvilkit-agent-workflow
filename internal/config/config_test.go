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

const minimal = "temporal:\n  address: 127.0.0.1:27233\n  tls:\n    ca_file: /etc/anvilkit/temporal-ca/ca.crt\ncontrol:\n  address: 127.0.0.1:9101\n"

// The process identity files (P0.1) follow the launcher placements; the
// loader does not read them.
var env = []string{"ANVILKIT_WORKFLOW_KUBECONFIG=/run/launcher.kubeconfig", "ANVILKIT_WORKFLOW_LAUNCH_BACKEND=kind-anvilkit-dev", "ANVILKIT_CONTROL_LISTEN=other-service",
	"ANVILKIT_WORKFLOW_IDENTITY_CERT_FILE=/etc/anvilkit/identity/tls.crt", "ANVILKIT_WORKFLOW_IDENTITY_KEY_FILE=/etc/anvilkit/identity/tls.key", "ANVILKIT_WORKFLOW_IDENTITY_CA_FILE=/etc/anvilkit/identity/ca.crt"}

func TestPrecedenceDefaultsFileEnvironment(t *testing.T) {
	c, err := config.LoadFrom(write(t, "temporal:\n  address: 127.0.0.1:27233\n  build_id: file-build\n  tls:\n    ca_file: /c\ncontrol:\n  address: 127.0.0.1:9101\nexecution:\n  launch_window: 3m\n"), append(env, "ANVILKIT_WORKFLOW_BUILD_ID=env-build"))
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
	require.Equal(t, 3*time.Minute, c.Execution.Cleanup.CreateRequestLifetime, "beyond the API server's default 60s request timeout")
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
	require.Equal(t, "mtls", c.Control.Identity.Mode)
	require.Equal(t, "mtls", c.ModelProxy.Identity.Mode)
	require.Equal(t, "tls", c.Temporal.TLS.Mode)
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
	_, err = config.LoadFrom(write(t, minimal+"execution:\n  cleanup:\n    create_request_lifetime: 10s\n"), env)
	require.ErrorContains(t, err, "create_request_lifetime", "shorter than any API server request timeout it must cover")
	_, err = config.LoadFrom(write(t, minimal+"execution:\n  cleanup:\n    create_request_lifetime: 30m\n    reconcile_max_duration: 20m\n"), env)
	require.ErrorContains(t, err, "create_request_lifetime 30m0s must be shorter than", "a lifetime the reconciliation never outlives could never settle a request")
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

func TestModelProxyPlacementAndSecret(t *testing.T) {
	c, err := config.LoadFrom(write(t, minimal), env)
	require.NoError(t, err)
	require.Empty(t, c.ModelProxy.Address, "no placement: the model Activities answer DEPENDENCY_UNAVAILABLE")
	require.Equal(t, 5*time.Minute, c.ModelProxy.Timeout)
	dev := minimal + "development:\n  enabled: true\nmodel_proxy:\n  identity:\n    mode: development\n"
	_, err = config.LoadFrom(write(t, dev), append(env, "ANVILKIT_WORKFLOW_MODEL_PROXY_ADDRESS=http://127.0.0.1:9103"))
	require.ErrorContains(t, err, "ANVILKIT_WORKFLOW_MODEL_PROXY_TOKEN")
	c, err = config.LoadFrom(write(t, dev), append(env, "ANVILKIT_WORKFLOW_MODEL_PROXY_ADDRESS=http://127.0.0.1:9103", "ANVILKIT_WORKFLOW_MODEL_PROXY_TOKEN=secret"))
	require.NoError(t, err)
	require.Equal(t, "secret", c.ModelProxy.Token)
	_, err = config.LoadFrom(write(t, minimal+"model_proxy:\n  identity:\n    mode: development\n"), append(env, "ANVILKIT_WORKFLOW_MODEL_PROXY_ADDRESS=http://127.0.0.1:9103", "ANVILKIT_WORKFLOW_MODEL_PROXY_TOKEN=secret"))
	require.ErrorContains(t, err, "model_proxy.identity.mode development (plaintext bearer) requires development.enabled", "the bearer path is DEVELOPMENT_ONLY")
	_, err = config.LoadFrom(write(t, minimal+"model_proxy:\n  token: in-file\n"), env)
	require.ErrorContains(t, err, "model_proxy.token is a secret")
	_, err = config.LoadFrom(write(t, dev), append(env, "ANVILKIT_WORKFLOW_MODEL_PROXY_ADDRESS=ftp://x", "ANVILKIT_WORKFLOW_MODEL_PROXY_TOKEN=secret"))
	require.ErrorContains(t, err, "model_proxy.address must be an absolute http(s) URL")
	_, err = config.LoadFrom(write(t, minimal+"model_proxy:\n  identity:\n    mode: mtls\n"), append(env[:3:3], "ANVILKIT_WORKFLOW_MODEL_PROXY_ADDRESS=https://proxy"))
	require.ErrorContains(t, err, "model_proxy.identity.mtls needs cert_file, key_file and ca_file")
	c, err = config.LoadFrom(write(t, minimal), append(env, "ANVILKIT_WORKFLOW_MODEL_PROXY_ADDRESS=https://proxy"))
	require.NoError(t, err, "the process identity serves the Proxy client")
	require.Equal(t, "/etc/anvilkit/identity/tls.key", c.Resolved(c.ModelProxy.Identity).KeyFile)
}

func TestTelemetryPlacement(t *testing.T) {
	_, err := config.LoadFrom(write(t, minimal), append(env, "ANVILKIT_WORKFLOW_TELEMETRY_OTLP_ENDPOINT=collector:4317"))
	require.ErrorContains(t, err, "telemetry.otlp_tls.ca_file is required", "a placed collector needs a verified transport")
	c, err := config.LoadFrom(write(t, minimal+"telemetry:\n  otlp_tls:\n    ca_file: /etc/anvilkit/otlp-ca/ca.crt\n"), append(env, "ANVILKIT_WORKFLOW_TELEMETRY_OTLP_ENDPOINT=collector:4317"))
	require.NoError(t, err)
	require.Equal(t, "collector:4317", c.Telemetry.OTLPEndpoint)
	require.Equal(t, 1.0, c.Telemetry.SampleRatio)
	_, err = config.LoadFrom(write(t, minimal+"telemetry:\n  sample_ratio: 2\n"), env)
	require.ErrorContains(t, err, "telemetry.sample_ratio")
}

// TestIdentityAndDevelopmentGuard (P0.1): plaintext anywhere needs that
// connection's development mode and the top-level guard; the guard
// downgrades nothing by itself; the sidecar's mtls identity needs its
// Secret; the guard is file-only.
func TestIdentityAndDevelopmentGuard(t *testing.T) {
	_, err := config.LoadFrom(write(t, minimal), env[:3:3])
	require.ErrorContains(t, err, "control.identity.mtls needs cert_file, key_file and ca_file")
	_, err = config.LoadFrom(write(t, "temporal:\n  address: 127.0.0.1:27233\n  tls:\n    ca_file: /c\ncontrol:\n  address: 127.0.0.1:9101\n  identity:\n    mode: development\n"), env)
	require.ErrorContains(t, err, "control.identity.mode development (plaintext) requires development.enabled")
	c, err := config.LoadFrom(write(t, "temporal:\n  address: 127.0.0.1:27233\n  tls:\n    mode: development\ncontrol:\n  address: 127.0.0.1:9101\ndevelopment:\n  enabled: true\n"), env)
	require.NoError(t, err)
	require.Equal(t, "development", c.Temporal.TLS.Mode)
	require.Equal(t, "mtls", c.Control.Identity.Mode, "the guard does not downgrade other connections")
	_, err = config.LoadFrom(write(t, "temporal:\n  address: 127.0.0.1:27233\n  tls:\n    mode: development\ncontrol:\n  address: 127.0.0.1:9101\n"), env)
	require.ErrorContains(t, err, "temporal.tls.mode development (plaintext) requires development.enabled")
	_, err = config.LoadFrom(write(t, "temporal:\n  address: 127.0.0.1:27233\n  tls:\n    mode: mtls\n    ca_file: /c\ncontrol:\n  address: 127.0.0.1:9101\n"), env)
	require.NoError(t, err, "temporal mtls takes the process certificate")
	_, err = config.LoadFrom(write(t, minimal+"kubernetes:\n  sidecar:\n    identity_mode: development\n"), env)
	require.ErrorContains(t, err, "kubernetes.sidecar.identity_mode development (a plaintext sidecar) requires development.enabled")
	_, err = config.LoadFrom(write(t, minimal+"kubernetes:\n  sidecar:\n    identity_mode: mtls\n    identity_secret: \"\"\n"), env)
	require.ErrorContains(t, err, "kubernetes.sidecar.identity_secret is required")
	c, err = config.LoadFrom(write(t, minimal+"kubernetes:\n  sidecar:\n    identity_mode: mtls\n"), env)
	require.NoError(t, err)
	require.Equal(t, "anvilkit-job-access-sidecar-identity", c.Kubernetes.Sidecar.IdentitySecret)
	_, err = config.LoadFrom(write(t, minimal+"development:\n  enabled: true\n"), append(env, "ANVILKIT_WORKFLOW_DEVELOPMENT_ENABLED=true"))
	require.ErrorContains(t, err, "not allowed overrides", "the guard is file-only")
}
