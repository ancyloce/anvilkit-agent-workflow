// Package config builds the Workflow worker's immutable configuration
// snapshot with koanf (A09, DD-09 §4). Precedence is defaults < the
// service's reviewed configuration file < the allowed ANVILKIT_WORKFLOW_*
// environment overrides; unknown keys, missing values, ranges and
// cross-field rules reject the candidate before the worker starts. Client,
// registration and Activity Task Queue settings come from this one snapshot
// (DD-01 §7); the execution bounds are frozen into every Workflow run's
// history at its start, never read live inside Workflow code.
package config

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

const (
	envPrefix         = "ANVILKIT_WORKFLOW_"
	EnvConfigFile     = "ANVILKIT_WORKFLOW_CONFIG"
	DefaultConfigFile = "config.yaml"
)

type Temporal struct {
	Address          string `koanf:"address"`
	Namespace        string `koanf:"namespace"`
	TaskQueue        string `koanf:"task_queue"`
	ControlTaskQueue string `koanf:"control_task_queue"`
	WorkerIdentity   string `koanf:"worker_identity"`
	BuildID          string `koanf:"build_id"`
}

type Control struct {
	Address string `koanf:"address"`
}

type Kubernetes struct {
	// Kubeconfig is the launcher identity for a worker outside the cluster
	// (a kubeconfig file, ANVILKIT_WORKFLOW_KUBECONFIG). Empty selects the
	// in-cluster identity: the Pod's mounted ServiceAccount token and CA
	// (client-go rest.InClusterConfig); a worker outside a cluster without
	// a kubeconfig fails at startup, nothing is guessed.
	Kubeconfig    string `koanf:"kubeconfig"`
	Namespace     string `koanf:"namespace"`
	LaunchBackend string `koanf:"launch_backend"`
	// ImageRegistry completes the repository of a profile image that names
	// no registry (the digest is what the profile pins); a placement.
	ImageRegistry string `koanf:"image_registry"`
	// EnabledProfiles lists the reviewed profiles this environment may
	// launch: a profile is enabled only after its required negative tests
	// passed on the target runtime (delivery.md P09). File-only.
	EnabledProfiles []string `koanf:"enabled_profiles"`
	// Sidecar is what the harness template tells the access sidecar of a
	// Job Pod: where Control is from inside the cluster and which identity
	// mode the environment provides (development or mtls).
	Sidecar Sidecar `koanf:"sidecar"`
	// CandidateSeccompProfile is the node-local syscall profile
	// (Localhost type) of the candidate/supervisor container, relative to
	// the kubelet's seccomp root.
	CandidateSeccompProfile string `koanf:"candidate_seccomp_profile"`
}

type Sidecar struct {
	ControlAddress string `koanf:"control_address"`
	IdentityMode   string `koanf:"identity_mode"`
}

// Cleanup bounds the protected cleanup and its reconciliation of an
// original launch whose stop could not be confirmed.
type Cleanup struct {
	// Timeout is the StartToClose bound of one DeleteJob Activity.
	Timeout time.Duration `koanf:"timeout"`
	// MaxAttempts is the retry cap of one cleanup round.
	MaxAttempts int32 `koanf:"max_attempts"`
	// UnresolvedSettleWindow is how long one cleanup call observes the
	// launch key while a create request of the launch was written but never
	// answered, before it reports cleanup unknown. Absence for the window is
	// not completion: such a request can still land, also after a later
	// request's Job was deleted, so the window only paces observation and
	// the Workflow's reconciliation rounds; cleanup completes once every
	// request is accounted for (answered, not created, or its object seen)
	// and the Job and its Pods are gone.
	UnresolvedSettleWindow time.Duration `koanf:"unresolved_settle_window"`
	// ReconcileInitialInterval and ReconcileMaxInterval pace the
	// reconciliation rounds after an unknown cleanup; ReconcileMaxDuration
	// bounds the whole reconciliation before it fails visibly.
	ReconcileInitialInterval time.Duration `koanf:"reconcile_initial_interval"`
	ReconcileMaxInterval     time.Duration `koanf:"reconcile_max_interval"`
	ReconcileMaxDuration     time.Duration `koanf:"reconcile_max_duration"`
}

// Execution holds every bound that affects Temporal decisions. It is
// captured once per Workflow run (workflow.SideEffect) so a later
// configuration change never alters the replay of an existing history.
type Execution struct {
	ControlActivityTimeout  time.Duration `koanf:"control_activity_timeout"`
	ControlRetryInitial     time.Duration `koanf:"control_retry_initial"`
	ControlRetryMaxInterval time.Duration `koanf:"control_retry_max_interval"`
	ControlRetryMaxAttempts int32         `koanf:"control_retry_max_attempts"`
	LaunchWindow            time.Duration `koanf:"launch_window"`
	ObserveHeartbeatTimeout time.Duration `koanf:"observe_heartbeat_timeout"`
	ObserveMaxAttempts      int32         `koanf:"observe_max_attempts"`
	Cleanup                 Cleanup       `koanf:"cleanup"`
}

// Development is DEVELOPMENT_ONLY fault injection for the integration
// scenarios of delivery.md P05: it can only be enabled explicitly and is
// reported at startup. LoseReceiptOnce names Activities whose first
// successful completion is not reported to Temporal, so the retry reenters
// the same durable command identity. HoldUntilCanceledOnce names Activities
// whose first successful call is not returned to the Workflow until the
// Activity is canceled: the request landed on the backend while the caller
// only sees the canceled request (an unresolved create).
type Development struct {
	Enabled               bool     `koanf:"enabled"`
	LoseReceiptOnce       []string `koanf:"lose_receipt_once"`
	HoldUntilCanceledOnce []string `koanf:"hold_until_canceled_once"`
}

// Health is the worker's health listener: /healthz answers while the
// process runs, /readyz while both pollers are started and not stopping.
// It is the only HTTP surface of the worker and serves no business route.
type Health struct {
	Listen string `koanf:"listen"`
}

type Config struct {
	Temporal        Temporal      `koanf:"temporal"`
	Control         Control       `koanf:"control"`
	Kubernetes      Kubernetes    `koanf:"kubernetes"`
	Execution       Execution     `koanf:"execution"`
	Development     Development   `koanf:"development"`
	Health          Health        `koanf:"health"`
	ShutdownTimeout time.Duration `koanf:"shutdown_timeout"`
}

var defaults = map[string]any{
	"temporal.namespace":                           "anvilkit",
	"temporal.task_queue":                          "anvilkit-workflow",
	"temporal.control_task_queue":                  "anvilkit-workflow-control",
	"temporal.worker_identity":                     "anvilkit-agent-workflow",
	"temporal.build_id":                            "dev",
	"kubernetes.namespace":                         "anvilkit-components",
	"kubernetes.enabled_profiles":                  []string{"local-check-v1"},
	"kubernetes.sidecar.identity_mode":             "disabled",
	"kubernetes.candidate_seccomp_profile":         "anvilkit/candidate.json",
	"execution.control_activity_timeout":           "30s",
	"execution.control_retry_initial":              "1s",
	"execution.control_retry_max_interval":         "30s",
	"execution.control_retry_max_attempts":         6,
	"execution.launch_window":                      "2m",
	"execution.observe_heartbeat_timeout":          "30s",
	"execution.observe_max_attempts":               3,
	"execution.cleanup.timeout":                    "3m",
	"execution.cleanup.max_attempts":               2,
	"execution.cleanup.unresolved_settle_window":   "60s",
	"execution.cleanup.reconcile_initial_interval": "5s",
	"execution.cleanup.reconcile_max_interval":     "1m",
	"execution.cleanup.reconcile_max_duration":     "24h",
	"development.enabled":                          false,
	"health.listen":                                "127.0.0.1:9102",
	"shutdown_timeout":                             "30s",
}

// envOverrides is the complete set of accepted environment variables:
// deployment placement only. Fault injection is never enabled from the
// environment.
var envOverrides = map[string]string{
	"ANVILKIT_WORKFLOW_TEMPORAL_ADDRESS":        "temporal.address",
	"ANVILKIT_WORKFLOW_CONTROL_ADDRESS":         "control.address",
	"ANVILKIT_WORKFLOW_KUBECONFIG":              "kubernetes.kubeconfig",
	"ANVILKIT_WORKFLOW_LAUNCH_BACKEND":          "kubernetes.launch_backend",
	"ANVILKIT_WORKFLOW_BUILD_ID":                "temporal.build_id",
	"ANVILKIT_WORKFLOW_IMAGE_REGISTRY":          "kubernetes.image_registry",
	"ANVILKIT_WORKFLOW_SIDECAR_CONTROL_ADDRESS": "kubernetes.sidecar.control_address",
	"ANVILKIT_WORKFLOW_HEALTH_LISTEN":           "health.listen",
}

func Load() (Config, error) {
	path := os.Getenv(EnvConfigFile)
	if path == "" {
		path = DefaultConfigFile
	}
	return LoadFrom(path, os.Environ())
}

// LoadFrom is Load with explicit inputs (tests).
func LoadFrom(path string, environ []string) (Config, error) {
	k := koanf.New(".")
	if err := k.Load(confmap.Provider(defaults, "."), nil); err != nil {
		return Config{}, err
	}
	if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
		return Config{}, fmt.Errorf("config file %s: %w", path, err)
	}
	if err := applyEnv(k, environ); err != nil {
		return Config{}, err
	}
	var c Config
	if err := k.UnmarshalWithConf("", &c, koanf.UnmarshalConf{DecoderConfig: &mapstructure.DecoderConfig{
		DecodeHook:       mapstructure.StringToTimeDurationHookFunc(),
		ErrorUnused:      true,
		WeaklyTypedInput: true,
		Result:           &c,
	}}); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	return c, c.validate()
}

func applyEnv(k *koanf.Koanf, environ []string) error {
	var unknown []string
	for _, kv := range environ {
		name, value, _ := strings.Cut(kv, "=")
		if !strings.HasPrefix(name, envPrefix) || name == EnvConfigFile {
			continue
		}
		key, ok := envOverrides[name]
		if !ok {
			unknown = append(unknown, name)
			continue
		}
		if err := k.Set(key, value); err != nil {
			return err
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return fmt.Errorf("config: environment variables are not allowed overrides: %s", strings.Join(unknown, ", "))
	}
	return nil
}

func (c Config) validate() error {
	var errs []error
	req := func(name, v string) {
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", name))
		}
	}
	req("temporal.address", c.Temporal.Address)
	req("temporal.namespace", c.Temporal.Namespace)
	req("temporal.task_queue", c.Temporal.TaskQueue)
	req("temporal.control_task_queue", c.Temporal.ControlTaskQueue)
	req("temporal.worker_identity", c.Temporal.WorkerIdentity)
	req("temporal.build_id", c.Temporal.BuildID)
	req("control.address", c.Control.Address)
	req("kubernetes.namespace", c.Kubernetes.Namespace)
	req("kubernetes.launch_backend", c.Kubernetes.LaunchBackend)
	switch c.Kubernetes.Sidecar.IdentityMode {
	case "disabled", "development", "mtls":
	default:
		errs = append(errs, fmt.Errorf("kubernetes.sidecar.identity_mode %q is not one of disabled, development, mtls", c.Kubernetes.Sidecar.IdentityMode))
	}
	if len(c.Kubernetes.EnabledProfiles) == 0 {
		errs = append(errs, errors.New("kubernetes.enabled_profiles must name at least one reviewed profile"))
	}
	if c.Temporal.TaskQueue == c.Temporal.ControlTaskQueue {
		errs = append(errs, errors.New("temporal.task_queue and temporal.control_task_queue must differ"))
	}
	within := func(name string, v, lo, hi time.Duration) {
		if v < lo || v > hi {
			errs = append(errs, fmt.Errorf("%s %s outside [%s, %s]", name, v, lo, hi))
		}
	}
	e := c.Execution
	within("execution.control_activity_timeout", e.ControlActivityTimeout, time.Second, 5*time.Minute)
	within("execution.control_retry_initial", e.ControlRetryInitial, 100*time.Millisecond, time.Minute)
	within("execution.control_retry_max_interval", e.ControlRetryMaxInterval, time.Second, 10*time.Minute)
	if e.ControlRetryMaxAttempts < 1 || e.ControlRetryMaxAttempts > 100 {
		errs = append(errs, fmt.Errorf("execution.control_retry_max_attempts %d outside [1, 100]", e.ControlRetryMaxAttempts))
	}
	within("execution.launch_window", e.LaunchWindow, 30*time.Second, 24*time.Hour)
	within("execution.observe_heartbeat_timeout", e.ObserveHeartbeatTimeout, 5*time.Second, 10*time.Minute)
	if e.ObserveMaxAttempts < 1 || e.ObserveMaxAttempts > 100 {
		errs = append(errs, fmt.Errorf("execution.observe_max_attempts %d outside [1, 100]", e.ObserveMaxAttempts))
	}
	cl := e.Cleanup
	within("execution.cleanup.timeout", cl.Timeout, 10*time.Second, 30*time.Minute)
	if cl.MaxAttempts < 1 || cl.MaxAttempts > 100 {
		errs = append(errs, fmt.Errorf("execution.cleanup.max_attempts %d outside [1, 100]", cl.MaxAttempts))
	}
	within("execution.cleanup.unresolved_settle_window", cl.UnresolvedSettleWindow, 5*time.Second, 10*time.Minute)
	within("execution.cleanup.reconcile_initial_interval", cl.ReconcileInitialInterval, time.Second, time.Hour)
	within("execution.cleanup.reconcile_max_interval", cl.ReconcileMaxInterval, time.Second, time.Hour)
	within("execution.cleanup.reconcile_max_duration", cl.ReconcileMaxDuration, time.Minute, 7*24*time.Hour)
	req("health.listen", c.Health.Listen)
	if c.ShutdownTimeout < time.Second || c.ShutdownTimeout > 5*time.Minute {
		errs = append(errs, fmt.Errorf("shutdown_timeout %s outside [1s, 5m]", c.ShutdownTimeout))
	}
	// Cross-field rules: one observation of an unresolved create lasts at
	// least the create Activity's bound, so a request in flight at that
	// bound can land and be stopped within the same call; a cleanup round
	// must be able to wait out the settle window; reconciliation intervals
	// must be ordered and fit into the reconciliation bound.
	if cl.UnresolvedSettleWindow < e.ControlActivityTimeout {
		errs = append(errs, fmt.Errorf("execution.cleanup.unresolved_settle_window %s must be at least execution.control_activity_timeout %s (the create Activity's bound)", cl.UnresolvedSettleWindow, e.ControlActivityTimeout))
	}
	if cl.Timeout <= cl.UnresolvedSettleWindow {
		errs = append(errs, fmt.Errorf("execution.cleanup.timeout %s must exceed execution.cleanup.unresolved_settle_window %s", cl.Timeout, cl.UnresolvedSettleWindow))
	}
	if cl.ReconcileInitialInterval > cl.ReconcileMaxInterval {
		errs = append(errs, fmt.Errorf("execution.cleanup.reconcile_initial_interval %s exceeds reconcile_max_interval %s", cl.ReconcileInitialInterval, cl.ReconcileMaxInterval))
	}
	if cl.ReconcileMaxDuration <= cl.ReconcileMaxInterval+cl.Timeout {
		errs = append(errs, fmt.Errorf("execution.cleanup.reconcile_max_duration %s must exceed one reconciliation round (reconcile_max_interval + timeout)", cl.ReconcileMaxDuration))
	}
	if len(c.Development.LoseReceiptOnce) > 0 && !c.Development.Enabled {
		errs = append(errs, errors.New("development.lose_receipt_once requires development.enabled: true (DEVELOPMENT_ONLY fault injection)"))
	}
	if len(c.Development.HoldUntilCanceledOnce) > 0 && !c.Development.Enabled {
		errs = append(errs, errors.New("development.hold_until_canceled_once requires development.enabled: true (DEVELOPMENT_ONLY fault injection)"))
	}
	return errors.Join(errs...)
}
