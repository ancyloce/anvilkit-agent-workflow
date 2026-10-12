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
	"net/url"
	"os"
	"regexp"
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
	// TLS is the frontend transport (P0.1): development is the plaintext
	// frontend of the development foundation (development.enabled only),
	// tls verifies the frontend against ca_file and server_name, mtls also
	// presents the workload certificate (cert_file/key_file, defaulting to
	// the process identity).
	TLS ClientTLS `koanf:"tls"`
	// Versioning opts both pollers into Worker Deployment Versioning
	// (P1.7): the worker is version build_id of deployment
	// deployment_name, and every Workflow is pinned to the version it
	// started on, so a run never replays on a build with other code. Only
	// a placement that keeps the previous version's workers until their
	// pinned runs drain may enable it (a plain rolling update would leave
	// those runs without a worker). promote (DEVELOPMENT_ONLY) makes this
	// version the deployment's current one once its pollers registered;
	// elsewhere promotion is a rollout step.
	Versioning Versioning `koanf:"versioning"`
}

type Versioning struct {
	Enabled        bool   `koanf:"enabled"`
	DeploymentName string `koanf:"deployment_name"`
	Promote        bool   `koanf:"promote"`
}

type Control struct {
	Address  string         `koanf:"address"`
	Identity ClientIdentity `koanf:"identity"`
}

// ClientIdentity is how one outbound gRPC or HTTP connection to an
// AnvilKit workload authenticates (P0.1): mtls presents the workload
// certificate (the files of MTLS, defaulting to the process identity) and
// verifies the server by server_name; development is plaintext and is
// admitted only with development.enabled.
type ClientIdentity struct {
	Mode string    `koanf:"mode"`
	MTLS MTLSFiles `koanf:"mtls"`
}

// MTLSFiles names certificate, key, CA bundle and the expected server name
// (a DNS SAN of the server certificate).
type MTLSFiles struct {
	CertFile   string `koanf:"cert_file"`
	KeyFile    string `koanf:"key_file"`
	CAFile     string `koanf:"ca_file"`
	ServerName string `koanf:"server_name"`
}

// Identity is the process's own workload identity material (P0.1):
// placements (ANVILKIT_WORKFLOW_IDENTITY_{CERT,KEY,CA}_FILE) every mtls
// client block falls back to; watched and reloaded as a whole.
type Identity struct {
	CertFile       string        `koanf:"cert_file"`
	KeyFile        string        `koanf:"key_file"`
	CAFile         string        `koanf:"ca_file"`
	ReloadInterval time.Duration `koanf:"reload_interval"`
}

// ClientTLS is the transport of an outbound connection whose server is not
// an AnvilKit workload (Temporal, the OTLP collector): development is
// plaintext (development.enabled only), tls verifies against ca_file and
// server_name, mtls additionally presents cert_file and key_file.
type ClientTLS struct {
	Mode       string `koanf:"mode"`
	CAFile     string `koanf:"ca_file"`
	CertFile   string `koanf:"cert_file"`
	KeyFile    string `koanf:"key_file"`
	ServerName string `koanf:"server_name"`
}

// ModelProxy is the Model Proxy placement of the controlled model call
// Activities (P11): the base URL, the request bound, and the identity the
// worker presents to the Proxy — development is a DEVELOPMENT_ONLY bearer
// token supplied only through ANVILKIT_WORKFLOW_MODEL_PROXY_TOKEN, mtls the
// workload certificate files. Without an address the Activities answer
// DEPENDENCY_UNAVAILABLE and reach no network.
type ModelProxy struct {
	Address  string         `koanf:"address"`
	Timeout  time.Duration  `koanf:"timeout"`
	Token    string         `koanf:"token"`
	Identity ClientIdentity `koanf:"identity"`
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
	// ClientQPS and ClientBurst are the launcher's client-go rate limit
	// (P1.6): an observed Job costs two reads per ObserveInterval, so 200
	// Jobs per replica at 1s need 400 reads/s; client-go's own default
	// (5/10) would stretch their cadence to over a minute. The default 500
	// leaves headroom; the API server's priority and fairness is the
	// placement's bound. ObserveInterval is that cadence.
	ClientQPS       float32       `koanf:"client_qps"`
	ClientBurst     int           `koanf:"client_burst"`
	ObserveInterval time.Duration `koanf:"observe_interval"`
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
	// IdentitySecret is the Secret of the sidecar's workload certificate in
	// the Job namespace (the Workflow chart's Certificate), mounted into the
	// access sidecar container alone under identity_mode mtls.
	IdentitySecret string `koanf:"identity_secret"`
	// P2.4: the placements the sidecar relays to (B-32): the Model Proxy
	// URL of the controlled model relay, the MCP CallService address of the
	// tool relay and the Knowledge RetrievalService address of the
	// retrieval specialist. Each is rendered into the sidecar's environment
	// only when set, under identity_mode mtls (the sidecar presents its own
	// workload certificate to each); empty answers DEPENDENCY_UNAVAILABLE.
	ModelProxyURL    string `koanf:"model_proxy_url"`
	MCPAddress       string `koanf:"mcp_address"`
	KnowledgeAddress string `koanf:"knowledge_address"`
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
	// CreateRequestLifetime is how long, after its create Activity ended
	// without an answer, a written create request can still commit: the API
	// server handles a request at most for its own request timeout
	// (kube-apiserver --request-timeout, 60s by default), and an etcd write
	// that did not commit before the next committed one never commits later.
	// Once a request's lifetime has ended, a cleanup call that starts
	// afterwards and confirms the launch key empty (a consistent read) is
	// evidence the request never committed. It must exceed the placement's
	// API server request timeout (a deployment input, ENV-01).
	CreateRequestLifetime time.Duration `koanf:"create_request_lifetime"`
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
	// ControlOutageWindow bounds how long an ordinary Control command is
	// retried while Control is unreachable (P1.6): a refusal ends at once,
	// a transient failure is retried within the window rather than for a
	// fixed attempt count, so a short outage is a retried success.
	ControlOutageWindow time.Duration `koanf:"control_outage_window"`
	// ControlReadWindow is the wider window of read-only commands.
	ControlReadWindow time.Duration `koanf:"control_read_window"`
	// ModelKeepalive is the Model Proxy's SSE keepalive period
	// (http.sse_heartbeat there); a model call heartbeats on every event
	// and its heartbeat timeout (observe_heartbeat_timeout) must be at
	// least twice it.
	ModelKeepalive time.Duration `koanf:"model_keepalive"`
	// MaxConcurrentActivities bounds the Activities one queue's worker runs
	// at once (worker.Options.MaxConcurrentActivityExecutionSize).
	MaxConcurrentActivities int `koanf:"max_concurrent_activities"`
	// Settle paces the settle, close and lease-release waits on Control.
	Settle Settle `koanf:"settle"`
}

// Settle paces the waits on Control's reconciliation (P1.6): capped
// exponential backoff from cleanup.reconcile_initial_interval to
// MaxInterval, and after PollBound polls in one run the settlement
// continues as new so a run's history stays bounded however long Control
// keeps the operation reconciling.
type Settle struct {
	MaxInterval time.Duration `koanf:"max_interval"`
	PollBound   int32         `koanf:"poll_bound"`
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
// process runs, /readyz while both pollers are started and neither the shutdown
// nor a fatal worker error has ended readiness.
// It is the only HTTP surface of the worker and serves no business route.
type Health struct {
	Listen string `koanf:"listen"`
}

// The supported range of shutdown_timeout, the worker's drain bound. The
// bootstrap's own stop ceiling and the chart's termination grace period are
// derived from it, so no accepted value outlives the process or the Pod.
const (
	MinShutdownTimeout = time.Second
	MaxShutdownTimeout = 5 * time.Minute
)

// Telemetry places the redacted signals (security.md "data classification,
// logging and deletion"): Temporal and Control-call spans over OTLP to the
// collector when an endpoint is placed (no exporter otherwise), sampled at
// SampleRatio; the SDK and process metrics are served on the health
// listener's /metrics. Neither enters a Workflow's history or its decisions.
type Telemetry struct {
	OTLPEndpoint string  `koanf:"otlp_endpoint"`
	SampleRatio  float64 `koanf:"sample_ratio"`
	// OTLPTLS is validated only while an endpoint is placed.
	OTLPTLS ClientTLS `koanf:"otlp_tls"`
}

type Config struct {
	Identity    Identity    `koanf:"identity"`
	Telemetry   Telemetry   `koanf:"telemetry"`
	Temporal    Temporal    `koanf:"temporal"`
	Control     Control     `koanf:"control"`
	ModelProxy  ModelProxy  `koanf:"model_proxy"`
	Kubernetes  Kubernetes  `koanf:"kubernetes"`
	Execution   Execution   `koanf:"execution"`
	Lifecycle   Lifecycle   `koanf:"lifecycle"`
	Development Development `koanf:"development"`
	Health      Health      `koanf:"health"`
	// ShutdownTimeout is the service-level drain bound: once the shutdown
	// begins, both Task Queues stop accepting work at once and their
	// in-flight Activities have this long, together, to finish before they
	// are abandoned to Temporal's retries; the clients and the health
	// listener close after it within the bootstrap's fixed allowance.
	ShutdownTimeout time.Duration `koanf:"shutdown_timeout"`
}

// Lifecycle bounds the P13 business workflows: the Preparation analysis
// call (route, output and exposure bounds, the Activity bound, the frozen
// content-repair allowance) and the Generation admission and lease
// supervision (the permit poll interval, the lease TTL asked of the port,
// the renewal lead, the lease call bound, the definition activations this
// worker registers) plus the placements of the trusted artifact port and
// the DEVELOPMENT_ONLY Pagix/Knowledge doubles.
type Lifecycle struct {
	Preparation struct {
		RouteID                string        `koanf:"route_id"`
		MaxOutputTokens        int           `koanf:"max_output_tokens"`
		MaxExposureCurrency    string        `koanf:"max_exposure_currency"`
		MaxExposureAmount      string        `koanf:"max_exposure_amount"`
		CallTimeout            time.Duration `koanf:"call_timeout"`
		ContentRepairAllowance int           `koanf:"content_repair_allowance"`
		MaxInputBytes          int64         `koanf:"max_input_bytes"`
	} `koanf:"preparation"`
	Generation struct {
		PermitPollInterval time.Duration `koanf:"permit_poll_interval"`
		LeaseTTL           time.Duration `koanf:"lease_ttl"`
		LeaseRenewLead     time.Duration `koanf:"lease_renew_lead"`
		LeaseCallTimeout   time.Duration `koanf:"lease_call_timeout"`
		Definitions        []Definition  `koanf:"definitions"`
	} `koanf:"generation"`
	Artifacts struct {
		TransferWindow time.Duration `koanf:"transfer_window"`
	} `koanf:"artifacts"`
	// Preview: the reviewed job profile that builds a preview's saved
	// revision (P20; DEVELOPMENT_ONLY: the fixed validator profile).
	Preview struct {
		BuildProfile string `koanf:"build_profile"`
	} `koanf:"preview"`
	// PagixDoubleDir is the directory of the DEVELOPMENT_ONLY lease,
	// source and content-digest doubles (ANVILKIT_WORKFLOW_PAGIX_DOUBLE_DIR);
	// empty leaves the Generation ports unavailable (every generation
	// fails at bootstrap with DEPENDENCY_UNAVAILABLE) until ENV-07.
	PagixDoubleDir string `koanf:"pagix_double_dir"`
	// HostAbiProfile is the Studio host ABI profile of the environment
	// (P2.2, ENV-08): host-abi-dev-v1 serves development only; a real
	// environment names the profile generated from its host declaration.
	HostAbiProfile string `koanf:"host_abi_profile"`
	// Pagix (P2.2) places the declaration-driven adapter of the lease,
	// source, review, publication and activation ports: the ENV-07 adapter
	// declaration file, the upstream base URL (https outside development,
	// ANVILKIT_WORKFLOW_PAGIX_URL) and the client credential (the workload
	// certificate under identity mtls, or the CSI token file under token).
	// Exclusive with pagix_double_dir.
	Pagix struct {
		DeclarationFile string        `koanf:"declaration_file"`
		BaseURL         string        `koanf:"base_url"`
		Timeout         time.Duration `koanf:"timeout"`
		Identity        string        `koanf:"identity"`
		TokenFile       string        `koanf:"token_file"`
		ServerName      string        `koanf:"server_name"`
	} `koanf:"pagix"`
	// Knowledge (P2.2) places the content-digest read of brief references
	// through Knowledge's SourceService (gRPC with the workload certificate,
	// ANVILKIT_WORKFLOW_KNOWLEDGE_ADDRESS).
	Knowledge struct {
		Address    string        `koanf:"address"`
		ServerName string        `koanf:"server_name"`
		Timeout    time.Duration `koanf:"timeout"`
	} `koanf:"knowledge"`
	// Release (P21): the reviewed validator profile that certifies the
	// exact source, the release profile's destinations (DEVELOPMENT_ONLY
	// until ENV-07/08 name the real registry and origin), the maintainer's
	// approval wait (bounded again by the operation deadline), its poll
	// interval, the polls one run makes before it continues as new, and the
	// bound on original-identity queries of an unknown target.
	Release struct {
		ValidatorProfile string        `koanf:"validator_profile"`
		NpmRegistry      string        `koanf:"npm_registry"`
		BrowserOrigin    string        `koanf:"browser_origin"`
		ApprovalWait     time.Duration `koanf:"approval_wait"`
		ApprovalPoll     time.Duration `koanf:"approval_poll"`
		PollsPerRun      int           `koanf:"polls_per_run"`
		ReconcileRounds  int           `koanf:"reconcile_rounds"`
		ReconcilePause   time.Duration `koanf:"reconcile_pause"`
		// OriginDir places the double's browser origin (the Studio's
		// DEVELOPMENT_ONLY release directory; ANVILKIT_WORKFLOW_RELEASE_ORIGIN_DIR);
		// empty keeps it under the double's directory.
		OriginDir string `koanf:"origin_dir"`
	} `koanf:"release"`
}

// Definition is one reviewed definition activation this worker registers
// for the Generation profile (its id must be one Control's profile lists).
type Definition struct {
	ID               string `koanf:"id"`
	CodegenProfile   string `koanf:"codegen_profile"`
	ValidatorProfile string `koanf:"validator_profile"`
	MaxRepairs       int64  `koanf:"max_repairs"`
}

var defaults = map[string]any{
	"lifecycle.preparation.route_id":                 "controlled-openai-v1",
	"lifecycle.preparation.max_output_tokens":        2048,
	"lifecycle.preparation.max_exposure_currency":    "USD",
	"lifecycle.preparation.max_exposure_amount":      "100000",
	"lifecycle.preparation.call_timeout":             "5m",
	"lifecycle.preparation.content_repair_allowance": 1,
	"lifecycle.preparation.max_input_bytes":          262144,
	"lifecycle.generation.permit_poll_interval":      "30s",
	"lifecycle.generation.lease_ttl":                 "10m",
	"lifecycle.generation.lease_renew_lead":          "3m",
	"lifecycle.generation.lease_call_timeout":        "20s",
	"lifecycle.generation.definitions":               []map[string]any{{"id": "generation-v1:def-1", "max_repairs": -1}, {"id": "generation-v1:def-2", "max_repairs": 0}},
	"lifecycle.artifacts.transfer_window":            "15m",
	"lifecycle.preview.build_profile":                "validator-source-v1",
	"lifecycle.release.validator_profile":            "validator-source-v1",
	"lifecycle.pagix.timeout":                        "30s",
	"lifecycle.host_abi_profile":                     "host-abi-dev-v1",
	"lifecycle.pagix.identity":                       "mtls",
	"lifecycle.pagix.server_name":                    "",
	"lifecycle.knowledge.server_name":                "anvilkit-agent-knowledge",
	"lifecycle.knowledge.timeout":                    "10s",
	"lifecycle.release.npm_registry":                 "https://registry.anvilkit.invalid/",
	"lifecycle.release.browser_origin":               "https://components.anvilkit.invalid",
	"lifecycle.release.approval_wait":                "336h",
	"lifecycle.release.approval_poll":                "30s",
	"lifecycle.release.polls_per_run":                200,
	"lifecycle.release.reconcile_rounds":             20,
	"lifecycle.release.reconcile_pause":              "30s",
	"temporal.namespace":                             "anvilkit",
	"temporal.task_queue":                            "anvilkit-workflow",
	"temporal.control_task_queue":                    "anvilkit-workflow-control",
	"temporal.worker_identity":                       "anvilkit-agent-workflow",
	"temporal.build_id":                              "dev",
	"temporal.versioning.enabled":                    false,
	"temporal.versioning.deployment_name":            "anvilkit-agent-workflow",
	"temporal.versioning.promote":                    false,
	"model_proxy.timeout":                            "5m",
	"model_proxy.identity.mode":                      "mtls",
	"control.identity.mode":                          "mtls",
	"temporal.tls.mode":                              "tls",
	"telemetry.otlp_tls.mode":                        "tls",
	"identity.reload_interval":                       "5s",
	"kubernetes.sidecar.identity_secret":             "anvilkit-job-access-sidecar-identity",
	"kubernetes.namespace":                           "anvilkit-components",
	"kubernetes.enabled_profiles":                    []string{"local-check-v1"},
	"kubernetes.sidecar.identity_mode":               "disabled",
	"kubernetes.candidate_seccomp_profile":           "anvilkit/candidate.json",
	"execution.control_activity_timeout":             "30s",
	"execution.control_retry_initial":                "1s",
	"execution.control_retry_max_interval":           "30s",
	"execution.control_retry_max_attempts":           6,
	"execution.launch_window":                        "2m",
	"execution.observe_heartbeat_timeout":            "30s",
	"execution.observe_max_attempts":                 3,
	"execution.cleanup.timeout":                      "3m",
	"execution.cleanup.max_attempts":                 2,
	"execution.cleanup.unresolved_settle_window":     "60s",
	"execution.cleanup.create_request_lifetime":      "3m",
	"execution.cleanup.reconcile_initial_interval":   "5s",
	"execution.cleanup.reconcile_max_interval":       "1m",
	"execution.cleanup.reconcile_max_duration":       "24h",
	"execution.control_outage_window":                "5m",
	"execution.control_read_window":                  "15m",
	"execution.model_keepalive":                      "15s",
	"execution.max_concurrent_activities":            200,
	"execution.settle.max_interval":                  "5m",
	"execution.settle.poll_bound":                    500,
	"kubernetes.client_qps":                          500,
	"kubernetes.client_burst":                        1000,
	"kubernetes.observe_interval":                    "1s",
	"development.enabled":                            false,
	"health.listen":                                  "127.0.0.1:9102",
	"telemetry.sample_ratio":                         1.0,
	"shutdown_timeout":                               "30s",
}

// envOverrides is the complete set of accepted environment variables:
// deployment placement only. Fault injection is never enabled from the
// environment.
var envOverrides = map[string]string{
	"ANVILKIT_WORKFLOW_TEMPORAL_ADDRESS":          "temporal.address",
	"ANVILKIT_WORKFLOW_CONTROL_ADDRESS":           "control.address",
	"ANVILKIT_WORKFLOW_MODEL_PROXY_ADDRESS":       "model_proxy.address",
	"ANVILKIT_WORKFLOW_MODEL_PROXY_TOKEN":         "model_proxy.token",
	"ANVILKIT_WORKFLOW_KUBECONFIG":                "kubernetes.kubeconfig",
	"ANVILKIT_WORKFLOW_LAUNCH_BACKEND":            "kubernetes.launch_backend",
	"ANVILKIT_WORKFLOW_BUILD_ID":                  "temporal.build_id",
	"ANVILKIT_WORKFLOW_IMAGE_REGISTRY":            "kubernetes.image_registry",
	"ANVILKIT_WORKFLOW_SIDECAR_CONTROL_ADDRESS":   "kubernetes.sidecar.control_address",
	"ANVILKIT_WORKFLOW_SIDECAR_MODEL_PROXY_URL":   "kubernetes.sidecar.model_proxy_url",
	"ANVILKIT_WORKFLOW_SIDECAR_MCP_ADDRESS":       "kubernetes.sidecar.mcp_address",
	"ANVILKIT_WORKFLOW_SIDECAR_KNOWLEDGE_ADDRESS": "kubernetes.sidecar.knowledge_address",
	"ANVILKIT_WORKFLOW_HEALTH_LISTEN":             "health.listen",
	"ANVILKIT_WORKFLOW_TELEMETRY_OTLP_ENDPOINT":   "telemetry.otlp_endpoint",
	"ANVILKIT_WORKFLOW_PAGIX_DOUBLE_DIR":          "lifecycle.pagix_double_dir",
	"ANVILKIT_WORKFLOW_RELEASE_ORIGIN_DIR":        "lifecycle.release.origin_dir",
	"ANVILKIT_WORKFLOW_PAGIX_URL":                 "lifecycle.pagix.base_url",
	"ANVILKIT_WORKFLOW_PAGIX_DECLARATION_FILE":    "lifecycle.pagix.declaration_file",
	"ANVILKIT_WORKFLOW_PAGIX_TOKEN_FILE":          "lifecycle.pagix.token_file",
	"ANVILKIT_WORKFLOW_KNOWLEDGE_ADDRESS":         "lifecycle.knowledge.address",
	"ANVILKIT_WORKFLOW_IDENTITY_CERT_FILE":        "identity.cert_file",
	"ANVILKIT_WORKFLOW_IDENTITY_KEY_FILE":         "identity.key_file",
	"ANVILKIT_WORKFLOW_IDENTITY_CA_FILE":          "identity.ca_file",
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
	if k.Exists("model_proxy.token") {
		return Config{}, fmt.Errorf("config file %s: model_proxy.token is a secret and is supplied only through ANVILKIT_WORKFLOW_MODEL_PROXY_TOKEN", path)
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

var (
	moneyPattern    = regexp.MustCompile(`^(0|[1-9][0-9]{0,29})$`)
	currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)
)

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
	if v := c.Temporal.Versioning; v.Enabled {
		req("temporal.versioning.deployment_name", v.DeploymentName)
		if v.Promote && !c.Development.Enabled {
			errs = append(errs, errors.New("temporal.versioning.promote is DEVELOPMENT_ONLY: it needs development.enabled"))
		}
	} else if v.Promote {
		errs = append(errs, errors.New("temporal.versioning.promote needs temporal.versioning.enabled"))
	}
	req("control.address", c.Control.Address)
	req("kubernetes.namespace", c.Kubernetes.Namespace)
	req("kubernetes.launch_backend", c.Kubernetes.LaunchBackend)
	switch c.Kubernetes.Sidecar.IdentityMode {
	case "disabled":
	case "development":
		if !c.Development.Enabled {
			errs = append(errs, errors.New("kubernetes.sidecar.identity_mode development (a plaintext sidecar) requires development.enabled: true (DEVELOPMENT_ONLY)"))
		}
	case "mtls":
		if c.Kubernetes.Sidecar.IdentitySecret == "" {
			errs = append(errs, errors.New("kubernetes.sidecar.identity_secret is required under identity_mode mtls"))
		}
		if u := c.Kubernetes.Sidecar.ModelProxyURL; u != "" {
			if parsed, err := url.Parse(u); err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil {
				errs = append(errs, errors.New("kubernetes.sidecar.model_proxy_url must be an https URL (the sidecar presents its workload certificate)"))
			}
		}
	default:
		errs = append(errs, fmt.Errorf("kubernetes.sidecar.identity_mode %q is not one of disabled, development, mtls", c.Kubernetes.Sidecar.IdentityMode))
	}
	errs = append(errs, c.validateIdentity()...)
	within := func(name string, v, lo, hi time.Duration) {
		if v < lo || v > hi {
			errs = append(errs, fmt.Errorf("%s %s outside [%s, %s]", name, v, lo, hi))
		}
	}
	mp := c.ModelProxy
	within("model_proxy.timeout", mp.Timeout, time.Second, time.Hour)
	switch mp.Identity.Mode {
	case "development":
		if mp.Address != "" && mp.Token == "" {
			errs = append(errs, errors.New("ANVILKIT_WORKFLOW_MODEL_PROXY_TOKEN (model_proxy.token) is required with a model_proxy.address under identity mode development"))
		}
		if mp.Address != "" && !c.Development.Enabled {
			errs = append(errs, errors.New("model_proxy.identity.mode development (plaintext bearer) requires development.enabled: true (DEVELOPMENT_ONLY)"))
		}
	case "mtls":
		m := c.Resolved(mp.Identity)
		if mp.Address != "" && (m.CertFile == "" || m.KeyFile == "" || m.CAFile == "") {
			errs = append(errs, errors.New("model_proxy.identity.mtls needs cert_file, key_file and ca_file, or the process identity (ANVILKIT_WORKFLOW_IDENTITY_{CERT,KEY,CA}_FILE)"))
		}
		if mp.Token != "" {
			errs = append(errs, errors.New("model_proxy.token is not used under identity mode mtls"))
		}
	default:
		errs = append(errs, fmt.Errorf("model_proxy.identity.mode %q is not one of development, mtls", mp.Identity.Mode))
	}
	if mp.Address != "" {
		if u, err := url.Parse(mp.Address); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
			errs = append(errs, errors.New("model_proxy.address must be an absolute http(s) URL without credentials"))
		}
	}
	if len(c.Kubernetes.EnabledProfiles) == 0 {
		errs = append(errs, errors.New("kubernetes.enabled_profiles must name at least one reviewed profile"))
	}
	if c.Temporal.TaskQueue == c.Temporal.ControlTaskQueue {
		errs = append(errs, errors.New("temporal.task_queue and temporal.control_task_queue must differ"))
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
	within("execution.cleanup.create_request_lifetime", cl.CreateRequestLifetime, 30*time.Second, time.Hour)
	within("execution.cleanup.reconcile_initial_interval", cl.ReconcileInitialInterval, time.Second, time.Hour)
	within("execution.cleanup.reconcile_max_interval", cl.ReconcileMaxInterval, time.Second, time.Hour)
	within("execution.cleanup.reconcile_max_duration", cl.ReconcileMaxDuration, time.Minute, 7*24*time.Hour)
	within("execution.control_outage_window", e.ControlOutageWindow, 30*time.Second, 24*time.Hour)
	within("execution.control_read_window", e.ControlReadWindow, 30*time.Second, 24*time.Hour)
	if e.ControlReadWindow < e.ControlOutageWindow {
		errs = append(errs, fmt.Errorf("execution.control_read_window %s must be at least execution.control_outage_window %s", e.ControlReadWindow, e.ControlOutageWindow))
	}
	within("execution.model_keepalive", e.ModelKeepalive, time.Second, 10*time.Minute)
	if e.ObserveHeartbeatTimeout < 2*e.ModelKeepalive {
		errs = append(errs, fmt.Errorf("execution.observe_heartbeat_timeout %s must be at least twice execution.model_keepalive %s (a model call heartbeats on keepalives)", e.ObserveHeartbeatTimeout, e.ModelKeepalive))
	}
	if e.MaxConcurrentActivities < 1 || e.MaxConcurrentActivities > 10000 {
		errs = append(errs, fmt.Errorf("execution.max_concurrent_activities %d outside [1, 10000]", e.MaxConcurrentActivities))
	}
	within("execution.settle.max_interval", e.Settle.MaxInterval, time.Second, time.Hour)
	if e.Settle.MaxInterval < cl.ReconcileInitialInterval {
		errs = append(errs, fmt.Errorf("execution.settle.max_interval %s must be at least execution.cleanup.reconcile_initial_interval %s", e.Settle.MaxInterval, cl.ReconcileInitialInterval))
	}
	if e.Settle.PollBound < 10 || e.Settle.PollBound > 1500 {
		errs = append(errs, fmt.Errorf("execution.settle.poll_bound %d outside [10, 1500] (a run's history stays under the event limit)", e.Settle.PollBound))
	}
	k := c.Kubernetes
	if k.ClientQPS <= 0 || k.ClientQPS > 5000 {
		errs = append(errs, fmt.Errorf("kubernetes.client_qps %v outside (0, 5000]", k.ClientQPS))
	}
	if k.ClientBurst < 1 || k.ClientBurst > 10000 || float32(k.ClientBurst) < k.ClientQPS {
		errs = append(errs, fmt.Errorf("kubernetes.client_burst %d outside [client_qps, 10000]", k.ClientBurst))
	}
	within("kubernetes.observe_interval", k.ObserveInterval, 100*time.Millisecond, time.Minute)
	req("health.listen", c.Health.Listen)
	if c.Telemetry.SampleRatio < 0 || c.Telemetry.SampleRatio > 1 {
		errs = append(errs, fmt.Errorf("telemetry.sample_ratio %v outside [0, 1]", c.Telemetry.SampleRatio))
	}
	within("shutdown_timeout", c.ShutdownTimeout, MinShutdownTimeout, MaxShutdownTimeout)
	lp, lg := c.Lifecycle.Preparation, c.Lifecycle.Generation
	req("lifecycle.preparation.route_id", lp.RouteID)
	req("lifecycle.preview.build_profile", c.Lifecycle.Preview.BuildProfile)
	if lp.MaxOutputTokens < 1 || lp.MaxOutputTokens > 1<<20 {
		errs = append(errs, fmt.Errorf("lifecycle.preparation.max_output_tokens %d outside [1, 1048576]", lp.MaxOutputTokens))
	}
	if !moneyPattern.MatchString(lp.MaxExposureAmount) || !currencyPattern.MatchString(lp.MaxExposureCurrency) {
		errs = append(errs, errors.New("lifecycle.preparation.max_exposure_currency/amount must be a currency and a canonical scale-6 amount"))
	}
	within("lifecycle.preparation.call_timeout", lp.CallTimeout, 10*time.Second, time.Hour)
	if lp.ContentRepairAllowance < 0 || lp.ContentRepairAllowance > 8 {
		errs = append(errs, fmt.Errorf("lifecycle.preparation.content_repair_allowance %d outside [0, 8]", lp.ContentRepairAllowance))
	}
	if lp.MaxInputBytes < 1024 || lp.MaxInputBytes > 16<<20 {
		errs = append(errs, fmt.Errorf("lifecycle.preparation.max_input_bytes %d outside [1024, 16777216]", lp.MaxInputBytes))
	}
	within("lifecycle.generation.permit_poll_interval", lg.PermitPollInterval, time.Second, time.Hour)
	within("lifecycle.generation.lease_ttl", lg.LeaseTTL, 30*time.Second, 24*time.Hour)
	within("lifecycle.generation.lease_renew_lead", lg.LeaseRenewLead, 5*time.Second, 12*time.Hour)
	within("lifecycle.generation.lease_call_timeout", lg.LeaseCallTimeout, time.Second, 10*time.Minute)
	if lg.LeaseRenewLead >= lg.LeaseTTL {
		errs = append(errs, fmt.Errorf("lifecycle.generation.lease_renew_lead %s must be shorter than lease_ttl %s", lg.LeaseRenewLead, lg.LeaseTTL))
	}
	if len(lg.Definitions) == 0 {
		errs = append(errs, errors.New("lifecycle.generation.definitions needs at least one activation"))
	}
	for i, d := range lg.Definitions {
		if d.ID == "" {
			errs = append(errs, fmt.Errorf("lifecycle.generation.definitions[%d] needs an id", i))
		}
	}
	lr := c.Lifecycle.Release
	req("lifecycle.release.validator_profile", lr.ValidatorProfile)
	for key, v := range map[string]string{"lifecycle.release.npm_registry": lr.NpmRegistry, "lifecycle.release.browser_origin": lr.BrowserOrigin} {
		u, err := url.Parse(v)
		if err != nil || u.Scheme != "https" || u.Host == "" {
			errs = append(errs, fmt.Errorf("%s %q must be an https URL", key, v))
		} else if strings.HasSuffix(u.Hostname(), ".invalid") && !c.Development.Enabled {
			// P2.2 AC6: the reserved .invalid names are the double's; a real
			// environment names its ENV-07/ENV-08 destinations.
			errs = append(errs, fmt.Errorf("%s %q is a DEVELOPMENT_ONLY placeholder; outside development the environment names its real destination (ENV-07/ENV-08)", key, v))
		}
	}
	errs = append(errs, c.validateLifecyclePorts()...)
	within("lifecycle.release.approval_wait", lr.ApprovalWait, time.Minute, 2160*time.Hour)
	within("lifecycle.release.approval_poll", lr.ApprovalPoll, time.Second, time.Hour)
	within("lifecycle.release.reconcile_pause", lr.ReconcilePause, time.Second, time.Hour)
	if lr.PollsPerRun < 10 || lr.PollsPerRun > 10000 {
		errs = append(errs, fmt.Errorf("lifecycle.release.polls_per_run %d outside [10, 10000]", lr.PollsPerRun))
	}
	if lr.ReconcileRounds < 1 || lr.ReconcileRounds > 1000 {
		errs = append(errs, fmt.Errorf("lifecycle.release.reconcile_rounds %d outside [1, 1000]", lr.ReconcileRounds))
	}
	within("lifecycle.artifacts.transfer_window", c.Lifecycle.Artifacts.TransferWindow, time.Minute, 24*time.Hour)
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
	if cl.CreateRequestLifetime >= cl.ReconcileMaxDuration {
		errs = append(errs, fmt.Errorf("execution.cleanup.create_request_lifetime %s must be shorter than execution.cleanup.reconcile_max_duration %s", cl.CreateRequestLifetime, cl.ReconcileMaxDuration))
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

var hostAbiPattern = regexp.MustCompile(`^host-abi-[a-z0-9][a-z0-9.-]{0,62}$`)

// validateLifecyclePorts checks the Pagix and Knowledge placements (P2.2):
// the filesystem double only under development.enabled and never beside a
// declared adapter; a declared adapter with an https base URL outside
// development and a credential.
func (c Config) validateLifecyclePorts() []error {
	var errs []error
	l := c.Lifecycle
	if l.PagixDoubleDir != "" && !c.Development.Enabled {
		errs = append(errs, errors.New("lifecycle.pagix_double_dir is the DEVELOPMENT_ONLY Pagix/Knowledge double: it requires development.enabled: true (no double serves a real environment)"))
	}
	switch {
	case !hostAbiPattern.MatchString(l.HostAbiProfile):
		errs = append(errs, fmt.Errorf("lifecycle.host_abi_profile %q is not a host-abi-<revision> profile id", l.HostAbiProfile))
	case l.HostAbiProfile == "host-abi-dev-v1" && !c.Development.Enabled:
		errs = append(errs, errors.New("lifecycle.host_abi_profile host-abi-dev-v1 is DEVELOPMENT_ONLY: a real environment names the profile of its Studio host declaration (ENV-08)"))
	}
	if l.PagixDoubleDir != "" && l.Pagix.DeclarationFile != "" {
		errs = append(errs, errors.New("lifecycle.pagix_double_dir and lifecycle.pagix.declaration_file are exclusive"))
	}
	p := l.Pagix
	if p.DeclarationFile != "" {
		u, err := url.Parse(p.BaseURL)
		switch {
		case p.BaseURL == "":
			errs = append(errs, errors.New("lifecycle.pagix.base_url (ANVILKIT_WORKFLOW_PAGIX_URL) is required with a declaration"))
		case err != nil || u.Host == "" || u.User != nil || (u.Scheme != "https" && u.Scheme != "http"):
			errs = append(errs, errors.New("lifecycle.pagix.base_url must be an absolute http(s) URL without credentials"))
		case u.Scheme == "http" && !c.Development.Enabled:
			errs = append(errs, errors.New("lifecycle.pagix.base_url must be https outside development"))
		}
		switch p.Identity {
		case "mtls":
			if c.Identity.CertFile == "" {
				errs = append(errs, errors.New("lifecycle.pagix.identity mtls presents the workload certificate: identity.cert_file is required"))
			}
		case "token":
			if p.TokenFile == "" {
				errs = append(errs, errors.New("lifecycle.pagix.token_file (ANVILKIT_WORKFLOW_PAGIX_TOKEN_FILE) is required under identity token"))
			}
		default:
			errs = append(errs, fmt.Errorf("lifecycle.pagix.identity %q is not one of mtls, token", p.Identity))
		}
		if p.Timeout < time.Second || p.Timeout > 5*time.Minute {
			errs = append(errs, fmt.Errorf("lifecycle.pagix.timeout %s outside [1s, 5m]", p.Timeout))
		}
	}
	if l.Knowledge.Address != "" && (l.Knowledge.Timeout < time.Second || l.Knowledge.Timeout > time.Minute) {
		errs = append(errs, fmt.Errorf("lifecycle.knowledge.timeout %s outside [1s, 1m]", l.Knowledge.Timeout))
	}
	return errs
}

// validateIdentity checks the Control client identity, the Temporal and
// collector transports and the process identity (P0.1).
func (c Config) validateIdentity() []error {
	var errs []error
	if c.Identity.ReloadInterval < 100*time.Millisecond || c.Identity.ReloadInterval > time.Hour {
		errs = append(errs, fmt.Errorf("identity.reload_interval %s outside [100ms, 1h]", c.Identity.ReloadInterval))
	}
	switch c.Control.Identity.Mode {
	case "mtls":
		m := c.Resolved(c.Control.Identity)
		if m.CertFile == "" || m.KeyFile == "" || m.CAFile == "" {
			errs = append(errs, errors.New("control.identity.mtls needs cert_file, key_file and ca_file, or the process identity (ANVILKIT_WORKFLOW_IDENTITY_{CERT,KEY,CA}_FILE)"))
		}
	case "development":
		if !c.Development.Enabled {
			errs = append(errs, errors.New("control.identity.mode development (plaintext) requires development.enabled: true (DEVELOPMENT_ONLY)"))
		}
	default:
		errs = append(errs, fmt.Errorf("control.identity.mode %q is not one of mtls, development", c.Control.Identity.Mode))
	}
	errs = append(errs, c.ResolvedTLS(c.Temporal.TLS).validate("temporal.tls", c.Development.Enabled)...)
	if c.Telemetry.OTLPEndpoint != "" {
		errs = append(errs, c.ResolvedTLS(c.Telemetry.OTLPTLS).validate("telemetry.otlp_tls", c.Development.Enabled)...)
	}
	return errs
}

// Resolved fills a client's mtls files from the process identity.
func (c Config) Resolved(ci ClientIdentity) MTLSFiles {
	m := ci.MTLS
	if m.CertFile == "" {
		m.CertFile = c.Identity.CertFile
	}
	if m.KeyFile == "" {
		m.KeyFile = c.Identity.KeyFile
	}
	if m.CAFile == "" {
		m.CAFile = c.Identity.CAFile
	}
	return m
}

// ResolvedTLS fills an mtls transport's certificate and key from the
// process identity (its CA bundle is the target's, never the workload's).
func (c Config) ResolvedTLS(t ClientTLS) ClientTLS {
	if t.Mode == "mtls" {
		if t.CertFile == "" {
			t.CertFile = c.Identity.CertFile
		}
		if t.KeyFile == "" {
			t.KeyFile = c.Identity.KeyFile
		}
	}
	return t
}

func (t ClientTLS) validate(name string, development bool) []error {
	var errs []error
	switch t.Mode {
	case "development":
		if !development {
			errs = append(errs, fmt.Errorf("%s.mode development (plaintext) requires development.enabled: true (DEVELOPMENT_ONLY)", name))
		}
	case "tls":
		if t.CAFile == "" {
			errs = append(errs, fmt.Errorf("%s.ca_file is required under %s.mode tls", name, name))
		}
	case "mtls":
		if t.CAFile == "" || t.CertFile == "" || t.KeyFile == "" {
			errs = append(errs, fmt.Errorf("%s.ca_file, cert_file and key_file are required under %s.mode mtls", name, name))
		}
	default:
		errs = append(errs, fmt.Errorf("%s.mode %q is not one of development, tls, mtls", name, t.Mode))
	}
	return errs
}
