// Package bootstrap assembles the Workflow worker with Fx (A08): one Temporal
// client, two pollers in the same Deployment (business and reserved control
// queues), stable Workflow/Activity registration. Every address, queue and
// bound comes from the validated configuration snapshot.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
	controladapter "github.com/ancyloce/anvilkit-agent-workflow/internal/adapters/control"
	k8sadapter "github.com/ancyloce/anvilkit-agent-workflow/internal/adapters/kubernetes"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/adapters/modelproxy"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/config"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/workflows"
)

// The shutdown of the worker is bounded as a whole (DD-09 §3): the drain
// bound shutdown_timeout covers both pollers together, closeAllowance is
// what closing the clients and the health listener may take after it, and
// the container's stop ceiling covers the largest configurable drain. The
// chart's termination grace period is derived the same way (drain plus
// the close allowance and a kill margin), so no supported value outlives
// the Pod.
const (
	closeAllowance = 10 * time.Second
	// reportGrace is the part of the allowance the abandoned Activities get
	// to report their failure before the clients close, so Temporal retries
	// them at once rather than at their heartbeat or start-to-close bound.
	reportGrace = 2 * time.Second
	stopCeiling = config.MaxShutdownTimeout + closeAllowance + 5*time.Second
)

// errDrainBoundElapsed is the cause the in-flight Activities see when the
// drain bound ends before they finish: they are abandoned to Temporal,
// which retries them elsewhere under their command identities.
var errDrainBoundElapsed = errors.New("worker drain bound elapsed")

// queueWorker is one Task Queue's poller with its stop serialized: the
// SDK's Stop is not safe to enter twice concurrently, and it is entered
// from two places - the shutdown hook and, after a fatal poll error, the
// fatal callback on the poller goroutine (the SDK calls Stop itself right
// after that callback and finds the worker already stopped).
type queueWorker struct {
	worker.Worker
	name string
	mu   sync.Mutex
	// stopped records that Stop ran (or is running under mu).
	stopped bool
}

func (q *queueWorker) stop() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return
	}
	q.stopped = true
	q.Worker.Stop()
}

type workers struct {
	business *queueWorker
	control  *queueWorker
	// drain is the service-level drain bound (shutdown_timeout).
	drain time.Duration
	// activities is the root of every Activity context of both pollers;
	// abandon cancels it when the drain bound ends.
	activities context.Context
	abandon    context.CancelCauseFunc
	health     *health
	shutdowner fx.Shutdowner
	log        *slog.Logger
	// closers close the Temporal and Control clients after the drain.
	closers []func() error
}

// Bounds maps the validated execution section onto the Workflow's frozen
// bounds.
func Bounds(e config.Execution) workflows.Bounds {
	return workflows.Bounds{
		ControlActivityTimeout: e.ControlActivityTimeout, ControlRetryInitial: e.ControlRetryInitial, ControlRetryMaxInterval: e.ControlRetryMaxInterval,
		ControlRetryMaxAttempts: e.ControlRetryMaxAttempts, LaunchWindow: e.LaunchWindow, ObserveHeartbeatTimeout: e.ObserveHeartbeatTimeout,
		ObserveMaxAttempts: e.ObserveMaxAttempts, CleanupTimeout: e.Cleanup.Timeout, CleanupMaxAttempts: e.Cleanup.MaxAttempts,
		UnresolvedSettleWindow: e.Cleanup.UnresolvedSettleWindow, ReconcileInitialInterval: e.Cleanup.ReconcileInitialInterval,
		ReconcileMaxInterval: e.Cleanup.ReconcileMaxInterval, ReconcileMaxDuration: e.Cleanup.ReconcileMaxDuration,
	}
}

func Module() fx.Option {
	return fx.Options(
		// The stop hook drains for at most shutdown_timeout and closes within
		// closeAllowance; the container's ceiling covers the largest
		// configurable drain so it never cuts a supported bound short.
		fx.StopTimeout(stopCeiling),
		fx.WithLogger(func(log *slog.Logger) fxevent.Logger { return &fxevent.SlogLogger{Logger: log} }),
		fx.Provide(
			config.Load,
			func() *slog.Logger {
				return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
			},
			func(cfg config.Config) *health { return newHealth(cfg.Health.Listen) },
			func(cfg config.Config) (client.Client, error) {
				return client.Dial(client.Options{HostPort: cfg.Temporal.Address, Namespace: cfg.Temporal.Namespace, Identity: cfg.Temporal.WorkerIdentity + "@" + cfg.Temporal.BuildID})
			},
			func(cfg config.Config) (*controladapter.Client, error) {
				return controladapter.Dial(cfg.Control.Address, cfg.Kubernetes.LaunchBackend, cfg.Temporal.WorkerIdentity)
			},
			func(cfg config.Config) (*k8sadapter.Launcher, error) {
				return k8sadapter.New(cfg.Kubernetes.Kubeconfig, cfg.Kubernetes.Namespace, k8sadapter.Options{
					Backend: cfg.Kubernetes.LaunchBackend, ImageRegistry: cfg.Kubernetes.ImageRegistry, EnabledProfiles: cfg.Kubernetes.EnabledProfiles,
					SidecarControlAddress: cfg.Kubernetes.Sidecar.ControlAddress, SidecarIdentityMode: cfg.Kubernetes.Sidecar.IdentityMode,
					CandidateSeccompProfile: cfg.Kubernetes.CandidateSeccompProfile,
				})
			},
			func(cfg config.Config, log *slog.Logger) (activities.ModelCaller, error) {
				mp := cfg.ModelProxy
				if mp.Address == "" {
					log.Warn("model proxy address not configured; the model call Activities answer DEPENDENCY_UNAVAILABLE")
					return modelproxy.Unavailable(), nil
				}
				o := modelproxy.Options{BaseURL: mp.Address, Timeout: mp.Timeout}
				if mp.Identity.Mode == "mtls" {
					m := mp.Identity.MTLS
					o.TLS = &modelproxy.TLSFiles{CertFile: m.CertFile, KeyFile: m.KeyFile, CAFile: m.CAFile, ServerName: m.ServerName}
				} else {
					log.Warn("DEVELOPMENT_ONLY model proxy identity: bearer token; qualifies no production identity")
					o.Token = mp.Token
				}
				return modelproxy.New(o)
			},
			func(cfg config.Config, ctl *controladapter.Client, l *k8sadapter.Launcher, m activities.ModelCaller) *activities.Activities {
				return &activities.Activities{Control: ctl, Recovery: ctl, Launcher: l, Model: m, Observer: cfg.Temporal.WorkerIdentity}
			},
			newWorkers,
		),
		fx.Invoke(run),
	)
}

// Register binds the stable names to the implementations on their queues.
func Register(business, control worker.Worker, acts *activities.Activities, q workflows.Queues, b workflows.Bounds) {
	business.RegisterWorkflowWithOptions(workflows.LocalCheck(q, b), workflow.RegisterOptions{Name: workflows.LocalCheckWorkflowName})
	business.RegisterWorkflowWithOptions(workflows.Recovery(q, b), workflow.RegisterOptions{Name: workflows.RecoveryWorkflowName})
	for name, fn := range map[string]any{
		activities.NameOpenAttempt: acts.OpenAttempt, activities.NamePrepareLaunch: acts.PrepareLaunch, activities.NameCreateJob: acts.CreateJob,
		activities.NameObserveJob: acts.ObserveJob, activities.NameRegisterInstance: acts.RegisterInstance, activities.NameObserveInstance: acts.ObserveInstance,
		activities.NameVerifyResult: acts.VerifyResult,
		activities.NameAcceptResult: acts.AcceptResult,
		activities.NameCallModel:    acts.CallModel, activities.NameGetModelCall: acts.GetModelCall,
	} {
		business.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
	for name, fn := range map[string]any{
		activities.NameObserveLaunch: acts.ObserveLaunch, activities.NameDeleteJob: acts.DeleteJob, activities.NameCloseAttempt: acts.CloseAttempt,
		activities.NameCancelModelCall:    acts.CancelModelCall,
		activities.NameEnumerateInventory: acts.EnumerateInventory, activities.NameListFindings: acts.ListFindings, activities.NameReconcileFinding: acts.ReconcileFinding,
		activities.NameRecordLaunchOutcome: acts.RecordLaunchOutcome, activities.NameEvaluateRecovery: acts.EvaluateRecovery,
	} {
		control.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
}

// newLifecycle is the part of the worker set that the pollers and the tests
// share: the drain bound, the shared Activity root context, the readiness
// state and the shutdown request.
func newLifecycle(drain time.Duration, h *health, sd fx.Shutdowner, log *slog.Logger) *workers {
	w := &workers{drain: drain, health: h, shutdowner: sd, log: log}
	w.activities, w.abandon = context.WithCancelCause(context.Background())
	return w
}

// options are one poller's SDK options: the identity, the shared Activity
// root, the SDK's own per-worker stop wait aligned with the drain bound,
// and the fatal callback for this queue.
func (w *workers) options(identity string, q *queueWorker) worker.Options {
	return worker.Options{
		Identity:                  identity,
		BackgroundActivityContext: w.activities,
		// The SDK waits this long for each of the poller's internal workers
		// in turn, so an Activity abandoned at the drain bound can still
		// report within the grace; the service-level stop bounds the sum.
		WorkerStopTimeout: w.drain + reportGrace,
		OnFatalError:      func(err error) { w.fatal(q, err) },
	}
}

// fatal is the SDK's report that a poller received a non-retriable error
// after its own retries and is shutting itself down (a transient dependency
// error never reaches here). It withdraws readiness for good, asks Fx to
// stop the process with exit code 1 (once, for the first fatal error) and
// stops the failed queue under its serialized stop, so the SDK's own Stop
// that follows this callback finds nothing left to do. Startup and shutdown
// races resolve through the readiness state: a start that finishes after
// this never reports ready, and a shutdown already in progress simply
// keeps going.
func (w *workers) fatal(q *queueWorker, err error) {
	first := w.health.fail(err)
	w.log.Error("workflow worker terminated fatally", "queueWorker", q.name, "error", err, "first", first)
	if first && w.shutdowner != nil {
		if serr := w.shutdowner.Shutdown(fx.ExitCode(1)); serr != nil {
			w.log.Warn("shutdown request after the fatal error", "error", serr)
		}
	}
	q.stop()
}

func newWorkers(cfg config.Config, c client.Client, acts *activities.Activities, log *slog.Logger, h *health, sd fx.Shutdowner) *workers {
	w := newLifecycle(cfg.ShutdownTimeout, h, sd, log)
	w.business = &queueWorker{name: cfg.Temporal.TaskQueue}
	w.control = &queueWorker{name: cfg.Temporal.ControlTaskQueue}
	identity := cfg.Temporal.WorkerIdentity + "@" + cfg.Temporal.BuildID
	businessOpts, controlOpts := w.options(identity, w.business), w.options(identity, w.control)
	if cfg.Development.Enabled && (len(cfg.Development.LoseReceiptOnce) > 0 || len(cfg.Development.HoldUntilCanceledOnce) > 0) {
		log.Warn("DEVELOPMENT_ONLY fault injection enabled", "loseReceiptOnce", cfg.Development.LoseReceiptOnce, "holdUntilCanceledOnce", cfg.Development.HoldUntilCanceledOnce)
		// The hold wraps the call (outermost) so a held result is withheld
		// before the lost-receipt fault decides whether to drop it.
		var faults []interceptor.WorkerInterceptor
		if len(cfg.Development.HoldUntilCanceledOnce) > 0 {
			faults = append(faults, NewHoldUntilCanceledOnce(cfg.Development.HoldUntilCanceledOnce, log))
		}
		if len(cfg.Development.LoseReceiptOnce) > 0 {
			faults = append(faults, NewLoseReceiptOnce(cfg.Development.LoseReceiptOnce, log))
		}
		businessOpts.Interceptors, controlOpts.Interceptors = faults, faults
	}
	w.business.Worker = worker.New(c, cfg.Temporal.TaskQueue, businessOpts)
	w.control.Worker = worker.New(c, cfg.Temporal.ControlTaskQueue, controlOpts)
	Register(w.business, w.control, acts, workflows.Queues{Business: cfg.Temporal.TaskQueue, Control: cfg.Temporal.ControlTaskQueue}, Bounds(cfg.Execution))
	return w
}

// start binds the health listener first, starts the two pollers and only
// then reports ready - unless a fatal error came first, in which case the
// worker stays not ready and the shutdown the fatal callback requested ends
// the process with its exit code. A failed start leaves nothing running.
func (w *workers) start() (net.Addr, error) {
	h := w.health
	addr, err := h.start()
	if err != nil {
		return nil, fmt.Errorf("health listener %s: %w", h.srv.Addr, err)
	}
	if err := w.business.Start(); err != nil {
		_ = h.stop(context.Background())
		return nil, fmt.Errorf("business queue %s: %w", w.business.name, err)
	}
	if err := w.control.Start(); err != nil {
		w.business.stop()
		_ = h.stop(context.Background())
		return nil, fmt.Errorf("control queue %s: %w", w.control.name, err)
	}
	if err := h.becomeReady(); err != nil {
		w.log.Error("workflow worker started but is not ready", "error", err)
	}
	return addr, nil
}

// stop is the bounded shutdown: readiness is withdrawn first, both queues
// stop accepting work at once and drain concurrently; when the drain bound
// (or the container's ceiling) ends first, the in-flight Activities are
// abandoned - their contexts are canceled with errDrainBoundElapsed, they
// get reportGrace to report, and Temporal retries them elsewhere under
// their command identities. The clients and the health listener close
// afterwards within closeAllowance, whether or not the SDK's own stop
// calls have returned.
func (w *workers) stop(ctx context.Context) error {
	w.health.withdraw()
	started := time.Now()
	drained := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for _, q := range []*queueWorker{w.business, w.control} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				q.stop()
			}()
		}
		wg.Wait()
		close(drained)
	}()
	bound := time.NewTimer(w.drain)
	defer bound.Stop()
	abandoned := false
	select {
	case <-drained:
		w.log.Info("workflow worker drained", "elapsed", time.Since(started).String())
	case <-bound.C:
		abandoned = true
		w.abandon(errDrainBoundElapsed)
		w.log.Warn("workflow worker drain bound elapsed; in-flight Activities abandoned to Temporal's retries", "drain", w.drain.String())
	case <-ctx.Done():
		abandoned = true
		w.abandon(ctx.Err())
		w.log.Warn("workflow worker stop context ended before the drain bound; in-flight Activities abandoned to Temporal's retries", "error", ctx.Err())
	}
	closeCtx, cancel := context.WithTimeout(ctx, closeAllowance)
	defer cancel()
	if abandoned {
		select {
		case <-drained:
		case <-time.After(reportGrace):
		case <-closeCtx.Done():
		}
	}
	var errs []error
	for _, closeClient := range w.closers {
		if err := closeClient(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := w.health.stop(closeCtx); err != nil {
		errs = append(errs, err)
	}
	// The stop ceiling covered the drain and the close; whatever the SDK
	// still waits for ends with the process.
	return errors.Join(errs...)
}

// run orders the lifecycle: the health listener binds first, the two
// pollers start and only then the worker reports ready; on stop it reports
// not ready first, drains both pollers under the one drain bound, closes
// the clients and ends the listener last, so probes observe the drain
// (DD-09 §3). A fatal poller error withdraws readiness and ends the
// process through the same stop with exit code 1.
func run(lc fx.Lifecycle, cfg config.Config, w *workers, c client.Client, ctl *controladapter.Client) {
	w.closers = []func() error{func() error { c.Close(); return nil }, ctl.Close}
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			addr, err := w.start()
			if err != nil {
				return err
			}
			w.log.Info("workflow worker polling", "namespace", cfg.Temporal.Namespace, "taskQueue", cfg.Temporal.TaskQueue, "controlTaskQueue", cfg.Temporal.ControlTaskQueue,
				"buildId", cfg.Temporal.BuildID, "health", addr.String(), "ready", w.health.isReady(), "drain", cfg.ShutdownTimeout.String())
			return nil
		},
		OnStop: w.stop,
	})
}
