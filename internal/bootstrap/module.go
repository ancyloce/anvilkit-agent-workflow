// Package bootstrap assembles the Workflow worker with Fx (A08): one Temporal
// client, two pollers in the same Deployment (business and reserved control
// queues), stable Workflow/Activity registration. Every address, queue and
// bound comes from the validated configuration snapshot.
package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
	controladapter "github.com/ancyloce/anvilkit-agent-workflow/internal/adapters/control"
	k8sadapter "github.com/ancyloce/anvilkit-agent-workflow/internal/adapters/kubernetes"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/config"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/workflows"
)

type workers struct {
	business worker.Worker
	control  worker.Worker
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
		// The stop hooks drain the pollers for up to shutdown_timeout (at most
		// 5m by validation); the container's own bound must not cut that short.
		fx.StopTimeout(6*time.Minute),
		fx.WithLogger(func(log *slog.Logger) fxevent.Logger { return &fxevent.SlogLogger{Logger: log} }),
		fx.Provide(
			config.Load,
			func() *slog.Logger {
				return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
			},
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
			func(cfg config.Config, ctl *controladapter.Client, l *k8sadapter.Launcher) *activities.Activities {
				return &activities.Activities{Control: ctl, Recovery: ctl, Launcher: l, Observer: cfg.Temporal.WorkerIdentity}
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
	} {
		business.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
	for name, fn := range map[string]any{
		activities.NameObserveLaunch: acts.ObserveLaunch, activities.NameDeleteJob: acts.DeleteJob, activities.NameCloseAttempt: acts.CloseAttempt,
		activities.NameEnumerateInventory: acts.EnumerateInventory, activities.NameListFindings: acts.ListFindings, activities.NameReconcileFinding: acts.ReconcileFinding,
		activities.NameRecordLaunchOutcome: acts.RecordLaunchOutcome, activities.NameEvaluateRecovery: acts.EvaluateRecovery,
	} {
		control.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
}

func newWorkers(cfg config.Config, c client.Client, acts *activities.Activities, log *slog.Logger) *workers {
	// A stopping worker drains its in-flight Activities for at most the
	// configured shutdown bound before it lets go of them (Temporal retries
	// them elsewhere under their command identities).
	opts := worker.Options{Identity: cfg.Temporal.WorkerIdentity + "@" + cfg.Temporal.BuildID, WorkerStopTimeout: cfg.ShutdownTimeout}
	if cfg.Development.Enabled && (len(cfg.Development.LoseReceiptOnce) > 0 || len(cfg.Development.HoldUntilCanceledOnce) > 0) {
		log.Warn("DEVELOPMENT_ONLY fault injection enabled", "loseReceiptOnce", cfg.Development.LoseReceiptOnce, "holdUntilCanceledOnce", cfg.Development.HoldUntilCanceledOnce)
		// The hold wraps the call (outermost) so a held result is withheld
		// before the lost-receipt fault decides whether to drop it.
		if len(cfg.Development.HoldUntilCanceledOnce) > 0 {
			opts.Interceptors = append(opts.Interceptors, NewHoldUntilCanceledOnce(cfg.Development.HoldUntilCanceledOnce, log))
		}
		if len(cfg.Development.LoseReceiptOnce) > 0 {
			opts.Interceptors = append(opts.Interceptors, NewLoseReceiptOnce(cfg.Development.LoseReceiptOnce, log))
		}
	}
	w := &workers{
		business: worker.New(c, cfg.Temporal.TaskQueue, opts),
		control:  worker.New(c, cfg.Temporal.ControlTaskQueue, opts),
	}
	Register(w.business, w.control, acts, workflows.Queues{Business: cfg.Temporal.TaskQueue, Control: cfg.Temporal.ControlTaskQueue}, Bounds(cfg.Execution))
	return w
}

// run orders the lifecycle: the health listener binds first, the two
// pollers start and only then the worker reports ready; on stop it reports
// not ready first, drains the pollers, closes the clients and ends the
// listener last, so probes observe the drain (DD-09 §3).
func run(lc fx.Lifecycle, cfg config.Config, w *workers, c client.Client, ctl *controladapter.Client, log *slog.Logger) {
	h := newHealth(cfg.Health.Listen)
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			addr, err := h.start()
			if err != nil {
				return fmt.Errorf("health listener %s: %w", cfg.Health.Listen, err)
			}
			if err := w.business.Start(); err != nil {
				return err
			}
			if err := w.control.Start(); err != nil {
				w.business.Stop()
				return err
			}
			h.ready.Store(true)
			log.Info("workflow worker polling", "namespace", cfg.Temporal.Namespace, "taskQueue", cfg.Temporal.TaskQueue, "controlTaskQueue", cfg.Temporal.ControlTaskQueue, "buildId", cfg.Temporal.BuildID, "health", addr.String())
			return nil
		},
		OnStop: func(ctx context.Context) error {
			h.ready.Store(false)
			w.business.Stop()
			w.control.Stop()
			c.Close()
			err := ctl.Close()
			if herr := h.stop(ctx); err == nil {
				err = herr
			}
			return err
		},
	})
}
