package bootstrap

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"
)

// LoseReceiptOnce is a DEVELOPMENT_ONLY worker interceptor (Temporal SDK
// interceptor API) for the lost-completion-receipt scenario of delivery.md
// P05: the first successful execution of each named Activity runs to
// completion (the durable Control command or backend call really happens)
// but its result is not reported; Temporal retries the Activity under the
// same command identity and Control answers with the original record. It
// is enabled only by the configuration file's development section.
type LoseReceiptOnce struct {
	interceptor.WorkerInterceptorBase
	names map[string]bool
	fired sync.Map
	log   *slog.Logger
}

func NewLoseReceiptOnce(names []string, log *slog.Logger) *LoseReceiptOnce {
	set := make(map[string]bool, len(names))
	for _, n := range names {
		set[n] = true
	}
	return &LoseReceiptOnce{names: set, log: log}
}

func (f *LoseReceiptOnce) InterceptActivity(_ context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	return &loseReceiptInbound{ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next}, fault: f}
}

type loseReceiptInbound struct {
	interceptor.ActivityInboundInterceptorBase
	fault *LoseReceiptOnce
}

func (a *loseReceiptInbound) ExecuteActivity(ctx context.Context, in *interceptor.ExecuteActivityInput) (any, error) {
	out, err := a.Next.ExecuteActivity(ctx, in)
	name := activity.GetInfo(ctx).ActivityType.Name
	if err == nil && a.fault.names[name] {
		if _, already := a.fault.fired.LoadOrStore(name, struct{}{}); !already {
			a.fault.log.Warn("DEVELOPMENT_ONLY fault: completion receipt dropped once", "activity", name, "attempt", activity.GetInfo(ctx).Attempt)
			return nil, errors.New("injected fault: completion receipt lost after the call succeeded")
		}
	}
	return out, err
}

// HoldUntilCanceledOnce is a DEVELOPMENT_ONLY worker interceptor for the
// unresolved-create scenario of delivery.md P05: the first successful
// execution of each named Activity runs to completion (the backend create
// really happens) but its result is withheld and the Activity keeps
// heartbeating until the Workflow cancels it, so the caller sees a canceled
// request whose create landed anyway. It is enabled only by the
// configuration file's development section.
type HoldUntilCanceledOnce struct {
	interceptor.WorkerInterceptorBase
	names map[string]bool
	fired sync.Map
	log   *slog.Logger
}

func NewHoldUntilCanceledOnce(names []string, log *slog.Logger) *HoldUntilCanceledOnce {
	set := make(map[string]bool, len(names))
	for _, n := range names {
		set[n] = true
	}
	return &HoldUntilCanceledOnce{names: set, log: log}
}

func (f *HoldUntilCanceledOnce) InterceptActivity(_ context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	return &holdInbound{ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next}, fault: f}
}

type holdInbound struct {
	interceptor.ActivityInboundInterceptorBase
	fault *HoldUntilCanceledOnce
}

func (a *holdInbound) ExecuteActivity(ctx context.Context, in *interceptor.ExecuteActivityInput) (any, error) {
	out, err := a.Next.ExecuteActivity(ctx, in)
	name := activity.GetInfo(ctx).ActivityType.Name
	if err == nil && a.fault.names[name] {
		if _, already := a.fault.fired.LoadOrStore(name, struct{}{}); !already {
			a.fault.log.Warn("DEVELOPMENT_ONLY fault: result withheld until the Activity is canceled", "activity", name, "attempt", activity.GetInfo(ctx).Attempt)
			for {
				activity.RecordHeartbeat(ctx)
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(time.Second):
				}
			}
		}
	}
	return out, err
}
