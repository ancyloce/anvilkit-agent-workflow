package bootstrap

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/worker"
	"go.uber.org/fx"

	"github.com/ancyloce/anvilkit-agent-workflow/internal/config"
)

// fakeWorker stands in for one SDK poller: the lifecycle only starts and
// stops it. Stop blocks in hold (an in-flight Activity that ends only when
// its context is canceled) when one is set.
type fakeWorker struct {
	worker.Worker
	onStart func() error
	hold    context.Context
	starts  atomic.Int32
	stops   atomic.Int32
	entered chan struct{}
	done    chan struct{}
	once    sync.Once
}

func newFakeWorker() *fakeWorker {
	return &fakeWorker{entered: make(chan struct{}), done: make(chan struct{})}
}

func (f *fakeWorker) Start() error {
	f.starts.Add(1)
	if f.onStart != nil {
		return f.onStart()
	}
	return nil
}

func (f *fakeWorker) Stop() {
	f.stops.Add(1)
	f.once.Do(func() { close(f.entered) })
	if f.hold != nil {
		<-f.hold.Done()
	}
	close(f.done)
}

type recordingShutdowner struct{ calls atomic.Int32 }

func (r *recordingShutdowner) Shutdown(...fx.ShutdownOption) error {
	r.calls.Add(1)
	return nil
}

var errPollerFatal = errors.New("poller: namespace not found")

func newTestLifecycle(t *testing.T, drain time.Duration, business, control *fakeWorker) (*workers, *recordingShutdowner) {
	t.Helper()
	sd := &recordingShutdowner{}
	w := newLifecycle(drain, newHealth("127.0.0.1:0"), sd, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.business = &queueWorker{Worker: business, name: "business"}
	w.control = &queueWorker{Worker: control, name: "control"}
	return w, sd
}

func readyStatus(t *testing.T, addr string) int {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/readyz")
	require.NoError(t, err)
	defer resp.Body.Close()
	return resp.StatusCode
}

func TestLifecycleStartsReadyAndStopsBothQueuesTogether(t *testing.T) {
	business, control := newFakeWorker(), newFakeWorker()
	w, sd := newTestLifecycle(t, time.Second, business, control)
	closed := atomic.Int32{}
	w.closers = []func() error{func() error { closed.Add(1); return nil }}

	addr, err := w.start()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, readyStatus(t, addr.String()))
	require.EqualValues(t, 1, business.starts.Load())
	require.EqualValues(t, 1, control.starts.Load())

	started := time.Now()
	require.NoError(t, w.stop(context.Background()))
	require.Less(t, time.Since(started), time.Second, "nothing in flight: the stop must not wait for the drain bound")
	require.EqualValues(t, 1, business.stops.Load())
	require.EqualValues(t, 1, control.stops.Load())
	require.EqualValues(t, 1, closed.Load())
	require.Zero(t, sd.calls.Load(), "a normal stop requests no shutdown")
	require.Nil(t, w.health.fatalError())
	require.NoError(t, context.Cause(w.activities), "activities that finished within the bound were never abandoned")
	_, err = http.Get("http://" + addr.String() + "/readyz")
	require.Error(t, err, "the listener ends last")
}

func TestLifecycleDrainBoundCoversBothQueues(t *testing.T) {
	// Both pollers hold an Activity that ends only when it is abandoned:
	// the stop of the second queue must not wait for the first to drain
	// (sequential drains would take twice the bound) and the whole stop
	// stays within the bound plus the close allowance.
	business, control := newFakeWorker(), newFakeWorker()
	w, _ := newTestLifecycle(t, 300*time.Millisecond, business, control)
	business.hold, control.hold = w.activities, w.activities
	_, err := w.start()
	require.NoError(t, err)

	started := time.Now()
	require.NoError(t, w.stop(context.Background()))
	elapsed := time.Since(started)
	require.GreaterOrEqual(t, elapsed, 300*time.Millisecond)
	require.Less(t, elapsed, 600*time.Millisecond, "the second queue waited for the first: %s", elapsed)
	require.ErrorIs(t, context.Cause(w.activities), errDrainBoundElapsed)
	<-business.done
	<-control.done
	require.EqualValues(t, 1, business.stops.Load())
	require.EqualValues(t, 1, control.stops.Load())
}

func TestLifecycleStopContextEndsTheDrainEarly(t *testing.T) {
	business, control := newFakeWorker(), newFakeWorker()
	w, _ := newTestLifecycle(t, time.Minute, business, control)
	business.hold = w.activities
	_, err := w.start()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	_ = w.stop(ctx)
	require.Less(t, time.Since(started), time.Second)
	require.ErrorIs(t, context.Cause(w.activities), context.DeadlineExceeded)
	<-business.done
}

func TestLifecycleFatalAfterReadyWithdrawsReadinessAndRequestsExit(t *testing.T) {
	business, control := newFakeWorker(), newFakeWorker()
	w, sd := newTestLifecycle(t, time.Second, business, control)
	addr, err := w.start()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, readyStatus(t, addr.String()))

	// The SDK's callback for the business queue, on its poller goroutine;
	// it stops that queue before the SDK's own Stop would.
	w.fatal(w.business, errPollerFatal)
	require.Equal(t, http.StatusServiceUnavailable, readyStatus(t, addr.String()))
	require.EqualValues(t, 1, sd.calls.Load(), "one shutdown request with the error exit code")
	require.EqualValues(t, 1, business.stops.Load())
	require.Zero(t, control.stops.Load(), "the other queue drains in the shutdown hook")
	require.ErrorIs(t, w.health.fatalError(), errPollerFatal)

	// A second fatal error (the other queue) changes nothing.
	w.fatal(w.control, errors.New("second"))
	require.EqualValues(t, 1, sd.calls.Load())
	require.ErrorIs(t, w.health.fatalError(), errPollerFatal)
	require.ErrorIs(t, w.health.becomeReady(), errPollerFatal, "readiness can never be granted again")

	// The shutdown hook Fx runs on the request: no queue is stopped twice.
	require.NoError(t, w.stop(context.Background()))
	require.EqualValues(t, 1, business.stops.Load())
	require.EqualValues(t, 1, control.stops.Load())
}

func TestLifecycleFatalDuringStartupNeverReportsReady(t *testing.T) {
	// The business poller fails fatally while the control queue is still
	// starting: the start completes without readiness, the exit was
	// requested, and the stop that follows drains what runs.
	business, control := newFakeWorker(), newFakeWorker()
	w, sd := newTestLifecycle(t, time.Second, business, control)
	control.onStart = func() error {
		w.fatal(w.business, errPollerFatal)
		return nil
	}
	addr, err := w.start()
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, readyStatus(t, addr.String()))
	require.False(t, w.health.isReady())
	require.EqualValues(t, 1, sd.calls.Load())
	require.ErrorIs(t, w.health.becomeReady(), errPollerFatal)
	require.NoError(t, w.stop(context.Background()))
	require.EqualValues(t, 1, business.stops.Load())
	require.EqualValues(t, 1, control.stops.Load())
}

func TestLifecycleFatalDuringStopIsSerializedWithTheDrain(t *testing.T) {
	// The shutdown is draining the business queue when its poller reports
	// a fatal error: the callback's stop waits for the drain in progress
	// instead of entering the SDK's Stop a second time, and the process
	// keeps leaving through the shutdown already under way.
	business, control := newFakeWorker(), newFakeWorker()
	w, sd := newTestLifecycle(t, 300*time.Millisecond, business, control)
	business.hold = w.activities
	_, err := w.start()
	require.NoError(t, err)
	stopped := make(chan error, 1)
	go func() { stopped <- w.stop(context.Background()) }()
	<-business.entered
	fatalReturned := make(chan struct{})
	go func() {
		w.fatal(w.business, errPollerFatal)
		close(fatalReturned)
	}()
	select {
	case <-fatalReturned:
		t.Fatal("the fatal callback returned before the drain in progress ended")
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, <-stopped)
	<-fatalReturned
	require.EqualValues(t, 1, business.stops.Load(), "the SDK's Stop is entered once")
	require.EqualValues(t, 1, control.stops.Load())
	require.EqualValues(t, 1, sd.calls.Load())
}

func TestLifecycleStartFailureLeavesNothingRunning(t *testing.T) {
	business, control := newFakeWorker(), newFakeWorker()
	control.onStart = func() error { return errors.New("dial: connection refused") }
	w, sd := newTestLifecycle(t, time.Second, business, control)
	_, err := w.start()
	require.ErrorContains(t, err, "control queue control: dial: connection refused")
	require.EqualValues(t, 1, business.stops.Load(), "the started queue is stopped again")
	require.False(t, w.health.isReady())
	require.Zero(t, sd.calls.Load())
	select {
	case <-w.health.done:
	case <-time.After(time.Second):
		t.Fatal("the health listener kept serving after the failed start")
	}
}

func TestWorkerOptionsBindTheLifecycle(t *testing.T) {
	business, control := newFakeWorker(), newFakeWorker()
	w, sd := newTestLifecycle(t, 42*time.Second, business, control)
	opts := w.options("anvilkit-agent-workflow@dev", w.control)
	require.Equal(t, "anvilkit-agent-workflow@dev", opts.Identity)
	require.Equal(t, 42*time.Second+reportGrace, opts.WorkerStopTimeout, "the SDK's per-worker wait is the drain bound plus the report grace")
	require.Same(t, w.activities, opts.BackgroundActivityContext, "every Activity descends from the abandonable root")
	require.NotNil(t, opts.OnFatalError)
	opts.OnFatalError(errPollerFatal)
	require.ErrorIs(t, w.health.fatalError(), errPollerFatal)
	require.EqualValues(t, 1, control.stops.Load())
	require.EqualValues(t, 1, sd.calls.Load())
}

func TestLifecycleFatalExitsThroughFx(t *testing.T) {
	// The real Fx path: a fatal error after the start makes Run's wait
	// receive the shutdown signal with exit code 1, whose stop drains the
	// remaining queue; the stop ceiling and the hook's own bound agree.
	require.Greater(t, stopCeiling, config.MaxShutdownTimeout+closeAllowance, "the container's ceiling covers the largest drain and the close")
	business, control := newFakeWorker(), newFakeWorker()
	var w *workers
	app := fx.New(fx.NopLogger, fx.StopTimeout(stopCeiling), fx.Invoke(func(lc fx.Lifecycle, sd fx.Shutdowner) {
		w = newLifecycle(200*time.Millisecond, newHealth("127.0.0.1:0"), sd, slog.New(slog.NewTextHandler(io.Discard, nil)))
		w.business = &queueWorker{Worker: business, name: "business"}
		w.control = &queueWorker{Worker: control, name: "control"}
		lc.Append(fx.Hook{OnStart: func(context.Context) error { _, err := w.start(); return err }, OnStop: w.stop})
	}))
	require.NoError(t, app.Err())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, app.Start(ctx))
	require.True(t, w.health.isReady())

	go w.fatal(w.control, errPollerFatal)
	select {
	case sig := <-app.Wait():
		require.Equal(t, 1, sig.ExitCode, "the process leaves through its error path")
	case <-time.After(5 * time.Second):
		t.Fatal("no shutdown signal after the fatal error")
	}
	require.False(t, w.health.isReady())
	require.NoError(t, app.Stop(ctx))
	require.EqualValues(t, 1, control.stops.Load())
	require.EqualValues(t, 1, business.stops.Load())
}
