package bootstrap

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"
)

// errStopping is what a start that completes after the shutdown began sees:
// readiness is never granted once it has been withdrawn.
var errStopping = errors.New("worker is stopping")

// health is the worker's only HTTP surface: the liveness and readiness
// signals of the Worker lifecycle. /healthz answers 200 while the process
// runs; /readyz answers 200 only between the start of both pollers and the
// first of the beginning of the shutdown or a fatal worker error, so a
// draining or failed worker is taken out of service before its pollers
// stop. Readiness is granted once and withdrawn for good: a start that
// finishes after a fatal error or after the shutdown began cannot report
// ready again. No business route, no Temporal or Control data.
type health struct {
	srv  *http.Server
	done chan struct{}

	mu        sync.Mutex
	ready     bool
	withdrawn bool  // the shutdown began or a fatal error was recorded
	fatal     error // the first fatal worker error
}

func newHealth(listen string) *health {
	h := &health{done: make(chan struct{})}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !h.isReady() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	h.srv = &http.Server{Addr: listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return h
}

// becomeReady grants readiness unless a fatal error or the shutdown came
// first, in which case it returns that reason and readiness stays withdrawn.
func (h *health) becomeReady() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.fatal != nil {
		return h.fatal
	}
	if h.withdrawn {
		return errStopping
	}
	h.ready = true
	return nil
}

// withdraw ends readiness for the rest of the process lifetime (the
// shutdown began).
func (h *health) withdraw() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ready, h.withdrawn = false, true
}

// fail records a fatal worker error and withdraws readiness. It reports
// whether err is the first fatal error, so the caller requests the process
// shutdown exactly once; later errors are kept out of the record.
func (h *health) fail(err error) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ready, h.withdrawn = false, true
	if h.fatal != nil {
		return false
	}
	h.fatal = err
	return true
}

func (h *health) isReady() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ready
}

// fatalError is the first fatal worker error, nil while none was recorded.
func (h *health) fatalError() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.fatal
}

// start binds the listener now, so an occupied port or an invalid address
// fails the worker's startup instead of being discovered by the first probe.
func (h *health) start() (net.Addr, error) {
	ln, err := net.Listen("tcp", h.srv.Addr)
	if err != nil {
		return nil, err
	}
	go func() {
		defer close(h.done)
		if err := h.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// The listener died while the worker runs: the process is no
			// longer observable, so it ends the way a failed probe would.
			panic("health listener stopped: " + err.Error())
		}
	}()
	return ln.Addr(), nil
}

func (h *health) stop(ctx context.Context) error {
	h.withdraw()
	err := h.srv.Shutdown(ctx)
	select {
	case <-h.done:
	case <-ctx.Done():
	}
	return err
}
