package bootstrap

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// health is the worker's only HTTP surface: the liveness and readiness
// signals of the Worker lifecycle. /healthz answers 200 while the process
// runs; /readyz answers 200 only between the start of both pollers and the
// beginning of the shutdown, so a draining worker is taken out of service
// before its pollers stop. No business route, no Temporal or Control data.
type health struct {
	srv   *http.Server
	ready atomic.Bool
	done  chan struct{}
}

func newHealth(listen string) *health {
	h := &health{done: make(chan struct{})}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !h.ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	h.srv = &http.Server{Addr: listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return h
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
	h.ready.Store(false)
	err := h.srv.Shutdown(ctx)
	select {
	case <-h.done:
	case <-ctx.Done():
	}
	return err
}
