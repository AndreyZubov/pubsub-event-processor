// Package httpserver runs the admin HTTP endpoints (health, readiness, metrics)
// with chi routing and graceful shutdown.
package httpserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/health"
)

const (
	shutdownTimeout   = 10 * time.Second
	readHeaderTimeout = 5 * time.Second
)

// Server wraps an http.Server with chi routing, admin endpoints, and graceful shutdown.
type Server struct {
	addr string
	log  *zap.Logger
	mux  *chi.Mux
}

// New builds a Server that exposes /healthz, /readyz (aggregating the given checkers)
// and /metrics (Prometheus default registry).
func New(addr string, log *zap.Logger, checkers map[string]health.Checker) *Server {
	mux := chi.NewRouter()
	mux.Use(middleware.Recoverer)
	mux.Handle("/healthz", health.Healthz())
	mux.Handle("/readyz", health.Readiness(checkers))
	mux.Handle("/metrics", promhttp.Handler())
	return &Server{addr: addr, log: log, mux: mux}
}

// Run binds the listener and serves until ctx is canceled or the server fails.
// On ctx cancellation it performs a graceful shutdown bounded by shutdownTimeout.
func (s *Server) Run(ctx context.Context) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}
	return s.serve(ctx, ln)
}

func (s *Server) serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{
		Handler:           s.mux,
		ReadHeaderTimeout: readHeaderTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		close(errCh)
	}()

	s.log.Info("http server listening", zap.String("addr", ln.Addr().String()))

	select {
	case <-ctx.Done():
		s.log.Info("http server shutting down")
		// Parent ctx is already canceled here, so derive Shutdown's deadline
		// from Background — otherwise Shutdown would return immediately.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil { //nolint:contextcheck // see comment above
			return fmt.Errorf("shutdown: %w", err)
		}
		return nil
	case err := <-errCh:
		return err
	}
}
