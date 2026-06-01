package httpserver

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/health"
)

func newTestServer(t *testing.T, checkers map[string]health.Checker) (*Server, net.Listener) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return New("", zap.NewNop(), checkers), ln
}

func waitReady(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url) //nolint:noctx,gosec
		if err == nil {
			_ = resp.Body.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server did not become ready: %s", url)
}

func TestServer_HealthzAndReadyzAndMetrics(t *testing.T) {
	checkers := map[string]health.Checker{
		"x": health.CheckerFunc(func(context.Context) error { return nil }),
	}
	s, ln := newTestServer(t, checkers)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- s.serve(ctx, ln) }()

	base := "http://" + ln.Addr().String()
	waitReady(t, base+"/healthz")

	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		resp, err := http.Get(base + path) //nolint:noctx,gosec
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s: status %d", path, resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if path == "/metrics" && !strings.Contains(string(body), "go_goroutines") {
			t.Errorf("/metrics missing default go metrics, got: %s", body[:min(200, len(body))])
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("serve returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("serve did not return after ctx cancel")
	}
}

func TestServer_GracefulShutdownOnCtxCancel(t *testing.T) {
	s, ln := newTestServer(t, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.serve(ctx, ln) }()

	waitReady(t, "http://"+ln.Addr().String()+"/healthz")
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("graceful shutdown returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown timed out")
	}
}
