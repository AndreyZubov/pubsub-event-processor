package pubsub

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newTestInterceptor(t *testing.T) grpc.UnaryClientInterceptor {
	t.Helper()
	return newUnaryInterceptor(zap.NewNop(), newClientMetrics(prometheus.NewRegistry()))
}

func TestInterceptor_SuccessNoRetry(t *testing.T) {
	var calls int64
	invoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		atomic.AddInt64(&calls, 1)
		return nil
	}
	err := newTestInterceptor(t)(context.Background(), "/svc/Method", nil, nil, nil, invoker)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Errorf("calls: got %d, want 1", got)
	}
}

func TestInterceptor_NonRetryableImmediate(t *testing.T) {
	var calls int64
	invoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		atomic.AddInt64(&calls, 1)
		return status.Error(codes.PermissionDenied, "denied")
	}
	err := newTestInterceptor(t)(context.Background(), "/svc/Method", nil, nil, nil, invoker)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("code: %v", status.Code(err))
	}
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Errorf("calls: got %d, want 1 (no retry on PermissionDenied)", got)
	}
}

func TestInterceptor_RetryableSucceedsEventually(t *testing.T) {
	var calls int64
	invoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		n := atomic.AddInt64(&calls, 1)
		if n < 3 {
			return status.Error(codes.Unavailable, "transient")
		}
		return nil
	}
	err := newTestInterceptor(t)(context.Background(), "/svc/Method", nil, nil, nil, invoker)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got := atomic.LoadInt64(&calls); got != 3 {
		t.Errorf("calls: got %d, want 3", got)
	}
}

func TestInterceptor_RetryExhausted(t *testing.T) {
	var calls int64
	invoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		atomic.AddInt64(&calls, 1)
		return status.Error(codes.Unavailable, "down")
	}
	err := newTestInterceptor(t)(context.Background(), "/svc/Method", nil, nil, nil, invoker)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code: %v", status.Code(err))
	}
	if got := atomic.LoadInt64(&calls); got != int64(maxRetryAttempts) {
		t.Errorf("calls: got %d, want %d", got, maxRetryAttempts)
	}
}

func TestInterceptor_ContextCancelBreaksRetry(t *testing.T) {
	var calls int64
	invoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		atomic.AddInt64(&calls, 1)
		return status.Error(codes.Unavailable, "transient")
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	err := newTestInterceptor(t)(ctx, "/svc/Method", nil, nil, nil, invoker)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if got := atomic.LoadInt64(&calls); got >= int64(maxRetryAttempts) {
		t.Errorf("expected fewer than %d calls, got %d", maxRetryAttempts, got)
	}
}

func TestBackoffDelay_Bounded(t *testing.T) {
	for attempt := range 10 {
		d := backoffDelay(attempt)
		if d < 0 {
			t.Errorf("attempt %d: negative delay %s", attempt, d)
		}
		upper := maxRetryDelay + time.Duration(float64(maxRetryDelay)*retryJitterFactor)
		if d > upper {
			t.Errorf("attempt %d: delay %s exceeds upper bound %s", attempt, d, upper)
		}
	}
}
