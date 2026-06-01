package pubsub

import (
	"context"
	"math/rand/v2"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxRetryAttempts  = 5
	baseRetryDelay    = 100 * time.Millisecond
	maxRetryDelay     = 5 * time.Second
	retryJitterFactor = 0.25
)

func newUnaryInterceptor(log *zap.Logger, m clientMetrics) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		start := time.Now()
		var lastErr error

		for attempt := range maxRetryAttempts {
			err := invoker(ctx, method, req, reply, cc, opts...)
			if err == nil {
				m.rpcTotal.WithLabelValues(method, codes.OK.String()).Inc()
				m.rpcDuration.WithLabelValues(method).Observe(time.Since(start).Seconds())
				return nil
			}

			code := status.Code(err)
			if !isRetryable(code) || attempt == maxRetryAttempts-1 {
				m.rpcTotal.WithLabelValues(method, code.String()).Inc()
				m.rpcDuration.WithLabelValues(method).Observe(time.Since(start).Seconds())
				return err
			}

			m.rpcRetries.WithLabelValues(method, code.String()).Inc()
			delay := backoffDelay(attempt)
			log.Warn("rpc retry",
				zap.String("method", method),
				zap.Int("attempt", attempt+1),
				zap.String("code", code.String()),
				zap.Duration("delay", delay),
			)

			select {
			case <-ctx.Done():
				m.rpcTotal.WithLabelValues(method, codes.Canceled.String()).Inc()
				return ctx.Err()
			case <-time.After(delay):
			}
			lastErr = err
		}
		return lastErr
	}
}

func isRetryable(c codes.Code) bool {
	return c == codes.Unavailable || c == codes.DeadlineExceeded
}

// backoffDelay returns base * 2^attempt clamped to maxRetryDelay, with ±25% jitter
// to spread reconnect storms across clients.
func backoffDelay(attempt int) time.Duration {
	d := baseRetryDelay << attempt
	if d > maxRetryDelay {
		d = maxRetryDelay
	}
	spread := time.Duration(float64(d) * retryJitterFactor)
	if spread <= 0 {
		return d
	}
	jitter := time.Duration(rand.Int64N(int64(2*spread))) - spread //nolint:gosec // jitter only needs to spread retries, not be cryptographic
	return d + jitter
}
