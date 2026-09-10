//go:build integration

package rediscache_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/config"
	"github.com/AndreyZubov/pubsub-event-processor/internal/rediscache"
)

const startupTimeout = 60 * time.Second

// startRedis boots a Redis container and returns its host:port.
func startRedis(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	container, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	t.Cleanup(func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := container.Terminate(shutdown); err != nil {
			t.Logf("terminate redis: %v", err)
		}
	})

	endpoint, err := container.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("endpoint: %v", err)
	}
	return endpoint
}

func newCache(t *testing.T, addr string, ttl time.Duration) *rediscache.Cache {
	t.Helper()
	c := rediscache.New(config.RedisConfig{
		Enabled:   true,
		Addr:      addr,
		KeyPrefix: "test:seen:",
		TTL:       ttl,
		Timeout:   2 * time.Second,
	}, zap.NewNop(), prometheus.NewRegistry())
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestCache_MarkThenSeen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()

	c := newCache(t, startRedis(t), time.Hour)

	seen, err := c.Seen(ctx, "evt-1")
	if err != nil {
		t.Fatalf("Seen before mark: %v", err)
	}
	if seen {
		t.Fatal("an unknown event reported as seen")
	}

	if err := c.MarkSeen(ctx, "evt-1"); err != nil {
		t.Fatalf("MarkSeen: %v", err)
	}

	seen, err = c.Seen(ctx, "evt-1")
	if err != nil {
		t.Fatalf("Seen after mark: %v", err)
	}
	if !seen {
		t.Error("a marked event reported as unseen")
	}
}

// Entries must expire: past the replay window a duplicate can no longer arrive,
// so remembering the event only wastes memory.
func TestCache_EntriesExpire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()

	c := newCache(t, startRedis(t), time.Second)

	if err := c.MarkSeen(ctx, "evt-ttl"); err != nil {
		t.Fatalf("MarkSeen: %v", err)
	}

	time.Sleep(1500 * time.Millisecond)

	seen, err := c.Seen(ctx, "evt-ttl")
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if seen {
		t.Error("entry outlived its TTL")
	}
}

// Keys are namespaced, so two services sharing one Redis cannot collide.
func TestCache_KeysArePrefixed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()

	addr := startRedis(t)
	a := newCache(t, addr, time.Hour)

	b := rediscache.New(config.RedisConfig{
		Enabled: true, Addr: addr, KeyPrefix: "other:seen:",
		TTL: time.Hour, Timeout: 2 * time.Second,
	}, zap.NewNop(), prometheus.NewRegistry())
	t.Cleanup(func() { _ = b.Close() })

	if err := a.MarkSeen(ctx, "evt-shared"); err != nil {
		t.Fatalf("MarkSeen: %v", err)
	}

	seen, err := b.Seen(ctx, "evt-shared")
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if seen {
		t.Error("a different prefix saw another service's key")
	}
}

func TestCache_CheckReportsReachability(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()

	if err := newCache(t, startRedis(t), time.Hour).Check(ctx); err != nil {
		t.Errorf("Check against a live Redis: %v", err)
	}
}

// An unreachable cache must report unready rather than hang.
func TestCache_CheckFailsWhenUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := newCache(t, "127.0.0.1:1", time.Hour)
	if err := c.Check(ctx); err == nil {
		t.Error("Check against a dead address returned nil")
	}
}

// A lookup failure must surface as an error so the caller falls through to the
// database instead of trusting a false negative.
func TestCache_SeenFailsWhenUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := newCache(t, "127.0.0.1:1", time.Hour)
	if _, err := c.Seen(ctx, "evt-x"); err == nil {
		t.Error("Seen against a dead address returned nil error")
	}
}
