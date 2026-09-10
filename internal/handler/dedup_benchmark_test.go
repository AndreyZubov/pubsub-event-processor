//go:build integration

// Benchmarks the claim the dedup cache is built on: that skipping the database
// transaction for an already-processed event is meaningfully cheaper than
// letting Postgres reject the duplicate on its UNIQUE constraint.
//
// Run with:
//
//	go test -tags=integration -bench=BenchmarkDuplicates -benchtime=2000x \
//	    -run='^$' ./internal/handler/
package handler_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/config"
	"github.com/AndreyZubov/pubsub-event-processor/internal/event"
	"github.com/AndreyZubov/pubsub-event-processor/internal/handler"
	"github.com/AndreyZubov/pubsub-event-processor/internal/rediscache"
	"github.com/AndreyZubov/pubsub-event-processor/internal/storage"
)

const benchStartupTimeout = 90 * time.Second

func startPostgres(b *testing.B) string {
	b.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("bench"),
		tcpostgres.WithUsername("bench"),
		tcpostgres.WithPassword("bench"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(benchStartupTimeout),
		),
	)
	if err != nil {
		b.Fatalf("start postgres: %v", err)
	}
	b.Cleanup(func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = container.Terminate(shutdown)
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		b.Fatalf("connection string: %v", err)
	}
	if err := storage.Migrate(dsn, zap.NewNop()); err != nil {
		b.Fatalf("migrate: %v", err)
	}
	return dsn
}

func startRedis(b *testing.B) string {
	b.Helper()
	ctx := context.Background()

	container, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		b.Fatalf("start redis: %v", err)
	}
	b.Cleanup(func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = container.Terminate(shutdown)
	})

	endpoint, err := container.Endpoint(ctx, "")
	if err != nil {
		b.Fatalf("endpoint: %v", err)
	}
	return endpoint
}

func benchEvent(i int) event.DecodedEvent {
	return event.DecodedEvent{
		Topic:      "/event/Order_Event__e",
		EventID:    fmt.Sprintf("bench-evt-%d", i),
		SchemaID:   "sch-bench",
		ReplayID:   []byte{0x01},
		Payload:    map[string]any{"Amount__c": 42.0, "Name": "benchmark"},
		ReceivedAt: time.Now().UTC(),
	}
}

// newPersist builds the persistence handler against a fresh pool.
func newPersist(b *testing.B, dsn string) handler.Handler {
	b.Helper()
	ctx := context.Background()

	pool, err := storage.NewPool(ctx, dsn, 20)
	if err != nil {
		b.Fatalf("pool: %v", err)
	}
	store := storage.NewStore(pool)
	b.Cleanup(store.Close)

	return handler.NewPersist(store, zap.NewNop())
}

// BenchmarkDuplicates_WithoutCache measures the current behaviour: every
// duplicate opens a transaction that Postgres then rejects.
func BenchmarkDuplicates_WithoutCache(b *testing.B) {
	ctx := context.Background()
	h := newPersist(b, startPostgres(b))

	// Prime the row so every timed iteration is a duplicate.
	if err := h.Handle(ctx, benchEvent(0)); err != nil {
		b.Fatalf("prime: %v", err)
	}

	b.ResetTimer()
	for b.Loop() {
		if err := h.Handle(ctx, benchEvent(0)); err != nil {
			b.Fatalf("handle: %v", err)
		}
	}
}

// BenchmarkDuplicates_WithCache measures the same load with the cache in front.
func BenchmarkDuplicates_WithCache(b *testing.B) {
	ctx := context.Background()
	persist := newPersist(b, startPostgres(b))

	cache := rediscache.New(config.RedisConfig{
		Enabled:   true,
		Addr:      startRedis(b),
		KeyPrefix: "bench:seen:",
		TTL:       time.Hour,
		Timeout:   2 * time.Second,
	}, zap.NewNop(), prometheus.NewRegistry())
	b.Cleanup(func() { _ = cache.Close() })

	h := handler.NewDedup(cache, persist, zap.NewNop())

	// Prime both the row and the cache entry.
	if err := h.Handle(ctx, benchEvent(0)); err != nil {
		b.Fatalf("prime: %v", err)
	}

	b.ResetTimer()
	for b.Loop() {
		if err := h.Handle(ctx, benchEvent(0)); err != nil {
			b.Fatalf("handle: %v", err)
		}
	}
}
