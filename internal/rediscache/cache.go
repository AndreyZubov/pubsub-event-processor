// Package rediscache implements the processed-event cache backed by Redis.
//
// The cache answers one question: has this event already been processed end to
// end? A hit lets the pipeline skip a database transaction that would do
// nothing. A miss — including one caused by Redis being unavailable — costs
// only the work the service would have done anyway, so the cache is never
// authoritative and never blocks processing.
package rediscache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/config"
)

// Cache records which event UUIDs have completed the whole handler chain.
type Cache struct {
	client  *redis.Client
	prefix  string
	ttl     time.Duration
	timeout time.Duration
	log     *zap.Logger
	metrics cacheMetrics
}

// New builds a Cache from configuration. It does not dial: go-redis connects
// lazily, so availability is reported by Check rather than here.
func New(cfg config.RedisConfig, log *zap.Logger, reg prometheus.Registerer) *Cache {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	return &Cache{
		client:  client,
		prefix:  cfg.KeyPrefix,
		ttl:     cfg.TTL,
		timeout: cfg.Timeout,
		log:     log,
		metrics: newCacheMetrics(reg),
	}
}

// key namespaces an event UUID so one Redis instance can serve several services.
func (c *Cache) key(eventID string) string { return c.prefix + eventID }

// Seen reports whether the event has already been processed in full.
//
// An error means "unknown", not "not seen": the caller treats it as a miss and
// falls through to the database, which is authoritative.
func (c *Cache) Seen(ctx context.Context, eventID string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	n, err := c.client.Exists(ctx, c.key(eventID)).Result()
	if err != nil {
		c.metrics.errors.WithLabelValues("seen").Inc()
		return false, fmt.Errorf("redis exists: %w", err)
	}

	if n > 0 {
		c.metrics.hits.Inc()
		return true, nil
	}
	c.metrics.misses.Inc()
	return false, nil
}

// MarkSeen records the event as fully processed.
//
// Call this only after every downstream step has succeeded. Marking earlier
// would let a later failure be skipped on redelivery, and the event would never
// reach the stage that failed.
func (c *Cache) MarkSeen(ctx context.Context, eventID string) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	if err := c.client.Set(ctx, c.key(eventID), 1, c.ttl).Err(); err != nil {
		c.metrics.errors.WithLabelValues("mark").Inc()
		return fmt.Errorf("redis set: %w", err)
	}
	return nil
}

// Check reports reachability for the readiness probe.
func (c *Cache) Check(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	if err := c.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	return nil
}

// Close releases the connection pool.
func (c *Cache) Close() error {
	if err := c.client.Close(); err != nil && !errors.Is(err, redis.ErrClosed) {
		return fmt.Errorf("close redis: %w", err)
	}
	return nil
}

type cacheMetrics struct {
	hits   prometheus.Counter
	misses prometheus.Counter
	errors *prometheus.CounterVec
}

func newCacheMetrics(reg prometheus.Registerer) cacheMetrics {
	m := cacheMetrics{
		hits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "dedup_cache_hits_total",
			Help: "Events recognised as already processed, skipping the database transaction.",
		}),
		misses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "dedup_cache_misses_total",
			Help: "Events not found in the cache and passed through to the handler chain.",
		}),
		errors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dedup_cache_errors_total",
				Help: "Cache operations that failed, labelled by operation. These degrade to a miss.",
			},
			[]string{"op"},
		),
	}
	if reg != nil {
		reg.MustRegister(m.hits, m.misses, m.errors)
	}
	return m
}
