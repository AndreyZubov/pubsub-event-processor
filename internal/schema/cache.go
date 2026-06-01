// Package schema caches parsed Avro schemas keyed by Salesforce schema ID.
// Salesforce events ship a schema_id alongside their Avro-binary payload; the
// schema itself is fetched separately via Pub/Sub GetSchema. Parsing is not
// free, so the cache stores the parsed schema and deduplicates concurrent
// cold-start fetches for the same ID.
package schema

import (
	"context"
	"fmt"
	"sync"

	"github.com/hamba/avro/v2"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"
)

// Source fetches the raw Avro JSON schema for the given schema ID.
// Implementations typically call the Salesforce Pub/Sub GetSchema RPC.
type Source func(ctx context.Context, schemaID string) (string, error)

// Cache memoizes parsed Avro schemas by schema ID.
type Cache struct {
	src     Source
	parsed  sync.Map // schemaID (string) -> avro.Schema
	sf      singleflight.Group
	metrics cacheMetrics
}

// NewCache constructs a cache that delegates fetches to src. reg may be nil
// to skip metrics registration.
func NewCache(src Source, reg prometheus.Registerer) *Cache {
	return &Cache{
		src:     src,
		metrics: newCacheMetrics(reg),
	}
}

// Get returns the parsed schema for id, fetching and parsing on a miss.
// Concurrent calls for the same id share a single underlying fetch.
func (c *Cache) Get(ctx context.Context, id string) (avro.Schema, error) {
	if v, ok := c.parsed.Load(id); ok {
		c.metrics.hits.Inc()
		return v.(avro.Schema), nil
	}
	c.metrics.misses.Inc()

	v, err, _ := c.sf.Do(id, func() (any, error) {
		if v, ok := c.parsed.Load(id); ok {
			return v, nil
		}
		raw, err := c.src(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("fetch schema %q: %w", id, err)
		}
		parsed, err := avro.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("parse schema %q: %w", id, err)
		}
		c.parsed.Store(id, parsed)
		return parsed, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(avro.Schema), nil
}

type cacheMetrics struct {
	hits   prometheus.Counter
	misses prometheus.Counter
}

func newCacheMetrics(reg prometheus.Registerer) cacheMetrics {
	m := cacheMetrics{
		hits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "schema_cache_hits_total",
			Help: "Number of schema cache lookups served from memory.",
		}),
		misses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "schema_cache_misses_total",
			Help: "Number of schema cache lookups that triggered an upstream fetch.",
		}),
	}
	if reg != nil {
		reg.MustRegister(m.hits, m.misses)
	}
	return m
}
