// Package worker runs a bounded pool of goroutines that decode and dispatch
// Salesforce events through a Handler. The pool is the bridge between the
// pubsub subscriber's events channel and downstream business handlers.
package worker

import (
	"context"
	"sync"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/event"
	"github.com/AndreyZubov/pubsub-event-processor/internal/pubsub"
	"github.com/AndreyZubov/pubsub-event-processor/internal/schema"
)

const (
	retryMinDelay = 100 * time.Millisecond
	retryMaxDelay = 10 * time.Second
)

// Handler processes one decoded Salesforce event. A nil return acknowledges
// the event; a non-nil error makes the worker retry it with backoff. Handlers
// must therefore be idempotent: a retry repeats every stage that ran before
// the failure.
type Handler interface {
	Handle(ctx context.Context, e event.DecodedEvent) error
}

// AckFn is called exactly once for every event the pool finishes: either
// handled successfully, or skipped because its payload cannot be decoded. It
// is not called for events abandoned when ctx is canceled.
type AckFn func(e pubsub.RawEvent)

// Pool is a bounded set of worker goroutines that all read from the same events
// channel and dispatch into one shared Handler.
type Pool struct {
	n       int
	cache   *schema.Cache
	handler Handler
	ack     AckFn
	log     *zap.Logger
	metrics poolMetrics
}

// New constructs a pool with n worker goroutines. cache and handler are shared
// across all workers; both must be safe for concurrent use (Cache is, by
// design; Handler is the caller's responsibility).
func New(
	n int,
	cache *schema.Cache,
	handler Handler,
	ack AckFn,
	log *zap.Logger,
	reg prometheus.Registerer,
) *Pool {
	return &Pool{
		n:       n,
		cache:   cache,
		handler: handler,
		ack:     ack,
		log:     log,
		metrics: newPoolMetrics(reg),
	}
}

// Run spawns n worker goroutines reading from in and blocks until either ctx
// is canceled or in is closed and drained. Always returns nil; failures show
// up as logged warnings and metric increments per event.
func (p *Pool) Run(ctx context.Context, in <-chan pubsub.RawEvent) error {
	var wg sync.WaitGroup
	wg.Add(p.n)
	for range p.n {
		go func() {
			defer wg.Done()
			p.worker(ctx, in)
		}()
	}
	wg.Wait()
	return nil
}

func (p *Pool) worker(ctx context.Context, in <-chan pubsub.RawEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case raw, ok := <-in:
			if !ok {
				return
			}
			p.process(ctx, raw)
		}
	}
}

// process takes one event to completion. Transient failures — fetching the
// schema, running the handler — are retried until they succeed or ctx is
// canceled: Salesforce redelivers only after a reconnect, so giving up would
// lose the event, and a downstream outage should stall the pipeline rather
// than drain it. An abandoned event is not acked, so the replay cursor stays
// below it and a restart redelivers it.
func (p *Pool) process(ctx context.Context, raw pubsub.RawEvent) {
	start := time.Now()
	schemaID := raw.Event.GetEvent().GetSchemaId()

	var sch avro.Schema
	fetched := p.retry(ctx, "schema", raw, func() error {
		var err error
		sch, err = p.cache.Get(ctx, schemaID)
		return err
	})
	if !fetched {
		return
	}

	payload, err := schema.Decode(sch, raw.Event.GetEvent().GetPayload())
	if err != nil {
		// A payload that does not decode against its own schema fails the same
		// way on every attempt, and retrying would hold the topic's replay
		// cursor in place forever. The event is skipped instead, logged with
		// its replay ID so it can be recovered from Salesforce by hand.
		p.metrics.failed.WithLabelValues("decode").Inc()
		p.log.Error("avro decode failed; skipping event",
			zap.String("topic", raw.Topic),
			zap.String("event_id", raw.Event.GetEvent().GetId()),
			zap.String("schema_id", schemaID),
			zap.Binary("replay_id", raw.Event.GetReplayId()),
			zap.Error(err),
		)
		p.ack(raw)
		return
	}

	decoded := event.DecodedEvent{
		Topic:      raw.Topic,
		EventID:    raw.Event.GetEvent().GetId(),
		SchemaID:   schemaID,
		ReplayID:   raw.Event.GetReplayId(),
		Payload:    payload,
		ReceivedAt: raw.ReceivedAt,
	}

	handled := p.retry(ctx, "handler", raw, func() error {
		return p.handler.Handle(ctx, decoded)
	})
	if !handled {
		return
	}

	p.metrics.processed.Inc()
	p.metrics.duration.Observe(time.Since(start).Seconds())
	p.ack(raw)
}

// retry runs fn until it returns nil, backing off exponentially between
// attempts. It reports false if ctx was canceled before fn succeeded.
func (p *Pool) retry(ctx context.Context, stage string, raw pubsub.RawEvent, fn func() error) bool {
	delay := retryMinDelay
	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}

		p.metrics.failed.WithLabelValues(stage).Inc()
		p.log.Warn(stage+" failed; retrying",
			zap.String("topic", raw.Topic),
			zap.String("event_id", raw.Event.GetEvent().GetId()),
			zap.Int("attempt", attempt),
			zap.Duration("backoff", delay),
			zap.Error(err),
		)

		select {
		case <-ctx.Done():
			return false
		case <-time.After(delay):
		}
		delay = min(delay*2, retryMaxDelay)
	}
}

type poolMetrics struct {
	processed prometheus.Counter
	failed    *prometheus.CounterVec
	duration  prometheus.Histogram
}

func newPoolMetrics(reg prometheus.Registerer) poolMetrics {
	m := poolMetrics{
		processed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "events_processed_total",
			Help: "Number of events that completed handler.Handle successfully and were ack'd.",
		}),
		failed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "events_failed_total",
				Help: "Number of failed processing attempts, labelled by stage. Retried stages count every attempt.",
			},
			[]string{"stage"},
		),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "processing_latency_seconds",
			Help:    "Duration of full event processing (decode + handler) per worker.",
			Buckets: prometheus.DefBuckets,
		}),
	}
	if reg != nil {
		reg.MustRegister(m.processed, m.failed, m.duration)
	}
	return m
}
