// Package worker runs a bounded pool of goroutines that decode and dispatch
// Salesforce events through a Handler. The pool is the bridge between the
// pubsub subscriber's events channel and downstream business handlers.
package worker

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/event"
	"github.com/AndreyZubov/pubsub-event-processor/internal/pubsub"
	"github.com/AndreyZubov/pubsub-event-processor/internal/schema"
)

// Handler processes one decoded Salesforce event. A nil return acknowledges
// the event; a non-nil error means the worker will NOT ack and the source
// stream will eventually redeliver — relying on idempotency for safety.
type Handler interface {
	Handle(ctx context.Context, e event.DecodedEvent) error
}

// AckFn signals the source subscriber for a topic that n events have been
// processed and the stream may replenish its flow-control window.
type AckFn func(topic string, n int)

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

func (p *Pool) process(ctx context.Context, raw pubsub.RawEvent) {
	start := time.Now()
	schemaID := raw.Event.GetEvent().GetSchemaId()

	sch, err := p.cache.Get(ctx, schemaID)
	if err != nil {
		p.metrics.failed.WithLabelValues("schema").Inc()
		p.log.Warn("schema fetch failed",
			zap.String("topic", raw.Topic),
			zap.String("schema_id", schemaID),
			zap.Error(err),
		)
		return
	}

	payload, err := schema.Decode(sch, raw.Event.GetEvent().GetPayload())
	if err != nil {
		p.metrics.failed.WithLabelValues("decode").Inc()
		p.log.Warn("avro decode failed",
			zap.String("topic", raw.Topic),
			zap.String("schema_id", schemaID),
			zap.Error(err),
		)
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

	if err := p.handler.Handle(ctx, decoded); err != nil {
		p.metrics.failed.WithLabelValues("handler").Inc()
		p.log.Warn("handler failed",
			zap.String("topic", raw.Topic),
			zap.String("event_id", decoded.EventID),
			zap.Error(err),
		)
		return
	}

	p.metrics.processed.Inc()
	p.metrics.duration.Observe(time.Since(start).Seconds())
	p.ack(raw.Topic, 1)
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
				Help: "Number of events that failed processing, labelled by stage.",
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
