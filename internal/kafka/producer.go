// Package kafka wraps the franz-go client with the settings, instrumentation,
// and delivery guarantees this service needs to publish decoded Salesforce
// events downstream.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/config"
)

// Producer publishes records to a single Kafka topic.
//
// Every produce is synchronous and waits for acknowledgement from all in-sync
// replicas. That is deliberate: the worker pool only acknowledges an event to
// Salesforce after the handler returns nil, so a produce that has not been
// durably accepted must surface as an error rather than sit in a background
// buffer the caller cannot observe.
type Producer struct {
	client  *kgo.Client
	topic   string
	timeout time.Duration
	cfg     config.KafkaConfig
	log     *zap.Logger
	metrics producerMetrics
}

// New builds a Producer from configuration. It does not dial: franz-go connects
// lazily, so broker availability is reported by Check rather than here.
func New(cfg config.KafkaConfig, log *zap.Logger, reg prometheus.Registerer) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.DefaultProduceTopic(cfg.Topic),
		// AllISRAcks keeps franz-go's idempotent producer enabled (it disables
		// itself under weaker acks). Idempotency is what makes a retry safe:
		// the broker deduplicates on producer ID and sequence number, so a
		// produce retried after a timeout cannot write the record twice.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression(), kgo.NoCompression()),
		kgo.RecordDeliveryTimeout(cfg.ProduceTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("new kafka client: %w", err)
	}

	return &Producer{
		client:  client,
		topic:   cfg.Topic,
		timeout: cfg.ProduceTimeout,
		cfg:     cfg,
		log:     log,
		metrics: newProducerMetrics(reg),
	}, nil
}

// EnsureTopic creates the destination topic if it does not exist.
//
// Only for development and demo clusters. Real deployments provision topics
// deliberately — partition count caps consumer parallelism and cannot be
// lowered later, and replication factor decides how much broker loss the data
// survives. Guessing those at service startup is worse than failing loudly.
func (p *Producer) EnsureTopic(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	admin := kadm.NewClient(p.client)
	// kadm.CreateTopic surfaces the per-topic broker error through err as well
	// as through the response, so this single check covers both it and any
	// transport failure.
	_, err := admin.CreateTopic(ctx, p.cfg.TopicPartitions, p.cfg.TopicReplicationFactor, nil, p.topic)
	// Already existing is the normal outcome on every restart after the first.
	if err != nil && !isTopicAlreadyExists(err) {
		return fmt.Errorf("create topic %q: %w", p.topic, err)
	}

	p.log.Info("kafka topic ready",
		zap.String("topic", p.topic),
		zap.Int32("partitions", p.cfg.TopicPartitions),
		zap.Int16("replication_factor", p.cfg.TopicReplicationFactor),
	)
	return nil
}

// Produce publishes one record and blocks until the brokers acknowledge it or
// the timeout elapses. key selects the partition, so records sharing a key keep
// their relative order.
func (p *Producer) Produce(ctx context.Context, key string, value []byte, headers []kgo.RecordHeader) error {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	start := time.Now()
	err := p.client.ProduceSync(ctx, &kgo.Record{
		Topic:   p.topic,
		Key:     []byte(key),
		Value:   value,
		Headers: headers,
	}).FirstErr()

	p.metrics.duration.Observe(time.Since(start).Seconds())
	if err != nil {
		p.metrics.failed.Inc()
		return fmt.Errorf("produce to %q: %w", p.topic, err)
	}

	p.metrics.produced.Inc()
	p.metrics.bytes.Add(float64(len(value)))
	return nil
}

// Check reports broker reachability. Wired into the readiness probe so a
// cluster the service cannot reach takes the pod out of rotation instead of
// silently failing every event.
func (p *Producer) Check(ctx context.Context) error {
	if err := p.client.Ping(ctx); err != nil {
		return fmt.Errorf("kafka ping: %w", err)
	}
	return nil
}

// Topic returns the destination topic. Used for logging and tests.
func (p *Producer) Topic() string { return p.topic }

// Close flushes buffered records and shuts the client down.
func (p *Producer) Close() {
	p.client.Close()
}

// isTopicAlreadyExists reports whether err is the broker's TOPIC_ALREADY_EXISTS.
func isTopicAlreadyExists(err error) bool {
	var kerrErr *kerr.Error
	return errors.As(err, &kerrErr) && kerrErr.Code == kerr.TopicAlreadyExists.Code
}

type producerMetrics struct {
	produced prometheus.Counter
	failed   prometheus.Counter
	bytes    prometheus.Counter
	duration prometheus.Histogram
}

func newProducerMetrics(reg prometheus.Registerer) producerMetrics {
	m := producerMetrics{
		produced: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "kafka_records_produced_total",
			Help: "Records successfully acknowledged by Kafka.",
		}),
		failed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "kafka_records_failed_total",
			Help: "Produce attempts that returned an error.",
		}),
		bytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "kafka_record_bytes_total",
			Help: "Total size of record values produced.",
		}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "kafka_produce_duration_seconds",
			Help:    "Latency of a synchronous produce, including retries.",
			Buckets: prometheus.DefBuckets,
		}),
	}
	if reg != nil {
		reg.MustRegister(m.produced, m.failed, m.bytes, m.duration)
	}
	return m
}
