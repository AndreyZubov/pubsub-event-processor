//go:build integration

package kafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/config"
	appkafka "github.com/AndreyZubov/pubsub-event-processor/internal/kafka"
)

const (
	startupTimeout = 90 * time.Second
	testTopic      = "salesforce.events.test"
)

// startKafka boots a single-node Kafka in KRaft mode and returns its brokers.
func startKafka(t *testing.T) []string {
	t.Helper()
	ctx := context.Background()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1")
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	t.Cleanup(func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := container.Terminate(shutdown); err != nil {
			t.Logf("terminate kafka: %v", err)
		}
	})

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("brokers: %v", err)
	}
	return brokers
}

func testConfig(brokers []string) config.KafkaConfig {
	return config.KafkaConfig{
		Enabled:                true,
		Brokers:                brokers,
		Topic:                  testTopic,
		ClientID:               "integration-test",
		ProduceTimeout:         30 * time.Second,
		CreateTopic:            true,
		TopicPartitions:        3,
		TopicReplicationFactor: 1,
	}
}

// newProducer builds a producer against brokers and provisions the topic. The
// broker under test runs with auto-creation off, matching production.
func newProducer(ctx context.Context, t *testing.T, brokers []string) *appkafka.Producer {
	t.Helper()

	p, err := appkafka.New(testConfig(brokers), zap.NewNop(), prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	t.Cleanup(p.Close)

	if err := p.EnsureTopic(ctx); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}
	return p
}

// A produced record must be readable back with its key, value, and headers
// intact — the contract downstream consumers depend on.
func TestProducer_ProduceAndConsume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()

	brokers := startKafka(t)
	p := newProducer(ctx, t, brokers)

	headers := []kgo.RecordHeader{{Key: "event_id", Value: []byte("evt-42")}}
	if err := p.Produce(ctx, "/event/Order_Event__e", []byte(`{"event_id":"evt-42"}`), headers); err != nil {
		t.Fatalf("produce: %v", err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(testTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close()

	pollCtx, pollCancel := context.WithTimeout(ctx, 30*time.Second)
	defer pollCancel()

	fetches := consumer.PollRecords(pollCtx, 1)
	if errs := fetches.Errors(); len(errs) > 0 {
		t.Fatalf("poll: %v", errs)
	}

	records := fetches.Records()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	rec := records[0]
	if got := string(rec.Key); got != "/event/Order_Event__e" {
		t.Errorf("key = %q, want the Salesforce topic", got)
	}
	if got := string(rec.Value); got != `{"event_id":"evt-42"}` {
		t.Errorf("value = %q", got)
	}
	if len(rec.Headers) != 1 || string(rec.Headers[0].Value) != "evt-42" {
		t.Errorf("headers = %+v, want event_id=evt-42", rec.Headers)
	}
}

// Records sharing a key must land on one partition, which is what preserves
// their relative order for consumers.
func TestProducer_SameKeySamePartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()

	brokers := startKafka(t)
	p := newProducer(ctx, t, brokers)

	const key = "/event/Order_Event__e"
	for i := range 5 {
		if err := p.Produce(ctx, key, []byte(`{"n":`+string(rune('0'+i))+`}`), nil); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(testTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close()

	pollCtx, pollCancel := context.WithTimeout(ctx, 30*time.Second)
	defer pollCancel()

	var partitions []int32
	for len(partitions) < 5 {
		fetches := consumer.PollRecords(pollCtx, 5)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("poll: %v", errs)
		}
		for _, rec := range fetches.Records() {
			partitions = append(partitions, rec.Partition)
		}
	}

	for _, p := range partitions[1:] {
		if p != partitions[0] {
			t.Fatalf("records with one key spread across partitions %v", partitions)
		}
	}
}

func TestProducer_CheckReportsReachability(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()

	p := newProducer(ctx, t, startKafka(t))

	if err := p.Check(ctx); err != nil {
		t.Errorf("Check on a live broker: %v", err)
	}
}

// An unreachable broker must fail readiness rather than hang.
func TestProducer_CheckFailsOnDeadBroker(t *testing.T) {
	cfg := testConfig([]string{"127.0.0.1:1"})
	p, err := appkafka.New(cfg, zap.NewNop(), prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	t.Cleanup(p.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := p.Check(ctx); err == nil {
		t.Error("Check on an unreachable broker returned nil")
	}
}

// Restarting the service must not fail because the topic already exists.
func TestProducer_EnsureTopicIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()

	brokers := startKafka(t)
	p := newProducer(ctx, t, brokers)

	if err := p.EnsureTopic(ctx); err != nil {
		t.Errorf("second EnsureTopic: %v", err)
	}
}
