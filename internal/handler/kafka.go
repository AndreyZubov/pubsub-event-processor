package handler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/event"
)

// RecordProducer is the Kafka dependency ForwardHandler needs, declared at the
// point of use so the handler can be tested without a broker.
type RecordProducer interface {
	Produce(ctx context.Context, key string, value []byte, headers []kgo.RecordHeader) error
}

// Envelope is the wire contract published to Kafka. It is deliberately explicit
// rather than the raw Avro payload: consumers get the Salesforce metadata they
// need to route and deduplicate without holding a schema registry.
type Envelope struct {
	EventID    string         `json:"event_id"`
	Topic      string         `json:"topic"`
	SchemaID   string         `json:"schema_id"`
	ReplayID   string         `json:"replay_id"`
	ReceivedAt time.Time      `json:"received_at"`
	Payload    map[string]any `json:"payload"`
}

// ForwardHandler publishes decoded events to Kafka.
type ForwardHandler struct {
	producer RecordProducer
	log      *zap.Logger
}

// NewForward constructs a ForwardHandler publishing through producer.
func NewForward(producer RecordProducer, log *zap.Logger) *ForwardHandler {
	return &ForwardHandler{producer: producer, log: log}
}

// Handle satisfies worker.Handler.
func (h *ForwardHandler) Handle(ctx context.Context, e event.DecodedEvent) error {
	env := Envelope{
		EventID:  e.EventID,
		Topic:    e.Topic,
		SchemaID: e.SchemaID,
		// Replay IDs are opaque bytes, not text; base64 keeps the JSON valid
		// and round-trippable.
		ReplayID:   base64.StdEncoding.EncodeToString(e.ReplayID),
		ReceivedAt: e.ReceivedAt,
		Payload:    e.Payload,
	}

	value, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope for %q: %w", e.EventID, err)
	}

	// Keying by Salesforce topic sends every event from one channel to the same
	// partition, which is what preserves their relative order downstream. The
	// trade-off is that a single busy channel cannot spread across partitions;
	// keying by an entity id would trade topic ordering for that parallelism.
	headers := []kgo.RecordHeader{
		{Key: "event_id", Value: []byte(e.EventID)},
		{Key: "schema_id", Value: []byte(e.SchemaID)},
		{Key: "sf_topic", Value: []byte(e.Topic)},
	}

	if err := h.producer.Produce(ctx, e.Topic, value, headers); err != nil {
		return fmt.Errorf("forward event %q: %w", e.EventID, err)
	}

	h.log.Debug("event forwarded to kafka",
		zap.String("topic", e.Topic),
		zap.String("event_id", e.EventID),
		zap.Int("bytes", len(value)),
	)
	return nil
}
