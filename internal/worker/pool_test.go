package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/event"
	"github.com/AndreyZubov/pubsub-event-processor/internal/pubsub"
	"github.com/AndreyZubov/pubsub-event-processor/internal/schema"
	salesforcepb "github.com/AndreyZubov/pubsub-event-processor/proto/salesforce"
)

const testSchemaJSON = `{
  "type": "record",
  "name": "Test",
  "fields": [
    {"name": "Name", "type": "string"}
  ]
}`

type recordingHandler struct {
	mu         sync.Mutex
	received   []event.DecodedEvent
	returnErr  error
	delay      time.Duration
	totalCalls atomic.Int64
}

func (h *recordingHandler) Handle(_ context.Context, e event.DecodedEvent) error {
	h.totalCalls.Add(1)
	if h.delay > 0 {
		time.Sleep(h.delay)
	}
	h.mu.Lock()
	h.received = append(h.received, e)
	h.mu.Unlock()
	return h.returnErr
}

type ackRecorder struct {
	mu     sync.Mutex
	calls  []string
	totalN atomic.Int64
}

func (a *ackRecorder) ack(topic string, n int) {
	a.totalN.Add(int64(n))
	a.mu.Lock()
	for range n {
		a.calls = append(a.calls, topic)
	}
	a.mu.Unlock()
}

func buildRawEvent(t *testing.T, schemaID, topic string) pubsub.RawEvent {
	t.Helper()
	sch, err := avro.Parse(testSchemaJSON)
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	payload, err := avro.Marshal(sch, map[string]any{"Name": "bob"})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return pubsub.RawEvent{
		Topic: topic,
		Event: &salesforcepb.ConsumerEvent{
			Event: &salesforcepb.ProducerEvent{
				Id:       "evt-" + schemaID,
				SchemaId: schemaID,
				Payload:  payload,
			},
			ReplayId: []byte{0x01},
		},
		ReceivedAt: time.Now(),
	}
}

func newTestPool(t *testing.T, n int, handler Handler, ack AckFn) *Pool {
	t.Helper()
	cache := schema.NewCache(
		func(_ context.Context, _ string) (string, error) { return testSchemaJSON, nil },
		prometheus.NewRegistry(),
	)
	return New(n, cache, handler, ack, zap.NewNop(), prometheus.NewRegistry())
}

func TestPool_ProcessesAllEvents(t *testing.T) {
	h := &recordingHandler{}
	a := &ackRecorder{}
	p := newTestPool(t, 1, h, a.ack)

	in := make(chan pubsub.RawEvent, 5)
	for i := range 5 {
		in <- buildRawEvent(t, "S1", "/event/T")
		_ = i
	}
	close(in)

	if err := p.Run(context.Background(), in); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := h.totalCalls.Load(); got != 5 {
		t.Errorf("Handle calls: got %d, want 5", got)
	}
	if got := a.totalN.Load(); got != 5 {
		t.Errorf("ack count: got %d, want 5", got)
	}
}

func TestPool_ConcurrentWorkersAllProcess(t *testing.T) {
	const events = 200
	h := &recordingHandler{}
	a := &ackRecorder{}
	p := newTestPool(t, 8, h, a.ack)

	in := make(chan pubsub.RawEvent, events)
	for range events {
		in <- buildRawEvent(t, "S1", "/event/T")
	}
	close(in)

	if err := p.Run(context.Background(), in); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := h.totalCalls.Load(); got != events {
		t.Errorf("Handle calls: got %d, want %d", got, events)
	}
	if got := a.totalN.Load(); got != events {
		t.Errorf("ack count: got %d, want %d", got, events)
	}
}

func TestPool_HandlerErrorDoesNotAck(t *testing.T) {
	h := &recordingHandler{returnErr: errors.New("downstream failure")}
	a := &ackRecorder{}
	p := newTestPool(t, 1, h, a.ack)

	in := make(chan pubsub.RawEvent, 3)
	for range 3 {
		in <- buildRawEvent(t, "S1", "/event/T")
	}
	close(in)

	if err := p.Run(context.Background(), in); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := h.totalCalls.Load(); got != 3 {
		t.Errorf("Handle calls: got %d, want 3", got)
	}
	if got := a.totalN.Load(); got != 0 {
		t.Errorf("ack count: got %d, want 0 (handler errored)", got)
	}
}

func TestPool_ExitsOnCtxCancel(t *testing.T) {
	h := &recordingHandler{delay: 100 * time.Millisecond}
	a := &ackRecorder{}
	p := newTestPool(t, 2, h, a.ack)

	in := make(chan pubsub.RawEvent, 100)
	for range 100 {
		in <- buildRawEvent(t, "S1", "/event/T")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, in) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after ctx cancel")
	}
}

func TestPool_AckCarriesTopic(t *testing.T) {
	h := &recordingHandler{}
	a := &ackRecorder{}
	p := newTestPool(t, 1, h, a.ack)

	in := make(chan pubsub.RawEvent, 2)
	in <- buildRawEvent(t, "S1", "/event/X")
	in <- buildRawEvent(t, "S1", "/event/Y")
	close(in)

	if err := p.Run(context.Background(), in); err != nil {
		t.Fatalf("Run: %v", err)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.calls) != 2 {
		t.Fatalf("ack calls: got %v, want 2", a.calls)
	}
	gotX, gotY := false, false
	for _, topic := range a.calls {
		if topic == "/event/X" {
			gotX = true
		}
		if topic == "/event/Y" {
			gotY = true
		}
	}
	if !gotX || !gotY {
		t.Errorf("expected acks for both topics, got %v", a.calls)
	}
}
