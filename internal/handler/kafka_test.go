package handler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap/zaptest"

	"github.com/AndreyZubov/pubsub-event-processor/internal/event"
)

type fakeProducer struct {
	key     string
	value   []byte
	headers []kgo.RecordHeader
	err     error
	calls   int
}

func (f *fakeProducer) Produce(_ context.Context, key string, value []byte, headers []kgo.RecordHeader) error {
	f.calls++
	f.key, f.value, f.headers = key, value, headers
	return f.err
}

func headerValue(headers []kgo.RecordHeader, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func TestForwardHandler_PublishesEnvelope(t *testing.T) {
	p := &fakeProducer{}
	h := NewForward(p, zaptest.NewLogger(t))

	e := testEvent()
	require.NoError(t, h.Handle(context.Background(), e))
	require.Equal(t, 1, p.calls)

	var got Envelope
	require.NoError(t, json.Unmarshal(p.value, &got))

	assert.Equal(t, e.EventID, got.EventID)
	assert.Equal(t, e.Topic, got.Topic)
	assert.Equal(t, e.SchemaID, got.SchemaID)
	assert.Equal(t, e.Payload, got.Payload)
	assert.True(t, e.ReceivedAt.Equal(got.ReceivedAt))

	// Replay IDs are opaque bytes and must survive the JSON round trip.
	decoded, err := base64.StdEncoding.DecodeString(got.ReplayID)
	require.NoError(t, err)
	assert.Equal(t, e.ReplayID, decoded)
}

// Ordering downstream depends on the partition key, so the key contract is
// worth pinning: every event from one Salesforce channel shares a key.
func TestForwardHandler_KeysByTopic(t *testing.T) {
	p := &fakeProducer{}
	h := NewForward(p, zaptest.NewLogger(t))

	require.NoError(t, h.Handle(context.Background(), testEvent()))

	assert.Equal(t, "/event/Order_Event__e", p.key)
}

func TestForwardHandler_SetsRoutingHeaders(t *testing.T) {
	p := &fakeProducer{}
	h := NewForward(p, zaptest.NewLogger(t))

	require.NoError(t, h.Handle(context.Background(), testEvent()))

	assert.Equal(t, "evt-1", headerValue(p.headers, "event_id"))
	assert.Equal(t, "sch-1", headerValue(p.headers, "schema_id"))
	assert.Equal(t, "/event/Order_Event__e", headerValue(p.headers, "sf_topic"))
}

func TestForwardHandler_PropagatesProduceFailure(t *testing.T) {
	sentinel := errors.New("broker unreachable")
	h := NewForward(&fakeProducer{err: sentinel}, zaptest.NewLogger(t))

	err := h.Handle(context.Background(), testEvent())

	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
	assert.Contains(t, err.Error(), "evt-1")
}

// A payload the JSON encoder cannot represent must fail before reaching the
// broker rather than producing a malformed record.
func TestForwardHandler_RejectsUnmarshalablePayload(t *testing.T) {
	p := &fakeProducer{}
	h := NewForward(p, zaptest.NewLogger(t))

	e := event.DecodedEvent{
		EventID: "evt-bad",
		Topic:   "/event/Test__e",
		Payload: map[string]any{"fn": func() {}},
	}

	require.Error(t, h.Handle(context.Background(), e))
	assert.Zero(t, p.calls, "nothing may be produced when encoding fails")
}
