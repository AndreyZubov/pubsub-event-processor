package handler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/AndreyZubov/pubsub-event-processor/internal/event"
)

// fakeStore records calls and returns scripted results.
type fakeStore struct {
	inserted bool
	err      error
	calls    []event.DecodedEvent
}

func (f *fakeStore) PersistEvent(_ context.Context, e event.DecodedEvent) (bool, error) {
	f.calls = append(f.calls, e)
	return f.inserted, f.err
}

func testEvent() event.DecodedEvent {
	return event.DecodedEvent{
		Topic:      "/event/Order_Event__e",
		EventID:    "evt-1",
		SchemaID:   "sch-1",
		ReplayID:   []byte{0x01, 0x02},
		Payload:    map[string]any{"Amount__c": 42.0},
		ReceivedAt: time.Unix(1700000000, 0).UTC(),
	}
}

func TestPersistHandler_StoresEvent(t *testing.T) {
	store := &fakeStore{inserted: true}
	h := NewPersist(store, zaptest.NewLogger(t))

	require.NoError(t, h.Handle(context.Background(), testEvent()))

	require.Len(t, store.calls, 1)
	assert.Equal(t, "evt-1", store.calls[0].EventID)
	assert.Equal(t, []byte{0x01, 0x02}, store.calls[0].ReplayID)
}

// A duplicate is the normal outcome of replay after a reconnect: the handler
// must succeed so the pipeline acknowledges and moves on.
func TestPersistHandler_DuplicateIsNotAnError(t *testing.T) {
	store := &fakeStore{inserted: false}
	h := NewPersist(store, zaptest.NewLogger(t))

	assert.NoError(t, h.Handle(context.Background(), testEvent()))
	assert.Len(t, store.calls, 1)
}

func TestPersistHandler_PropagatesStoreFailure(t *testing.T) {
	sentinel := errors.New("connection refused")
	h := NewPersist(&fakeStore{err: sentinel}, zaptest.NewLogger(t))

	err := h.Handle(context.Background(), testEvent())

	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel, "the original error must stay unwrappable")
	assert.Contains(t, err.Error(), "evt-1", "the event id belongs in the message")
}
