package handler

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/event"
)

// EventStore is the persistence dependency PersistHandler needs. Declared here,
// at the point of use, so the handler package does not depend on the storage
// package and can be tested with a fake.
type EventStore interface {
	// PersistEvent writes the event and advances the topic's replay cursor in
	// one transaction. It reports inserted=false when the event UUID was
	// already stored, which is a successful no-op, not an error.
	PersistEvent(ctx context.Context, e event.DecodedEvent) (bool, error)
}

// PersistHandler writes decoded events to durable storage.
//
// Delivery is at-least-once: after a reconnect the subscriber replays events it
// has already seen. Idempotency comes from the UNIQUE constraint on event_uuid,
// so a duplicate is reported and skipped rather than failing the pipeline.
type PersistHandler struct {
	store EventStore
	log   *zap.Logger
}

// NewPersist constructs a PersistHandler backed by store.
func NewPersist(store EventStore, log *zap.Logger) *PersistHandler {
	return &PersistHandler{store: store, log: log}
}

// Handle satisfies worker.Handler.
func (h *PersistHandler) Handle(ctx context.Context, e event.DecodedEvent) error {
	inserted, err := h.store.PersistEvent(ctx, e)
	if err != nil {
		return fmt.Errorf("persist event %q: %w", e.EventID, err)
	}

	if !inserted {
		// Expected during replay after a reconnect. Logged at debug so a normal
		// recovery does not look like a problem in production logs.
		h.log.Debug("duplicate event skipped",
			zap.String("topic", e.Topic),
			zap.String("event_id", e.EventID),
		)
		return nil
	}

	h.log.Debug("event persisted",
		zap.String("topic", e.Topic),
		zap.String("event_id", e.EventID),
	)
	return nil
}
