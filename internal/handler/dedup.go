package handler

import (
	"context"

	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/event"
)

// SeenCache remembers which events have completed processing. Declared at the
// point of use so the handler package stays independent of the cache
// implementation.
type SeenCache interface {
	// Seen reports whether the event was already processed in full. An error
	// means "unknown" and is treated as a miss.
	Seen(ctx context.Context, eventID string) (bool, error)
	// MarkSeen records the event as fully processed.
	MarkSeen(ctx context.Context, eventID string) error
}

// DedupHandler wraps the rest of the pipeline with a processed-event cache.
//
// It is a decorator rather than a step in the chain, and that is the whole
// point: the event is marked as seen only after the wrapped handler returns
// nil, so "seen" means "finished every stage", not "reached the cache".
//
// A step-shaped design would mark the event once persistence succeeded. An
// event persisted but not yet forwarded to Kafka would then be marked, and on
// redelivery the cache would skip it — so the stage that actually failed would
// never run and the record would never reach Kafka.
//
// Cache failures are logged and ignored. Redis being down makes the service
// slower on replay, never wrong: every miss falls through to the database,
// which enforces idempotency with a UNIQUE constraint.
type DedupHandler struct {
	cache SeenCache
	next  Handler
	log   *zap.Logger
}

// NewDedup wraps next with the cache.
func NewDedup(cache SeenCache, next Handler, log *zap.Logger) *DedupHandler {
	return &DedupHandler{cache: cache, next: next, log: log}
}

// Handle satisfies worker.Handler.
func (h *DedupHandler) Handle(ctx context.Context, e event.DecodedEvent) error {
	seen, err := h.cache.Seen(ctx, e.EventID)
	switch {
	case err != nil:
		// Unknown, not "not seen". Fall through to the authoritative path.
		h.log.Warn("dedup cache lookup failed, falling through",
			zap.String("event_id", e.EventID),
			zap.Error(err),
		)
	case seen:
		h.log.Debug("event already processed, skipping",
			zap.String("topic", e.Topic),
			zap.String("event_id", e.EventID),
		)
		return nil
	}

	if err := h.next.Handle(ctx, e); err != nil {
		return err
	}

	if err := h.cache.MarkSeen(ctx, e.EventID); err != nil {
		// The event is safely processed; failing here would cause it to be
		// redelivered and redone, which is worse than losing a cache entry.
		h.log.Warn("dedup cache write failed",
			zap.String("event_id", e.EventID),
			zap.Error(err),
		)
	}
	return nil
}
