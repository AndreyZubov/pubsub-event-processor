package handler

import (
	"context"
	"fmt"

	"github.com/AndreyZubov/pubsub-event-processor/internal/event"
)

// Handler processes one decoded event. It mirrors worker.Handler so this
// package can compose handlers without importing the worker package, which
// would create an import cycle.
type Handler interface {
	Handle(ctx context.Context, e event.DecodedEvent) error
}

// Chain runs handlers in order and stops at the first failure.
//
// Order is a correctness decision, not a preference. Persistence runs before
// forwarding: the worker pool acknowledges an event to Salesforce only when the
// whole chain succeeds, so a Kafka outage leaves the event unacknowledged and it
// is redelivered. On redelivery the persist step deduplicates on event UUID and
// the forward step retries — the failing stage is the one that repeats.
//
// Reversing the order would publish to Kafka and then, on a database failure,
// republish the same record after redelivery. The Kafka idempotent producer
// only deduplicates retries within a producer session, not across restarts, so
// downstream consumers would see genuine duplicates.
type Chain struct {
	handlers []Handler
}

// NewChain builds a Chain over the given handlers, in execution order.
func NewChain(handlers ...Handler) *Chain {
	return &Chain{handlers: handlers}
}

// Handle satisfies worker.Handler.
func (c *Chain) Handle(ctx context.Context, e event.DecodedEvent) error {
	for i, h := range c.handlers {
		if err := h.Handle(ctx, e); err != nil {
			return fmt.Errorf("chain step %d: %w", i, err)
		}
	}
	return nil
}
