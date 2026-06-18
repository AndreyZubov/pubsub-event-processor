package storage

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/AndreyZubov/pubsub-event-processor/internal/event"
)

// EventRepo persists Salesforce events with idempotent semantics: a duplicate
// event_uuid is a no-op insert reported via inserted=false.
type EventRepo struct {
	q Querier
}

// NewEventRepo wraps any Querier (pool for autocommit, tx for batched writes).
func NewEventRepo(q Querier) *EventRepo { return &EventRepo{q: q} }

// Upsert inserts the event if its UUID has not been seen, otherwise returns
// inserted=false without modifying any row. The UNIQUE constraint on event_uuid
// is what enforces at-least-once safety.
func (r *EventRepo) Upsert(ctx context.Context, e event.DecodedEvent) (bool, error) {
	payload, err := json.Marshal(e.Payload)
	if err != nil {
		return false, fmt.Errorf("marshal payload: %w", err)
	}

	var id int64
	err = r.q.QueryRow(ctx, `
		INSERT INTO processed_events (event_uuid, topic, schema_id, replay_id, payload, received_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (event_uuid) DO NOTHING
		RETURNING id
	`, e.EventID, e.Topic, e.SchemaID, e.ReplayID, payload, e.ReceivedAt).Scan(&id)

	if isNoRows(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("insert event %q: %w", e.EventID, err)
	}
	return true, nil
}
