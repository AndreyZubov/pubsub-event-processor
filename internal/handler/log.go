// Package handler hosts the Handler implementations the worker pool dispatches
// events into. The default for early milestones is LogHandler; PersistHandler
// (Postgres-backed) and ForwardHandler (webhook-backed) will live here too.
package handler

import (
	"context"

	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/event"
)

// LogHandler emits one structured log line per event and reports success. It
// serves as the minimal Handler before persistence and forwarding handlers
// arrive in later milestones.
type LogHandler struct {
	log *zap.Logger
}

// NewLog constructs a LogHandler bound to log.
func NewLog(log *zap.Logger) *LogHandler { return &LogHandler{log: log} }

// Handle satisfies worker.Handler.
func (h *LogHandler) Handle(_ context.Context, e event.DecodedEvent) error {
	h.log.Info("event decoded",
		zap.String("topic", e.Topic),
		zap.String("event_id", e.EventID),
		zap.String("schema_id", e.SchemaID),
		zap.Int("payload_fields", len(e.Payload)),
	)
	return nil
}
