package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/AndreyZubov/pubsub-event-processor/internal/event"
)

func TestLogHandler_LogsExpectedFields(t *testing.T) {
	var buf bytes.Buffer
	enc := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	core := zapcore.NewCore(enc, zapcore.AddSync(&buf), zapcore.InfoLevel)
	logger := zap.New(core)

	h := NewLog(logger)
	e := event.DecodedEvent{
		Topic:      "/event/T",
		EventID:    "evt-1",
		SchemaID:   "schema-1",
		Payload:    map[string]any{"a": 1, "b": 2},
		ReceivedAt: time.Now(),
	}
	if err := h.Handle(context.Background(), e); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	var out map[string]any
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("JSON: %v: %s", err, buf.String())
	}
	if out["topic"] != "/event/T" {
		t.Errorf("topic: %v", out["topic"])
	}
	if out["event_id"] != "evt-1" {
		t.Errorf("event_id: %v", out["event_id"])
	}
	if out["schema_id"] != "schema-1" {
		t.Errorf("schema_id: %v", out["schema_id"])
	}
	if out["payload_fields"] != float64(2) {
		t.Errorf("payload_fields: %v", out["payload_fields"])
	}
}
