package log

import (
	"bytes"
	"encoding/json"
	"testing"

	"go.uber.org/zap/zapcore"
)

func TestNew_InvalidLevel(t *testing.T) {
	_, err := New("nope", "v1")
	if err == nil {
		t.Fatal("expected error for invalid level")
	}
}

func TestNew_ValidLevels(t *testing.T) {
	for _, lvl := range []string{"debug", "info", "warn", "error"} {
		t.Run(lvl, func(t *testing.T) {
			if _, err := New(lvl, "v1"); err != nil {
				t.Fatalf("New(%q): %v", lvl, err)
			}
		})
	}
}

func TestNew_DefaultFieldsAndFormat(t *testing.T) {
	var buf bytes.Buffer
	logger, err := newCore("info", "v1.2.3", zapcore.AddSync(&buf))
	if err != nil {
		t.Fatalf("newCore: %v", err)
	}
	logger.Info("hello")

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("expected JSON output, got %q: %v", buf.String(), err)
	}

	want := map[string]string{
		"service": "pubsub-event-processor",
		"version": "v1.2.3",
		"msg":     "hello",
		"level":   "info",
	}
	for k, v := range want {
		if got, ok := entry[k]; !ok || got != v {
			t.Errorf("field %q: got %v, want %q", k, got, v)
		}
	}
	if _, ok := entry["ts"]; !ok {
		t.Errorf("expected ts field, got %v", entry)
	}
}

func TestNew_LevelFilter(t *testing.T) {
	var buf bytes.Buffer
	logger, err := newCore("warn", "v1", zapcore.AddSync(&buf))
	if err != nil {
		t.Fatal(err)
	}
	logger.Info("info-msg")
	logger.Warn("warn-msg")

	out := buf.String()
	if bytes.Contains(buf.Bytes(), []byte("info-msg")) {
		t.Errorf("info should be filtered at warn level, got: %s", out)
	}
	if !bytes.Contains(buf.Bytes(), []byte("warn-msg")) {
		t.Errorf("warn should appear, got: %s", out)
	}
}
