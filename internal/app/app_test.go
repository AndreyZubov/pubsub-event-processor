package app

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/config"
)

func testConfig() *config.Config {
	return &config.Config{
		Salesforce: config.SalesforceConfig{
			ClientID:     "id",
			ClientSecret: "secret",
			LoginURL:     "https://login.salesforce.com",
		},
		PubSub: config.PubSubConfig{
			Endpoint: "api.pubsub.salesforce.com:7443",
			Topics:   []string{"/event/Test__e"},
		},
		Database: config.DatabaseConfig{URL: "postgres://x@y/z"},
		HTTP:     config.HTTPConfig{Addr: "127.0.0.1:0"},
		Worker:   config.WorkerConfig{Count: 8, FlowBatchSize: 100},
		LogLevel: "info",
	}
}

func TestApp_RunStopsCleanly(t *testing.T) {
	a, err := New(testConfig(), zap.NewNop(), prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	// Give the HTTP server a moment to bind.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error on clean shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}
