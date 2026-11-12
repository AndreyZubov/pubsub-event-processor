// Command fakeorg runs an in-process fake Salesforce org — an OAuth token
// endpoint and a Pub/Sub API server — and emits synthetic order events at a
// steady rate. It lets the processor run end to end without a Salesforce org;
// see "Demo without Salesforce" in the README.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	applog "github.com/AndreyZubov/pubsub-event-processor/internal/log"
	"github.com/AndreyZubov/pubsub-event-processor/internal/pubsub/pubsubtest"
)

const schemaJSON = `{
  "type": "record",
  "name": "Order_Event__e",
  "fields": [
    {"name": "CreatedDate", "type": "long"},
    {"name": "Order_Number__c", "type": "string"},
    {"name": "Status__c", "type": "string"},
    {"name": "Amount__c", "type": "double"}
  ]
}`

var statuses = []string{"Draft", "Submitted", "Approved", "Shipped"}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fakeorg: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	httpAddr := flag.String("http-addr", "127.0.0.1:8090", "listen address of the OAuth endpoint")
	grpcAddr := flag.String("grpc-addr", "127.0.0.1:7443", "listen address of the Pub/Sub API")
	caOut := flag.String("ca-out", "fakeorg-ca.pem", "file to write the TLS certificate to (PUBSUB_CA_FILE)")
	topic := flag.String("topic", "/event/Order_Event__e", "topic to emit events on")
	rate := flag.Float64("rate", 20, "events per second")
	flag.Parse()

	if *rate <= 0 {
		return fmt.Errorf("-rate must be > 0, got %v", *rate)
	}

	log, err := applog.New("info", "fakeorg")
	if err != nil {
		return err
	}
	defer func() { _ = log.Sync() }()

	org, err := pubsubtest.Start(pubsubtest.Options{HTTPAddr: *httpAddr, GRPCAddr: *grpcAddr})
	if err != nil {
		return err
	}
	defer org.Close()

	if err := org.AddTopic(*topic, schemaJSON); err != nil {
		return err
	}
	if err := os.WriteFile(*caOut, org.CAPEM(), 0o600); err != nil {
		return fmt.Errorf("write certificate: %w", err)
	}

	log.Info("fake org started",
		zap.String("login_url", org.LoginURL()),
		zap.String("pubsub_endpoint", org.Endpoint()),
		zap.String("ca_file", *caOut),
		zap.String("topic", *topic),
		zap.Float64("events_per_second", *rate),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ticker := time.NewTicker(time.Duration(float64(time.Second) / *rate))
	defer ticker.Stop()

	for n := 1; ; n++ {
		select {
		case <-ctx.Done():
			log.Info("fake org stopped", zap.Int("events_emitted", n-1))
			return nil
		case <-ticker.C:
		}
		_, err := org.Emit(*topic, map[string]any{
			"CreatedDate":     time.Now().UnixMilli(),
			"Order_Number__c": fmt.Sprintf("ORD-%06d", n),
			"Status__c":       statuses[rand.IntN(len(statuses))], //nolint:gosec // demo data
			"Amount__c":       float64(rand.IntN(100000)) / 100,   //nolint:gosec // demo data
		})
		if err != nil {
			return fmt.Errorf("emit event: %w", err)
		}
	}
}
