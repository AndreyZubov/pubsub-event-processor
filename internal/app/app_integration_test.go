//go:build integration

package app_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/app"
	"github.com/AndreyZubov/pubsub-event-processor/internal/config"
	"github.com/AndreyZubov/pubsub-event-processor/internal/pubsub/pubsubtest"
	"github.com/AndreyZubov/pubsub-event-processor/internal/storage"
	salesforcepb "github.com/AndreyZubov/pubsub-event-processor/proto/salesforce"
)

const (
	topic = "/event/Order_Event__e"

	schemaJSON = `{"type":"record","name":"Order_Event__e","fields":[` +
		`{"name":"Order_Number__c","type":"string"},` +
		`{"name":"Amount__c","type":"double"}]}`

	startupTimeout = 60 * time.Second
	settleTimeout  = 60 * time.Second
	stopTimeout    = 15 * time.Second
)

// TestPipeline_EndToEnd runs the whole service against a fake Salesforce org
// and a real PostgreSQL: OAuth, the TLS gRPC stream, Avro decoding, the worker
// pool, persistence, and the replay cursor. It checks the delivery guarantee
// the service exists for — every event is stored exactly once across a
// restart and a dropped stream.
func TestPipeline_EndToEnd(t *testing.T) {
	dsn := startPostgres(t)
	db, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(db.Close)

	org, err := pubsubtest.Start(pubsubtest.Options{})
	require.NoError(t, err)
	t.Cleanup(org.Close)
	require.NoError(t, org.AddTopic(topic, schemaJSON))

	caFile := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caFile, org.CAPEM(), 0o600))

	cfg := &config.Config{
		Salesforce: config.SalesforceConfig{ClientID: "id", ClientSecret: "secret", LoginURL: org.LoginURL()},
		PubSub:     config.PubSubConfig{Endpoint: org.Endpoint(), Topics: []string{topic}, CAFile: caFile},
		Database:   config.DatabaseConfig{URL: dsn, MaxConns: 10},
		HTTP:       config.HTTPConfig{Addr: "127.0.0.1:0"},
		Worker:     config.WorkerConfig{Count: 8, FlowBatchSize: 50},
		LogLevel:   "info",
	}

	// First run. A topic with no committed cursor starts at LATEST, so the
	// events are emitted only once the stream is open.
	stop := runApp(t, cfg)
	waitForStreams(t, org, 1)
	emit(t, org, 0, 1000)
	waitForRows(t, db, 1000)
	stop()

	first := org.InitialFetches()[0]
	require.Equal(t, salesforcepb.ReplayPreset_LATEST, first.GetReplayPreset())
	require.Equal(t, pubsubtest.ReplayID(1000), committedCursor(t, db),
		"shutdown must flush the cursor of the last finished event")

	// Events published while the service is down must be picked up on restart.
	emit(t, org, 1000, 200)
	stop = runApp(t, cfg)
	waitForRows(t, db, 1200)

	resumed := org.InitialFetches()[1]
	require.Equal(t, salesforcepb.ReplayPreset_CUSTOM, resumed.GetReplayPreset())
	require.Equal(t, pubsubtest.ReplayID(1000), resumed.GetReplayId())

	// A dropped stream mid-flow must neither lose nor duplicate events.
	emit(t, org, 1200, 150)
	org.DropStreams()
	emit(t, org, 1350, 150)
	waitForRows(t, db, 1500)
	stop()

	var distinct int
	require.NoError(t, db.QueryRow(context.Background(),
		`SELECT count(DISTINCT payload->>'Order_Number__c') FROM processed_events`).Scan(&distinct))
	require.Equal(t, 1500, distinct, "every emitted order must be stored")
	require.Equal(t, pubsubtest.ReplayID(1500), committedCursor(t, db))
}

// runApp starts the service and returns a function that stops it and waits
// for a clean shutdown.
func runApp(t *testing.T, cfg *config.Config) (stop func()) {
	t.Helper()
	a, err := app.New(context.Background(), cfg, zap.NewNop(), prometheus.NewRegistry())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(stopTimeout):
			t.Fatal("service did not stop")
		}
		require.NoError(t, a.Close())
	}
	t.Cleanup(stop)
	return stop
}

func emit(t *testing.T, org *pubsubtest.Org, from, n int) {
	t.Helper()
	for i := from; i < from+n; i++ {
		_, err := org.Emit(topic, map[string]any{
			"Order_Number__c": fmt.Sprintf("ORD-%05d", i),
			"Amount__c":       float64(i) + 0.5,
		})
		require.NoError(t, err)
	}
}

func waitForStreams(t *testing.T, org *pubsubtest.Org, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return len(org.InitialFetches()) >= n },
		settleTimeout, 20*time.Millisecond, "subscribe stream was not opened")
}

func waitForRows(t *testing.T, db *pgxpool.Pool, want int) {
	t.Helper()
	var got int
	require.Eventuallyf(t, func() bool {
		if err := db.QueryRow(context.Background(), `SELECT count(*) FROM processed_events`).Scan(&got); err != nil {
			return false
		}
		return got >= want
	}, settleTimeout, 50*time.Millisecond, "want %d stored events", want)
	require.Equal(t, want, got, "more rows than emitted events")
}

func committedCursor(t *testing.T, db *pgxpool.Pool) []byte {
	t.Helper()
	var rid []byte
	require.NoError(t, db.QueryRow(context.Background(),
		`SELECT replay_id FROM replay_state WHERE topic = $1`, topic).Scan(&rid))
	return rid
}

func startPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("test"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(startupTimeout),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	require.NoError(t, storage.Migrate(dsn, zap.NewNop()))
	return dsn
}
