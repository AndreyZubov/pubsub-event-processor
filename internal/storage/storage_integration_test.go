//go:build integration

package storage_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/event"
	"github.com/AndreyZubov/pubsub-event-processor/internal/storage"
)

const startupTimeout = 60 * time.Second

func startPostgres(t *testing.T) (string, func()) {
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
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("dsn: %v", err)
	}

	if err := storage.Migrate(dsn, zap.NewNop()); err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("migrate: %v", err)
	}

	cleanup := func() { _ = container.Terminate(ctx) }
	return dsn, cleanup
}

func newTestStore(t *testing.T) *storage.Store {
	t.Helper()
	dsn, cleanup := startPostgres(t)
	t.Cleanup(cleanup)

	pool, err := storage.NewPool(context.Background(), dsn, 10)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	t.Cleanup(pool.Close)

	return storage.NewStore(pool)
}

func sampleEvent(uuid, topic string) event.DecodedEvent {
	return event.DecodedEvent{
		Topic:    topic,
		EventID:  uuid,
		SchemaID: "schema-1",
		ReplayID: []byte{0x01, 0x02, 0x03},
		Payload: map[string]any{
			"OrderId": "ORD-1",
			"Amount":  9.99,
		},
		ReceivedAt: time.Now().UTC(),
	}
}

func TestEventRepo_UpsertIdempotency(t *testing.T) {
	s := newTestStore(t)
	repo := storage.NewEventRepo(s.Pool())

	e := sampleEvent("uuid-A", "/event/Test__e")

	inserted, err := repo.Upsert(context.Background(), e)
	if err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	if !inserted {
		t.Error("first call should report inserted=true")
	}

	inserted, err = repo.Upsert(context.Background(), e)
	if err != nil {
		t.Fatalf("second upsert: %v", err)
	}
	if inserted {
		t.Error("duplicate event_uuid should report inserted=false")
	}

	var count int
	if err := s.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM processed_events WHERE event_uuid = $1`, e.EventID).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("processed_events rows for uuid: got %d, want 1", count)
	}
}

func TestReplayRepo_GetReturnsNilOnFirstRun(t *testing.T) {
	s := newTestStore(t)
	repo := storage.NewReplayRepo(s.Pool())

	got, err := repo.Get(context.Background(), "/event/Empty__e")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for never-committed topic, got %v", got)
	}
}

func TestReplayRepo_SetThenGet(t *testing.T) {
	s := newTestStore(t)
	repo := storage.NewReplayRepo(s.Pool())

	want := []byte{0xAA, 0xBB, 0xCC}
	if err := repo.Set(context.Background(), "/event/X", want); err != nil {
		t.Fatalf("set: %v", err)
	}

	got, err := repo.Get(context.Background(), "/event/X")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("got %v, want %v", got, want)
	}

	updated := []byte{0xDD}
	if err := repo.Set(context.Background(), "/event/X", updated); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, err = repo.Get(context.Background(), "/event/X")
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if string(got) != string(updated) {
		t.Errorf("after update: got %v, want %v", got, updated)
	}
}

func TestStore_PersistEvent_DoesNotMoveReplayCursor(t *testing.T) {
	s := newTestStore(t)
	e := sampleEvent("uuid-1", "/event/T")

	inserted, err := s.PersistEvent(context.Background(), e)
	if err != nil {
		t.Fatalf("persist: %v", err)
	}
	if !inserted {
		t.Error("expected inserted=true on first persist")
	}

	rid, err := s.LoadReplay(context.Background(), e.Topic)
	if err != nil {
		t.Fatalf("load replay: %v", err)
	}
	if rid != nil {
		t.Errorf("replay cursor must be left to the committer: got %v", rid)
	}
}

func TestStore_SaveReplayThenLoad(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	for _, rid := range [][]byte{{0x01}, {0x02, 0x03}} {
		if err := s.SaveReplay(ctx, "/event/T", rid); err != nil {
			t.Fatalf("save %v: %v", rid, err)
		}
		got, err := s.LoadReplay(ctx, "/event/T")
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if string(got) != string(rid) {
			t.Errorf("cursor: got %v, want %v", got, rid)
		}
	}
}

func TestStore_PersistEvent_CanceledContext(t *testing.T) {
	s := newTestStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	e := sampleEvent("uuid-cancel", "/event/T")
	_, err := s.PersistEvent(ctx, e)
	if err == nil {
		t.Fatal("expected error on canceled ctx")
	}
	if !isCanceledLike(err) {
		t.Errorf("expected canceled-like error, got %v", err)
	}
}

func isCanceledLike(err error) bool {
	// pgx wraps context cancellation; either context.Canceled or driver-level message.
	if err == nil {
		return false
	}
	for _, target := range []error{context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}
