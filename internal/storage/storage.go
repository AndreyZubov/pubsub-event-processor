package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AndreyZubov/pubsub-event-processor/internal/event"
)

// Querier is the common subset of pgxpool.Pool and pgx.Tx that the repositories
// need. It lets the same EventRepo / ReplayRepo work in both autocommit mode
// (with the pool) and inside an explicit transaction.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

const poolHealthCheckPeriod = 30 * time.Second

// NewPool constructs a pgxpool.Pool from the DSN with MaxConns clamped to int32.
// Caller validates maxConns at the config layer.
func NewPool(ctx context.Context, dsn string, maxConns int) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConns = int32(maxConns) //nolint:gosec // maxConns validated to 1..1000 in config
	cfg.HealthCheckPeriod = poolHealthCheckPeriod

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("new pool: %w", err)
	}
	return pool, nil
}

// Store is the high-level data-access facade for the service. It owns the
// connection pool and exposes the EventRepo and ReplayRepo operations the
// pipeline needs.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore wraps an existing pool.
func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Pool returns the underlying pool for callers that need it (health checks, etc.).
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases the pool's connections.
func (s *Store) Close() { s.pool.Close() }

// LoadReplay returns the persisted replay_id for the given topic, or nil if the
// service has never committed one (first run, or topic just added).
func (s *Store) LoadReplay(ctx context.Context, topic string) ([]byte, error) {
	return NewReplayRepo(s.pool).Get(ctx, topic)
}

// SaveReplay records replayID as the topic's committed replay cursor.
func (s *Store) SaveReplay(ctx context.Context, topic string, replayID []byte) error {
	return NewReplayRepo(s.pool).Set(ctx, topic, replayID)
}

// PersistEvent stores the decoded event. Returns inserted=false if the event
// was already stored (idempotency on event_uuid), which is not an error.
//
// The replay cursor is deliberately not advanced here: persistence is only
// the first stage of the pipeline, and a cursor that moved before forwarding
// succeeded would let a restart skip an event that never reached the sink.
func (s *Store) PersistEvent(ctx context.Context, e event.DecodedEvent) (bool, error) {
	inserted, err := NewEventRepo(s.pool).Upsert(ctx, e)
	if err != nil {
		return false, fmt.Errorf("persist event: %w", err)
	}
	return inserted, nil
}

// errNoRows is a small convenience to centralize pgx.ErrNoRows handling.
func isNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }
