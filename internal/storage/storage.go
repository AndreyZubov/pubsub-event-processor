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
// connection pool and composes EventRepo + ReplayRepo into atomic operations.
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

// PersistEvent upserts the decoded event and advances the topic's replay_state
// in a single transaction. Returns inserted=false if the event was already
// processed (idempotency on event_uuid); in that case replay_state is NOT
// advanced — the canonical replay cursor stays with the original committer.
func (s *Store) PersistEvent(ctx context.Context, e event.DecodedEvent) (bool, error) {
	var inserted bool
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		ok, err := NewEventRepo(tx).Upsert(ctx, e)
		if err != nil {
			return err
		}
		inserted = ok
		if !ok {
			return nil
		}
		return NewReplayRepo(tx).Set(ctx, e.Topic, e.ReplayID)
	})
	if err != nil {
		return false, fmt.Errorf("persist event: %w", err)
	}
	return inserted, nil
}

// errNoRows is a small convenience to centralize pgx.ErrNoRows handling.
func isNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }
