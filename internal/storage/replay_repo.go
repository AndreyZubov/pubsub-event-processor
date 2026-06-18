package storage

import (
	"context"
	"fmt"
)

// ReplayRepo persists the last successfully committed replay_id per topic so
// the subscriber can resume from that cursor instead of LATEST after a restart.
type ReplayRepo struct {
	q Querier
}

// NewReplayRepo wraps any Querier.
func NewReplayRepo(q Querier) *ReplayRepo { return &ReplayRepo{q: q} }

// Get returns the stored replay_id for the topic, or nil if none has ever been
// committed (the caller should then start at ReplayPreset.LATEST).
func (r *ReplayRepo) Get(ctx context.Context, topic string) ([]byte, error) {
	var rid []byte
	err := r.q.QueryRow(ctx, `SELECT replay_id FROM replay_state WHERE topic = $1`, topic).Scan(&rid)
	if isNoRows(err) {
		return nil, nil //nolint:nilnil // documented contract: nil + nil means "never committed"
	}
	if err != nil {
		return nil, fmt.Errorf("load replay for %q: %w", topic, err)
	}
	return rid, nil
}

// Set upserts the replay cursor for the topic.
func (r *ReplayRepo) Set(ctx context.Context, topic string, replayID []byte) error {
	_, err := r.q.Exec(ctx, `
		INSERT INTO replay_state (topic, replay_id) VALUES ($1, $2)
		ON CONFLICT (topic) DO UPDATE
		  SET replay_id = EXCLUDED.replay_id, updated_at = now()
	`, topic, replayID)
	if err != nil {
		return fmt.Errorf("set replay for %q: %w", topic, err)
	}
	return nil
}
