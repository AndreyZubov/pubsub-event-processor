// Package replay tracks which events have finished processing and persists a
// per-topic replay cursor that is safe to resume a subscription from.
package replay

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// DefaultInterval is how often Run persists cursors that have advanced.
const DefaultInterval = time.Second

// CursorStore persists the replay cursor for a topic.
type CursorStore interface {
	SaveReplay(ctx context.Context, topic string, replayID []byte) error
}

// Committer maintains a low-water mark per topic and persists it periodically.
//
// Workers finish events out of order, so the cursor cannot simply follow the
// latest completion: a restart would resume after events that are still in
// flight and lose them. Instead every event carries its stream position (a
// per-topic sequence number starting at 1), and the cursor only moves to the
// replay ID of the highest position below which every event is done. Resuming
// from it can redeliver events, which idempotent handlers absorb, but never
// skips one.
//
// Batching the writes keeps the database cost independent of the event rate.
// The price is that a crash replays up to one interval of extra events.
type Committer struct {
	store    CursorStore
	interval time.Duration
	log      *zap.Logger
	metrics  committerMetrics

	mu     sync.Mutex
	topics map[string]*topicCursor

	// flushMu serializes flushes so an older snapshot is never written after
	// a newer one.
	flushMu sync.Mutex
}

type topicCursor struct {
	// next is the lowest sequence number not yet done.
	next uint64
	// done holds completed events above the gap at next, keyed by sequence.
	done map[uint64][]byte
	// replayID is the cursor for every event below next; nil until the first
	// contiguous completion.
	replayID []byte
	dirty    bool
}

// NewCommitter constructs a Committer that persists into store every interval.
func NewCommitter(store CursorStore, interval time.Duration, log *zap.Logger, reg prometheus.Registerer) *Committer {
	return &Committer{
		store:    store,
		interval: interval,
		log:      log,
		metrics:  newCommitterMetrics(reg),
		topics:   make(map[string]*topicCursor),
	}
}

// Done records that the event at seq on topic has finished processing. seq
// must be unique per topic and assigned in stream order starting at 1; every
// position must eventually be reported, or the cursor stops at the gap.
func (c *Committer) Done(topic string, seq uint64, replayID []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	t, ok := c.topics[topic]
	if !ok {
		t = &topicCursor{next: 1, done: make(map[uint64][]byte)}
		c.topics[topic] = t
	}
	if seq < t.next {
		return
	}
	t.done[seq] = replayID
	for {
		rid, ok := t.done[t.next]
		if !ok {
			return
		}
		delete(t.done, t.next)
		t.next++
		t.replayID = rid
		t.dirty = true
	}
}

// Run flushes advanced cursors every interval until ctx is canceled. It does
// not flush on exit: the caller must call Flush once the workers have stopped,
// so completions that land during shutdown are included.
func (c *Committer) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := c.Flush(ctx); err != nil {
				// Not fatal: the cursor stays dirty and the next tick retries.
				c.log.Warn("replay cursor commit failed", zap.Error(err))
			}
		}
	}
}

// Flush persists every cursor that advanced since the last successful flush.
// A cursor that fails to save stays pending and is retried by the next Flush;
// the first error is returned after all topics have been attempted.
func (c *Committer) Flush(ctx context.Context) error {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()

	pending := c.snapshot()
	var firstErr error
	for topic, rid := range pending {
		if err := c.store.SaveReplay(ctx, topic, rid); err != nil {
			c.metrics.commits.WithLabelValues("error").Inc()
			c.markDirty(topic)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		c.metrics.commits.WithLabelValues("ok").Inc()
	}
	return firstErr
}

// snapshot returns the dirty cursors and marks them clean.
func (c *Committer) snapshot() map[string][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make(map[string][]byte)
	for topic, t := range c.topics {
		if t.dirty {
			out[topic] = t.replayID
			t.dirty = false
		}
	}
	return out
}

// markDirty re-queues a topic whose save failed. The cursor itself only ever
// moves forward, so whatever it holds now is at least as new as the value
// that failed to save.
func (c *Committer) markDirty(topic string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.topics[topic].dirty = true
}

type committerMetrics struct {
	commits *prometheus.CounterVec
}

func newCommitterMetrics(reg prometheus.Registerer) committerMetrics {
	m := committerMetrics{
		commits: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "replay_cursor_commits_total",
				Help: "Number of replay cursor writes to storage, labelled by result.",
			},
			[]string{"result"},
		),
	}
	if reg != nil {
		reg.MustRegister(m.commits)
	}
	return m
}
