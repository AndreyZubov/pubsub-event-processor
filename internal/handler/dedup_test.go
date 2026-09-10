package handler

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/AndreyZubov/pubsub-event-processor/internal/event"
)

// fakeCache is an in-memory SeenCache with injectable failures.
type fakeCache struct {
	seen     map[string]bool
	seenErr  error
	markErr  error
	marked   []string
	lookups  int
	markCall int
}

func newFakeCache() *fakeCache { return &fakeCache{seen: map[string]bool{}} }

func (f *fakeCache) Seen(_ context.Context, eventID string) (bool, error) {
	f.lookups++
	if f.seenErr != nil {
		return false, f.seenErr
	}
	return f.seen[eventID], nil
}

func (f *fakeCache) MarkSeen(_ context.Context, eventID string) error {
	f.markCall++
	if f.markErr != nil {
		return f.markErr
	}
	f.seen[eventID] = true
	f.marked = append(f.marked, eventID)
	return nil
}

// countingHandler stands in for the wrapped chain.
type countingHandler struct {
	calls int
	err   error
}

func (c *countingHandler) Handle(_ context.Context, _ event.DecodedEvent) error {
	c.calls++
	return c.err
}

func TestDedup_FirstEventRunsChainAndIsMarked(t *testing.T) {
	cache, next := newFakeCache(), &countingHandler{}
	h := NewDedup(cache, next, zaptest.NewLogger(t))

	require.NoError(t, h.Handle(context.Background(), testEvent()))

	assert.Equal(t, 1, next.calls)
	assert.Equal(t, []string{"evt-1"}, cache.marked)
}

func TestDedup_SecondEventSkipsChain(t *testing.T) {
	cache, next := newFakeCache(), &countingHandler{}
	h := NewDedup(cache, next, zaptest.NewLogger(t))

	require.NoError(t, h.Handle(context.Background(), testEvent()))
	require.NoError(t, h.Handle(context.Background(), testEvent()))

	assert.Equal(t, 1, next.calls, "the chain must run only once for one event")
	assert.Equal(t, 1, cache.markCall, "no second write for a cached event")
}

// The reason this is a decorator and not a chain step: a failure anywhere
// downstream must leave the event unmarked, so redelivery retries the stage
// that failed instead of skipping it.
func TestDedup_FailedChainIsNotMarked(t *testing.T) {
	sentinel := errors.New("kafka unreachable")
	cache, next := newFakeCache(), &countingHandler{err: sentinel}
	h := NewDedup(cache, next, zaptest.NewLogger(t))

	err := h.Handle(context.Background(), testEvent())

	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
	assert.Empty(t, cache.marked, "a failed event must not be remembered")
	assert.Zero(t, cache.markCall)

	// Redelivery must run the chain again rather than skip it.
	next.err = nil
	require.NoError(t, h.Handle(context.Background(), testEvent()))
	assert.Equal(t, 2, next.calls)
	assert.Equal(t, []string{"evt-1"}, cache.marked)
}

// Redis down must make the service slower, never wrong.
func TestDedup_LookupFailureFallsThrough(t *testing.T) {
	cache, next := newFakeCache(), &countingHandler{}
	cache.seenErr = errors.New("connection refused")
	h := NewDedup(cache, next, zaptest.NewLogger(t))

	require.NoError(t, h.Handle(context.Background(), testEvent()))

	assert.Equal(t, 1, next.calls, "an unavailable cache must not block processing")
}

// Losing a cache write is cheaper than reprocessing the event, so it must not
// fail the handler.
func TestDedup_MarkFailureDoesNotFailTheEvent(t *testing.T) {
	cache, next := newFakeCache(), &countingHandler{}
	cache.markErr = errors.New("connection refused")
	h := NewDedup(cache, next, zaptest.NewLogger(t))

	assert.NoError(t, h.Handle(context.Background(), testEvent()))
	assert.Equal(t, 1, next.calls)
}

// The full pipeline shape: dedup wrapping persist and forward.
func TestDedup_WrapsTheWholeChain(t *testing.T) {
	var seen []string
	cache := newFakeCache()
	chain := NewChain(
		&recordingHandler{name: "persist", seen: &seen},
		&recordingHandler{name: "forward", seen: &seen},
	)
	h := NewDedup(cache, chain, zaptest.NewLogger(t))

	require.NoError(t, h.Handle(context.Background(), testEvent()))
	require.NoError(t, h.Handle(context.Background(), testEvent()))

	assert.Equal(t, []string{"persist", "forward"}, seen, "the second pass must run nothing")
}
