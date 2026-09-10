package handler

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AndreyZubov/pubsub-event-processor/internal/event"
)

// recordingHandler notes that it ran and optionally fails.
type recordingHandler struct {
	name string
	err  error
	seen *[]string
}

func (r *recordingHandler) Handle(_ context.Context, _ event.DecodedEvent) error {
	*r.seen = append(*r.seen, r.name)
	return r.err
}

func TestChain_RunsHandlersInOrder(t *testing.T) {
	var seen []string
	c := NewChain(
		&recordingHandler{name: "persist", seen: &seen},
		&recordingHandler{name: "forward", seen: &seen},
	)

	require.NoError(t, c.Handle(context.Background(), testEvent()))
	assert.Equal(t, []string{"persist", "forward"}, seen)
}

// The chain must stop at the first failure: if persistence fails there is
// nothing durable to forward, and the event will be redelivered anyway.
func TestChain_StopsAtFirstFailure(t *testing.T) {
	var seen []string
	sentinel := errors.New("db down")
	c := NewChain(
		&recordingHandler{name: "persist", err: sentinel, seen: &seen},
		&recordingHandler{name: "forward", seen: &seen},
	)

	err := c.Handle(context.Background(), testEvent())

	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
	assert.Equal(t, []string{"persist"}, seen, "forward must not run after persist failed")
}

func TestChain_EmptyIsNoOp(t *testing.T) {
	assert.NoError(t, NewChain().Handle(context.Background(), testEvent()))
}
