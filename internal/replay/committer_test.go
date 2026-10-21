package replay_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/replay"
)

type fakeStore struct {
	mu    sync.Mutex
	saved map[string][]byte
	calls int
	err   error
}

func newFakeStore() *fakeStore { return &fakeStore{saved: make(map[string][]byte)} }

func (f *fakeStore) SaveReplay(_ context.Context, topic string, replayID []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return f.err
	}
	f.saved[topic] = replayID
	return nil
}

func (f *fakeStore) get(topic string) ([]byte, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.saved[topic], f.calls
}

func (f *fakeStore) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func newCommitter(store replay.CursorStore) *replay.Committer {
	return replay.NewCommitter(store, time.Hour, zap.NewNop(), prometheus.NewRegistry())
}

func flush(t *testing.T, c *replay.Committer) {
	t.Helper()
	if err := c.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func TestCommitter_InOrderCompletionAdvancesCursor(t *testing.T) {
	store := newFakeStore()
	c := newCommitter(store)

	c.Done("/event/A", 1, []byte{1})
	c.Done("/event/A", 2, []byte{2})
	flush(t, c)

	if got, _ := store.get("/event/A"); string(got) != string([]byte{2}) {
		t.Errorf("cursor: got %v, want [2]", got)
	}
}

func TestCommitter_GapHoldsCursorUntilFilled(t *testing.T) {
	store := newFakeStore()
	c := newCommitter(store)

	c.Done("/event/A", 1, []byte{1})
	c.Done("/event/A", 3, []byte{3})
	flush(t, c)
	if got, _ := store.get("/event/A"); string(got) != string([]byte{1}) {
		t.Fatalf("cursor must stop below the gap: got %v, want [1]", got)
	}

	c.Done("/event/A", 2, []byte{2})
	flush(t, c)
	if got, _ := store.get("/event/A"); string(got) != string([]byte{3}) {
		t.Errorf("cursor after gap filled: got %v, want [3]", got)
	}
}

func TestCommitter_NothingContiguousWritesNothing(t *testing.T) {
	store := newFakeStore()
	c := newCommitter(store)

	c.Done("/event/A", 2, []byte{2})
	flush(t, c)

	if got, calls := store.get("/event/A"); got != nil || calls != 0 {
		t.Errorf("cursor must not move while seq 1 is in flight: got %v after %d calls", got, calls)
	}
}

func TestCommitter_TopicsAreIndependent(t *testing.T) {
	store := newFakeStore()
	c := newCommitter(store)

	c.Done("/event/A", 1, []byte{0xA1})
	c.Done("/event/B", 2, []byte{0xB2})
	flush(t, c)

	if got, _ := store.get("/event/A"); string(got) != string([]byte{0xA1}) {
		t.Errorf("topic A: got %v", got)
	}
	if got, _ := store.get("/event/B"); got != nil {
		t.Errorf("topic B must wait for its own seq 1: got %v", got)
	}
}

func TestCommitter_UnchangedCursorIsNotRewritten(t *testing.T) {
	store := newFakeStore()
	c := newCommitter(store)

	c.Done("/event/A", 1, []byte{1})
	flush(t, c)
	flush(t, c)

	if _, calls := store.get("/event/A"); calls != 1 {
		t.Errorf("store calls: got %d, want 1", calls)
	}
}

func TestCommitter_FailedSaveIsRetried(t *testing.T) {
	store := newFakeStore()
	store.setErr(errors.New("db down"))
	c := newCommitter(store)

	c.Done("/event/A", 1, []byte{1})
	if err := c.Flush(context.Background()); err == nil {
		t.Fatal("expected flush error")
	}

	store.setErr(nil)
	flush(t, c)
	if got, _ := store.get("/event/A"); string(got) != string([]byte{1}) {
		t.Errorf("cursor after retry: got %v, want [1]", got)
	}
}

func TestCommitter_RunFlushesPeriodically(t *testing.T) {
	store := newFakeStore()
	c := replay.NewCommitter(store, 10*time.Millisecond, zap.NewNop(), prometheus.NewRegistry())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	c.Done("/event/A", 1, []byte{1})

	deadline := time.After(2 * time.Second)
	for {
		if got, _ := store.get("/event/A"); got != nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("cursor was not flushed by Run")
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run: %v", err)
	}
}
