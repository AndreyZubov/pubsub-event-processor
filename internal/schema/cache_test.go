package schema

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const sampleSchemaJSON = `{
  "type": "record",
  "name": "OrderEvent",
  "fields": [
    {"name": "OrderId", "type": "string"},
    {"name": "Amount", "type": "double"}
  ]
}`

func newCounting(json string) (Source, *atomic.Int64) {
	var calls atomic.Int64
	src := func(_ context.Context, _ string) (string, error) {
		calls.Add(1)
		return json, nil
	}
	return src, &calls
}

func TestCache_FetchesOnFirstCall(t *testing.T) {
	src, calls := newCounting(sampleSchemaJSON)
	c := NewCache(src, prometheus.NewRegistry())

	s, err := c.Get(context.Background(), "id-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if s == nil {
		t.Fatal("nil schema")
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("fetches: got %d, want 1", got)
	}
}

func TestCache_HitsCacheOnSubsequentCalls(t *testing.T) {
	src, calls := newCounting(sampleSchemaJSON)
	c := NewCache(src, prometheus.NewRegistry())

	for range 5 {
		if _, err := c.Get(context.Background(), "id-1"); err != nil {
			t.Fatalf("Get: %v", err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("fetches: got %d, want 1 (cached)", got)
	}
}

func TestCache_ConcurrentGetsDeduped(t *testing.T) {
	const goroutines = 32

	var calls atomic.Int64
	src := func(_ context.Context, _ string) (string, error) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		return sampleSchemaJSON, nil
	}
	c := NewCache(src, prometheus.NewRegistry())

	var wg sync.WaitGroup
	wg.Add(goroutines)
	errs := make(chan error, goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			if _, err := c.Get(context.Background(), "id-1"); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("Get: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("fetches: got %d, want 1 (singleflight dedup)", got)
	}
}

func TestCache_DifferentIDsAreSeparate(t *testing.T) {
	src, calls := newCounting(sampleSchemaJSON)
	c := NewCache(src, prometheus.NewRegistry())

	for _, id := range []string{"id-1", "id-2", "id-3"} {
		if _, err := c.Get(context.Background(), id); err != nil {
			t.Fatalf("Get %s: %v", id, err)
		}
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("fetches: got %d, want 3", got)
	}
}

func TestCache_FetchErrorIsNotCached(t *testing.T) {
	var calls atomic.Int64
	src := func(_ context.Context, _ string) (string, error) {
		n := calls.Add(1)
		if n == 1 {
			return "", errors.New("upstream timeout")
		}
		return sampleSchemaJSON, nil
	}
	c := NewCache(src, prometheus.NewRegistry())

	_, err := c.Get(context.Background(), "id-1")
	if err == nil {
		t.Fatal("expected error on first call")
	}
	if !strings.Contains(err.Error(), "upstream timeout") {
		t.Errorf("error should wrap upstream: %v", err)
	}

	s, err := c.Get(context.Background(), "id-1")
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if s == nil {
		t.Fatal("nil schema on retry")
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("fetches: got %d, want 2 (error not cached)", got)
	}
}

func TestCache_InvalidJSONErrors(t *testing.T) {
	src := func(_ context.Context, _ string) (string, error) {
		return "not avro json", nil
	}
	c := NewCache(src, prometheus.NewRegistry())

	_, err := c.Get(context.Background(), "id-1")
	if err == nil {
		t.Fatal("expected parse error")
	}
	if !strings.Contains(err.Error(), "parse schema") {
		t.Errorf("expected parse error wrap, got %v", err)
	}
}
