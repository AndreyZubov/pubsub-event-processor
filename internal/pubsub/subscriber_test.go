package pubsub

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	salesforcepb "github.com/AndreyZubov/pubsub-event-processor/proto/salesforce"
)

type fakeSubServer struct {
	salesforcepb.UnimplementedPubSubServer

	mu               sync.Mutex
	calls            int
	receivedRequests []*salesforcepb.FetchRequest

	behavior func(srv *fakeSubServer, stream salesforcepb.PubSub_SubscribeServer) error
}

func (f *fakeSubServer) Subscribe(stream salesforcepb.PubSub_SubscribeServer) error {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return f.behavior(f, stream)
}

func (f *fakeSubServer) recordRequest(r *salesforcepb.FetchRequest) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.receivedRequests = append(f.receivedRequests, r)
}

func (f *fakeSubServer) snapshotRequests() []*salesforcepb.FetchRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*salesforcepb.FetchRequest, len(f.receivedRequests))
	copy(out, f.receivedRequests)
	return out
}

func (f *fakeSubServer) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type noCursor struct{}

func (noCursor) LoadReplay(context.Context, string) ([]byte, error) { return nil, nil } //nolint:nilnil // matches CursorLoader's "never committed" contract

type fixedCursor struct {
	mu    sync.Mutex
	rid   []byte
	err   error
	calls int
}

func (f *fixedCursor) LoadReplay(context.Context, string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.rid, f.err
}

func (f *fixedCursor) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fixedCursor) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func newBufconnSubClient(t *testing.T, fake *fakeSubServer) *Client {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	salesforcepb.RegisterPubSubServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return newClient(conn, zap.NewNop())
}

func TestSubscriber_ReceivesEventsAndInitialFetchRequest(t *testing.T) {
	fake := &fakeSubServer{}
	fake.behavior = func(srv *fakeSubServer, stream salesforcepb.PubSub_SubscribeServer) error {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		srv.recordRequest(req)
		for i := range 5 {
			err := stream.Send(&salesforcepb.FetchResponse{
				Events: []*salesforcepb.ConsumerEvent{{
					Event:    &salesforcepb.ProducerEvent{Id: fmt.Sprintf("evt-%d", i)},
					ReplayId: []byte{byte(i)},
				}},
				LatestReplayId: []byte{byte(i)},
			})
			if err != nil {
				return err
			}
		}
		<-stream.Context().Done()
		return nil
	}

	client := newBufconnSubClient(t, fake)
	sub := NewSubscriber(client, "/event/Test__e", 10, noCursor{}, zap.NewNop(), prometheus.NewRegistry())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sub.Run(ctx) }()

	received := make([]string, 0, 5)
	for range 5 {
		select {
		case ev := <-sub.Out():
			received = append(received, ev.Event.GetEvent().GetId())
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out after %d events", len(received))
		}
	}

	cancel()
	<-done

	if len(received) != 5 {
		t.Errorf("events: got %d, want 5", len(received))
	}
	for i, id := range received {
		want := fmt.Sprintf("evt-%d", i)
		if id != want {
			t.Errorf("event %d: got %q, want %q", i, id, want)
		}
	}

	reqs := fake.snapshotRequests()
	if len(reqs) < 1 {
		t.Fatal("no FetchRequests recorded")
	}
	initial := reqs[0]
	if initial.GetTopicName() != "/event/Test__e" {
		t.Errorf("initial topic: %q", initial.GetTopicName())
	}
	if initial.GetNumRequested() != 10 {
		t.Errorf("initial NumRequested: %d", initial.GetNumRequested())
	}
	if initial.GetReplayPreset() != salesforcepb.ReplayPreset_LATEST {
		t.Errorf("initial ReplayPreset: %v", initial.GetReplayPreset())
	}
}

func TestSubscriber_SendsReplenishOnAck(t *testing.T) {
	fake := &fakeSubServer{}
	gotReplenish := make(chan int32, 4)

	fake.behavior = func(srv *fakeSubServer, stream salesforcepb.PubSub_SubscribeServer) error {
		first, err := stream.Recv()
		if err != nil {
			return err
		}
		srv.recordRequest(first)

		// Background reader for subsequent FetchRequests.
		errCh := make(chan error, 1)
		go func() {
			for {
				req, err := stream.Recv()
				if err != nil {
					errCh <- err
					return
				}
				srv.recordRequest(req)
				select {
				case gotReplenish <- req.GetNumRequested():
				default:
				}
			}
		}()

		select {
		case <-stream.Context().Done():
			return nil
		case <-errCh:
			return nil
		}
	}

	client := newBufconnSubClient(t, fake)
	sub := NewSubscriber(client, "/event/X", 50, noCursor{}, zap.NewNop(), prometheus.NewRegistry())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sub.Run(ctx) }()

	time.Sleep(100 * time.Millisecond) // let initial FetchRequest land
	sub.Ack(7)

	select {
	case n := <-gotReplenish:
		if n != 7 {
			t.Errorf("replenish NumRequested: got %d, want 7", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no replenish FetchRequest observed")
	}

	cancel()
	<-done
}

func TestSubscriber_ReconnectsAfterStreamError(t *testing.T) {
	fake := &fakeSubServer{}
	var firstCall atomic.Bool

	fake.behavior = func(_ *fakeSubServer, stream salesforcepb.PubSub_SubscribeServer) error {
		_, err := stream.Recv()
		if err != nil {
			return err
		}
		if firstCall.CompareAndSwap(false, true) {
			return status.Error(codes.Unavailable, "simulated transient")
		}
		// On subsequent calls, send one event then keep stream open.
		_ = stream.Send(&salesforcepb.FetchResponse{
			Events: []*salesforcepb.ConsumerEvent{{
				Event:    &salesforcepb.ProducerEvent{Id: "after-reconnect"},
				ReplayId: []byte{0xFF},
			}},
		})
		<-stream.Context().Done()
		return nil
	}

	client := newBufconnSubClient(t, fake)
	sub := NewSubscriber(client, "/event/X", 5, noCursor{}, zap.NewNop(), prometheus.NewRegistry())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sub.Run(ctx) }()

	select {
	case ev := <-sub.Out():
		if ev.Event.GetEvent().GetId() != "after-reconnect" {
			t.Errorf("event after reconnect: %q", ev.Event.GetEvent().GetId())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no event observed after reconnect; calls=%d", fake.callCount())
	}

	cancel()
	<-done

	if c := fake.callCount(); c < 2 {
		t.Errorf("expected at least 2 Subscribe calls (reconnect), got %d", c)
	}
}

func TestSubscriber_StopsCleanlyOnCtxCancel(t *testing.T) {
	fake := &fakeSubServer{}
	fake.behavior = func(_ *fakeSubServer, stream salesforcepb.PubSub_SubscribeServer) error {
		_, err := stream.Recv()
		if err != nil {
			return err
		}
		<-stream.Context().Done()
		return nil
	}

	client := newBufconnSubClient(t, fake)
	sub := NewSubscriber(client, "/event/X", 5, noCursor{}, zap.NewNop(), prometheus.NewRegistry())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sub.Run(ctx) }()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}

	// Out channel should be closed by Run.
	if _, open := <-sub.Out(); open {
		t.Error("Out channel should be closed after Run returns")
	}
}

func TestNewSubscriber_SharesMetricsAcrossTopics(t *testing.T) {
	reg := prometheus.NewRegistry()
	a := NewSubscriber(nil, "/event/A", 1, noCursor{}, zap.NewNop(), reg)
	b := NewSubscriber(nil, "/event/B", 1, noCursor{}, zap.NewNop(), reg)

	a.metrics.eventsReceived.WithLabelValues("/event/A").Inc()
	b.metrics.eventsReceived.WithLabelValues("/event/B").Inc()

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == "pubsub_events_received_total" {
			if n := len(f.GetMetric()); n != 2 {
				t.Errorf("series: got %d, want 2 (one per topic)", n)
			}
			return
		}
	}
	t.Error("pubsub_events_received_total not registered")
}

// recordFirstRequestThenHold records the stream's initial FetchRequest and
// keeps the stream open until the client goes away.
func recordFirstRequestThenHold(srv *fakeSubServer, stream salesforcepb.PubSub_SubscribeServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	srv.recordRequest(req)
	<-stream.Context().Done()
	return nil
}

func waitForRequests(t *testing.T, fake *fakeSubServer, n int) []*salesforcepb.FetchRequest {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		if reqs := fake.snapshotRequests(); len(reqs) >= n {
			return reqs
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %d FetchRequests", n)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestSubscriber_FirstStreamResumesFromCommittedCursor(t *testing.T) {
	fake := &fakeSubServer{behavior: recordFirstRequestThenHold}
	client := newBufconnSubClient(t, fake)
	cursor := &fixedCursor{rid: []byte{0xAB}}
	sub := NewSubscriber(client, "/event/X", 5, cursor, zap.NewNop(), prometheus.NewRegistry())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sub.Run(ctx) }()

	initial := waitForRequests(t, fake, 1)[0]
	cancel()
	<-done

	if initial.GetReplayPreset() != salesforcepb.ReplayPreset_CUSTOM {
		t.Errorf("ReplayPreset: got %v, want CUSTOM", initial.GetReplayPreset())
	}
	if string(initial.GetReplayId()) != string([]byte{0xAB}) {
		t.Errorf("ReplayId: got %v, want [0xAB]", initial.GetReplayId())
	}
}

func TestSubscriber_ReconnectResumesAfterLastReceivedEvent(t *testing.T) {
	fake := &fakeSubServer{}
	var firstCall atomic.Bool
	fake.behavior = func(srv *fakeSubServer, stream salesforcepb.PubSub_SubscribeServer) error {
		if !firstCall.CompareAndSwap(false, true) {
			return recordFirstRequestThenHold(srv, stream)
		}
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		srv.recordRequest(req)
		err = stream.Send(&salesforcepb.FetchResponse{
			Events: []*salesforcepb.ConsumerEvent{
				{Event: &salesforcepb.ProducerEvent{Id: "a"}, ReplayId: []byte{0x04}},
				{Event: &salesforcepb.ProducerEvent{Id: "b"}, ReplayId: []byte{0x05}},
			},
		})
		if err != nil {
			return err
		}
		return status.Error(codes.Unavailable, "simulated transient")
	}

	client := newBufconnSubClient(t, fake)
	cursor := &fixedCursor{rid: []byte{0x01}}
	sub := NewSubscriber(client, "/event/X", 5, cursor, zap.NewNop(), prometheus.NewRegistry())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sub.Run(ctx) }()

	var seqs []uint64
	for range 2 {
		select {
		case ev := <-sub.Out():
			seqs = append(seqs, ev.Seq)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for events")
		}
	}

	reqs := waitForRequests(t, fake, 2)
	cancel()
	<-done

	if seqs[0] != 1 || seqs[1] != 2 {
		t.Errorf("seqs: got %v, want [1 2]", seqs)
	}
	resumed := reqs[1]
	if resumed.GetReplayPreset() != salesforcepb.ReplayPreset_CUSTOM {
		t.Errorf("reconnect ReplayPreset: got %v, want CUSTOM", resumed.GetReplayPreset())
	}
	if string(resumed.GetReplayId()) != string([]byte{0x05}) {
		t.Errorf("reconnect ReplayId: got %v, want last received [0x05]", resumed.GetReplayId())
	}
	if n := cursor.callCount(); n != 1 {
		t.Errorf("committed cursor loads: got %d, want 1 (reconnects use the in-memory position)", n)
	}
}

func TestSubscriber_DoesNotSubscribeUntilCursorLoads(t *testing.T) {
	fake := &fakeSubServer{behavior: recordFirstRequestThenHold}
	client := newBufconnSubClient(t, fake)
	cursor := &fixedCursor{err: fmt.Errorf("database unavailable")}
	sub := NewSubscriber(client, "/event/X", 5, cursor, zap.NewNop(), prometheus.NewRegistry())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sub.Run(ctx) }()

	time.Sleep(200 * time.Millisecond)
	if c := fake.callCount(); c != 0 {
		t.Fatalf("Subscribe calls while cursor is unavailable: got %d, want 0", c)
	}

	cursor.setErr(nil)
	initial := waitForRequests(t, fake, 1)[0]
	cancel()
	<-done

	if initial.GetReplayPreset() != salesforcepb.ReplayPreset_LATEST {
		t.Errorf("ReplayPreset with no committed cursor: got %v, want LATEST", initial.GetReplayPreset())
	}
}
