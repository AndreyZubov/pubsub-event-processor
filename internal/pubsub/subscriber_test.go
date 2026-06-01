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
	sub := NewSubscriber(client, "/event/Test__e", 10, zap.NewNop(), prometheus.NewRegistry())

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
	sub := NewSubscriber(client, "/event/X", 50, zap.NewNop(), prometheus.NewRegistry())

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
	sub := NewSubscriber(client, "/event/X", 5, zap.NewNop(), prometheus.NewRegistry())

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
	sub := NewSubscriber(client, "/event/X", 5, zap.NewNop(), prometheus.NewRegistry())

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
