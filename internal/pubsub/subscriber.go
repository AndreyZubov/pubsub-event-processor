package pubsub

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc/status"

	salesforcepb "github.com/AndreyZubov/pubsub-event-processor/proto/salesforce"
)

const (
	subscriberOutBuffer = 1024
	reconnectMinDelay   = 1 * time.Second
	reconnectMaxDelay   = 30 * time.Second
)

// RawEvent is a single Salesforce event received over the Subscribe stream,
// carried with the topic that produced it for downstream multiplexing.
type RawEvent struct {
	Topic      string
	Event      *salesforcepb.ConsumerEvent
	ReceivedAt time.Time
}

// Subscriber owns one Subscribe stream for one topic. It pushes received events
// onto Out() and accepts flow-control replenishment signals via Ack().
type Subscriber struct {
	client    *Client
	topic     string
	batchSize int32
	log       *zap.Logger
	metrics   subscriberMetrics

	out chan RawEvent

	pendingAck atomic.Int64
	ackWake    chan struct{}
}

// NewSubscriber constructs a subscriber for a single topic. batchSize is the
// number of events requested per FetchRequest (initial and replenishment).
// batchSize is expected to be a small positive integer from config (validated
// >= 1); values exceeding int32 are clamped.
func NewSubscriber(client *Client, topic string, batchSize int, log *zap.Logger, reg prometheus.Registerer) *Subscriber {
	return &Subscriber{
		client:    client,
		topic:     topic,
		batchSize: int32(clampToInt32(int64(batchSize))), //nolint:gosec // clamped above to int32 range
		log:       log.With(zap.String("topic", topic)),
		metrics:   newSubscriberMetrics(reg),
		out:       make(chan RawEvent, subscriberOutBuffer),
		ackWake:   make(chan struct{}, 1),
	}
}

const maxInt32 = 1<<31 - 1

func clampToInt32(n int64) int64 {
	if n > maxInt32 {
		return maxInt32
	}
	if n < 0 {
		return 0
	}
	return n
}

// Out returns the read-only channel of received events. Downstream consumers
// must read from it to keep the stream flowing; the channel is closed when
// Run exits.
func (s *Subscriber) Out() <-chan RawEvent { return s.out }

// Topic returns the Salesforce topic this subscriber is bound to.
func (s *Subscriber) Topic() string { return s.topic }

// Ack signals that n events have been processed downstream and the subscriber
// should request n more from the server. Calls coalesce until the sender wakes.
func (s *Subscriber) Ack(n int) {
	if n <= 0 {
		return
	}
	s.pendingAck.Add(int64(n))
	select {
	case s.ackWake <- struct{}{}:
	default:
	}
}

// Run opens the Subscribe stream and pumps events until ctx is canceled.
// Stream errors trigger reconnects with exponential backoff and jitter.
func (s *Subscriber) Run(ctx context.Context) error {
	defer close(s.out)

	attempt := 0
	for {
		err := s.runOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			s.metrics.reconnects.WithLabelValues(s.topic, reasonFromErr(err)).Inc()
			s.log.Warn("subscribe stream ended; will reconnect",
				zap.Error(err),
				zap.String("reason", reasonFromErr(err)),
			)
		}

		delay := reconnectDelay(attempt)
		attempt++
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
	}
}

func (s *Subscriber) runOnce(ctx context.Context) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := s.client.Subscribe(streamCtx)
	if err != nil {
		return fmt.Errorf("open subscribe stream: %w", err)
	}

	s.metrics.streamOpen.WithLabelValues(s.topic).Set(1)
	defer s.metrics.streamOpen.WithLabelValues(s.topic).Set(0)

	initial := &salesforcepb.FetchRequest{
		TopicName:    s.topic,
		ReplayPreset: salesforcepb.ReplayPreset_LATEST,
		NumRequested: s.batchSize,
	}
	if err := stream.Send(initial); err != nil {
		return fmt.Errorf("send initial fetch: %w", err)
	}
	s.log.Info("subscribe stream opened", zap.Int32("batch_size", s.batchSize))

	senderErr := make(chan error, 1)
	receiverErr := make(chan error, 1)
	go func() { senderErr <- s.runSender(streamCtx, stream) }()
	go func() { receiverErr <- s.runReceiver(streamCtx, stream) }()

	select {
	case err := <-senderErr:
		return err
	case err := <-receiverErr:
		return err
	case <-ctx.Done():
		return nil
	}
}

func (s *Subscriber) runSender(ctx context.Context, stream salesforcepb.PubSub_SubscribeClient) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.ackWake:
			n := s.pendingAck.Swap(0)
			if n <= 0 {
				continue
			}
			ask := int32(clampToInt32(n)) //nolint:gosec // clamped above to int32 range
			if err := stream.Send(&salesforcepb.FetchRequest{NumRequested: ask}); err != nil {
				return fmt.Errorf("send replenish: %w", err)
			}
		}
	}
}

func (s *Subscriber) runReceiver(ctx context.Context, stream salesforcepb.PubSub_SubscribeClient) error {
	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		for _, e := range resp.GetEvents() {
			select {
			case <-ctx.Done():
				return nil
			case s.out <- RawEvent{Topic: s.topic, Event: e, ReceivedAt: time.Now()}:
				s.metrics.eventsReceived.WithLabelValues(s.topic).Inc()
			}
		}
	}
}

func reconnectDelay(attempt int) time.Duration {
	d := reconnectMinDelay << attempt
	if d > reconnectMaxDelay || d <= 0 {
		d = reconnectMaxDelay
	}
	spread := time.Duration(float64(d) * 0.25)
	if spread <= 0 {
		return d
	}
	jitter := time.Duration(rand.Int64N(int64(2*spread))) - spread //nolint:gosec // jitter only needs to spread reconnects, not be cryptographic
	return d + jitter
}

func reasonFromErr(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, io.EOF):
		return "eof"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return status.Code(err).String()
	}
}

type subscriberMetrics struct {
	eventsReceived *prometheus.CounterVec
	reconnects     *prometheus.CounterVec
	streamOpen     *prometheus.GaugeVec
}

func newSubscriberMetrics(reg prometheus.Registerer) subscriberMetrics {
	m := subscriberMetrics{
		eventsReceived: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "pubsub_events_received_total",
				Help: "Number of events received from Salesforce Pub/Sub per topic.",
			},
			[]string{"topic"},
		),
		reconnects: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "pubsub_reconnects_total",
				Help: "Number of subscribe-stream reconnect attempts, labelled by topic and reason.",
			},
			[]string{"topic", "reason"},
		),
		streamOpen: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pubsub_stream_open",
				Help: "1 if the Subscribe stream is currently open for the topic, 0 otherwise.",
			},
			[]string{"topic"},
		),
	}
	if reg != nil {
		reg.MustRegister(m.eventsReceived, m.reconnects, m.streamOpen)
	}
	return m
}
