// Package app wires the service's subsystems and owns their lifecycle.
package app

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/AndreyZubov/pubsub-event-processor/internal/auth"
	"github.com/AndreyZubov/pubsub-event-processor/internal/config"
	"github.com/AndreyZubov/pubsub-event-processor/internal/event"
	"github.com/AndreyZubov/pubsub-event-processor/internal/health"
	"github.com/AndreyZubov/pubsub-event-processor/internal/httpserver"
	"github.com/AndreyZubov/pubsub-event-processor/internal/pubsub"
	"github.com/AndreyZubov/pubsub-event-processor/internal/schema"
)

// App is the running service: wired subsystems with a shared lifecycle.
type App struct {
	cfg    *config.Config
	log    *zap.Logger
	client *pubsub.Client
	cache  *schema.Cache
	subs   []*pubsub.Subscriber
	http   *httpserver.Server
}

// New constructs the App graph from configuration. reg receives all subsystem
// metrics; pass prometheus.DefaultRegisterer in production and a fresh
// prometheus.NewRegistry() in tests.
func New(cfg *config.Config, log *zap.Logger, reg prometheus.Registerer) (*App, error) {
	tp := auth.New(cfg.Salesforce, reg)

	client, err := pubsub.Dial(cfg.PubSub, tp, log, reg)
	if err != nil {
		return nil, fmt.Errorf("pubsub dial: %w", err)
	}

	cache := schema.NewCache(
		func(ctx context.Context, id string) (string, error) {
			info, err := client.GetSchema(ctx, id)
			if err != nil {
				return "", err
			}
			return info.GetSchemaJson(), nil
		},
		reg,
	)

	subs := make([]*pubsub.Subscriber, 0, len(cfg.PubSub.Topics))
	for _, topic := range cfg.PubSub.Topics {
		subs = append(subs, pubsub.NewSubscriber(client, topic, cfg.Worker.FlowBatchSize, log, reg))
	}

	checkers := map[string]health.Checker{
		"auth": health.NewAuthChecker(tp),
	}

	return &App{
		cfg:    cfg,
		log:    log,
		client: client,
		cache:  cache,
		subs:   subs,
		http:   httpserver.New(cfg.HTTP.Addr, log, checkers),
	}, nil
}

// Close releases resources held by the App. Safe to call multiple times.
func (a *App) Close() error {
	return a.client.Close()
}

// Run starts all subsystems and blocks until ctx is canceled or any subsystem
// fails. Returns nil on a clean shutdown, an error otherwise.
func (a *App) Run(ctx context.Context) error {
	a.log.Info("app starting",
		zap.String("log_level", a.cfg.LogLevel),
		zap.Strings("topics", a.cfg.PubSub.Topics),
		zap.Int("worker_count", a.cfg.Worker.Count),
	)

	a.discoverTopics(ctx)

	g, gctx := errgroup.WithContext(ctx)

	events := make(chan pubsub.RawEvent, a.cfg.Worker.FlowBatchSize*2)
	topicSubs := make(map[string]*pubsub.Subscriber, len(a.subs))
	for _, sub := range a.subs {
		topicSubs[sub.Topic()] = sub
	}

	for _, sub := range a.subs {
		g.Go(func() error { return sub.Run(gctx) })
	}

	var fanInWG sync.WaitGroup
	for _, sub := range a.subs {
		fanInWG.Add(1)
		go a.fanIn(gctx, &fanInWG, sub, events)
	}
	go func() {
		fanInWG.Wait()
		close(events)
	}()

	g.Go(func() error {
		a.consume(gctx, events, topicSubs)
		return nil
	})

	g.Go(func() error { return a.http.Run(gctx) })

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	a.log.Info("app stopped")
	return nil
}

// fanIn forwards events from one subscriber's Out channel onto the shared
// events channel. Exits when the source Out is closed or ctx is canceled.
func (a *App) fanIn(ctx context.Context, wg *sync.WaitGroup, sub *pubsub.Subscriber, out chan<- pubsub.RawEvent) {
	defer wg.Done()
	for e := range sub.Out() {
		select {
		case out <- e:
		case <-ctx.Done():
			return
		}
	}
}

// consume drains the shared events channel, decoding each event and logging
// the result. After successful processing the source subscriber is ack'd for
// flow-control replenishment.
func (a *App) consume(ctx context.Context, events <-chan pubsub.RawEvent, topicSubs map[string]*pubsub.Subscriber) {
	for {
		select {
		case <-ctx.Done():
			return
		case raw, ok := <-events:
			if !ok {
				return
			}
			a.processEvent(ctx, raw)
			if sub, ok := topicSubs[raw.Topic]; ok {
				sub.Ack(1)
			}
		}
	}
}

func (a *App) processEvent(ctx context.Context, raw pubsub.RawEvent) {
	schemaID := raw.Event.GetEvent().GetSchemaId()
	sch, err := a.cache.Get(ctx, schemaID)
	if err != nil {
		a.log.Warn("schema fetch failed",
			zap.String("topic", raw.Topic),
			zap.String("schema_id", schemaID),
			zap.Error(err),
		)
		return
	}

	payload, err := schema.Decode(sch, raw.Event.GetEvent().GetPayload())
	if err != nil {
		a.log.Warn("avro decode failed",
			zap.String("topic", raw.Topic),
			zap.String("schema_id", schemaID),
			zap.Error(err),
		)
		return
	}

	decoded := event.DecodedEvent{
		Topic:      raw.Topic,
		EventID:    raw.Event.GetEvent().GetId(),
		SchemaID:   schemaID,
		ReplayID:   raw.Event.GetReplayId(),
		Payload:    payload,
		ReceivedAt: raw.ReceivedAt,
	}

	a.log.Info("event decoded",
		zap.String("topic", decoded.Topic),
		zap.String("event_id", decoded.EventID),
		zap.String("schema_id", decoded.SchemaID),
		zap.Int("payload_fields", len(decoded.Payload)),
	)
}

// discoverTopics queries Salesforce for each configured topic and its schema,
// logging what it finds. Best-effort: errors are logged and do not abort startup.
func (a *App) discoverTopics(ctx context.Context) {
	for _, topic := range a.cfg.PubSub.Topics {
		info, err := a.client.GetTopic(ctx, topic)
		if err != nil {
			a.log.Warn("get topic failed",
				zap.String("topic", topic),
				zap.Error(err),
			)
			continue
		}
		a.log.Info("topic discovered",
			zap.String("topic", info.GetTopicName()),
			zap.String("schema_id", info.GetSchemaId()),
			zap.Bool("can_subscribe", info.GetCanSubscribe()),
			zap.Bool("can_publish", info.GetCanPublish()),
		)

		schemaInfo, err := a.client.GetSchema(ctx, info.GetSchemaId())
		if err != nil {
			a.log.Warn("get schema failed",
				zap.String("schema_id", info.GetSchemaId()),
				zap.Error(err),
			)
			continue
		}
		a.log.Info("schema fetched",
			zap.String("schema_id", schemaInfo.GetSchemaId()),
			zap.Int("schema_json_bytes", len(schemaInfo.GetSchemaJson())),
		)
	}
}
