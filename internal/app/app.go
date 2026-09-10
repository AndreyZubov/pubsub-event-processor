// Package app wires the service's subsystems and owns their lifecycle.
package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/AndreyZubov/pubsub-event-processor/internal/auth"
	"github.com/AndreyZubov/pubsub-event-processor/internal/config"
	"github.com/AndreyZubov/pubsub-event-processor/internal/handler"
	"github.com/AndreyZubov/pubsub-event-processor/internal/health"
	"github.com/AndreyZubov/pubsub-event-processor/internal/httpserver"
	appkafka "github.com/AndreyZubov/pubsub-event-processor/internal/kafka"
	"github.com/AndreyZubov/pubsub-event-processor/internal/pubsub"
	"github.com/AndreyZubov/pubsub-event-processor/internal/rediscache"
	"github.com/AndreyZubov/pubsub-event-processor/internal/schema"
	"github.com/AndreyZubov/pubsub-event-processor/internal/storage"
	"github.com/AndreyZubov/pubsub-event-processor/internal/worker"
)

// dbCheckTimeout bounds the readiness probe's database ping.
const dbCheckTimeout = 3 * time.Second

// App is the running service: wired subsystems with a shared lifecycle.
type App struct {
	cfg      *config.Config
	log      *zap.Logger
	client   *pubsub.Client
	cache    *schema.Cache
	subs     []*pubsub.Subscriber
	pool     *worker.Pool
	http     *httpserver.Server
	store    *storage.Store
	producer *appkafka.Producer
	redis    *rediscache.Cache
}

// New constructs the App graph from configuration. reg receives all subsystem
// metrics; pass prometheus.DefaultRegisterer in production and a fresh
// prometheus.NewRegistry() in tests.
func New(ctx context.Context, cfg *config.Config, log *zap.Logger, reg prometheus.Registerer) (*App, error) {
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

	dbPool, err := storage.NewPool(ctx, cfg.Database.URL, cfg.Database.MaxConns)
	if err != nil {
		// Everything constructed so far owns a connection; release it before
		// returning, or a failed startup leaks the gRPC client.
		_ = client.Close()
		return nil, fmt.Errorf("database pool: %w", err)
	}
	store := storage.NewStore(dbPool)

	checkers := map[string]health.Checker{
		"auth":     health.NewAuthChecker(tp),
		"database": newDBChecker(store),
	}

	// Persistence first, forwarding second — see handler.Chain for why the
	// order is a correctness constraint rather than a preference.
	handlers := []handler.Handler{handler.NewPersist(store, log)}

	var producer *appkafka.Producer
	if cfg.Kafka.Enabled {
		producer, err = appkafka.New(cfg.Kafka, log, reg)
		if err != nil {
			store.Close()
			_ = client.Close()
			return nil, fmt.Errorf("kafka producer: %w", err)
		}
		if cfg.Kafka.CreateTopic {
			if err := producer.EnsureTopic(ctx); err != nil {
				producer.Close()
				store.Close()
				_ = client.Close()
				return nil, fmt.Errorf("ensure kafka topic: %w", err)
			}
		}

		handlers = append(handlers, handler.NewForward(producer, log))
		checkers["kafka"] = newKafkaChecker(producer)
		log.Info("kafka sink enabled",
			zap.Strings("brokers", cfg.Kafka.Brokers),
			zap.String("topic", cfg.Kafka.Topic),
		)
	}

	// The chain is what must fully succeed before an event counts as processed.
	var root handler.Handler = handler.NewChain(handlers...)

	// The dedup cache wraps the chain rather than joining it, so an event is
	// remembered only once every stage has succeeded.
	var dedup *rediscache.Cache
	if cfg.Redis.Enabled {
		dedup = rediscache.New(cfg.Redis, log, reg)
		root = handler.NewDedup(dedup, root, log)
		checkers["redis"] = newRedisChecker(dedup)
		log.Info("dedup cache enabled",
			zap.String("addr", cfg.Redis.Addr),
			zap.Duration("ttl", cfg.Redis.TTL),
		)
	}

	a := &App{
		cfg:      cfg,
		log:      log,
		client:   client,
		cache:    cache,
		subs:     subs,
		http:     httpserver.New(cfg.HTTP.Addr, log, checkers),
		store:    store,
		producer: producer,
		redis:    dedup,
	}
	a.pool = worker.New(cfg.Worker.Count, cache, root, a.ack, log, reg)
	return a, nil
}

// newRedisChecker reports cache reachability to the readiness probe.
//
// Readiness is gated even though the cache is optional: an unreachable cache
// means every event pays the full database cost, which is worth surfacing
// rather than hiding behind a green probe.
func newRedisChecker(c *rediscache.Cache) health.Checker {
	return health.CheckerFunc(func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, dbCheckTimeout)
		defer cancel()
		return c.Check(ctx)
	})
}

// newDBChecker reports database reachability to the readiness probe.
func newDBChecker(store *storage.Store) health.Checker {
	return health.CheckerFunc(func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, dbCheckTimeout)
		defer cancel()
		if err := store.Pool().Ping(ctx); err != nil {
			return fmt.Errorf("database ping: %w", err)
		}
		return nil
	})
}

// newKafkaChecker reports broker reachability to the readiness probe.
func newKafkaChecker(p *appkafka.Producer) health.Checker {
	return health.CheckerFunc(func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, dbCheckTimeout)
		defer cancel()
		return p.Check(ctx)
	})
}

// Close releases resources held by the App. Safe to call multiple times.
// Shutdown order mirrors construction in reverse: stop producing, then drop the
// database pool, then close the gRPC client.
func (a *App) Close() error {
	if a.redis != nil {
		if err := a.redis.Close(); err != nil {
			a.log.Warn("close redis", zap.Error(err))
		}
	}
	if a.producer != nil {
		a.producer.Close()
	}
	if a.store != nil {
		a.store.Close()
	}
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

	g.Go(func() error { return a.pool.Run(gctx, events) })

	g.Go(func() error { return a.http.Run(gctx) })

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	a.log.Info("app stopped")
	return nil
}

// ack is the worker pool's flow-control callback: it looks up the source
// subscriber by topic and signals N events have been processed.
func (a *App) ack(topic string, n int) {
	for _, sub := range a.subs {
		if sub.Topic() == topic {
			sub.Ack(n)
			return
		}
	}
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
