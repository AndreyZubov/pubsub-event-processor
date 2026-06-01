// Package app wires the service's subsystems and owns their lifecycle.
package app

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/auth"
	"github.com/AndreyZubov/pubsub-event-processor/internal/config"
	"github.com/AndreyZubov/pubsub-event-processor/internal/health"
	"github.com/AndreyZubov/pubsub-event-processor/internal/httpserver"
	"github.com/AndreyZubov/pubsub-event-processor/internal/pubsub"
)

// App is the running service: wired subsystems with a shared lifecycle.
type App struct {
	cfg    *config.Config
	log    *zap.Logger
	client *pubsub.Client
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

	checkers := map[string]health.Checker{
		"auth": health.NewAuthChecker(tp),
	}

	return &App{
		cfg:    cfg,
		log:    log,
		client: client,
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

	if err := a.http.Run(ctx); err != nil {
		return fmt.Errorf("http server: %w", err)
	}

	a.log.Info("app stopped")
	return nil
}

// discoverTopics queries Salesforce for each configured topic and its schema,
// logging what it finds. Best-effort: errors are logged and do not abort startup,
// so the service stays healthy even if Salesforce is temporarily slow.
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

		schema, err := a.client.GetSchema(ctx, info.GetSchemaId())
		if err != nil {
			a.log.Warn("get schema failed",
				zap.String("schema_id", info.GetSchemaId()),
				zap.Error(err),
			)
			continue
		}
		a.log.Info("schema fetched",
			zap.String("schema_id", schema.GetSchemaId()),
			zap.Int("schema_json_bytes", len(schema.GetSchemaJson())),
		)
	}
}
