// Package app wires the service's subsystems and owns their lifecycle.
package app

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/config"
	"github.com/AndreyZubov/pubsub-event-processor/internal/health"
	"github.com/AndreyZubov/pubsub-event-processor/internal/httpserver"
)

// App is the running service: wired subsystems with a shared lifecycle.
type App struct {
	cfg  *config.Config
	log  *zap.Logger
	http *httpserver.Server
}

// New constructs the App graph from configuration and a logger.
func New(cfg *config.Config, log *zap.Logger) *App {
	checkers := map[string]health.Checker{}
	return &App{
		cfg:  cfg,
		log:  log,
		http: httpserver.New(cfg.HTTP.Addr, log, checkers),
	}
}

// Run starts all subsystems and blocks until ctx is canceled or any subsystem fails.
// Returns nil on a clean shutdown, an error otherwise.
func (a *App) Run(ctx context.Context) error {
	a.log.Info("app starting",
		zap.String("log_level", a.cfg.LogLevel),
		zap.Strings("topics", a.cfg.PubSub.Topics),
		zap.Int("worker_count", a.cfg.Worker.Count),
	)

	if err := a.http.Run(ctx); err != nil {
		return fmt.Errorf("http server: %w", err)
	}

	a.log.Info("app stopped")
	return nil
}
