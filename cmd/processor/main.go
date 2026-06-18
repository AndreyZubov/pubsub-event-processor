// Command processor is the Salesforce Pub/Sub event processor entry point.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/app"
	"github.com/AndreyZubov/pubsub-event-processor/internal/config"
	applog "github.com/AndreyZubov/pubsub-event-processor/internal/log"
	"github.com/AndreyZubov/pubsub-event-processor/internal/storage"
)

// version is injected at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		return err
	}

	logger, err := applog.New(cfg.LogLevel, version)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger: %v\n", err)
		return err
	}
	defer func() { _ = logger.Sync() }()

	if err := storage.Migrate(cfg.Database.URL, logger); err != nil {
		logger.Error("migrate", zap.Error(err))
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	a, err := app.New(cfg, logger, prometheus.DefaultRegisterer)
	if err != nil {
		logger.Error("app init failed", zap.Error(err))
		return err
	}
	defer func() { _ = a.Close() }()

	if err := a.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("app failed", zap.Error(err))
		return err
	}

	logger.Info("shutdown complete")
	return nil
}
