// Command processor is the Salesforce Pub/Sub event processor entry point.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/app"
	"github.com/AndreyZubov/pubsub-event-processor/internal/config"
	applog "github.com/AndreyZubov/pubsub-event-processor/internal/log"
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	a := app.New(cfg, logger)
	if err := a.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("app failed", zap.Error(err))
		return err
	}

	logger.Info("shutdown complete")
	return nil
}
