// Package log builds the application's structured zap logger.
package log

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const serviceName = "pubsub-event-processor"

// New constructs a JSON zap logger writing to stdout at the given level.
// version is injected at build time via -ldflags "-X main.version=...".
func New(level, version string) (*zap.Logger, error) {
	return newCore(level, version, zapcore.AddSync(os.Stdout))
}

func newCore(level, version string, ws zapcore.WriteSyncer) (*zap.Logger, error) {
	lvl, err := zapcore.ParseLevel(level)
	if err != nil {
		return nil, fmt.Errorf("parse log level %q: %w", level, err)
	}

	encCfg := zap.NewProductionEncoderConfig()
	encCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	encCfg.TimeKey = "ts"
	encCfg.MessageKey = "msg"

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encCfg),
		ws,
		zap.NewAtomicLevelAt(lvl),
	)

	logger := zap.New(
		core,
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel),
		zap.Fields(
			zap.String("service", serviceName),
			zap.String("version", version),
		),
	)
	return logger, nil
}
