// Package storage owns the service's PostgreSQL persistence: schema migrations
// at startup and the data-access types used by the worker pipeline.
package storage

import (
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres" // registers the "postgres" scheme
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/migrations"
)

// Migrate applies all pending SQL migrations embedded in the binary against the
// database identified by dsn. Returning nil means the schema is at the latest
// version, including when nothing changed.
func Migrate(dsn string, log *zap.Logger) error {
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("init embedded migration source: %w", err)
	}

	m, err := migrate.NewWithSourceInstance("iofs", src, dsn)
	if err != nil {
		return fmt.Errorf("init migrate: %w", err)
	}
	defer func() {
		if srcErr, dbErr := m.Close(); srcErr != nil || dbErr != nil {
			log.Warn("close migrator", zap.NamedError("source", srcErr), zap.NamedError("database", dbErr))
		}
	}()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("apply migrations: %w", err)
	}

	version, dirty, vErr := m.Version()
	switch {
	case errors.Is(vErr, migrate.ErrNilVersion):
		log.Info("migrations applied", zap.String("version", "none"))
	case vErr != nil:
		log.Warn("could not read migration version", zap.Error(vErr))
	default:
		log.Info("migrations applied", zap.Uint("version", version), zap.Bool("dirty", dirty))
	}
	return nil
}
