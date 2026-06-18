// Package config loads and validates service configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"

	"github.com/caarlos0/env/v11"
)

// Config aggregates all runtime configuration loaded from the environment.
type Config struct {
	Salesforce SalesforceConfig
	PubSub     PubSubConfig
	Database   DatabaseConfig
	Sink       SinkConfig
	HTTP       HTTPConfig
	Worker     WorkerConfig
	LogLevel   string `env:"LOG_LEVEL" envDefault:"info"`
}

// SalesforceConfig holds OAuth credentials and the org login endpoint.
type SalesforceConfig struct {
	ClientID     string `env:"SF_CLIENT_ID,required"`
	ClientSecret string `env:"SF_CLIENT_SECRET,required"`
	LoginURL     string `env:"SF_LOGIN_URL"     envDefault:"https://login.salesforce.com"`
}

// PubSubConfig holds the Salesforce Pub/Sub API endpoint and the list of subscribed topics.
type PubSubConfig struct {
	Endpoint string   `env:"PUBSUB_ENDPOINT"      envDefault:"api.pubsub.salesforce.com:7443"`
	Topics   []string `env:"SF_TOPICS,required"   envSeparator:","`
}

// DatabaseConfig holds the Postgres connection DSN and pool sizing.
type DatabaseConfig struct {
	URL      string `env:"DATABASE_URL,required"`
	MaxConns int    `env:"DATABASE_MAX_CONNS" envDefault:"20"`
}

// SinkConfig holds optional downstream sink settings.
type SinkConfig struct {
	WebhookURL string `env:"SINK_WEBHOOK_URL"`
}

// HTTPConfig holds the admin HTTP server settings (health, readiness, metrics).
type HTTPConfig struct {
	Addr string `env:"HTTP_ADDR" envDefault:":8080"`
}

// WorkerConfig holds worker-pool sizing and flow-control parameters.
type WorkerConfig struct {
	Count         int `env:"WORKER_COUNT"     envDefault:"8"`
	FlowBatchSize int `env:"FLOW_BATCH_SIZE"  envDefault:"100"`
}

// Load reads environment variables into a Config and validates them.
func Load() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("parse env: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}
	return cfg, nil
}

// Validate checks invariants the env parser cannot express.
func (c *Config) Validate() error {
	var errs []error

	if c.Worker.Count < 1 {
		errs = append(errs, fmt.Errorf("WORKER_COUNT must be >= 1, got %d", c.Worker.Count))
	}
	if c.Worker.FlowBatchSize < 1 {
		errs = append(errs, fmt.Errorf("FLOW_BATCH_SIZE must be >= 1, got %d", c.Worker.FlowBatchSize))
	}
	if c.Database.MaxConns < 1 || c.Database.MaxConns > 1000 {
		errs = append(errs, fmt.Errorf("DATABASE_MAX_CONNS must be in 1..1000, got %d", c.Database.MaxConns))
	}

	if len(c.PubSub.Topics) == 0 {
		errs = append(errs, errors.New("SF_TOPICS must contain at least one topic"))
	}
	for i, t := range c.PubSub.Topics {
		if t == "" {
			errs = append(errs, fmt.Errorf("SF_TOPICS[%d] is empty", i))
		}
	}

	if _, _, err := net.SplitHostPort(c.PubSub.Endpoint); err != nil {
		errs = append(errs, fmt.Errorf("PUBSUB_ENDPOINT must be host:port: %w", err))
	}

	if u, err := url.Parse(c.Salesforce.LoginURL); err != nil || u.Scheme == "" || u.Host == "" {
		errs = append(errs, fmt.Errorf("SF_LOGIN_URL is not a valid absolute URL: %q", c.Salesforce.LoginURL))
	}

	if c.Sink.WebhookURL != "" {
		if u, err := url.Parse(c.Sink.WebhookURL); err != nil || u.Scheme == "" || u.Host == "" {
			errs = append(errs, fmt.Errorf("SINK_WEBHOOK_URL is not a valid absolute URL: %q", c.Sink.WebhookURL))
		}
	}

	return errors.Join(errs...)
}
