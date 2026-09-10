// Package config loads and validates service configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/caarlos0/env/v11"
)

// Config aggregates all runtime configuration loaded from the environment.
type Config struct {
	Salesforce SalesforceConfig
	PubSub     PubSubConfig
	Database   DatabaseConfig
	Kafka      KafkaConfig
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

// KafkaConfig holds settings for the Kafka sink. Disabled by default so the
// service runs without a broker during local development.
type KafkaConfig struct {
	Enabled bool     `env:"KAFKA_ENABLED"  envDefault:"false"`
	Brokers []string `env:"KAFKA_BROKERS"  envSeparator:","`
	Topic   string   `env:"KAFKA_TOPIC"    envDefault:"salesforce.events"`
	// ClientID identifies this producer in broker logs and metrics.
	ClientID string `env:"KAFKA_CLIENT_ID" envDefault:"pubsub-event-processor"`
	// ProduceTimeout bounds a single synchronous produce, including retries.
	ProduceTimeout time.Duration `env:"KAFKA_PRODUCE_TIMEOUT" envDefault:"30s"`

	// CreateTopic makes the service create the topic at startup if it is
	// missing. Off by default: production clusters normally run with
	// auto.create.topics.enable=false and provision topics deliberately, with
	// partition counts and replication chosen for the workload. Enable it for
	// local development and demo environments.
	CreateTopic bool `env:"KAFKA_CREATE_TOPIC" envDefault:"false"`
	// TopicPartitions applies only when CreateTopic is true.
	TopicPartitions int32 `env:"KAFKA_TOPIC_PARTITIONS" envDefault:"3"`
	// TopicReplicationFactor applies only when CreateTopic is true. A single
	// replica is valid only for a single-broker development cluster.
	TopicReplicationFactor int16 `env:"KAFKA_TOPIC_REPLICATION_FACTOR" envDefault:"1"`
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

	if c.Kafka.Enabled {
		if len(c.Kafka.Brokers) == 0 {
			errs = append(errs, errors.New("KAFKA_BROKERS must be set when KAFKA_ENABLED is true"))
		}
		for i, b := range c.Kafka.Brokers {
			if _, _, err := net.SplitHostPort(b); err != nil {
				errs = append(errs, fmt.Errorf("KAFKA_BROKERS[%d] must be host:port: %w", i, err))
			}
		}
		// Not reachable through the environment — the parser substitutes the
		// default for an empty value — but it guards a Config built in code.
		if c.Kafka.Topic == "" {
			errs = append(errs, errors.New("KAFKA_TOPIC must not be empty when KAFKA_ENABLED is true"))
		}
		if c.Kafka.ProduceTimeout <= 0 {
			errs = append(errs, fmt.Errorf("KAFKA_PRODUCE_TIMEOUT must be > 0, got %s", c.Kafka.ProduceTimeout))
		}
		if c.Kafka.CreateTopic {
			if c.Kafka.TopicPartitions < 1 {
				errs = append(errs, fmt.Errorf("KAFKA_TOPIC_PARTITIONS must be >= 1, got %d", c.Kafka.TopicPartitions))
			}
			if c.Kafka.TopicReplicationFactor < 1 {
				errs = append(errs, fmt.Errorf("KAFKA_TOPIC_REPLICATION_FACTOR must be >= 1, got %d", c.Kafka.TopicReplicationFactor))
			}
		}
	}

	if c.Sink.WebhookURL != "" {
		if u, err := url.Parse(c.Sink.WebhookURL); err != nil || u.Scheme == "" || u.Host == "" {
			errs = append(errs, fmt.Errorf("SINK_WEBHOOK_URL is not a valid absolute URL: %q", c.Sink.WebhookURL))
		}
	}

	return errors.Join(errs...)
}
