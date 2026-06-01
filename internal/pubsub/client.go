// Package pubsub provides a typed gRPC client for the Salesforce Pub/Sub API,
// wrapping the generated proto stubs with authentication, observability, and
// graceful retry for unary calls.
package pubsub

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/AndreyZubov/pubsub-event-processor/internal/auth"
	"github.com/AndreyZubov/pubsub-event-processor/internal/config"
	salesforcepb "github.com/AndreyZubov/pubsub-event-processor/proto/salesforce"
)

// ErrTopicNotFound is returned by GetTopic when the topic does not exist or the
// caller is not allowed to see it.
var ErrTopicNotFound = errors.New("topic not found")

// ErrSchemaNotFound is returned by GetSchema when the schema ID is unknown.
var ErrSchemaNotFound = errors.New("schema not found")

// Client is a typed wrapper around the generated PubSub gRPC client.
// It owns the underlying connection and must be closed when no longer needed.
type Client struct {
	conn *grpc.ClientConn
	grpc salesforcepb.PubSubClient
	log  *zap.Logger
}

// Dial constructs a Client connected to cfg.Endpoint using TLS, keepalive, and
// per-RPC credentials sourced from tp. The unary interceptor adds metrics,
// logging, and retry on transient errors. The connection is established lazily
// on first RPC.
func Dial(
	cfg config.PubSubConfig,
	tp auth.TokenProvider,
	log *zap.Logger,
	reg prometheus.Registerer,
) (*Client, error) {
	metrics := newClientMetrics(reg)

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
		})),
		grpc.WithPerRPCCredentials(newPerRPCCredentials(tp)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithUnaryInterceptor(newUnaryInterceptor(log, metrics)),
	}

	conn, err := grpc.NewClient(cfg.Endpoint, opts...)
	if err != nil {
		return nil, fmt.Errorf("grpc new client: %w", err)
	}
	return newClient(conn, log), nil
}

// newClient builds a Client around an existing connection. Used by Dial and by tests.
func newClient(conn *grpc.ClientConn, log *zap.Logger) *Client {
	return &Client{
		conn: conn,
		grpc: salesforcepb.NewPubSubClient(conn),
		log:  log,
	}
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// GetTopic returns metadata for the given topic name. Returns ErrTopicNotFound
// (wrapped) if the topic does not exist or is not accessible.
func (c *Client) GetTopic(ctx context.Context, name string) (*salesforcepb.TopicInfo, error) {
	info, err := c.grpc.GetTopic(ctx, &salesforcepb.TopicRequest{TopicName: name})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, fmt.Errorf("%w: %s", ErrTopicNotFound, name)
		}
		return nil, fmt.Errorf("GetTopic %q: %w", name, err)
	}
	return info, nil
}

// Subscribe opens the bidirectional Subscribe stream. The caller drives
// flow control by sending FetchRequest messages and reads ConsumerEvents
// via the returned stream.
func (c *Client) Subscribe(ctx context.Context) (salesforcepb.PubSub_SubscribeClient, error) {
	return c.grpc.Subscribe(ctx)
}

// GetSchema returns the Avro schema for the given schema ID. Returns
// ErrSchemaNotFound (wrapped) if the schema is unknown.
func (c *Client) GetSchema(ctx context.Context, schemaID string) (*salesforcepb.SchemaInfo, error) {
	info, err := c.grpc.GetSchema(ctx, &salesforcepb.SchemaRequest{SchemaId: schemaID})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, fmt.Errorf("%w: %s", ErrSchemaNotFound, schemaID)
		}
		return nil, fmt.Errorf("GetSchema %q: %w", schemaID, err)
	}
	return info, nil
}

type clientMetrics struct {
	rpcDuration *prometheus.HistogramVec
	rpcTotal    *prometheus.CounterVec
	rpcRetries  *prometheus.CounterVec
}

func newClientMetrics(reg prometheus.Registerer) clientMetrics {
	m := clientMetrics{
		rpcDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "pubsub_grpc_rpc_duration_seconds",
				Help:    "Duration of Salesforce Pub/Sub gRPC calls.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"method"},
		),
		rpcTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "pubsub_grpc_rpc_total",
				Help: "Total Salesforce Pub/Sub gRPC calls by method and final status code.",
			},
			[]string{"method", "code"},
		),
		rpcRetries: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "pubsub_grpc_rpc_retries_total",
				Help: "Retries performed by the unary interceptor, labelled by method and retried code.",
			},
			[]string{"method", "code"},
		),
	}
	if reg != nil {
		reg.MustRegister(m.rpcDuration, m.rpcTotal, m.rpcRetries)
	}
	return m
}
