package pubsubtest_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/AndreyZubov/pubsub-event-processor/internal/auth"
	"github.com/AndreyZubov/pubsub-event-processor/internal/config"
	"github.com/AndreyZubov/pubsub-event-processor/internal/pubsub"
	"github.com/AndreyZubov/pubsub-event-processor/internal/pubsub/pubsubtest"
	salesforcepb "github.com/AndreyZubov/pubsub-event-processor/proto/salesforce"
)

const (
	topic = "/event/Order_Event__e"

	schemaJSON = `{"type":"record","name":"Order_Event__e","fields":[` +
		`{"name":"Order_Number__c","type":"string"}]}`

	waitFor = 5 * time.Second
)

type cursor struct{ rid []byte }

func (c cursor) LoadReplay(context.Context, string) ([]byte, error) { return c.rid, nil }

func startOrg(t *testing.T) *pubsubtest.Org {
	t.Helper()
	org, err := pubsubtest.Start(pubsubtest.Options{})
	require.NoError(t, err)
	t.Cleanup(org.Close)
	require.NoError(t, org.AddTopic(topic, schemaJSON))
	return org
}

// dial connects the real client stack to the fake org: OAuth, TLS against the
// generated certificate, and per-RPC credentials.
func dial(t *testing.T, org *pubsubtest.Org) *pubsub.Client {
	t.Helper()
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caFile, org.CAPEM(), 0o600))

	tp := auth.New(config.SalesforceConfig{
		ClientID:     "id",
		ClientSecret: "secret",
		LoginURL:     org.LoginURL(),
	}, nil)
	client, err := pubsub.Dial(
		config.PubSubConfig{Endpoint: org.Endpoint(), CAFile: caFile},
		tp, zap.NewNop(), prometheus.NewRegistry(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func emit(t *testing.T, org *pubsubtest.Org, n int) []string {
	t.Helper()
	ids := make([]string, 0, n)
	for range n {
		id, err := org.Emit(topic, map[string]any{"Order_Number__c": "ORD-1"})
		require.NoError(t, err)
		ids = append(ids, id)
	}
	return ids
}

func receive(t *testing.T, sub *pubsub.Subscriber, n int) []string {
	t.Helper()
	ids := make([]string, 0, n)
	deadline := time.After(waitFor)
	for len(ids) < n {
		select {
		case e := <-sub.Out():
			ids = append(ids, e.Event.GetEvent().GetId())
		case <-deadline:
			t.Fatalf("received %d of %d events", len(ids), n)
		}
	}
	return ids
}

func TestOrg_TopicAndSchemaDiscovery(t *testing.T) {
	org := startOrg(t)
	client := dial(t, org)

	info, err := client.GetTopic(context.Background(), topic)
	require.NoError(t, err)
	require.True(t, info.GetCanSubscribe())

	schema, err := client.GetSchema(context.Background(), info.GetSchemaId())
	require.NoError(t, err)
	require.JSONEq(t, schemaJSON, schema.GetSchemaJson())

	_, err = client.GetTopic(context.Background(), "/event/Missing__e")
	require.ErrorIs(t, err, pubsub.ErrTopicNotFound)
}

func TestOrg_ResumesAfterCustomReplayID(t *testing.T) {
	org := startOrg(t)
	ids := emit(t, org, 5)

	sub := pubsub.NewSubscriber(dial(t, org), topic, 10,
		cursor{rid: pubsubtest.ReplayID(2)}, zap.NewNop(), prometheus.NewRegistry())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = sub.Run(ctx) }()

	require.Equal(t, ids[2:], receive(t, sub, 3))

	fetches := org.InitialFetches()
	require.Len(t, fetches, 1)
	require.Equal(t, salesforcepb.ReplayPreset_CUSTOM, fetches[0].GetReplayPreset())
}

func TestOrg_SendsOnlyRequestedEvents(t *testing.T) {
	org := startOrg(t)
	ids := emit(t, org, 6)

	// A batch size of 2 grants credit for two events; the rest must wait for Ack.
	sub := pubsub.NewSubscriber(dial(t, org), topic, 2,
		cursor{rid: pubsubtest.ReplayID(0)}, zap.NewNop(), prometheus.NewRegistry())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = sub.Run(ctx) }()

	require.Equal(t, ids[:2], receive(t, sub, 2))
	select {
	case e := <-sub.Out():
		t.Fatalf("received event %s beyond the requested credit", e.Event.GetEvent().GetId())
	case <-time.After(200 * time.Millisecond):
	}

	sub.Ack(4)
	require.Equal(t, ids[2:], receive(t, sub, 4))
}

func TestOrg_RejectsReplayIDOutsideTheLog(t *testing.T) {
	org := startOrg(t)
	emit(t, org, 1)

	sub := pubsub.NewSubscriber(dial(t, org), topic, 10,
		cursor{rid: pubsubtest.ReplayID(99)}, zap.NewNop(), prometheus.NewRegistry())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = sub.Run(ctx) }()

	select {
	case e := <-sub.Out():
		t.Fatalf("received event %s for a replay id the org does not hold", e.Event.GetEvent().GetId())
	case <-time.After(300 * time.Millisecond):
	}
	require.Empty(t, org.InitialFetches())
}
