package pubsub

import (
	"context"
	"errors"
	"net"
	"testing"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	salesforcepb "github.com/AndreyZubov/pubsub-event-processor/proto/salesforce"
)

type fakeServer struct {
	salesforcepb.UnimplementedPubSubServer

	topicByName map[string]*salesforcepb.TopicInfo
	schemaByID  map[string]*salesforcepb.SchemaInfo
}

func (f *fakeServer) GetTopic(_ context.Context, in *salesforcepb.TopicRequest) (*salesforcepb.TopicInfo, error) {
	t, ok := f.topicByName[in.GetTopicName()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "topic %s not found", in.GetTopicName())
	}
	return t, nil
}

func (f *fakeServer) GetSchema(_ context.Context, in *salesforcepb.SchemaRequest) (*salesforcepb.SchemaInfo, error) {
	s, ok := f.schemaByID[in.GetSchemaId()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "schema %s not found", in.GetSchemaId())
	}
	return s, nil
}

func newBufconnClient(t *testing.T, fake *fakeServer) *Client {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	salesforcepb.RegisterPubSubServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return newClient(conn, zap.NewNop())
}

func TestClient_GetTopic_Happy(t *testing.T) {
	c := newBufconnClient(t, &fakeServer{
		topicByName: map[string]*salesforcepb.TopicInfo{
			"/event/Order__e": {
				TopicName:    "/event/Order__e",
				TenantGuid:   "00DABC",
				CanSubscribe: true,
				SchemaId:     "S1",
			},
		},
	})

	info, err := c.GetTopic(context.Background(), "/event/Order__e")
	if err != nil {
		t.Fatalf("GetTopic: %v", err)
	}
	if info.GetTopicName() != "/event/Order__e" {
		t.Errorf("TopicName: %q", info.GetTopicName())
	}
	if info.GetSchemaId() != "S1" {
		t.Errorf("SchemaId: %q", info.GetSchemaId())
	}
}

func TestClient_GetTopic_NotFound(t *testing.T) {
	c := newBufconnClient(t, &fakeServer{topicByName: map[string]*salesforcepb.TopicInfo{}})
	_, err := c.GetTopic(context.Background(), "/event/Missing__e")
	if !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("expected ErrTopicNotFound, got %v", err)
	}
}

func TestClient_GetSchema_Happy(t *testing.T) {
	c := newBufconnClient(t, &fakeServer{
		schemaByID: map[string]*salesforcepb.SchemaInfo{
			"S1": {SchemaId: "S1", SchemaJson: `{"type":"record"}`},
		},
	})
	info, err := c.GetSchema(context.Background(), "S1")
	if err != nil {
		t.Fatalf("GetSchema: %v", err)
	}
	if info.GetSchemaJson() != `{"type":"record"}` {
		t.Errorf("SchemaJson: %q", info.GetSchemaJson())
	}
}

func TestClient_GetSchema_NotFound(t *testing.T) {
	c := newBufconnClient(t, &fakeServer{schemaByID: map[string]*salesforcepb.SchemaInfo{}})
	_, err := c.GetSchema(context.Background(), "missing")
	if !errors.Is(err, ErrSchemaNotFound) {
		t.Fatalf("expected ErrSchemaNotFound, got %v", err)
	}
}
