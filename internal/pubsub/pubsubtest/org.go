// Package pubsubtest provides an in-process fake Salesforce org for tests and
// local demos: an OAuth token endpoint and a Pub/Sub API gRPC server that
// honour the parts of the protocol the service depends on — per-RPC
// credentials, pull-based flow control, and replay presets.
//
// The gRPC server speaks TLS with a certificate generated at startup, because
// the service refuses to send access tokens over a plaintext connection. Point
// PUBSUB_CA_FILE at the certificate returned by CAPEM to trust it.
package pubsubtest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/hamba/avro/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	salesforcepb "github.com/AndreyZubov/pubsub-event-processor/proto/salesforce"
)

const (
	// orgID is the tenant the fake org reports in its identity URL.
	orgID  = "00D000000000001AAA"
	userID = "005000000000001AAA"

	tokenPath = "/services/oauth2/token" //nolint:gosec // URL path, not a credential

	// maxBatch caps the events in one FetchResponse, as the real API does.
	maxBatch = 100

	replayIDLen       = 8
	certValidity      = 24 * time.Hour
	httpHeaderTimeout = 5 * time.Second
)

// Options configures the listeners of a fake org. The zero value listens on
// ephemeral loopback ports.
type Options struct {
	// HTTPAddr is the listen address of the OAuth endpoint.
	HTTPAddr string
	// GRPCAddr is the listen address of the Pub/Sub API.
	GRPCAddr string
	// Hosts are extra DNS names or IP addresses to put in the TLS certificate,
	// in addition to localhost and the loopback addresses.
	Hosts []string
}

// Org is a running fake org. Create one with Start and release it with Close.
type Org struct {
	salesforcepb.UnimplementedPubSubServer

	httpLis net.Listener
	grpcLis net.Listener
	httpSrv *http.Server
	grpcSrv *grpc.Server
	caPEM   []byte
	token   string

	mu         sync.Mutex
	topics     map[string]*topicLog
	fetches    []*salesforcepb.FetchRequest
	streams    map[int]context.CancelFunc
	nextStream int
}

// topicLog is the append-only event log of one topic. Guarded by Org.mu.
type topicLog struct {
	schemaID   string
	schemaJSON string
	schema     avro.Schema
	events     []*salesforcepb.ConsumerEvent
	// changed is closed and replaced whenever an event is appended.
	changed chan struct{}
}

// Start launches a fake org and begins serving.
func Start(opts Options) (*Org, error) {
	if opts.HTTPAddr == "" {
		opts.HTTPAddr = "127.0.0.1:0"
	}
	if opts.GRPCAddr == "" {
		opts.GRPCAddr = "127.0.0.1:0"
	}

	cert, caPEM, err := selfSignedCert(opts.Hosts)
	if err != nil {
		return nil, fmt.Errorf("generate certificate: %w", err)
	}
	token, err := randomHex(16)
	if err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}

	var lc net.ListenConfig
	httpLis, err := lc.Listen(context.Background(), "tcp", opts.HTTPAddr)
	if err != nil {
		return nil, fmt.Errorf("listen http: %w", err)
	}
	grpcLis, err := lc.Listen(context.Background(), "tcp", opts.GRPCAddr)
	if err != nil {
		_ = httpLis.Close()
		return nil, fmt.Errorf("listen grpc: %w", err)
	}

	o := &Org{
		httpLis: httpLis,
		grpcLis: grpcLis,
		caPEM:   caPEM,
		token:   token,
		topics:  make(map[string]*topicLog),
		streams: make(map[int]context.CancelFunc),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST "+tokenPath, o.handleToken)
	o.httpSrv = &http.Server{Handler: mux, ReadHeaderTimeout: httpHeaderTimeout}

	o.grpcSrv = grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		})),
		grpc.UnaryInterceptor(o.authUnary),
		grpc.StreamInterceptor(o.authStream),
	)
	salesforcepb.RegisterPubSubServer(o.grpcSrv, o)

	go func() { _ = o.httpSrv.Serve(httpLis) }()
	go func() { _ = o.grpcSrv.Serve(grpcLis) }()
	return o, nil
}

// Close stops both servers and ends every open stream.
func (o *Org) Close() {
	o.grpcSrv.Stop()
	_ = o.httpSrv.Close()
}

// LoginURL is the value for SF_LOGIN_URL.
func (o *Org) LoginURL() string { return "http://" + o.httpLis.Addr().String() }

// Endpoint is the value for PUBSUB_ENDPOINT.
func (o *Org) Endpoint() string { return o.grpcLis.Addr().String() }

// CAPEM returns the PEM certificate to trust through PUBSUB_CA_FILE.
func (o *Org) CAPEM() []byte { return o.caPEM }

// AddTopic registers a topic whose events are encoded with the given Avro
// record schema.
func (o *Org) AddTopic(name, schemaJSON string) error {
	schema, err := avro.Parse(schemaJSON)
	if err != nil {
		return fmt.Errorf("parse schema: %w", err)
	}
	sum := sha256.Sum256([]byte(schemaJSON))

	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.topics[name]; ok {
		return fmt.Errorf("topic %q already exists", name)
	}
	o.topics[name] = &topicLog{
		schemaID:   hex.EncodeToString(sum[:8]),
		schemaJSON: schemaJSON,
		schema:     schema,
		changed:    make(chan struct{}),
	}
	return nil
}

// Emit appends an event to the topic, as if a Salesforce transaction had
// published it, and returns its ID. The payload is encoded with the topic's
// schema.
func (o *Org) Emit(topic string, payload map[string]any) (string, error) {
	id, err := randomUUID()
	if err != nil {
		return "", err
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	tl, ok := o.topics[topic]
	if !ok {
		return "", fmt.Errorf("unknown topic %q", topic)
	}
	data, err := avro.Marshal(tl.schema, payload)
	if err != nil {
		return "", fmt.Errorf("encode payload: %w", err)
	}

	tl.events = append(tl.events, &salesforcepb.ConsumerEvent{
		Event: &salesforcepb.ProducerEvent{
			Id:       id,
			SchemaId: tl.schemaID,
			Payload:  data,
		},
		ReplayId: ReplayID(uint64(len(tl.events)) + 1),
	})
	close(tl.changed)
	tl.changed = make(chan struct{})
	return id, nil
}

// ReplayID returns the replay ID of the n-th event published to a topic,
// counting from 1.
func ReplayID(n uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, n)
}

// InitialFetches returns the request that opened each Subscribe stream so far,
// in the order the streams were opened.
func (o *Org) InitialFetches() []*salesforcepb.FetchRequest {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]*salesforcepb.FetchRequest, len(o.fetches))
	for i, f := range o.fetches {
		out[i] = proto.Clone(f).(*salesforcepb.FetchRequest)
	}
	return out
}

// DropStreams ends every open Subscribe stream with an UNAVAILABLE status, the
// way a network fault or a server-side restart would.
func (o *Org) DropStreams() {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, cancel := range o.streams {
		cancel()
	}
}

// GetTopic implements the Pub/Sub API.
func (o *Org) GetTopic(_ context.Context, req *salesforcepb.TopicRequest) (*salesforcepb.TopicInfo, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	tl, ok := o.topics[req.GetTopicName()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "topic %q not found", req.GetTopicName())
	}
	return &salesforcepb.TopicInfo{
		TopicName:    req.GetTopicName(),
		TenantGuid:   orgID,
		CanSubscribe: true,
		SchemaId:     tl.schemaID,
	}, nil
}

// GetSchema implements the Pub/Sub API.
func (o *Org) GetSchema(_ context.Context, req *salesforcepb.SchemaRequest) (*salesforcepb.SchemaInfo, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, tl := range o.topics {
		if tl.schemaID == req.GetSchemaId() {
			return &salesforcepb.SchemaInfo{SchemaId: tl.schemaID, SchemaJson: tl.schemaJSON}, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "schema %q not found", req.GetSchemaId())
}

// Subscribe implements the Pub/Sub API. It sends events only against the
// credit the client has requested, as the real API does.
func (o *Org) Subscribe(stream salesforcepb.PubSub_SubscribeServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	tl, pos, err := o.openStream(first, cancel)
	if err != nil {
		return err
	}
	defer o.closeStream(pos.streamID)

	credit := int64(first.GetNumRequested())
	requests := make(chan int32)
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				cancel()
				return
			}
			select {
			case requests <- req.GetNumRequested():
			case <-ctx.Done():
				return
			}
		}
	}()

	next := pos.next
	for {
		o.mu.Lock()
		pending := tl.events[next:]
		changed := tl.changed
		o.mu.Unlock()

		if credit > 0 && len(pending) > 0 {
			n := min(int64(len(pending)), credit, maxBatch)
			batch := pending[:n]
			credit -= n
			err := stream.Send(&salesforcepb.FetchResponse{
				Events:              batch,
				LatestReplayId:      batch[n-1].GetReplayId(),
				PendingNumRequested: int32(credit), //nolint:gosec // credit is a sum of int32 requests in a test server
			})
			if err != nil {
				return err
			}
			next += int(n)
			continue
		}

		select {
		case <-ctx.Done():
			if stream.Context().Err() == nil {
				return status.Error(codes.Unavailable, "stream dropped")
			}
			return nil
		case n := <-requests:
			credit += int64(n)
		case <-changed:
		}
	}
}

type streamPos struct {
	streamID int
	next     int
}

// openStream records the opening request and resolves where in the topic's
// log the stream starts.
func (o *Org) openStream(first *salesforcepb.FetchRequest, cancel context.CancelFunc) (*topicLog, streamPos, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	tl, ok := o.topics[first.GetTopicName()]
	if !ok {
		return nil, streamPos{}, status.Errorf(codes.NotFound, "topic %q not found", first.GetTopicName())
	}

	var next int
	switch first.GetReplayPreset() {
	case salesforcepb.ReplayPreset_LATEST:
		next = len(tl.events)
	case salesforcepb.ReplayPreset_EARLIEST:
		next = 0
	case salesforcepb.ReplayPreset_CUSTOM:
		rid := first.GetReplayId()
		if len(rid) != replayIDLen {
			return nil, streamPos{}, status.Error(codes.InvalidArgument, "replay id is malformed")
		}
		n := binary.BigEndian.Uint64(rid)
		if n > uint64(len(tl.events)) {
			return nil, streamPos{}, status.Error(codes.InvalidArgument, "replay id is not in the retention window")
		}
		next = int(n) //nolint:gosec // bounded by the log length above
	default:
		return nil, streamPos{}, status.Errorf(codes.InvalidArgument, "unknown replay preset %v", first.GetReplayPreset())
	}

	o.fetches = append(o.fetches, proto.Clone(first).(*salesforcepb.FetchRequest))
	id := o.nextStream
	o.nextStream++
	o.streams[id] = cancel
	return tl, streamPos{streamID: id, next: next}, nil
}

func (o *Org) closeStream(id int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.streams, id)
}

// handleToken serves the OAuth client-credentials flow.
func (o *Org) handleToken(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "malformed form", http.StatusBadRequest)
		return
	}
	if r.PostForm.Get("grant_type") != "client_credentials" ||
		r.PostForm.Get("client_id") == "" || r.PostForm.Get("client_secret") == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_grant"}`))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token": o.token,
		"instance_url": o.LoginURL(),
		"id":           o.LoginURL() + "/id/" + orgID + "/" + userID,
		"token_type":   "Bearer",
		"issued_at":    strconv.FormatInt(time.Now().UnixMilli(), 10),
	})
}

func (o *Org) authUnary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	if err := o.authorize(ctx); err != nil {
		return nil, err
	}
	return next(ctx, req)
}

func (o *Org) authStream(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler) error {
	if err := o.authorize(ss.Context()); err != nil {
		return err
	}
	return next(srv, ss)
}

// authorize checks the three metadata headers the real API requires.
func (o *Org) authorize(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	if first(md, "accesstoken") != o.token {
		return status.Error(codes.Unauthenticated, "invalid access token")
	}
	if first(md, "tenantid") != orgID || first(md, "instanceurl") == "" {
		return status.Error(codes.Unauthenticated, "tenant id or instance url is missing")
	}
	return nil
}

func first(md metadata.MD, key string) string {
	if v := md.Get(key); len(v) > 0 {
		return v[0]
	}
	return ""
}

// selfSignedCert returns a serving certificate valid for the loopback
// addresses and hosts, and its PEM encoding for clients to trust.
func selfSignedCert(hosts []string) (tls.Certificate, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return tls.Certificate{}, nil, err
	}

	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "fake Salesforce org"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(certValidity),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else if h != "" {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, certPEM, nil
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// randomUUID returns a version 4 UUID, the format of Salesforce event IDs.
func randomUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", errors.Join(errors.New("generate event id"), err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
