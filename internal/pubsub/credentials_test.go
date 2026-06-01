package pubsub

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/AndreyZubov/pubsub-event-processor/internal/auth"
)

type fakeTokenProvider struct {
	creds auth.Credentials
	err   error
}

func (f *fakeTokenProvider) Token(_ context.Context) (auth.Credentials, error) {
	return f.creds, f.err
}

func TestPerRPCCredentials_MetadataKeys(t *testing.T) {
	tp := &fakeTokenProvider{creds: auth.Credentials{
		AccessToken: "AT",
		InstanceURL: "https://i",
		TenantID:    "00D",
	}}
	c := newPerRPCCredentials(tp)

	md, err := c.GetRequestMetadata(context.Background(), "any")
	if err != nil {
		t.Fatalf("GetRequestMetadata: %v", err)
	}
	if md[mdAccessToken] != "AT" {
		t.Errorf("accesstoken: %q", md[mdAccessToken])
	}
	if md[mdInstanceURL] != "https://i" {
		t.Errorf("instanceurl: %q", md[mdInstanceURL])
	}
	if md[mdTenantID] != "00D" {
		t.Errorf("tenantid: %q", md[mdTenantID])
	}
	if len(md) != 3 {
		t.Errorf("metadata keys: %v", md)
	}
}

func TestPerRPCCredentials_TokenError(t *testing.T) {
	tp := &fakeTokenProvider{err: errors.New("expired")}
	c := newPerRPCCredentials(tp)

	_, err := c.GetRequestMetadata(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "expired") {
		t.Errorf("error should wrap underlying: %v", err)
	}
}

func TestPerRPCCredentials_RequireTransportSecurity(t *testing.T) {
	c := newPerRPCCredentials(&fakeTokenProvider{})
	if !c.RequireTransportSecurity() {
		t.Error("must require TLS")
	}
}
