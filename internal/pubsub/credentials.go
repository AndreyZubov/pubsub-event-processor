package pubsub

import (
	"context"
	"fmt"

	"github.com/AndreyZubov/pubsub-event-processor/internal/auth"
)

const (
	mdAccessToken = "accesstoken"
	mdInstanceURL = "instanceurl"
	mdTenantID    = "tenantid"
)

// perRPCCredentials adapts an auth.TokenProvider to gRPC's
// credentials.PerRPCCredentials interface: on every RPC it fetches a valid
// token and injects the three Salesforce metadata headers.
type perRPCCredentials struct {
	tp auth.TokenProvider
}

func newPerRPCCredentials(tp auth.TokenProvider) *perRPCCredentials {
	return &perRPCCredentials{tp: tp}
}

// GetRequestMetadata is invoked by gRPC before every RPC call.
func (c *perRPCCredentials) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	creds, err := c.tp.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("get token: %w", err)
	}
	return map[string]string{
		mdAccessToken: creds.AccessToken,
		mdInstanceURL: creds.InstanceURL,
		mdTenantID:    creds.TenantID,
	}, nil
}

// RequireTransportSecurity reports whether these credentials require TLS.
// True: tokens must never traverse the wire in plaintext.
func (c *perRPCCredentials) RequireTransportSecurity() bool { return true }
