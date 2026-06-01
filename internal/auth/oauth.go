// Package auth obtains and caches Salesforce OAuth credentials for the gRPC
// Pub/Sub API. The Salesforce Pub/Sub API expects three values to be passed as
// gRPC metadata on every call: access_token, instance_url, and tenant_id.
package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"

	"github.com/AndreyZubov/pubsub-event-processor/internal/config"
)

// Credentials carries the three values that Salesforce Pub/Sub API requires on
// every gRPC call as metadata headers.
type Credentials struct {
	AccessToken string
	InstanceURL string
	TenantID    string
}

// TokenProvider returns a valid set of Salesforce credentials, refreshing as needed.
type TokenProvider interface {
	Token(ctx context.Context) (Credentials, error)
}

const (
	defaultRefreshAhead = 60 * time.Second
	defaultTTL          = time.Hour
	tokenEndpointPath   = "/services/oauth2/token" //nolint:gosec // public Salesforce OAuth URL path, not a credential
)

// Provider is a thread-safe TokenProvider that authenticates via the OAuth 2.0
// client-credentials flow and caches the token until it nears expiry.
type Provider struct {
	cfg          config.SalesforceConfig
	httpClient   *http.Client
	refreshAhead time.Duration
	defaultTTL   time.Duration

	mu        sync.Mutex
	cached    *Credentials
	expiresAt time.Time

	sf      singleflight.Group
	metrics providerMetrics
}

// Option customizes a Provider during construction.
type Option func(*Provider)

// WithHTTPClient overrides the HTTP client used to call the token endpoint.
func WithHTTPClient(c *http.Client) Option { return func(p *Provider) { p.httpClient = c } }

// WithDefaultTTL sets the cache TTL used when the token response omits expires_in.
func WithDefaultTTL(d time.Duration) Option { return func(p *Provider) { p.defaultTTL = d } }

// WithRefreshAhead sets how long before expiry the cached token is considered stale.
func WithRefreshAhead(d time.Duration) Option { return func(p *Provider) { p.refreshAhead = d } }

// New constructs a Provider. reg may be nil to skip metrics registration (useful in tests
// that don't care about metrics).
func New(cfg config.SalesforceConfig, reg prometheus.Registerer, opts ...Option) *Provider {
	p := &Provider{
		cfg:          cfg,
		httpClient:   http.DefaultClient,
		refreshAhead: defaultRefreshAhead,
		defaultTTL:   defaultTTL,
	}
	for _, o := range opts {
		o(p)
	}
	p.metrics = newProviderMetrics(reg, p)
	return p
}

// Token returns the cached credentials if still fresh, otherwise refreshes them.
// Concurrent callers during a refresh share a single HTTP call via singleflight.
func (p *Provider) Token(ctx context.Context) (Credentials, error) {
	p.mu.Lock()
	if p.cached != nil && time.Until(p.expiresAt) > p.refreshAhead {
		c := *p.cached
		p.mu.Unlock()
		return c, nil
	}
	p.mu.Unlock()

	v, err, _ := p.sf.Do("refresh", func() (any, error) {
		return p.refresh(ctx)
	})
	if err != nil {
		return Credentials{}, err
	}
	return v.(Credentials), nil
}

func (p *Provider) refresh(ctx context.Context) (Credentials, error) {
	creds, ttl, err := p.fetchToken(ctx)
	if err != nil {
		p.metrics.refreshTotal.WithLabelValues("error").Inc()
		return Credentials{}, fmt.Errorf("refresh token: %w", err)
	}
	p.mu.Lock()
	c := creds
	p.cached = &c
	p.expiresAt = time.Now().Add(ttl)
	p.mu.Unlock()
	p.metrics.refreshTotal.WithLabelValues("success").Inc()
	return creds, nil
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	InstanceURL string `json:"instance_url"`
	ID          string `json:"id"`
	TokenType   string `json:"token_type"`
	IssuedAt    string `json:"issued_at"`
	ExpiresIn   int    `json:"expires_in"`
}

func (p *Provider) fetchToken(ctx context.Context) (Credentials, time.Duration, error) {
	endpoint := strings.TrimRight(p.cfg.LoginURL, "/") + tokenEndpointPath

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", p.cfg.ClientID)
	form.Set("client_secret", p.cfg.ClientSecret)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return Credentials{}, 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return Credentials{}, 0, fmt.Errorf("post: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Credentials{}, 0, fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return Credentials{}, 0, fmt.Errorf("token endpoint returned %d: %s", resp.StatusCode, body)
	}

	var tr tokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return Credentials{}, 0, fmt.Errorf("decode response: %w", err)
	}
	if tr.AccessToken == "" {
		return Credentials{}, 0, errors.New("empty access_token in token response")
	}
	if tr.InstanceURL == "" {
		return Credentials{}, 0, errors.New("empty instance_url in token response")
	}

	tenantID, err := tenantIDFromIdentityURL(tr.ID)
	if err != nil {
		return Credentials{}, 0, fmt.Errorf("parse identity URL: %w", err)
	}

	ttl := p.defaultTTL
	if tr.ExpiresIn > 0 {
		ttl = time.Duration(tr.ExpiresIn) * time.Second
	}
	return Credentials{
		AccessToken: tr.AccessToken,
		InstanceURL: tr.InstanceURL,
		TenantID:    tenantID,
	}, ttl, nil
}

// tenantIDFromIdentityURL extracts the org ID from a Salesforce identity URL
// of the form https://<host>/id/<ORG_ID>/<USER_ID>.
func tenantIDFromIdentityURL(s string) (string, error) {
	if s == "" {
		return "", errors.New("identity URL is empty")
	}
	u, err := url.Parse(s)
	if err != nil {
		return "", err
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) < 2 || parts[0] != "id" {
		return "", fmt.Errorf("unexpected identity path: %q", u.Path)
	}
	if parts[1] == "" {
		return "", errors.New("empty org id segment")
	}
	return parts[1], nil
}

type providerMetrics struct {
	refreshTotal  *prometheus.CounterVec
	expirySeconds prometheus.GaugeFunc
}

func newProviderMetrics(reg prometheus.Registerer, p *Provider) providerMetrics {
	m := providerMetrics{
		refreshTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "auth_token_refresh_total",
				Help: "Number of Salesforce token refresh attempts, labelled by outcome.",
			},
			[]string{"result"},
		),
		expirySeconds: prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Name: "auth_token_expiry_seconds",
				Help: "Seconds until the cached Salesforce token expires; 0 if no token cached.",
			},
			func() float64 {
				p.mu.Lock()
				defer p.mu.Unlock()
				if p.cached == nil {
					return 0
				}
				if d := time.Until(p.expiresAt).Seconds(); d > 0 {
					return d
				}
				return 0
			},
		),
	}
	if reg != nil {
		reg.MustRegister(m.refreshTotal, m.expirySeconds)
	}
	return m
}
