package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/AndreyZubov/pubsub-event-processor/internal/config"
)

func newMockTokenServer(t *testing.T, handler func(callCount int, w http.ResponseWriter, r *http.Request)) (*httptest.Server, *int64) {
	t.Helper()
	var calls int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt64(&calls, 1)
		handler(int(n), w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func validResponse(w http.ResponseWriter, accessToken, instanceURL, orgID string, expiresIn int) {
	w.Header().Set("Content-Type", "application/json")
	resp := map[string]any{
		"access_token": accessToken,
		"instance_url": instanceURL,
		"id":           fmt.Sprintf("https://login.salesforce.com/id/%s/005XXXXX", orgID),
		"token_type":   "Bearer",
		"issued_at":    "1700000000",
	}
	if expiresIn > 0 {
		resp["expires_in"] = expiresIn
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func newProvider(t *testing.T, srvURL string, opts ...Option) *Provider {
	t.Helper()
	return New(
		config.SalesforceConfig{
			ClientID:     "id",
			ClientSecret: "secret",
			LoginURL:     srvURL,
		},
		prometheus.NewRegistry(),
		opts...,
	)
}

func TestProvider_HappyPath(t *testing.T) {
	srv, _ := newMockTokenServer(t, func(_ int, w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method: %s", r.Method)
		}
		if r.URL.Path != "/services/oauth2/token" {
			t.Errorf("path: %s", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		if g := r.PostForm.Get("grant_type"); g != "client_credentials" {
			t.Errorf("grant_type: %q", g)
		}
		if id := r.PostForm.Get("client_id"); id != "id" {
			t.Errorf("client_id: %q", id)
		}
		validResponse(w, "TOKEN-1", "https://test.salesforce.com", "00DABCDEF", 0)
	})

	p := newProvider(t, srv.URL)
	c, err := p.Token(context.Background())
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if c.AccessToken != "TOKEN-1" {
		t.Errorf("AccessToken: %q", c.AccessToken)
	}
	if c.InstanceURL != "https://test.salesforce.com" {
		t.Errorf("InstanceURL: %q", c.InstanceURL)
	}
	if c.TenantID != "00DABCDEF" {
		t.Errorf("TenantID: %q", c.TenantID)
	}
}

func TestProvider_CachedHit(t *testing.T) {
	srv, calls := newMockTokenServer(t, func(_ int, w http.ResponseWriter, _ *http.Request) {
		validResponse(w, "TOKEN-1", "https://i", "00D1", 0)
	})

	p := newProvider(t, srv.URL)
	if _, err := p.Token(context.Background()); err != nil {
		t.Fatalf("first Token: %v", err)
	}
	for range 5 {
		if _, err := p.Token(context.Background()); err != nil {
			t.Fatalf("repeat Token: %v", err)
		}
	}
	if got := atomic.LoadInt64(calls); got != 1 {
		t.Errorf("HTTP calls: got %d, want 1 (cached)", got)
	}
}

func TestProvider_RefreshAfterExpiry(t *testing.T) {
	srv, calls := newMockTokenServer(t, func(n int, w http.ResponseWriter, _ *http.Request) {
		validResponse(w, fmt.Sprintf("TOKEN-%d", n), "https://i", "00D1", 0)
	})

	p := newProvider(t, srv.URL,
		WithDefaultTTL(100*time.Millisecond),
		WithRefreshAhead(0),
	)

	c1, err := p.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if c1.AccessToken != "TOKEN-1" {
		t.Errorf("c1: %q", c1.AccessToken)
	}

	time.Sleep(150 * time.Millisecond)

	c2, err := p.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if c2.AccessToken != "TOKEN-2" {
		t.Errorf("c2: %q (expected refresh)", c2.AccessToken)
	}
	if got := atomic.LoadInt64(calls); got != 2 {
		t.Errorf("HTTP calls: got %d, want 2", got)
	}
}

func TestProvider_ConcurrentDedup(t *testing.T) {
	const goroutines = 20

	srv, calls := newMockTokenServer(t, func(_ int, w http.ResponseWriter, _ *http.Request) {
		time.Sleep(50 * time.Millisecond)
		validResponse(w, "TOKEN-1", "https://i", "00D1", 0)
	})

	p := newProvider(t, srv.URL)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	errs := make(chan error, goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			if _, err := p.Token(context.Background()); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("Token: %v", err)
	}
	if got := atomic.LoadInt64(calls); got != 1 {
		t.Errorf("HTTP calls: got %d, want 1 (singleflight dedup)", got)
	}
}

func TestProvider_ServerError(t *testing.T) {
	srv, _ := newMockTokenServer(t, func(_ int, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"server_error"}`))
	})

	p := newProvider(t, srv.URL)
	_, err := p.Token(context.Background())
	if err == nil {
		t.Fatal("expected error on 500 response")
	}
}

func TestProvider_MalformedJSON(t *testing.T) {
	srv, _ := newMockTokenServer(t, func(_ int, w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not json"))
	})

	p := newProvider(t, srv.URL)
	_, err := p.Token(context.Background())
	if err == nil {
		t.Fatal("expected error for malformed JSON")
	}
}

func TestProvider_MissingFields(t *testing.T) {
	cases := []struct {
		name string
		body map[string]any
	}{
		{
			name: "empty access_token",
			body: map[string]any{
				"access_token": "",
				"instance_url": "https://i",
				"id":           "https://login.salesforce.com/id/00D/005",
			},
		},
		{
			name: "empty instance_url",
			body: map[string]any{
				"access_token": "x",
				"instance_url": "",
				"id":           "https://login.salesforce.com/id/00D/005",
			},
		},
		{
			name: "malformed identity URL",
			body: map[string]any{
				"access_token": "x",
				"instance_url": "https://i",
				"id":           "not-a-url-with-id-path",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newMockTokenServer(t, func(_ int, w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(tc.body)
			})
			p := newProvider(t, srv.URL)
			_, err := p.Token(context.Background())
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestProvider_UsesExpiresIn(t *testing.T) {
	srv, _ := newMockTokenServer(t, func(_ int, w http.ResponseWriter, _ *http.Request) {
		validResponse(w, "TOKEN-1", "https://i", "00D1", 7200)
	})

	p := newProvider(t, srv.URL, WithDefaultTTL(10*time.Second))
	if _, err := p.Token(context.Background()); err != nil {
		t.Fatal(err)
	}

	p.mu.Lock()
	d := time.Until(p.expiresAt)
	p.mu.Unlock()
	if d < 1*time.Hour {
		t.Errorf("expected TTL ~2h from expires_in=7200, got %s", d)
	}
}

func TestTenantIDFromIdentityURL(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"https://login.salesforce.com/id/00DXXX/005YYY", "00DXXX", false},
		{"https://test.salesforce.com/id/00D000/005000/", "00D000", false},
		{"", "", true},
		{"https://login.salesforce.com/oauth/token", "", true},
		{"https://login.salesforce.com/id/", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := tenantIDFromIdentityURL(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err: got %v, wantErr=%v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}
