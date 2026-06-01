package health

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

func TestAuthChecker_OK(t *testing.T) {
	tp := &fakeTokenProvider{creds: auth.Credentials{AccessToken: "AT"}}
	c := NewAuthChecker(tp)
	if err := c.Check(context.Background()); err != nil {
		t.Errorf("Check: %v", err)
	}
}

func TestAuthChecker_PropagatesError(t *testing.T) {
	tp := &fakeTokenProvider{err: errors.New("upstream down")}
	c := NewAuthChecker(tp)
	err := c.Check(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "upstream down") {
		t.Errorf("expected wrapped error, got %v", err)
	}
}
