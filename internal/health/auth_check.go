package health

import (
	"context"
	"fmt"
	"time"

	"github.com/AndreyZubov/pubsub-event-processor/internal/auth"
)

const authCheckTimeout = 3 * time.Second

// NewAuthChecker returns a Checker that probes the Salesforce TokenProvider.
// A cached, fresh token returns immediately; a stale or absent one triggers a
// refresh attempt. A failed refresh propagates as Check error -> /readyz 503.
func NewAuthChecker(tp auth.TokenProvider) Checker {
	return &authCheck{tp: tp}
}

type authCheck struct {
	tp auth.TokenProvider
}

func (a *authCheck) Check(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, authCheckTimeout)
	defer cancel()
	if _, err := a.tp.Token(ctx); err != nil {
		return fmt.Errorf("salesforce token: %w", err)
	}
	return nil
}
