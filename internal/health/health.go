// Package health provides liveness and readiness HTTP handlers and a Checker interface
// that subsystems implement to participate in readiness aggregation.
package health

import (
	"context"
	"encoding/json"
	"net/http"
)

// Checker reports whether a subsystem is ready to serve. A nil error means ready.
type Checker interface {
	Check(ctx context.Context) error
}

// CheckerFunc adapts a function to the Checker interface.
type CheckerFunc func(ctx context.Context) error

// Check calls the underlying function.
func (f CheckerFunc) Check(ctx context.Context) error { return f(ctx) }

// Healthz returns an HTTP handler that always responds 200, signalling liveness.
func Healthz() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

// Readiness returns an HTTP handler that aggregates the given checkers.
// 200 if all pass, 503 if any fails. The response body is a JSON map of
// checker name to either "ok" or the failing error string.
func Readiness(checkers map[string]Checker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		results := make(map[string]string, len(checkers))
		allOK := true
		for name, c := range checkers {
			if err := c.Check(r.Context()); err != nil {
				allOK = false
				results[name] = err.Error()
				continue
			}
			results[name] = "ok"
		}

		status := http.StatusOK
		statusStr := "ready"
		if !allOK {
			status = http.StatusServiceUnavailable
			statusStr = "not ready"
		}
		writeJSON(w, status, struct {
			Status string            `json:"status"`
			Checks map[string]string `json:"checks"`
		}{Status: statusStr, Checks: results})
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
