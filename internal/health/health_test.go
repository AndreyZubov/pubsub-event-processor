package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthz_AlwaysOK(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	Healthz().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusOK)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type: got %q", ct)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON: %v: %s", err, rec.Body.String())
	}
	if body["status"] != "ok" {
		t.Errorf("body status: got %q", body["status"])
	}
}

func TestReadiness_NoCheckers(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	Readiness(nil).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d", rec.Code)
	}
}

func TestReadiness_AllHealthy(t *testing.T) {
	checkers := map[string]Checker{
		"db":   CheckerFunc(func(context.Context) error { return nil }),
		"auth": CheckerFunc(func(context.Context) error { return nil }),
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	Readiness(checkers).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d", rec.Code)
	}
	var body struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if body.Status != "ready" {
		t.Errorf("status: %q", body.Status)
	}
	if body.Checks["db"] != "ok" || body.Checks["auth"] != "ok" {
		t.Errorf("checks: %#v", body.Checks)
	}
}

func TestReadiness_OneFailing(t *testing.T) {
	checkers := map[string]Checker{
		"db":   CheckerFunc(func(context.Context) error { return nil }),
		"auth": CheckerFunc(func(context.Context) error { return errors.New("token expired") }),
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	Readiness(checkers).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d, want 503", rec.Code)
	}
	var body struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if body.Status != "not ready" {
		t.Errorf("status: %q", body.Status)
	}
	if body.Checks["db"] != "ok" {
		t.Errorf("db check should be ok: %q", body.Checks["db"])
	}
	if body.Checks["auth"] != "token expired" {
		t.Errorf("auth check: %q", body.Checks["auth"])
	}
}
