package config

import (
	"os"
	"strings"
	"testing"
)

var allConfigEnvKeys = []string{
	"SF_CLIENT_ID", "SF_CLIENT_SECRET", "SF_LOGIN_URL",
	"SF_TOPICS", "PUBSUB_ENDPOINT",
	"DATABASE_URL", "SINK_WEBHOOK_URL",
	"WORKER_COUNT", "FLOW_BATCH_SIZE",
	"HTTP_ADDR", "LOG_LEVEL",
}

func resetEnv(t *testing.T) {
	t.Helper()
	saved := make(map[string]string, len(allConfigEnvKeys))
	for _, k := range allConfigEnvKeys {
		if v, ok := os.LookupEnv(k); ok {
			saved[k] = v
		}
		_ = os.Unsetenv(k)
	}
	t.Cleanup(func() {
		for _, k := range allConfigEnvKeys {
			if v, ok := saved[k]; ok {
				_ = os.Setenv(k, v)
			} else {
				_ = os.Unsetenv(k)
			}
		}
	})
}

func setEnv(t *testing.T, vals map[string]string) {
	t.Helper()
	for k, v := range vals {
		if err := os.Setenv(k, v); err != nil {
			t.Fatalf("setenv %s: %v", k, err)
		}
	}
}

func baseEnv() map[string]string {
	return map[string]string{
		"SF_CLIENT_ID":     "id",
		"SF_CLIENT_SECRET": "secret",
		"SF_TOPICS":        "/event/Order_Event__e",
		"DATABASE_URL":     "postgres://u:p@localhost/db",
	}
}

func TestLoad_HappyPathDefaults(t *testing.T) {
	resetEnv(t)
	setEnv(t, baseEnv())

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Salesforce.ClientID != "id" {
		t.Errorf("ClientID: got %q", cfg.Salesforce.ClientID)
	}
	if cfg.Salesforce.LoginURL != "https://login.salesforce.com" {
		t.Errorf("LoginURL default: got %q", cfg.Salesforce.LoginURL)
	}
	if cfg.PubSub.Endpoint != "api.pubsub.salesforce.com:7443" {
		t.Errorf("Endpoint default: got %q", cfg.PubSub.Endpoint)
	}
	if cfg.Worker.Count != 8 {
		t.Errorf("WorkerCount default: got %d", cfg.Worker.Count)
	}
	if cfg.Worker.FlowBatchSize != 100 {
		t.Errorf("FlowBatchSize default: got %d", cfg.Worker.FlowBatchSize)
	}
	if cfg.HTTP.Addr != ":8080" {
		t.Errorf("HTTP.Addr default: got %q", cfg.HTTP.Addr)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel default: got %q", cfg.LogLevel)
	}
	if cfg.Sink.WebhookURL != "" {
		t.Errorf("Sink default should be empty: got %q", cfg.Sink.WebhookURL)
	}
	if got := cfg.PubSub.Topics; len(got) != 1 || got[0] != "/event/Order_Event__e" {
		t.Errorf("Topics parsing: got %#v", got)
	}
}

func TestLoad_CSVTopics(t *testing.T) {
	resetEnv(t)
	setEnv(t, baseEnv())
	setEnv(t, map[string]string{"SF_TOPICS": "/event/A,/event/B,/event/C"})

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := []string{"/event/A", "/event/B", "/event/C"}
	if got := cfg.PubSub.Topics; len(got) != 3 ||
		got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Errorf("Topics: got %#v, want %#v", got, want)
	}
}

func TestLoad_Overrides(t *testing.T) {
	resetEnv(t)
	setEnv(t, baseEnv())
	setEnv(t, map[string]string{
		"WORKER_COUNT":     "32",
		"FLOW_BATCH_SIZE":  "500",
		"HTTP_ADDR":        ":9090",
		"LOG_LEVEL":        "debug",
		"SINK_WEBHOOK_URL": "https://example.com/hook",
	})

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Worker.Count != 32 {
		t.Errorf("Count: %d", cfg.Worker.Count)
	}
	if cfg.Worker.FlowBatchSize != 500 {
		t.Errorf("FlowBatchSize: %d", cfg.Worker.FlowBatchSize)
	}
	if cfg.HTTP.Addr != ":9090" {
		t.Errorf("Addr: %q", cfg.HTTP.Addr)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel: %q", cfg.LogLevel)
	}
	if cfg.Sink.WebhookURL != "https://example.com/hook" {
		t.Errorf("WebhookURL: %q", cfg.Sink.WebhookURL)
	}
}

func TestLoad_Errors(t *testing.T) {
	cases := []struct {
		name    string
		env     map[string]string
		unset   []string
		wantErr string
	}{
		{
			name:    "missing SF_CLIENT_ID",
			unset:   []string{"SF_CLIENT_ID"},
			wantErr: "SF_CLIENT_ID",
		},
		{
			name:    "missing SF_CLIENT_SECRET",
			unset:   []string{"SF_CLIENT_SECRET"},
			wantErr: "SF_CLIENT_SECRET",
		},
		{
			name:    "missing SF_TOPICS",
			unset:   []string{"SF_TOPICS"},
			wantErr: "SF_TOPICS",
		},
		{
			name:    "missing DATABASE_URL",
			unset:   []string{"DATABASE_URL"},
			wantErr: "DATABASE_URL",
		},
		{
			name:    "invalid WORKER_COUNT",
			env:     map[string]string{"WORKER_COUNT": "abc"},
			wantErr: "parse env",
		},
		{
			name:    "zero WORKER_COUNT",
			env:     map[string]string{"WORKER_COUNT": "0"},
			wantErr: "WORKER_COUNT must be >= 1",
		},
		{
			name:    "zero FLOW_BATCH_SIZE",
			env:     map[string]string{"FLOW_BATCH_SIZE": "0"},
			wantErr: "FLOW_BATCH_SIZE must be >= 1",
		},
		{
			name:    "bad PUBSUB_ENDPOINT",
			env:     map[string]string{"PUBSUB_ENDPOINT": "not_a_host_port"},
			wantErr: "PUBSUB_ENDPOINT",
		},
		{
			name:    "bad SF_LOGIN_URL",
			env:     map[string]string{"SF_LOGIN_URL": "not-a-url"},
			wantErr: "SF_LOGIN_URL",
		},
		{
			name:    "bad SINK_WEBHOOK_URL",
			env:     map[string]string{"SINK_WEBHOOK_URL": "://broken"},
			wantErr: "SINK_WEBHOOK_URL",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetEnv(t)
			base := baseEnv()
			for _, k := range tc.unset {
				delete(base, k)
			}
			setEnv(t, base)
			setEnv(t, tc.env)

			_, err := Load()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}
