package config

import "testing"

func TestLoadDefaults(t *testing.T) {
	t.Setenv("PULSEQUEUE_HTTP_ADDR", "")
	t.Setenv("PULSEQUEUE_GRPC_ADDR", "")
	t.Setenv("PULSEQUEUE_DATABASE_URL", "")
	t.Setenv("PULSEQUEUE_NATS_URL", "")
	t.Setenv("PULSEQUEUE_API_URL", "")
	t.Setenv("PULSEQUEUE_RETRY_INITIAL_DELAY", "")
	t.Setenv("PULSEQUEUE_RETRY_MAX_DELAY", "")

	cfg := Load()
	if cfg.HTTPAddr != ":8080" {
		t.Fatalf("unexpected HTTPAddr: %s", cfg.HTTPAddr)
	}
	if cfg.GRPCAddr != ":9090" {
		t.Fatalf("unexpected GRPCAddr: %s", cfg.GRPCAddr)
	}
	if cfg.APIURL != "http://localhost:8080" {
		t.Fatalf("unexpected APIURL: %s", cfg.APIURL)
	}
	if cfg.WorkerQueue != "default" {
		t.Fatalf("unexpected WorkerQueue: %s", cfg.WorkerQueue)
	}
	if cfg.RetryInitialDelay.String() != "2s" {
		t.Fatalf("unexpected RetryInitialDelay: %s", cfg.RetryInitialDelay)
	}
	if cfg.RetryMaxDelay.String() != "30s" {
		t.Fatalf("unexpected RetryMaxDelay: %s", cfg.RetryMaxDelay)
	}
}

func TestValidateClientRequiresToken(t *testing.T) {
	cfg := Load()
	cfg.OperatorToken = ""
	if err := cfg.ValidateClient(); err == nil {
		t.Fatal("expected missing token error")
	}
}

func TestLoadRetryDurations(t *testing.T) {
	t.Setenv("PULSEQUEUE_RETRY_INITIAL_DELAY", "5s")
	t.Setenv("PULSEQUEUE_RETRY_MAX_DELAY", "2m")

	cfg := Load()
	if cfg.RetryInitialDelay.String() != "5s" {
		t.Fatalf("unexpected RetryInitialDelay: %s", cfg.RetryInitialDelay)
	}
	if cfg.RetryMaxDelay.String() != "2m0s" {
		t.Fatalf("unexpected RetryMaxDelay: %s", cfg.RetryMaxDelay)
	}
}
