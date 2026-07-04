package config

import "testing"

func TestLoadDefaults(t *testing.T) {
	t.Setenv("PULSEQUEUE_HTTP_ADDR", "")
	t.Setenv("PULSEQUEUE_GRPC_ADDR", "")
	t.Setenv("PULSEQUEUE_DATABASE_URL", "")
	t.Setenv("PULSEQUEUE_NATS_URL", "")
	t.Setenv("PULSEQUEUE_API_URL", "")

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
}

func TestValidateClientRequiresToken(t *testing.T) {
	cfg := Load()
	cfg.OperatorToken = ""
	if err := cfg.ValidateClient(); err == nil {
		t.Fatal("expected missing token error")
	}
}
