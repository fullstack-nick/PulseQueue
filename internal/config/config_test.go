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
	t.Setenv("PULSEQUEUE_WORKER_HEARTBEAT_INTERVAL", "")
	t.Setenv("PULSEQUEUE_SCHEDULER_INTERVAL", "")
	t.Setenv("PULSEQUEUE_SCHEDULER_BATCH_SIZE", "")
	t.Setenv("PULSEQUEUE_METRICS_ADDR", "")
	t.Setenv("PULSEQUEUE_OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("PULSEQUEUE_SERVICE_NAME", "")

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
	if cfg.WorkerHeartbeat.String() != "10s" {
		t.Fatalf("unexpected WorkerHeartbeat: %s", cfg.WorkerHeartbeat)
	}
	if cfg.SchedulerInterval.String() != "2s" {
		t.Fatalf("unexpected SchedulerInterval: %s", cfg.SchedulerInterval)
	}
	if cfg.SchedulerBatch != 50 {
		t.Fatalf("unexpected SchedulerBatch: %d", cfg.SchedulerBatch)
	}
	if cfg.RetryInitialDelay.String() != "2s" {
		t.Fatalf("unexpected RetryInitialDelay: %s", cfg.RetryInitialDelay)
	}
	if cfg.RetryMaxDelay.String() != "30s" {
		t.Fatalf("unexpected RetryMaxDelay: %s", cfg.RetryMaxDelay)
	}
	if cfg.MetricsAddr != "" {
		t.Fatalf("unexpected MetricsAddr: %s", cfg.MetricsAddr)
	}
	if cfg.OTLPEndpoint != "" {
		t.Fatalf("unexpected OTLPEndpoint: %s", cfg.OTLPEndpoint)
	}
	if cfg.ServiceName != "" {
		t.Fatalf("unexpected ServiceName: %s", cfg.ServiceName)
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

func TestLoadSchedulerConfig(t *testing.T) {
	t.Setenv("PULSEQUEUE_SCHEDULER_INTERVAL", "3s")
	t.Setenv("PULSEQUEUE_SCHEDULER_BATCH_SIZE", "7")
	t.Setenv("PULSEQUEUE_WORKER_HEARTBEAT_INTERVAL", "4s")

	cfg := Load()
	if cfg.SchedulerInterval.String() != "3s" {
		t.Fatalf("unexpected SchedulerInterval: %s", cfg.SchedulerInterval)
	}
	if cfg.SchedulerBatch != 7 {
		t.Fatalf("unexpected SchedulerBatch: %d", cfg.SchedulerBatch)
	}
	if cfg.WorkerHeartbeat.String() != "4s" {
		t.Fatalf("unexpected WorkerHeartbeat: %s", cfg.WorkerHeartbeat)
	}
}

func TestLoadObservabilityConfig(t *testing.T) {
	t.Setenv("PULSEQUEUE_METRICS_ADDR", ":2112")
	t.Setenv("PULSEQUEUE_OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")
	t.Setenv("PULSEQUEUE_SERVICE_NAME", "pulsequeue-worker")

	cfg := Load()
	if cfg.MetricsAddr != ":2112" {
		t.Fatalf("unexpected MetricsAddr: %s", cfg.MetricsAddr)
	}
	if cfg.OTLPEndpoint != "otel-collector:4317" {
		t.Fatalf("unexpected OTLPEndpoint: %s", cfg.OTLPEndpoint)
	}
	if cfg.ServiceName != "pulsequeue-worker" {
		t.Fatalf("unexpected ServiceName: %s", cfg.ServiceName)
	}
}
