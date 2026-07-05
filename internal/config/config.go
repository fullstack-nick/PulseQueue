package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr          string
	GRPCAddr          string
	DatabaseURL       string
	NATSURL           string
	OperatorToken     string
	APIURL            string
	WorkerID          string
	WorkerQueue       string
	PollInterval      time.Duration
	LeaseDuration     time.Duration
	WorkerHeartbeat   time.Duration
	SchedulerID       string
	SchedulerInterval time.Duration
	SchedulerBatch    int32
	RetryInitialDelay time.Duration
	RetryMaxDelay     time.Duration
	MetricsAddr       string
	OTLPEndpoint      string
	ServiceName       string
}

func Load() Config {
	return Config{
		HTTPAddr:          getEnv("PULSEQUEUE_HTTP_ADDR", ":8080"),
		GRPCAddr:          getEnv("PULSEQUEUE_GRPC_ADDR", ":9090"),
		DatabaseURL:       getEnv("PULSEQUEUE_DATABASE_URL", "postgres://pulsequeue:pulsequeue@localhost:5432/pulsequeue?sslmode=disable"),
		NATSURL:           getEnv("PULSEQUEUE_NATS_URL", "nats://localhost:4222"),
		OperatorToken:     os.Getenv("PULSEQUEUE_OPERATOR_TOKEN"),
		APIURL:            strings.TrimRight(getEnv("PULSEQUEUE_API_URL", "http://localhost:8080"), "/"),
		WorkerID:          getEnv("PULSEQUEUE_WORKER_ID", hostnameWorkerID()),
		WorkerQueue:       getEnv("PULSEQUEUE_WORKER_QUEUE", "default"),
		PollInterval:      getDurationEnv("PULSEQUEUE_WORKER_POLL_INTERVAL", 5*time.Second),
		LeaseDuration:     getDurationEnv("PULSEQUEUE_LEASE_DURATION", 60*time.Second),
		WorkerHeartbeat:   getDurationEnv("PULSEQUEUE_WORKER_HEARTBEAT_INTERVAL", 10*time.Second),
		SchedulerID:       getEnv("PULSEQUEUE_SCHEDULER_ID", hostnameSchedulerID()),
		SchedulerInterval: getDurationEnv("PULSEQUEUE_SCHEDULER_INTERVAL", 2*time.Second),
		SchedulerBatch:    int32(getIntEnv("PULSEQUEUE_SCHEDULER_BATCH_SIZE", 50)),
		RetryInitialDelay: getDurationEnv("PULSEQUEUE_RETRY_INITIAL_DELAY", 2*time.Second),
		RetryMaxDelay:     getDurationEnv("PULSEQUEUE_RETRY_MAX_DELAY", 30*time.Second),
		MetricsAddr:       os.Getenv("PULSEQUEUE_METRICS_ADDR"),
		OTLPEndpoint:      os.Getenv("PULSEQUEUE_OTEL_EXPORTER_OTLP_ENDPOINT"),
		ServiceName:       os.Getenv("PULSEQUEUE_SERVICE_NAME"),
	}
}

func (c Config) ValidateServer() error {
	var missing []string
	if c.DatabaseURL == "" {
		missing = append(missing, "PULSEQUEUE_DATABASE_URL")
	}
	if c.NATSURL == "" {
		missing = append(missing, "PULSEQUEUE_NATS_URL")
	}
	if c.OperatorToken == "" {
		missing = append(missing, "PULSEQUEUE_OPERATOR_TOKEN")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required config: %s", strings.Join(missing, ", "))
	}
	return nil
}

func (c Config) ValidateClient() error {
	if c.APIURL == "" {
		return errors.New("missing PULSEQUEUE_API_URL")
	}
	if c.OperatorToken == "" {
		return errors.New("missing PULSEQUEUE_OPERATOR_TOKEN")
	}
	return nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getDurationEnv(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	if parsed, err := time.ParseDuration(v); err == nil {
		return parsed
	}
	if seconds, err := strconv.Atoi(v); err == nil {
		return time.Duration(seconds) * time.Second
	}
	return fallback
}

func getIntEnv(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(v)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func hostnameWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "worker-local"
	}
	return "worker-" + host
}

func hostnameSchedulerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "scheduler-local"
	}
	return "scheduler-" + host
}
