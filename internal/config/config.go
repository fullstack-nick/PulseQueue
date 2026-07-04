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
	RetryInitialDelay time.Duration
	RetryMaxDelay     time.Duration
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
		RetryInitialDelay: getDurationEnv("PULSEQUEUE_RETRY_INITIAL_DELAY", 2*time.Second),
		RetryMaxDelay:     getDurationEnv("PULSEQUEUE_RETRY_MAX_DELAY", 30*time.Second),
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

func hostnameWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "worker-local"
	}
	return "worker-" + host
}
