package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/fullstack-nick/PulseQueue/internal/api"
	"github.com/fullstack-nick/PulseQueue/internal/config"
	"github.com/fullstack-nick/PulseQueue/internal/grpcserver"
	"github.com/fullstack-nick/PulseQueue/internal/scheduler"
	"github.com/fullstack-nick/PulseQueue/internal/signals"
	"github.com/fullstack-nick/PulseQueue/internal/storage"
	"github.com/fullstack-nick/PulseQueue/internal/worker"
)

func main() {
	if err := newRootCommand().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pulsequeue",
		Short: "PulseQueue durable job queue",
	}
	cmd.AddCommand(newServerCommand())
	cmd.AddCommand(newSchedulerCommand())
	cmd.AddCommand(newWorkerCommand())
	cmd.AddCommand(newHealthCommand())
	cmd.AddCommand(newJobsCommand())
	cmd.AddCommand(newWorkersCommand())
	return cmd
}

func newServerCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "server",
		Short: "Run the PulseQueue API server",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := config.Load()
			if err := cfg.ValidateServer(); err != nil {
				return err
			}
			logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			store, err := storage.Open(ctx, cfg.DatabaseURL)
			if err != nil {
				return err
			}
			defer store.Close()
			if err := store.ApplyMigrations(ctx, "migrations"); err != nil {
				return err
			}
			natsClient, err := signals.Connect(cfg.NATSURL)
			if err != nil {
				return err
			}
			defer natsClient.Close()

			httpServer := &http.Server{
				Addr:              cfg.HTTPAddr,
				Handler:           api.NewServer(store, natsClient, cfg.OperatorToken, logger),
				ReadHeaderTimeout: 5 * time.Second,
			}
			errCh := make(chan error, 2)
			go func() {
				logger.Info("http server listening", "addr", cfg.HTTPAddr)
				if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
					errCh <- err
				}
			}()
			go func() {
				errCh <- grpcserver.New(logger).Serve(ctx, cfg.GRPCAddr)
			}()

			select {
			case <-ctx.Done():
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				return httpServer.Shutdown(shutdownCtx)
			case err := <-errCh:
				return err
			}
		},
	}
}

func newSchedulerCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "scheduler",
		Short: "Run the PulseQueue scheduler",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := config.Load()
			if err := cfg.ValidateServer(); err != nil {
				return err
			}
			logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			store, err := storage.Open(ctx, cfg.DatabaseURL)
			if err != nil {
				return err
			}
			defer store.Close()
			natsClient, err := signals.Connect(cfg.NATSURL)
			if err != nil {
				return err
			}
			defer natsClient.Close()
			return scheduler.New(store, natsClient, cfg.SchedulerID, cfg.SchedulerInterval, cfg.SchedulerBatch, logger).Run(ctx)
		},
	}
}

func newWorkerCommand() *cobra.Command {
	var queue string
	var concurrency int
	cmd := &cobra.Command{
		Use:   "worker",
		Short: "Run a PulseQueue worker",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if concurrency <= 0 {
				return errors.New("concurrency must be positive")
			}
			cfg := config.Load()
			if err := cfg.ValidateServer(); err != nil {
				return err
			}
			if queue == "" {
				queue = cfg.WorkerQueue
			}
			logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			store, err := storage.Open(ctx, cfg.DatabaseURL)
			if err != nil {
				return err
			}
			defer store.Close()
			natsClient, err := signals.Connect(cfg.NATSURL)
			if err != nil {
				return err
			}
			defer natsClient.Close()
			return worker.New(store, natsClient, queue, cfg.WorkerID, concurrency, cfg.LeaseDuration, cfg.PollInterval, cfg.WorkerHeartbeat, storage.RetryPolicy{
				InitialDelay: cfg.RetryInitialDelay,
				MaxDelay:     cfg.RetryMaxDelay,
			}, logger).Run(ctx)
		},
	}
	cmd.Flags().StringVar(&queue, "queue", "default", "queue to process")
	cmd.Flags().IntVar(&concurrency, "concurrency", 1, "worker concurrency")
	return cmd
}

func newHealthCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "health",
		Short: "Check live and ready health endpoints",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := config.Load()
			client := &http.Client{Timeout: 5 * time.Second}
			for _, path := range []string{"/health/live", "/health/ready"} {
				req, err := http.NewRequestWithContext(cmd.Context(), http.MethodGet, cfg.APIURL+path, nil)
				if err != nil {
					return err
				}
				resp, err := client.Do(req)
				if err != nil {
					return err
				}
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				fmt.Printf("%s %d %s\n", path, resp.StatusCode, strings.TrimSpace(string(body)))
				if resp.StatusCode >= 300 {
					return fmt.Errorf("%s returned %d", path, resp.StatusCode)
				}
			}
			return nil
		},
	}
}

func newJobsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "jobs",
		Short: "Manage jobs",
	}
	cmd.AddCommand(newJobsSubmitCommand())
	cmd.AddCommand(newJobsListCommand())
	cmd.AddCommand(newJobsStatusCommand())
	cmd.AddCommand(newJobsAttemptsCommand())
	return cmd
}

func newJobsSubmitCommand() *cobra.Command {
	var queue, jobType, payload, idempotencyKey, output string
	var priority, maxAttempts, timeoutSeconds, delaySeconds int32
	cmd := &cobra.Command{
		Use:   "submit",
		Short: "Submit a job",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := config.Load()
			if err := cfg.ValidateClient(); err != nil {
				return err
			}
			rawPayload, err := readPayload(payload)
			if err != nil {
				return err
			}
			body := api.CreateJobRequest{
				Queue:          queue,
				Type:           jobType,
				Payload:        rawPayload,
				Priority:       priority,
				MaxAttempts:    maxAttempts,
				TimeoutSeconds: timeoutSeconds,
				DelaySeconds:   delaySeconds,
				IdempotencyKey: idempotencyKey,
			}
			var result api.CreateJobResponse
			if err := doJSON(cmd.Context(), cfg, http.MethodPost, "/jobs", body, &result); err != nil {
				return err
			}
			if output == "json" {
				return printJSON(result)
			}
			fmt.Printf("submitted job %s status=%s existing=%t\n", result.Job.ID, result.Job.Status, result.Existing)
			return nil
		},
	}
	cmd.Flags().StringVar(&queue, "queue", "default", "queue name")
	cmd.Flags().StringVar(&jobType, "type", "demo.echo", "job type")
	cmd.Flags().StringVar(&payload, "payload", "{}", "JSON payload, @file path, or file path")
	cmd.Flags().Int32Var(&priority, "priority", 0, "job priority")
	cmd.Flags().Int32Var(&maxAttempts, "max-attempts", 1, "maximum attempts")
	cmd.Flags().Int32Var(&timeoutSeconds, "timeout-seconds", 0, "job timeout in seconds, 0 disables timeout")
	cmd.Flags().Int32Var(&delaySeconds, "delay-seconds", 0, "delay before the job is eligible for execution")
	cmd.Flags().StringVar(&idempotencyKey, "idempotency-key", "", "idempotency key")
	cmd.Flags().StringVar(&output, "output", "text", "output format: text or json")
	return cmd
}

func newJobsListCommand() *cobra.Command {
	var status, queue, output string
	var limit int32
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List jobs",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := config.Load()
			if err := cfg.ValidateClient(); err != nil {
				return err
			}
			path := fmt.Sprintf("/jobs?status=%s&queue=%s&limit=%d", status, queue, limit)
			var result struct {
				Jobs []storage.Job `json:"jobs"`
			}
			if err := doJSON(cmd.Context(), cfg, http.MethodGet, path, nil, &result); err != nil {
				return err
			}
			if output == "json" {
				return printJSON(result)
			}
			for _, job := range result.Jobs {
				fmt.Printf("%s\t%s\t%s\t%s\tattempts=%d\n", job.ID, job.Queue, job.Type, job.Status, job.AttemptCount)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&status, "status", "", "filter by status")
	cmd.Flags().StringVar(&queue, "queue", "", "filter by queue")
	cmd.Flags().Int32Var(&limit, "limit", 50, "maximum rows")
	cmd.Flags().StringVar(&output, "output", "text", "output format: text or json")
	return cmd
}

func newJobsStatusCommand() *cobra.Command {
	var output string
	cmd := &cobra.Command{
		Use:   "status <job-id>",
		Short: "Show job status",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := config.Load()
			if err := cfg.ValidateClient(); err != nil {
				return err
			}
			var result struct {
				Job storage.Job `json:"job"`
			}
			if err := doJSON(cmd.Context(), cfg, http.MethodGet, "/jobs/"+args[0], nil, &result); err != nil {
				return err
			}
			if output == "json" {
				return printJSON(result)
			}
			fmt.Printf("%s\t%s\t%s\t%s\tattempts=%d\n", result.Job.ID, result.Job.Queue, result.Job.Type, result.Job.Status, result.Job.AttemptCount)
			return nil
		},
	}
	cmd.Flags().StringVar(&output, "output", "text", "output format: text or json")
	return cmd
}

func newJobsAttemptsCommand() *cobra.Command {
	var output string
	cmd := &cobra.Command{
		Use:   "attempts <job-id>",
		Short: "List attempts for a job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := config.Load()
			if err := cfg.ValidateClient(); err != nil {
				return err
			}
			var result struct {
				Attempts []storage.JobAttempt `json:"attempts"`
			}
			if err := doJSON(cmd.Context(), cfg, http.MethodGet, "/jobs/"+args[0]+"/attempts", nil, &result); err != nil {
				return err
			}
			if output == "json" {
				return printJSON(result)
			}
			for _, attempt := range result.Attempts {
				duration := ""
				if attempt.DurationMS != nil {
					duration = fmt.Sprintf(" duration_ms=%d", *attempt.DurationMS)
				}
				message := ""
				if attempt.ErrorMessage != nil {
					message = fmt.Sprintf(" error=%q", *attempt.ErrorMessage)
				}
				fmt.Printf("%d\t%s\t%s\t%s%s%s\n", attempt.AttemptNumber, attempt.ID, attempt.WorkerID, attempt.Status, duration, message)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&output, "output", "text", "output format: text or json")
	return cmd
}

func newWorkersCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workers",
		Short: "Inspect workers",
	}
	cmd.AddCommand(newWorkersListCommand())
	return cmd
}

func newWorkersListCommand() *cobra.Command {
	var output string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List registered workers",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := config.Load()
			if err := cfg.ValidateClient(); err != nil {
				return err
			}
			var result struct {
				Workers []storage.Worker `json:"workers"`
			}
			if err := doJSON(cmd.Context(), cfg, http.MethodGet, "/workers", nil, &result); err != nil {
				return err
			}
			if output == "json" {
				return printJSON(result)
			}
			for _, worker := range result.Workers {
				fmt.Printf("%s\t%s\tstatus=%s\tconcurrency=%d\tlast_heartbeat=%s\n",
					worker.ID,
					strings.Join(worker.Queues, ","),
					worker.Status,
					worker.Concurrency,
					worker.LastHeartbeatAt.Format(time.RFC3339),
				)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&output, "output", "text", "output format: text or json")
	return cmd
}

func readPayload(value string) (json.RawMessage, error) {
	if value == "" {
		return json.RawMessage(`{}`), nil
	}
	path := value
	if strings.HasPrefix(value, "@") {
		path = strings.TrimPrefix(value, "@")
	}
	if strings.HasPrefix(value, "@") || fileExists(path) {
		raw, err := os.ReadFile(filepath.Clean(path))
		if err != nil {
			return nil, err
		}
		if !json.Valid(raw) {
			return nil, errors.New("payload file must contain valid JSON")
		}
		return raw, nil
	}
	if !json.Valid([]byte(value)) {
		return nil, errors.New("payload must be valid JSON")
	}
	return json.RawMessage(value), nil
}

func doJSON(ctx context.Context, cfg config.Config, method, path string, input any, output any) error {
	var body io.Reader
	if input != nil {
		raw, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, cfg.APIURL+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+cfg.OperatorToken)
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s returned %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	if output == nil {
		return nil
	}
	return json.Unmarshal(raw, output)
}

func printJSON(value any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
