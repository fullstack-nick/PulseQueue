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
	"net/url"
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
	"github.com/fullstack-nick/PulseQueue/internal/observability"
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
	cmd.AddCommand(newQueuesCommand())
	cmd.AddCommand(newCronCommand())
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
			serviceName := serviceNameOrDefault(cfg, "pulsequeue-api")
			shutdownTracing, err := observability.InitTracing(ctx, observability.TracingConfig{
				ServiceName: serviceName,
				Endpoint:    cfg.OTLPEndpoint,
			})
			if err != nil {
				return err
			}
			defer shutdownTracingWithTimeout(shutdownTracing)
			metrics := observability.NewMetrics(serviceName)

			store, err := storage.Open(ctx, cfg.DatabaseURL)
			if err != nil {
				return err
			}
			defer store.Close()
			if err := store.ApplyMigrations(ctx, "migrations"); err != nil {
				return err
			}
			metrics.RegisterStoreCollector(store)
			natsClient, err := signals.Connect(cfg.NATSURL)
			if err != nil {
				return err
			}
			defer natsClient.Close()

			httpServer := &http.Server{
				Addr:              cfg.HTTPAddr,
				Handler:           api.NewServer(store, natsClient, cfg.OperatorToken, logger, metrics),
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
			serviceName := serviceNameOrDefault(cfg, "pulsequeue-scheduler")
			shutdownTracing, err := observability.InitTracing(ctx, observability.TracingConfig{
				ServiceName: serviceName,
				Endpoint:    cfg.OTLPEndpoint,
			})
			if err != nil {
				return err
			}
			defer shutdownTracingWithTimeout(shutdownTracing)
			metrics := observability.NewMetrics(serviceName)
			if err := observability.ServeMetrics(ctx, cfg.MetricsAddr, metrics.Handler(), logger); err != nil {
				return err
			}

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
			return scheduler.New(store, natsClient, cfg.SchedulerID, cfg.SchedulerInterval, cfg.SchedulerBatch, logger, metrics).Run(ctx)
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
			serviceName := serviceNameOrDefault(cfg, "pulsequeue-worker")
			shutdownTracing, err := observability.InitTracing(ctx, observability.TracingConfig{
				ServiceName: serviceName,
				Endpoint:    cfg.OTLPEndpoint,
			})
			if err != nil {
				return err
			}
			defer shutdownTracingWithTimeout(shutdownTracing)
			metrics := observability.NewMetrics(serviceName)
			if err := observability.ServeMetrics(ctx, cfg.MetricsAddr, metrics.Handler(), logger); err != nil {
				return err
			}
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
			}, logger, metrics).Run(ctx)
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
	cmd.AddCommand(newJobsLogsCommand())
	cmd.AddCommand(newJobsRetryCommand())
	cmd.AddCommand(newJobsCancelCommand())
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

func newJobsLogsCommand() *cobra.Command {
	var output string
	var limit int32
	cmd := &cobra.Command{
		Use:   "logs <job-id>",
		Short: "List durable logs for a job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := config.Load()
			if err := cfg.ValidateClient(); err != nil {
				return err
			}
			var result struct {
				Logs []storage.JobLog `json:"logs"`
			}
			path := fmt.Sprintf("/jobs/%s/logs?limit=%d", url.PathEscape(args[0]), limit)
			if err := doJSON(cmd.Context(), cfg, http.MethodGet, path, nil, &result); err != nil {
				return err
			}
			if output == "json" {
				return printJSON(result)
			}
			for _, log := range result.Logs {
				fields := strings.TrimSpace(string(log.Fields))
				if fields == "" || fields == "{}" {
					fmt.Printf("%s\t%s\t%s\n", log.Timestamp.Format(time.RFC3339), log.Level, log.Message)
					continue
				}
				fmt.Printf("%s\t%s\t%s\t%s\n", log.Timestamp.Format(time.RFC3339), log.Level, log.Message, fields)
			}
			return nil
		},
	}
	cmd.Flags().Int32Var(&limit, "limit", 100, "maximum rows")
	cmd.Flags().StringVar(&output, "output", "text", "output format: text or json")
	return cmd
}

func newJobsRetryCommand() *cobra.Command {
	var output string
	cmd := &cobra.Command{
		Use:   "retry <job-id>",
		Short: "Retry a failed, dead-lettered, or cancelled job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := config.Load()
			if err := cfg.ValidateClient(); err != nil {
				return err
			}
			var result struct {
				Job storage.Job `json:"job"`
			}
			if err := doJSON(cmd.Context(), cfg, http.MethodPost, "/jobs/"+url.PathEscape(args[0])+"/retry", nil, &result); err != nil {
				return err
			}
			if output == "json" {
				return printJSON(result)
			}
			fmt.Printf("retried job %s status=%s queue=%s attempts=%d max_attempts=%d\n",
				result.Job.ID,
				result.Job.Status,
				result.Job.Queue,
				result.Job.AttemptCount,
				result.Job.MaxAttempts,
			)
			return nil
		},
	}
	cmd.Flags().StringVar(&output, "output", "text", "output format: text or json")
	return cmd
}

func newJobsCancelCommand() *cobra.Command {
	var output string
	cmd := &cobra.Command{
		Use:   "cancel <job-id>",
		Short: "Cancel a queued or retry-scheduled job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := config.Load()
			if err := cfg.ValidateClient(); err != nil {
				return err
			}
			var result struct {
				Job storage.Job `json:"job"`
			}
			if err := doJSON(cmd.Context(), cfg, http.MethodPost, "/jobs/"+url.PathEscape(args[0])+"/cancel", nil, &result); err != nil {
				return err
			}
			if output == "json" {
				return printJSON(result)
			}
			fmt.Printf("cancelled job %s status=%s queue=%s\n", result.Job.ID, result.Job.Status, result.Job.Queue)
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

func newQueuesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "queues",
		Short: "Inspect queues",
	}
	cmd.AddCommand(newQueuesListCommand())
	return cmd
}

func newQueuesListCommand() *cobra.Command {
	var output string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List queue summaries",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := config.Load()
			if err := cfg.ValidateClient(); err != nil {
				return err
			}
			var result struct {
				Queues []storage.QueueSummary `json:"queues"`
			}
			if err := doJSON(cmd.Context(), cfg, http.MethodGet, "/queues", nil, &result); err != nil {
				return err
			}
			if output == "json" {
				return printJSON(result)
			}
			for _, queue := range result.Queues {
				oldest := ""
				if queue.OldestAvailableAt != nil {
					oldest = " oldest_available_at=" + queue.OldestAvailableAt.Format(time.RFC3339)
				}
				fmt.Printf("%s\ttotal=%d\tqueued=%d\tretry_scheduled=%d\trunning=%d\tsucceeded=%d\tdead_letter=%d\tcancelled=%d\tactive_workers=%d%s\n",
					queue.Queue,
					queue.TotalJobs,
					queue.Queued,
					queue.RetryScheduled,
					queue.Running,
					queue.Succeeded,
					queue.DeadLetter,
					queue.Cancelled,
					queue.ActiveWorkers,
					oldest,
				)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&output, "output", "text", "output format: text or json")
	return cmd
}

func newCronCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cron",
		Short: "Manage cron jobs",
	}
	cmd.AddCommand(newCronCreateCommand())
	cmd.AddCommand(newCronListCommand())
	cmd.AddCommand(newCronSetEnabledCommand("enable", true))
	cmd.AddCommand(newCronSetEnabledCommand("disable", false))
	return cmd
}

func newCronCreateCommand() *cobra.Command {
	var name, queue, jobType, payload, schedule, output string
	var priority, maxAttempts, timeoutSeconds int32
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a cron job",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if strings.TrimSpace(name) == "" {
				return errors.New("--name is required")
			}
			if strings.TrimSpace(schedule) == "" {
				return errors.New("--schedule is required")
			}
			if strings.TrimSpace(jobType) == "" {
				return errors.New("--type is required")
			}
			cfg := config.Load()
			if err := cfg.ValidateClient(); err != nil {
				return err
			}
			rawPayload, err := readPayload(payload)
			if err != nil {
				return err
			}
			body := api.CreateCronJobRequest{
				Name:           name,
				Queue:          queue,
				Type:           jobType,
				Payload:        rawPayload,
				Schedule:       schedule,
				Priority:       priority,
				MaxAttempts:    maxAttempts,
				TimeoutSeconds: timeoutSeconds,
			}
			var result struct {
				CronJob storage.CronJob `json:"cron_job"`
			}
			if err := doJSON(cmd.Context(), cfg, http.MethodPost, "/cron", body, &result); err != nil {
				return err
			}
			if output == "json" {
				return printJSON(result)
			}
			fmt.Printf("created cron %s id=%s schedule=%q next_run_at=%s\n",
				result.CronJob.Name,
				result.CronJob.ID,
				result.CronJob.Schedule,
				result.CronJob.NextRunAt.Format(time.RFC3339),
			)
			return nil
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "cron job name")
	cmd.Flags().StringVar(&queue, "queue", "default", "queue name")
	cmd.Flags().StringVar(&jobType, "type", "demo.echo", "job type")
	cmd.Flags().StringVar(&payload, "payload", "{}", "JSON payload, @file path, or file path")
	cmd.Flags().StringVar(&schedule, "schedule", "", "5-field UTC cron schedule")
	cmd.Flags().Int32Var(&priority, "priority", 0, "job priority")
	cmd.Flags().Int32Var(&maxAttempts, "max-attempts", 1, "maximum attempts")
	cmd.Flags().Int32Var(&timeoutSeconds, "timeout-seconds", 0, "job timeout in seconds, 0 disables timeout")
	cmd.Flags().StringVar(&output, "output", "text", "output format: text or json")
	return cmd
}

func newCronListCommand() *cobra.Command {
	var output string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List cron jobs",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := config.Load()
			if err := cfg.ValidateClient(); err != nil {
				return err
			}
			var result struct {
				CronJobs []storage.CronJob `json:"cron_jobs"`
			}
			if err := doJSON(cmd.Context(), cfg, http.MethodGet, "/cron", nil, &result); err != nil {
				return err
			}
			if output == "json" {
				return printJSON(result)
			}
			for _, cronJob := range result.CronJobs {
				last := ""
				if cronJob.LastRunAt != nil {
					last = " last_run_at=" + cronJob.LastRunAt.Format(time.RFC3339)
				}
				fmt.Printf("%s\t%s\t%s\t%s\tenabled=%t\tnext_run_at=%s%s\n",
					cronJob.ID,
					cronJob.Name,
					cronJob.Queue,
					cronJob.Schedule,
					cronJob.Enabled,
					cronJob.NextRunAt.Format(time.RFC3339),
					last,
				)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&output, "output", "text", "output format: text or json")
	return cmd
}

func newCronSetEnabledCommand(name string, enabled bool) *cobra.Command {
	verb := "Enable"
	if !enabled {
		verb = "Disable"
	}
	var output string
	cmd := &cobra.Command{
		Use:   name + " <id-or-name>",
		Short: verb + " a cron job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := config.Load()
			if err := cfg.ValidateClient(); err != nil {
				return err
			}
			var result struct {
				CronJob storage.CronJob `json:"cron_job"`
			}
			if err := doJSON(cmd.Context(), cfg, http.MethodPost, "/cron/"+url.PathEscape(args[0])+"/"+name, nil, &result); err != nil {
				return err
			}
			if output == "json" {
				return printJSON(result)
			}
			fmt.Printf("%s cron %s enabled=%t next_run_at=%s\n",
				name,
				result.CronJob.Name,
				result.CronJob.Enabled,
				result.CronJob.NextRunAt.Format(time.RFC3339),
			)
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

func serviceNameOrDefault(cfg config.Config, fallback string) string {
	if cfg.ServiceName != "" {
		return cfg.ServiceName
	}
	return fallback
}

func shutdownTracingWithTimeout(shutdown func(context.Context) error) {
	if shutdown == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = shutdown(ctx)
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
