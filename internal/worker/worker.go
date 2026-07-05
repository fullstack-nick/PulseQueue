package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fullstack-nick/PulseQueue/internal/signals"
	"github.com/fullstack-nick/PulseQueue/internal/storage"
)

type Runner struct {
	store             *storage.Store
	signals           *signals.Client
	queue             string
	workerID          string
	concurrency       int
	leaseDuration     time.Duration
	pollInterval      time.Duration
	heartbeatInterval time.Duration
	retryPolicy       storage.RetryPolicy
	logger            *slog.Logger
}

func New(store *storage.Store, signals *signals.Client, queue, workerID string, concurrency int, leaseDuration, pollInterval, heartbeatInterval time.Duration, retryPolicy storage.RetryPolicy, logger *slog.Logger) *Runner {
	if queue == "" {
		queue = "default"
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	if heartbeatInterval <= 0 {
		heartbeatInterval = 10 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Runner{
		store:             store,
		signals:           signals,
		queue:             queue,
		workerID:          workerID,
		concurrency:       concurrency,
		leaseDuration:     leaseDuration,
		pollInterval:      pollInterval,
		heartbeatInterval: heartbeatInterval,
		retryPolicy:       retryPolicy.Normalize(),
		logger:            logger,
	}
}

func (r *Runner) Run(ctx context.Context) error {
	if err := r.register(ctx); err != nil {
		return err
	}

	wake := make(chan struct{}, r.concurrency)
	sub, err := r.signals.SubscribeJobAvailable(r.queue, func() {
		r.signalWake(wake)
	})
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	var status atomic.Value
	status.Store(storage.WorkerStatusRunning)
	heartbeatCtx, stopHeartbeat := context.WithCancel(context.Background())
	defer stopHeartbeat()
	go r.heartbeat(heartbeatCtx, &status)

	acceptCtx, stopAccepting := context.WithCancel(context.Background())
	defer stopAccepting()

	var wg sync.WaitGroup
	for i := 0; i < r.concurrency; i++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			r.workLoop(acceptCtx, wake, slot)
		}(i + 1)
	}

	r.signalWake(wake)
	r.logger.Info("worker started", "queue", r.queue, "worker_id", r.workerID, "concurrency", r.concurrency)

	<-ctx.Done()
	status.Store(storage.WorkerStatusDraining)
	if err := r.store.MarkWorkerStatus(context.Background(), r.workerID, storage.WorkerStatusDraining); err != nil {
		r.logger.Warn("worker draining status update failed", "worker_id", r.workerID, "error", err)
	}
	stopAccepting()
	wg.Wait()
	stopHeartbeat()
	if err := r.store.MarkWorkerStatus(context.Background(), r.workerID, storage.WorkerStatusStopped); err != nil {
		r.logger.Warn("worker stopped status update failed", "worker_id", r.workerID, "error", err)
	}
	r.logger.Info("worker stopped", "queue", r.queue, "worker_id", r.workerID)
	return nil
}

func (r *Runner) register(ctx context.Context) error {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown"
	}
	_, err = r.store.RegisterWorker(ctx, storage.RegisterWorkerParams{
		ID:          r.workerID,
		Hostname:    hostname,
		Queues:      []string{r.queue},
		Concurrency: int32(r.concurrency),
		Metadata:    json.RawMessage(`{"runtime":"go"}`),
	})
	return err
}

func (r *Runner) heartbeat(ctx context.Context, status *atomic.Value) {
	ticker := time.NewTicker(r.heartbeatInterval)
	defer ticker.Stop()
	for {
		current, _ := status.Load().(string)
		if current == "" {
			current = storage.WorkerStatusRunning
		}
		if err := r.store.HeartbeatWorker(ctx, r.workerID, current, r.leaseDuration); err != nil {
			r.logger.Warn("worker heartbeat failed", "worker_id", r.workerID, "status", current, "error", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Runner) signalWake(wake chan<- struct{}) {
	for i := 0; i < r.concurrency; i++ {
		select {
		case wake <- struct{}{}:
		default:
			return
		}
	}
}

func (r *Runner) workLoop(ctx context.Context, wake <-chan struct{}, slot int) {
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		if err := r.drain(ctx, slot); err != nil {
			r.logger.Error("worker drain failed", "worker_id", r.workerID, "slot", slot, "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-ticker.C:
		}
	}
}

func (r *Runner) drain(ctx context.Context, slot int) error {
	for {
		claimed, ok, err := r.store.ClaimJob(ctx, r.queue, r.workerID, r.leaseDuration)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		r.execute(context.Background(), claimed, slot)
		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

func (r *Runner) execute(ctx context.Context, claimed storage.ClaimedJob, slot int) {
	job := claimed.Job
	attempt := claimed.Attempt
	if job.LeaseToken == nil {
		r.logger.Error("claimed job missing lease token", "job_id", job.ID)
		return
	}
	r.logger.Info("job started",
		"job_id", job.ID,
		"attempt_id", attempt.ID,
		"attempt", attempt.AttemptNumber,
		"queue", job.Queue,
		"type", job.Type,
		"worker_id", r.workerID,
		"slot", slot,
		"status", job.Status,
	)
	r.appendJobLog(context.Background(), job, attempt, "info", "job started", map[string]any{
		"slot":   slot,
		"status": job.Status,
	})

	runCtx := ctx
	cancel := func() {}
	if job.TimeoutSeconds != nil && *job.TimeoutSeconds > 0 {
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(*job.TimeoutSeconds)*time.Second)
	}
	defer cancel()

	started := time.Now()
	if err := r.runHandler(runCtx, job); err != nil {
		message := err.Error()
		if errors.Is(err, context.DeadlineExceeded) {
			message = "job timed out"
		}
		r.fail(context.Background(), claimed, message, time.Since(started), slot)
		return
	}

	if err := r.store.CompleteJob(context.Background(), job.ID, *job.LeaseToken); err != nil {
		r.logger.Error("job completion failed",
			"job_id", job.ID,
			"attempt_id", attempt.ID,
			"attempt", attempt.AttemptNumber,
			"queue", job.Queue,
			"type", job.Type,
			"worker_id", r.workerID,
			"slot", slot,
			"duration_ms", time.Since(started).Milliseconds(),
			"error", err,
		)
		return
	}
	durationMS := time.Since(started).Milliseconds()
	r.appendJobLog(context.Background(), job, attempt, "info", "job succeeded", map[string]any{
		"slot":        slot,
		"status":      storage.StatusSucceeded,
		"duration_ms": durationMS,
	})
	r.logger.Info("job succeeded",
		"job_id", job.ID,
		"attempt_id", attempt.ID,
		"attempt", attempt.AttemptNumber,
		"queue", job.Queue,
		"type", job.Type,
		"worker_id", r.workerID,
		"slot", slot,
		"status", storage.StatusSucceeded,
		"duration_ms", durationMS,
	)
}

func (r *Runner) runHandler(ctx context.Context, job storage.Job) error {
	switch job.Type {
	case "demo.echo":
		var payload map[string]any
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return errors.New("invalid payload JSON")
		}
		r.logger.Info("demo.echo payload", "job_id", job.ID, "payload", payload)
		return nil
	case "demo.fail":
		var payload struct {
			Message string `json:"message"`
		}
		if len(job.Payload) > 0 {
			if err := json.Unmarshal(job.Payload, &payload); err != nil {
				return errors.New("invalid payload JSON")
			}
		}
		if payload.Message == "" {
			payload.Message = "demo failure"
		}
		return errors.New(payload.Message)
	case "demo.sleep":
		var payload struct {
			DurationMS int64 `json:"duration_ms"`
		}
		if len(job.Payload) > 0 {
			if err := json.Unmarshal(job.Payload, &payload); err != nil {
				return errors.New("invalid payload JSON")
			}
		}
		if payload.DurationMS <= 0 {
			payload.DurationMS = 5000
		}
		timer := time.NewTimer(time.Duration(payload.DurationMS) * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	default:
		return fmt.Errorf("unknown job type: %s", job.Type)
	}
}

func (r *Runner) fail(ctx context.Context, claimed storage.ClaimedJob, message string, duration time.Duration, slot int) {
	job := claimed.Job
	attempt := claimed.Attempt
	if job.LeaseToken == nil {
		r.logger.Error("failed job missing lease token", "job_id", job.ID, "message", message)
		return
	}
	updated, err := r.store.FailJob(ctx, job.ID, *job.LeaseToken, message, r.retryPolicy)
	if err != nil {
		r.logger.Error("job failure update failed",
			"job_id", job.ID,
			"attempt_id", attempt.ID,
			"attempt", attempt.AttemptNumber,
			"queue", job.Queue,
			"type", job.Type,
			"worker_id", r.workerID,
			"slot", slot,
			"duration_ms", duration.Milliseconds(),
			"error", err,
		)
		return
	}
	r.appendJobLog(ctx, job, attempt, "warn", "job failed", map[string]any{
		"slot":        slot,
		"status":      updated.Status,
		"duration_ms": duration.Milliseconds(),
		"error":       message,
	})
	r.logger.Warn("job failed",
		"job_id", job.ID,
		"attempt_id", attempt.ID,
		"attempt", attempt.AttemptNumber,
		"queue", job.Queue,
		"type", job.Type,
		"worker_id", r.workerID,
		"slot", slot,
		"status", updated.Status,
		"duration_ms", duration.Milliseconds(),
		"error", message,
	)
}

func (r *Runner) appendJobLog(ctx context.Context, job storage.Job, attempt storage.JobAttempt, level, message string, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	fields["attempt"] = attempt.AttemptNumber
	fields["attempt_id"] = attempt.ID
	fields["job_type"] = job.Type
	fields["queue"] = job.Queue
	fields["worker_id"] = r.workerID
	attemptID := attempt.ID
	if _, err := r.store.AppendJobLog(ctx, storage.AppendJobLogParams{
		JobID:     job.ID,
		AttemptID: &attemptID,
		Level:     level,
		Message:   message,
		Fields:    mustJSON(fields),
	}); err != nil {
		r.logger.Warn("worker job log append failed", "job_id", job.ID, "attempt_id", attempt.ID, "error", err)
	}
}

func mustJSON(value any) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}
