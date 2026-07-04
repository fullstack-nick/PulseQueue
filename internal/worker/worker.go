package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/fullstack-nick/PulseQueue/internal/signals"
	"github.com/fullstack-nick/PulseQueue/internal/storage"
)

type Runner struct {
	store         *storage.Store
	signals       *signals.Client
	queue         string
	workerID      string
	leaseDuration time.Duration
	pollInterval  time.Duration
	retryPolicy   storage.RetryPolicy
	logger        *slog.Logger
}

func New(store *storage.Store, signals *signals.Client, queue, workerID string, leaseDuration, pollInterval time.Duration, retryPolicy storage.RetryPolicy, logger *slog.Logger) *Runner {
	if queue == "" {
		queue = "default"
	}
	return &Runner{
		store:         store,
		signals:       signals,
		queue:         queue,
		workerID:      workerID,
		leaseDuration: leaseDuration,
		pollInterval:  pollInterval,
		retryPolicy:   retryPolicy.Normalize(),
		logger:        logger,
	}
}

func (r *Runner) Run(ctx context.Context) error {
	wake := make(chan struct{}, 1)
	sub, err := r.signals.SubscribeJobAvailable(r.queue, func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	})
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	r.logger.Info("worker started", "queue", r.queue, "worker_id", r.workerID)
	for {
		if err := r.drain(ctx); err != nil {
			r.logger.Error("worker drain failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		case <-ticker.C:
		}
	}
}

func (r *Runner) drain(ctx context.Context) error {
	for {
		claimed, ok, err := r.store.ClaimJob(ctx, r.queue, r.workerID, r.leaseDuration)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		r.execute(ctx, claimed)
	}
}

func (r *Runner) execute(ctx context.Context, claimed storage.ClaimedJob) {
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
		"status", job.Status,
	)

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
		r.fail(ctx, claimed, message, time.Since(started))
		return
	}

	if err := r.store.CompleteJob(ctx, job.ID, *job.LeaseToken); err != nil {
		r.logger.Error("job completion failed",
			"job_id", job.ID,
			"attempt_id", attempt.ID,
			"attempt", attempt.AttemptNumber,
			"queue", job.Queue,
			"type", job.Type,
			"worker_id", r.workerID,
			"duration_ms", time.Since(started).Milliseconds(),
			"error", err,
		)
		return
	}
	r.logger.Info("job succeeded",
		"job_id", job.ID,
		"attempt_id", attempt.ID,
		"attempt", attempt.AttemptNumber,
		"queue", job.Queue,
		"type", job.Type,
		"worker_id", r.workerID,
		"status", storage.StatusSucceeded,
		"duration_ms", time.Since(started).Milliseconds(),
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

func (r *Runner) fail(ctx context.Context, claimed storage.ClaimedJob, message string, duration time.Duration) {
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
			"duration_ms", duration.Milliseconds(),
			"error", err,
		)
		return
	}
	r.logger.Warn("job failed",
		"job_id", job.ID,
		"attempt_id", attempt.ID,
		"attempt", attempt.AttemptNumber,
		"queue", job.Queue,
		"type", job.Type,
		"worker_id", r.workerID,
		"status", updated.Status,
		"duration_ms", duration.Milliseconds(),
		"error", message,
	)
}
