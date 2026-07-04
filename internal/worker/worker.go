package worker

import (
	"context"
	"encoding/json"
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
	logger        *slog.Logger
}

func New(store *storage.Store, signals *signals.Client, queue, workerID string, leaseDuration, pollInterval time.Duration, logger *slog.Logger) *Runner {
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
		job, ok, err := r.store.ClaimJob(ctx, r.queue, r.workerID, r.leaseDuration)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		r.execute(ctx, job)
	}
}

func (r *Runner) execute(ctx context.Context, job storage.Job) {
	if job.LeaseToken == nil {
		r.logger.Error("claimed job missing lease token", "job_id", job.ID)
		return
	}
	r.logger.Info("job started", "job_id", job.ID, "queue", job.Queue, "type", job.Type, "attempt", job.AttemptCount)

	switch job.Type {
	case "demo.echo":
		var payload map[string]any
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			r.fail(ctx, job, "invalid payload JSON")
			return
		}
		r.logger.Info("demo.echo payload", "job_id", job.ID, "payload", payload)
		if err := r.store.CompleteJob(ctx, job.ID, *job.LeaseToken); err != nil {
			r.logger.Error("job completion failed", "job_id", job.ID, "error", err)
			return
		}
		r.logger.Info("job succeeded", "job_id", job.ID)
	default:
		r.fail(ctx, job, "unknown job type: "+job.Type)
	}
}

func (r *Runner) fail(ctx context.Context, job storage.Job, message string) {
	if job.LeaseToken == nil {
		r.logger.Error("failed job missing lease token", "job_id", job.ID, "message", message)
		return
	}
	if err := r.store.FailJob(ctx, job.ID, *job.LeaseToken, message); err != nil {
		r.logger.Error("job failure update failed", "job_id", job.ID, "error", err)
		return
	}
	r.logger.Warn("job failed", "job_id", job.ID, "message", message)
}
