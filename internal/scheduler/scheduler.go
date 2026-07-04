package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/fullstack-nick/PulseQueue/internal/signals"
	"github.com/fullstack-nick/PulseQueue/internal/storage"
)

type Runner struct {
	store       *storage.Store
	signals     *signals.Client
	schedulerID string
	interval    time.Duration
	batchSize   int32
	logger      *slog.Logger
}

func New(store *storage.Store, signals *signals.Client, schedulerID string, interval time.Duration, batchSize int32, logger *slog.Logger) *Runner {
	if schedulerID == "" {
		schedulerID = "scheduler-local"
	}
	if interval <= 0 {
		interval = 2 * time.Second
	}
	if batchSize <= 0 {
		batchSize = 50
	}
	return &Runner{
		store:       store,
		signals:     signals,
		schedulerID: schedulerID,
		interval:    interval,
		batchSize:   batchSize,
		logger:      logger,
	}
}

func (r *Runner) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	r.logger.Info("scheduler started", "scheduler_id", r.schedulerID, "interval", r.interval.String(), "batch_size", r.batchSize)
	for {
		if err := r.tick(ctx); err != nil {
			r.logger.Error("scheduler tick failed", "scheduler_id", r.schedulerID, "error", err)
		}

		select {
		case <-ctx.Done():
			r.logger.Info("scheduler stopped", "scheduler_id", r.schedulerID)
			return nil
		case <-ticker.C:
		}
	}
}

func (r *Runner) tick(ctx context.Context) error {
	queues := map[string]struct{}{}

	recovered, err := r.store.RecoverExpiredJobs(ctx, r.batchSize, "job lease expired")
	if err != nil {
		return err
	}
	for _, job := range recovered {
		r.logger.Warn("scheduler recovered expired job",
			"scheduler_id", r.schedulerID,
			"job_id", job.ID,
			"queue", job.Queue,
			"type", job.Type,
			"status", job.Status,
			"attempts", job.AttemptCount,
		)
		if job.Status == storage.StatusRetryScheduled || job.Status == storage.StatusQueued {
			queues[job.Queue] = struct{}{}
		}
	}

	dueQueues, err := r.store.ListDueQueues(ctx, r.batchSize)
	if err != nil {
		return err
	}
	for _, queue := range dueQueues {
		queues[queue] = struct{}{}
	}

	for queue := range queues {
		if err := r.signals.PublishJobAvailable(queue); err != nil {
			r.logger.Warn("scheduler nats publish failed", "scheduler_id", r.schedulerID, "queue", queue, "error", err)
			continue
		}
		r.logger.Info("scheduler published job availability", "scheduler_id", r.schedulerID, "queue", queue)
	}
	return nil
}
