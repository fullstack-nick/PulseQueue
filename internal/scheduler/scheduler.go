package scheduler

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/fullstack-nick/PulseQueue/internal/observability"
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
	metrics     *observability.Metrics
	tracer      trace.Tracer
}

func New(store *storage.Store, signals *signals.Client, schedulerID string, interval time.Duration, batchSize int32, logger *slog.Logger, metrics *observability.Metrics) *Runner {
	if schedulerID == "" {
		schedulerID = "scheduler-local"
	}
	if interval <= 0 {
		interval = 2 * time.Second
	}
	if batchSize <= 0 {
		batchSize = 50
	}
	if metrics == nil {
		metrics = observability.NewMetrics("pulsequeue-scheduler")
	}
	return &Runner{
		store:       store,
		signals:     signals,
		schedulerID: schedulerID,
		interval:    interval,
		batchSize:   batchSize,
		logger:      logger,
		metrics:     metrics,
		tracer:      observability.Tracer("github.com/fullstack-nick/PulseQueue/internal/scheduler"),
	}
}

func (r *Runner) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	r.logger.Info("scheduler started", "scheduler_id", r.schedulerID, "interval", r.interval.String(), "batch_size", r.batchSize)
	for {
		if err := r.tick(ctx); err != nil {
			r.metrics.RecordSchedulerTick("error")
			r.logger.Error("scheduler tick failed", "scheduler_id", r.schedulerID, "error", err)
		} else {
			r.metrics.RecordSchedulerTick("success")
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
	ctx, span := r.tracer.Start(ctx, "scheduler.tick")
	defer span.End()

	queues := map[string]struct{}{}

	traceContext := observability.InjectTraceContext(ctx)
	fires, err := r.store.FireDueCronJobs(ctx, r.schedulerID, r.batchSize, traceContext)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	for _, fire := range fires {
		r.metrics.RecordCronJobFired(fire.Job)
		r.logger.Info("scheduler fired cron job",
			"scheduler_id", r.schedulerID,
			"cron_job_id", fire.CronJob.ID,
			"cron_name", fire.CronJob.Name,
			"job_id", fire.Job.ID,
			"queue", fire.Job.Queue,
			"job_type", fire.Job.Type,
			"scheduled_for", fire.Run.ScheduledFor.Format(time.RFC3339),
		)
		queues[fire.Job.Queue] = struct{}{}
	}

	recovered, err := r.store.RecoverExpiredJobs(ctx, r.batchSize, "job lease expired")
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	for _, job := range recovered {
		r.metrics.RecordSchedulerRecovered(job)
		fields := map[string]any{}
		addTraceFields(ctx, fields)
		if _, err := r.store.AppendJobLog(ctx, storage.AppendJobLogParams{
			JobID:   job.ID,
			Level:   "warn",
			Message: "job lease recovered",
			Fields:  mustJSON(fields),
		}); err != nil {
			r.logger.Warn("scheduler job log append failed", "scheduler_id", r.schedulerID, "job_id", job.ID, "error", err)
		}
		r.logger.Warn("scheduler recovered expired job",
			"scheduler_id", r.schedulerID,
			"job_id", job.ID,
			"queue", job.Queue,
			"job_type", job.Type,
			"status", job.Status,
			"attempts", job.AttemptCount,
		)
		if job.Status == storage.StatusRetryScheduled || job.Status == storage.StatusQueued {
			queues[job.Queue] = struct{}{}
		}
	}

	dueQueues, err := r.store.ListDueQueues(ctx, r.batchSize)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	for _, queue := range dueQueues {
		queues[queue] = struct{}{}
	}

	for queue := range queues {
		if err := r.signals.PublishJobAvailable(queue); err != nil {
			r.metrics.RecordNATSPublishFailure("scheduler", queue)
			r.logger.Warn("scheduler nats publish failed", "scheduler_id", r.schedulerID, "queue", queue, "error", err)
			continue
		}
		r.logger.Info("scheduler published job availability", "scheduler_id", r.schedulerID, "queue", queue)
	}
	span.SetAttributes(
		attribute.Int("scheduler.cron_fires", len(fires)),
		attribute.Int("scheduler.recovered_jobs", len(recovered)),
		attribute.Int("scheduler.signaled_queues", len(queues)),
	)
	return nil
}

func addTraceFields(ctx context.Context, fields map[string]any) {
	traceFields := observability.TraceLogFields(ctx)
	for i := 0; i+1 < len(traceFields); i += 2 {
		key, ok := traceFields[i].(string)
		if !ok {
			continue
		}
		fields[key] = traceFields[i+1]
	}
}

func mustJSON(value any) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}
