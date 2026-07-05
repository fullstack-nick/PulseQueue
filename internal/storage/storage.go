package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fullstack-nick/PulseQueue/internal/cronexpr"
)

const (
	StatusQueued         = "queued"
	StatusRunning        = "running"
	StatusSucceeded      = "succeeded"
	StatusFailed         = "failed"
	StatusRetryScheduled = "retry_scheduled"
	StatusDeadLetter     = "dead_letter"
	StatusCancelled      = "cancelled"

	AttemptStatusRunning   = "running"
	AttemptStatusSucceeded = "succeeded"
	AttemptStatusFailed    = "failed"

	WorkerStatusStarting = "starting"
	WorkerStatusRunning  = "running"
	WorkerStatusDraining = "draining"
	WorkerStatusStopped  = "stopped"
)

const jobSelectColumns = `
	id, queue, type, payload, status, priority, max_attempts, attempt_count,
	idempotency_key, locked_by, locked_until, lease_token, timeout_seconds,
	traceparent, tracestate, available_at, dead_lettered_at, created_at, updated_at,
	completed_at, last_error
`

const jobAttemptSelectColumns = `
	id, job_id, worker_id, lease_token, attempt_number, status,
	started_at, finished_at, error_message, duration_ms
`

const workerSelectColumns = `
	id, hostname, queues, status, concurrency, metadata,
	started_at, last_heartbeat_at, updated_at
`

const jobLogSelectColumns = `
	id, job_id, attempt_id, logged_at, level, message, fields
`

const cronJobSelectColumns = `
	id, name, queue, type, payload, schedule, enabled, priority, max_attempts,
	timeout_seconds, next_run_at, last_run_at, created_at, updated_at
`

const cronRunSelectColumns = `
	id, cron_job_id, scheduled_for, job_id, scheduler_id, created_at
`

type Store struct {
	pool *pgxpool.Pool
}

var ErrInvalidState = errors.New("invalid state transition")

func IsNotFound(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}

func IsInvalidState(err error) bool {
	return errors.Is(err, ErrInvalidState)
}

type Job struct {
	ID             uuid.UUID       `json:"id"`
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Status         string          `json:"status"`
	Priority       int32           `json:"priority"`
	MaxAttempts    int32           `json:"max_attempts"`
	AttemptCount   int32           `json:"attempt_count"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	LockedBy       *string         `json:"locked_by,omitempty"`
	LockedUntil    *time.Time      `json:"locked_until,omitempty"`
	LeaseToken     *uuid.UUID      `json:"lease_token,omitempty"`
	TimeoutSeconds *int32          `json:"timeout_seconds,omitempty"`
	TraceParent    *string         `json:"traceparent,omitempty"`
	TraceState     *string         `json:"tracestate,omitempty"`
	AvailableAt    time.Time       `json:"available_at"`
	DeadLetteredAt *time.Time      `json:"dead_lettered_at,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	CompletedAt    *time.Time      `json:"completed_at,omitempty"`
	LastError      *string         `json:"last_error,omitempty"`
}

type JobAttempt struct {
	ID            uuid.UUID  `json:"id"`
	JobID         uuid.UUID  `json:"job_id"`
	WorkerID      string     `json:"worker_id"`
	LeaseToken    uuid.UUID  `json:"lease_token"`
	AttemptNumber int32      `json:"attempt_number"`
	Status        string     `json:"status"`
	StartedAt     time.Time  `json:"started_at"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
	ErrorMessage  *string    `json:"error_message,omitempty"`
	DurationMS    *int64     `json:"duration_ms,omitempty"`
}

type ClaimedJob struct {
	Job     Job        `json:"job"`
	Attempt JobAttempt `json:"attempt"`
}

type Worker struct {
	ID              string          `json:"id"`
	Hostname        string          `json:"hostname"`
	Queues          []string        `json:"queues"`
	Status          string          `json:"status"`
	Concurrency     int32           `json:"concurrency"`
	Metadata        json.RawMessage `json:"metadata"`
	StartedAt       time.Time       `json:"started_at"`
	LastHeartbeatAt time.Time       `json:"last_heartbeat_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

type JobLog struct {
	ID        uuid.UUID       `json:"id"`
	JobID     uuid.UUID       `json:"job_id"`
	AttemptID *uuid.UUID      `json:"attempt_id,omitempty"`
	Timestamp time.Time       `json:"timestamp"`
	Level     string          `json:"level"`
	Message   string          `json:"message"`
	Fields    json.RawMessage `json:"fields"`
}

type CronJob struct {
	ID             uuid.UUID       `json:"id"`
	Name           string          `json:"name"`
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Schedule       string          `json:"schedule"`
	Enabled        bool            `json:"enabled"`
	Priority       int32           `json:"priority"`
	MaxAttempts    int32           `json:"max_attempts"`
	TimeoutSeconds *int32          `json:"timeout_seconds,omitempty"`
	NextRunAt      time.Time       `json:"next_run_at"`
	LastRunAt      *time.Time      `json:"last_run_at,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

type CronRun struct {
	ID           uuid.UUID  `json:"id"`
	CronJobID    uuid.UUID  `json:"cron_job_id"`
	ScheduledFor time.Time  `json:"scheduled_for"`
	JobID        *uuid.UUID `json:"job_id,omitempty"`
	SchedulerID  string     `json:"scheduler_id"`
	CreatedAt    time.Time  `json:"created_at"`
}

type CronFire struct {
	CronJob CronJob `json:"cron_job"`
	Run     CronRun `json:"run"`
	Job     Job     `json:"job"`
}

type QueueSummary struct {
	Queue             string     `json:"queue"`
	TotalJobs         int64      `json:"total_jobs"`
	Queued            int64      `json:"queued"`
	Running           int64      `json:"running"`
	RetryScheduled    int64      `json:"retry_scheduled"`
	Succeeded         int64      `json:"succeeded"`
	DeadLetter        int64      `json:"dead_letter"`
	Cancelled         int64      `json:"cancelled"`
	ActiveWorkers     int64      `json:"active_workers"`
	OldestAvailableAt *time.Time `json:"oldest_available_at,omitempty"`
}

type TraceContext struct {
	TraceParent string
	TraceState  string
}

type JobStatusMetric struct {
	Queue  string
	Status string
	Count  int64
}

type QueueMetric struct {
	Queue string
	Value int64
}

type ObservabilitySnapshot struct {
	JobsByStatus  []JobStatusMetric
	QueueDepth    []QueueMetric
	ActiveJobs    []QueueMetric
	ActiveWorkers []QueueMetric
}

type RetryPolicy struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

type CreateJobParams struct {
	Queue          string
	Type           string
	Payload        json.RawMessage
	Priority       int32
	MaxAttempts    int32
	TimeoutSeconds int32
	DelaySeconds   int32
	IdempotencyKey *string
	TraceParent    string
	TraceState     string
}

type RegisterWorkerParams struct {
	ID          string
	Hostname    string
	Queues      []string
	Concurrency int32
	Metadata    json.RawMessage
}

type ListJobsFilter struct {
	Status string
	Queue  string
	Limit  int32
}

type AppendJobLogParams struct {
	JobID     uuid.UUID
	AttemptID *uuid.UUID
	Level     string
	Message   string
	Fields    json.RawMessage
}

type CreateCronJobParams struct {
	Name           string
	Queue          string
	Type           string
	Payload        json.RawMessage
	Schedule       string
	Priority       int32
	MaxAttempts    int32
	TimeoutSeconds int32
}

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *Store) ApplyMigrations(ctx context.Context, dir string) error {
	if dir == "" {
		dir = "migrations"
	}
	if _, err := os.Stat(dir); err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		version := strings.TrimSuffix(entry.Name(), ".sql")
		var exists bool
		if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, version).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return err
		}
		upSQL := strings.Split(string(raw), "-- +goose Down")[0]
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, upSQL); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", entry.Name(), err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
			tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) CreateJob(ctx context.Context, p CreateJobParams) (Job, bool, error) {
	if p.Queue == "" {
		p.Queue = "default"
	}
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 1
	}
	if p.TimeoutSeconds < 0 {
		return Job{}, false, errors.New("timeout_seconds must be non-negative")
	}
	if p.DelaySeconds < 0 {
		return Job{}, false, errors.New("delay_seconds must be non-negative")
	}
	if len(p.Payload) == 0 {
		p.Payload = []byte(`{}`)
	}
	if !json.Valid(p.Payload) {
		return Job{}, false, errors.New("payload must be valid JSON")
	}

	job, err := scanJob(s.pool.QueryRow(ctx, fmt.Sprintf(`
		INSERT INTO jobs (queue, type, payload, status, priority, max_attempts, timeout_seconds, idempotency_key, traceparent, tracestate, available_at)
		VALUES ($1, $2, $3, 'queued', $4, $5, NULLIF($6, 0), NULLIF($7, ''), NULLIF($8, ''), NULLIF($9, ''), now() + $10::interval)
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
		RETURNING %s
	`, jobSelectColumns), p.Queue, p.Type, p.Payload, p.Priority, p.MaxAttempts, p.TimeoutSeconds, nullableStringValue(p.IdempotencyKey), p.TraceParent, p.TraceState, intervalLiteral(time.Duration(p.DelaySeconds)*time.Second)))
	if err == nil {
		return job, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, err
	}
	if p.IdempotencyKey == nil || *p.IdempotencyKey == "" {
		return Job{}, false, errors.New("job insert returned no row")
	}

	existing, err := scanJob(s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT %s FROM jobs WHERE idempotency_key = $1`, jobSelectColumns), *p.IdempotencyKey))
	if err != nil {
		return Job{}, false, err
	}
	return existing, true, nil
}

func (s *Store) GetJob(ctx context.Context, id uuid.UUID) (Job, error) {
	return scanJob(s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT %s FROM jobs WHERE id = $1`, jobSelectColumns), id))
}

func (s *Store) ListJobs(ctx context.Context, filter ListJobsFilter) ([]Job, error) {
	limit := filter.Limit
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT %s
		FROM jobs
		WHERE ($1 = '' OR status = $1)
		  AND ($2 = '' OR queue = $2)
		ORDER BY created_at DESC
		LIMIT $3
	`, jobSelectColumns), filter.Status, filter.Queue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *Store) ListJobAttempts(ctx context.Context, jobID uuid.UUID) ([]JobAttempt, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT %s
		FROM job_attempts
		WHERE job_id = $1
		ORDER BY attempt_number ASC
	`, jobAttemptSelectColumns), jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var attempts []JobAttempt
	for rows.Next() {
		attempt, err := scanJobAttempt(rows)
		if err != nil {
			return nil, err
		}
		attempts = append(attempts, attempt)
	}
	return attempts, rows.Err()
}

func (s *Store) AppendJobLog(ctx context.Context, p AppendJobLogParams) (JobLog, error) {
	return appendJobLog(ctx, s.pool, p)
}

func (s *Store) ListJobLogs(ctx context.Context, jobID uuid.UUID, limit int32) ([]JobLog, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT %s
		FROM job_logs
		WHERE job_id = $1
		ORDER BY logged_at ASC, id ASC
		LIMIT $2
	`, jobLogSelectColumns), jobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []JobLog
	for rows.Next() {
		log, err := scanJobLog(rows)
		if err != nil {
			return nil, err
		}
		logs = append(logs, log)
	}
	return logs, rows.Err()
}

func (s *Store) RetryJob(ctx context.Context, id uuid.UUID) (Job, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Job{}, err
	}
	defer tx.Rollback(ctx)

	current, err := scanJob(tx.QueryRow(ctx, fmt.Sprintf(`
		SELECT %s
		FROM jobs
		WHERE id = $1
		FOR UPDATE
	`, jobSelectColumns), id))
	if err != nil {
		return Job{}, err
	}
	if current.Status != StatusFailed && current.Status != StatusDeadLetter && current.Status != StatusCancelled {
		return Job{}, fmt.Errorf("%w: only failed, dead_letter, or cancelled jobs can be retried", ErrInvalidState)
	}

	job, err := scanJob(tx.QueryRow(ctx, fmt.Sprintf(`
		UPDATE jobs
		SET status = 'queued',
		    max_attempts = GREATEST(max_attempts, attempt_count + 1),
		    available_at = now(),
		    dead_lettered_at = NULL,
		    completed_at = NULL,
		    locked_by = NULL,
		    locked_until = NULL,
		    lease_token = NULL,
		    last_error = NULL,
		    updated_at = now()
		WHERE id = $1
		RETURNING %s
	`, jobSelectColumns), id))
	if err != nil {
		return Job{}, err
	}
	if _, err := appendJobLog(ctx, tx, AppendJobLogParams{
		JobID:   job.ID,
		Level:   "info",
		Message: "job manually retried",
		Fields:  mustJSON(map[string]any{"previous_status": current.Status}),
	}); err != nil {
		return Job{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, err
	}
	return job, nil
}

func (s *Store) CancelJob(ctx context.Context, id uuid.UUID) (Job, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Job{}, err
	}
	defer tx.Rollback(ctx)

	current, err := scanJob(tx.QueryRow(ctx, fmt.Sprintf(`
		SELECT %s
		FROM jobs
		WHERE id = $1
		FOR UPDATE
	`, jobSelectColumns), id))
	if err != nil {
		return Job{}, err
	}
	if current.Status != StatusQueued && current.Status != StatusRetryScheduled {
		return Job{}, fmt.Errorf("%w: only queued or retry_scheduled jobs can be cancelled", ErrInvalidState)
	}

	job, err := scanJob(tx.QueryRow(ctx, fmt.Sprintf(`
		UPDATE jobs
		SET status = 'cancelled',
		    completed_at = now(),
		    updated_at = now(),
		    locked_by = NULL,
		    locked_until = NULL,
		    lease_token = NULL,
		    last_error = 'cancelled by operator'
		WHERE id = $1
		RETURNING %s
	`, jobSelectColumns), id))
	if err != nil {
		return Job{}, err
	}
	if _, err := appendJobLog(ctx, tx, AppendJobLogParams{
		JobID:   job.ID,
		Level:   "warn",
		Message: "job cancelled",
		Fields:  mustJSON(map[string]any{"previous_status": current.Status}),
	}); err != nil {
		return Job{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, err
	}
	return job, nil
}

func (s *Store) ListQueues(ctx context.Context) ([]QueueSummary, error) {
	rows, err := s.pool.Query(ctx, `
		WITH job_counts AS (
			SELECT
				queue,
				count(*) AS total_jobs,
				count(*) FILTER (WHERE status = 'queued') AS queued,
				count(*) FILTER (WHERE status = 'running') AS running,
				count(*) FILTER (WHERE status = 'retry_scheduled') AS retry_scheduled,
				count(*) FILTER (WHERE status = 'succeeded') AS succeeded,
				count(*) FILTER (WHERE status = 'dead_letter') AS dead_letter,
				count(*) FILTER (WHERE status = 'cancelled') AS cancelled,
				min(available_at) FILTER (WHERE status IN ('queued', 'retry_scheduled')) AS oldest_available_at
			FROM jobs
			GROUP BY queue
		),
		worker_counts AS (
			SELECT
				queue,
				count(*) FILTER (WHERE status = 'running') AS active_workers
			FROM workers
			CROSS JOIN LATERAL unnest(queues) AS queue
			GROUP BY queue
		),
		all_queues AS (
			SELECT queue FROM job_counts
			UNION
			SELECT queue FROM worker_counts
		)
		SELECT
			q.queue,
			COALESCE(j.total_jobs, 0),
			COALESCE(j.queued, 0),
			COALESCE(j.running, 0),
			COALESCE(j.retry_scheduled, 0),
			COALESCE(j.succeeded, 0),
			COALESCE(j.dead_letter, 0),
			COALESCE(j.cancelled, 0),
			COALESCE(w.active_workers, 0),
			j.oldest_available_at
		FROM all_queues q
		LEFT JOIN job_counts j ON j.queue = q.queue
		LEFT JOIN worker_counts w ON w.queue = q.queue
		ORDER BY q.queue ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var queues []QueueSummary
	for rows.Next() {
		var queue QueueSummary
		if err := rows.Scan(
			&queue.Queue,
			&queue.TotalJobs,
			&queue.Queued,
			&queue.Running,
			&queue.RetryScheduled,
			&queue.Succeeded,
			&queue.DeadLetter,
			&queue.Cancelled,
			&queue.ActiveWorkers,
			&queue.OldestAvailableAt,
		); err != nil {
			return nil, err
		}
		queues = append(queues, queue)
	}
	return queues, rows.Err()
}

func (s *Store) ObservabilitySnapshot(ctx context.Context) (ObservabilitySnapshot, error) {
	statusRows, err := s.pool.Query(ctx, `
		SELECT queue, status, count(*)
		FROM jobs
		GROUP BY queue, status
		ORDER BY queue ASC, status ASC
	`)
	if err != nil {
		return ObservabilitySnapshot{}, err
	}
	defer statusRows.Close()

	var snapshot ObservabilitySnapshot
	for statusRows.Next() {
		var metric JobStatusMetric
		if err := statusRows.Scan(&metric.Queue, &metric.Status, &metric.Count); err != nil {
			return ObservabilitySnapshot{}, err
		}
		snapshot.JobsByStatus = append(snapshot.JobsByStatus, metric)
	}
	if err := statusRows.Err(); err != nil {
		return ObservabilitySnapshot{}, err
	}

	queueDepth, err := s.queryQueueMetrics(ctx, `
		WITH queues AS (
			SELECT DISTINCT queue FROM jobs
			UNION
			SELECT DISTINCT unnest(queues) AS queue FROM workers
		),
		counts AS (
			SELECT queue, count(*) AS value
			FROM jobs
			WHERE status IN ('queued', 'retry_scheduled')
			GROUP BY queue
		)
		SELECT queues.queue, COALESCE(counts.value, 0)
		FROM queues
		LEFT JOIN counts ON counts.queue = queues.queue
		ORDER BY queues.queue ASC
	`)
	if err != nil {
		return ObservabilitySnapshot{}, err
	}
	activeJobs, err := s.queryQueueMetrics(ctx, `
		WITH queues AS (
			SELECT DISTINCT queue FROM jobs
			UNION
			SELECT DISTINCT unnest(queues) AS queue FROM workers
		),
		counts AS (
			SELECT queue, count(*) AS value
			FROM jobs
			WHERE status = 'running'
			GROUP BY queue
		)
		SELECT queues.queue, COALESCE(counts.value, 0)
		FROM queues
		LEFT JOIN counts ON counts.queue = queues.queue
		ORDER BY queues.queue ASC
	`)
	if err != nil {
		return ObservabilitySnapshot{}, err
	}
	activeWorkers, err := s.queryQueueMetrics(ctx, `
		SELECT queue, count(*)
		FROM workers
		CROSS JOIN LATERAL unnest(queues) AS queue
		WHERE status = 'running'
		GROUP BY queue
		ORDER BY queue ASC
	`)
	if err != nil {
		return ObservabilitySnapshot{}, err
	}

	snapshot.QueueDepth = queueDepth
	snapshot.ActiveJobs = activeJobs
	snapshot.ActiveWorkers = activeWorkers
	return snapshot, nil
}

func (s *Store) queryQueueMetrics(ctx context.Context, query string) ([]QueueMetric, error) {
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var metrics []QueueMetric
	for rows.Next() {
		var metric QueueMetric
		if err := rows.Scan(&metric.Queue, &metric.Value); err != nil {
			return nil, err
		}
		metrics = append(metrics, metric)
	}
	return metrics, rows.Err()
}

func (s *Store) CreateCronJob(ctx context.Context, p CreateCronJobParams) (CronJob, error) {
	p.Name = strings.TrimSpace(p.Name)
	p.Queue = strings.TrimSpace(p.Queue)
	p.Type = strings.TrimSpace(p.Type)
	p.Schedule = strings.TrimSpace(p.Schedule)
	if p.Name == "" {
		return CronJob{}, errors.New("cron name is required")
	}
	if p.Queue == "" {
		p.Queue = "default"
	}
	if p.Type == "" {
		return CronJob{}, errors.New("cron job type is required")
	}
	if len(p.Payload) == 0 {
		p.Payload = []byte(`{}`)
	}
	if !json.Valid(p.Payload) {
		return CronJob{}, errors.New("cron payload must be valid JSON")
	}
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 1
	}
	if p.TimeoutSeconds < 0 {
		return CronJob{}, errors.New("timeout_seconds must be non-negative")
	}
	nextRunAt, err := cronexpr.Next(p.Schedule, time.Now().UTC())
	if err != nil {
		return CronJob{}, err
	}

	return scanCronJob(s.pool.QueryRow(ctx, fmt.Sprintf(`
		INSERT INTO cron_jobs (name, queue, type, payload, schedule, enabled, priority, max_attempts, timeout_seconds, next_run_at)
		VALUES ($1, $2, $3, $4, $5, true, $6, $7, NULLIF($8, 0), $9)
		RETURNING %s
	`, cronJobSelectColumns), p.Name, p.Queue, p.Type, p.Payload, p.Schedule, p.Priority, p.MaxAttempts, p.TimeoutSeconds, nextRunAt))
}

func (s *Store) ListCronJobs(ctx context.Context) ([]CronJob, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT %s
		FROM cron_jobs
		ORDER BY name ASC
	`, cronJobSelectColumns))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cronJobs []CronJob
	for rows.Next() {
		cronJob, err := scanCronJob(rows)
		if err != nil {
			return nil, err
		}
		cronJobs = append(cronJobs, cronJob)
	}
	return cronJobs, rows.Err()
}

func (s *Store) SetCronJobEnabled(ctx context.Context, ref string, enabled bool) (CronJob, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return CronJob{}, errors.New("cron reference is required")
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return CronJob{}, err
	}
	defer tx.Rollback(ctx)

	current, err := findCronJobForUpdate(ctx, tx, ref)
	if err != nil {
		return CronJob{}, err
	}

	nextRunAt := current.NextRunAt
	if enabled {
		nextRunAt, err = cronexpr.Next(current.Schedule, time.Now().UTC())
		if err != nil {
			return CronJob{}, err
		}
	}

	updated, err := scanCronJob(tx.QueryRow(ctx, fmt.Sprintf(`
		UPDATE cron_jobs
		SET enabled = $2,
		    next_run_at = $3,
		    updated_at = now()
		WHERE id = $1
		RETURNING %s
	`, cronJobSelectColumns), current.ID, enabled, nextRunAt))
	if err != nil {
		return CronJob{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return CronJob{}, err
	}
	return updated, nil
}

func (s *Store) FireDueCronJobs(ctx context.Context, schedulerID string, batch int32, traceContexts ...TraceContext) ([]CronFire, error) {
	if strings.TrimSpace(schedulerID) == "" {
		schedulerID = "scheduler-unknown"
	}
	if batch <= 0 || batch > 100 {
		batch = 50
	}
	traceContext := TraceContext{}
	if len(traceContexts) > 0 {
		traceContext = traceContexts[0]
	}
	now := time.Now().UTC()

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, fmt.Sprintf(`
		SELECT %s
		FROM cron_jobs
		WHERE enabled = true
		  AND next_run_at <= $1
		ORDER BY next_run_at ASC, name ASC
		FOR UPDATE SKIP LOCKED
		LIMIT $2
	`, cronJobSelectColumns), now, batch)
	if err != nil {
		return nil, err
	}

	var due []CronJob
	for rows.Next() {
		cronJob, err := scanCronJob(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		due = append(due, cronJob)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	fires := make([]CronFire, 0, len(due))
	for _, cronJob := range due {
		scheduledFor := cronJob.NextRunAt.UTC().Truncate(time.Minute)
		idempotencyKey := cronIdempotencyKey(cronJob.ID, scheduledFor)

		job, err := scanJob(tx.QueryRow(ctx, fmt.Sprintf(`
			INSERT INTO jobs (queue, type, payload, status, priority, max_attempts, timeout_seconds, idempotency_key, traceparent, tracestate, available_at)
			VALUES ($1, $2, $3, 'queued', $4, $5, $6, $7, NULLIF($8, ''), NULLIF($9, ''), now())
			ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO UPDATE
			SET updated_at = jobs.updated_at
			RETURNING %s
		`, jobSelectColumns), cronJob.Queue, cronJob.Type, cronJob.Payload, cronJob.Priority, cronJob.MaxAttempts, cronJob.TimeoutSeconds, idempotencyKey, traceContext.TraceParent, traceContext.TraceState))
		if err != nil {
			return nil, err
		}

		run, inserted, err := insertCronRun(ctx, tx, cronJob.ID, scheduledFor, job.ID, schedulerID)
		if err != nil {
			return nil, err
		}
		if inserted {
			if _, err := appendJobLog(ctx, tx, AppendJobLogParams{
				JobID:   job.ID,
				Level:   "info",
				Message: "cron job fired",
				Fields: mustJSON(map[string]any{
					"cron_job_id":   cronJob.ID,
					"cron_name":     cronJob.Name,
					"scheduled_for": scheduledFor.Format(time.RFC3339),
					"scheduler_id":  schedulerID,
				}),
			}); err != nil {
				return nil, err
			}
			fires = append(fires, CronFire{CronJob: cronJob, Run: run, Job: job})
		}

		nextRunAt, err := cronexpr.Next(cronJob.Schedule, now)
		if err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE cron_jobs
			SET last_run_at = $2,
			    next_run_at = $3,
			    updated_at = now()
			WHERE id = $1
		`, cronJob.ID, scheduledFor, nextRunAt); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return fires, nil
}

func (s *Store) ListDueQueues(ctx context.Context, limit int32) ([]string, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT queue
		FROM jobs
		WHERE status IN ('queued', 'retry_scheduled')
		  AND available_at <= now()
		ORDER BY queue ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var queues []string
	for rows.Next() {
		var queue string
		if err := rows.Scan(&queue); err != nil {
			return nil, err
		}
		queues = append(queues, queue)
	}
	return queues, rows.Err()
}

func (s *Store) RegisterWorker(ctx context.Context, p RegisterWorkerParams) (Worker, error) {
	if strings.TrimSpace(p.ID) == "" {
		return Worker{}, errors.New("worker id is required")
	}
	if strings.TrimSpace(p.Hostname) == "" {
		p.Hostname = "unknown"
	}
	p.Queues = normalizeQueues(p.Queues)
	if p.Concurrency <= 0 {
		p.Concurrency = 1
	}
	if len(p.Metadata) == 0 {
		p.Metadata = []byte(`{}`)
	}
	if !json.Valid(p.Metadata) {
		return Worker{}, errors.New("worker metadata must be valid JSON")
	}

	return scanWorker(s.pool.QueryRow(ctx, fmt.Sprintf(`
		INSERT INTO workers (id, hostname, queues, status, concurrency, metadata, started_at, last_heartbeat_at, updated_at)
		VALUES ($1, $2, $3, 'running', $4, $5, now(), now(), now())
		ON CONFLICT (id) DO UPDATE
		SET hostname = EXCLUDED.hostname,
		    queues = EXCLUDED.queues,
		    status = 'running',
		    concurrency = EXCLUDED.concurrency,
		    metadata = EXCLUDED.metadata,
		    started_at = now(),
		    last_heartbeat_at = now(),
		    updated_at = now()
		RETURNING %s
	`, workerSelectColumns), p.ID, p.Hostname, p.Queues, p.Concurrency, p.Metadata))
}

func (s *Store) HeartbeatWorker(ctx context.Context, workerID, status string, leaseDuration time.Duration) error {
	if strings.TrimSpace(workerID) == "" {
		return errors.New("worker id is required")
	}
	if status == "" {
		status = WorkerStatusRunning
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE workers
		SET status = $2,
		    last_heartbeat_at = now(),
		    updated_at = now()
		WHERE id = $1
	`, workerID, status)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("worker heartbeat rejected for unknown worker")
	}

	if leaseDuration > 0 {
		if _, err := tx.Exec(ctx, `
			UPDATE jobs
			SET locked_until = now() + $2::interval,
			    updated_at = now()
			WHERE locked_by = $1
			  AND status = 'running'
			  AND lease_token IS NOT NULL
		`, workerID, intervalLiteral(leaseDuration)); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) MarkWorkerStatus(ctx context.Context, workerID, status string) error {
	if strings.TrimSpace(workerID) == "" {
		return errors.New("worker id is required")
	}
	if status == "" {
		return errors.New("worker status is required")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE workers
		SET status = $2,
		    updated_at = now()
		WHERE id = $1
	`, workerID, status)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("worker status update rejected for unknown worker")
	}
	return nil
}

func (s *Store) ListWorkers(ctx context.Context) ([]Worker, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT %s
		FROM workers
		ORDER BY last_heartbeat_at DESC, id ASC
	`, workerSelectColumns))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var workers []Worker
	for rows.Next() {
		worker, err := scanWorker(rows)
		if err != nil {
			return nil, err
		}
		workers = append(workers, worker)
	}
	return workers, rows.Err()
}

func (s *Store) RecoverExpiredJobs(ctx context.Context, batch int32, reason string) ([]Job, error) {
	if batch <= 0 || batch > 100 {
		batch = 50
	}
	if reason == "" {
		reason = "job lease expired"
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, fmt.Sprintf(`
		SELECT %s
		FROM jobs
		WHERE status = 'running'
		  AND locked_until IS NOT NULL
		  AND locked_until <= now()
		ORDER BY locked_until ASC, created_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT $1
	`, jobSelectColumns), batch)
	if err != nil {
		return nil, err
	}

	var expired []Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		expired = append(expired, job)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	updated := make([]Job, 0, len(expired))
	for _, job := range expired {
		if job.LeaseToken != nil {
			if _, err := tx.Exec(ctx, `
				UPDATE job_attempts
				SET status = 'failed',
				    finished_at = now(),
				    error_message = $3,
				    duration_ms = GREATEST(0, (EXTRACT(EPOCH FROM (now() - started_at)) * 1000)::bigint)
				WHERE job_id = $1
				  AND lease_token = $2
				  AND status = 'running'
			`, job.ID, *job.LeaseToken, reason); err != nil {
				return nil, err
			}
		}

		var recovered Job
		if job.AttemptCount < normalizedMaxAttempts(job.MaxAttempts) {
			recovered, err = scanJob(tx.QueryRow(ctx, fmt.Sprintf(`
				UPDATE jobs
				SET status = 'retry_scheduled',
				    available_at = now(),
				    completed_at = NULL,
				    updated_at = now(),
				    locked_by = NULL,
				    locked_until = NULL,
				    lease_token = NULL,
				    last_error = $2
				WHERE id = $1
				RETURNING %s
			`, jobSelectColumns), job.ID, reason))
		} else {
			recovered, err = scanJob(tx.QueryRow(ctx, fmt.Sprintf(`
				UPDATE jobs
				SET status = 'dead_letter',
				    completed_at = now(),
				    dead_lettered_at = now(),
				    updated_at = now(),
				    locked_by = NULL,
				    locked_until = NULL,
				    lease_token = NULL,
				    last_error = $2
				WHERE id = $1
				RETURNING %s
			`, jobSelectColumns), job.ID, reason))
		}
		if err != nil {
			return nil, err
		}
		updated = append(updated, recovered)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return updated, nil
}

func (s *Store) ClaimJob(ctx context.Context, queue, workerID string, leaseDuration time.Duration) (ClaimedJob, bool, error) {
	if queue == "" {
		queue = "default"
	}
	if workerID == "" {
		workerID = "worker-unknown"
	}
	leaseToken := uuid.New()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ClaimedJob{}, false, err
	}
	defer tx.Rollback(ctx)

	job, err := scanJob(tx.QueryRow(ctx, fmt.Sprintf(`
		WITH picked AS (
			SELECT id
			FROM jobs
			WHERE queue = $1
			  AND status IN ('queued', 'retry_scheduled')
			  AND available_at <= now()
			ORDER BY priority DESC, available_at ASC, created_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE jobs
		SET status = 'running',
		    locked_by = $2,
		    locked_until = now() + $3::interval,
		    lease_token = $4,
		    attempt_count = attempt_count + 1,
		    updated_at = now()
		WHERE id IN (SELECT id FROM picked)
		RETURNING %s
	`, jobSelectColumns), queue, workerID, intervalLiteral(leaseDuration), leaseToken))
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return ClaimedJob{}, false, err
		}
		return ClaimedJob{}, false, nil
	}
	if err != nil {
		return ClaimedJob{}, false, err
	}

	attempt, err := scanJobAttempt(tx.QueryRow(ctx, fmt.Sprintf(`
		INSERT INTO job_attempts (job_id, worker_id, lease_token, attempt_number, status)
		VALUES ($1, $2, $3, $4, 'running')
		RETURNING %s
	`, jobAttemptSelectColumns), job.ID, workerID, leaseToken, job.AttemptCount))
	if err != nil {
		return ClaimedJob{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ClaimedJob{}, false, err
	}
	return ClaimedJob{Job: job, Attempt: attempt}, true, nil
}

func (s *Store) CompleteJob(ctx context.Context, id uuid.UUID, leaseToken uuid.UUID) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE jobs
		SET status = 'succeeded',
		    completed_at = now(),
		    updated_at = now(),
		    locked_by = NULL,
		    locked_until = NULL,
		    lease_token = NULL,
		    last_error = NULL
		WHERE id = $1
		  AND lease_token = $2
		  AND status = 'running'
	`, id, leaseToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("job completion rejected by lease fencing")
	}

	tag, err = tx.Exec(ctx, `
		UPDATE job_attempts
		SET status = 'succeeded',
		    finished_at = now(),
		    duration_ms = GREATEST(0, (EXTRACT(EPOCH FROM (now() - started_at)) * 1000)::bigint)
		WHERE job_id = $1
		  AND lease_token = $2
		  AND status = 'running'
	`, id, leaseToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("job attempt completion rejected by lease fencing")
	}

	return tx.Commit(ctx)
}

func (s *Store) FailJob(ctx context.Context, id uuid.UUID, leaseToken uuid.UUID, message string, policy RetryPolicy) (Job, error) {
	policy = policy.Normalize()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Job{}, err
	}
	defer tx.Rollback(ctx)

	job, err := scanJob(tx.QueryRow(ctx, fmt.Sprintf(`
		SELECT %s
		FROM jobs
		WHERE id = $1
		  AND lease_token = $2
		  AND status = 'running'
		FOR UPDATE
	`, jobSelectColumns), id, leaseToken))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, errors.New("job failure rejected by lease fencing")
	}
	if err != nil {
		return Job{}, err
	}

	var updated Job
	if job.AttemptCount < normalizedMaxAttempts(job.MaxAttempts) {
		delay := policy.DelayForAttempt(job.AttemptCount)
		updated, err = scanJob(tx.QueryRow(ctx, fmt.Sprintf(`
			UPDATE jobs
			SET status = 'retry_scheduled',
			    available_at = now() + $2::interval,
			    completed_at = NULL,
			    updated_at = now(),
			    locked_by = NULL,
			    locked_until = NULL,
			    lease_token = NULL,
			    last_error = $3
			WHERE id = $1
			RETURNING %s
		`, jobSelectColumns), id, intervalLiteral(delay), message))
	} else {
		updated, err = scanJob(tx.QueryRow(ctx, fmt.Sprintf(`
			UPDATE jobs
			SET status = 'dead_letter',
			    completed_at = now(),
			    dead_lettered_at = now(),
			    updated_at = now(),
			    locked_by = NULL,
			    locked_until = NULL,
			    lease_token = NULL,
			    last_error = $2
			WHERE id = $1
			RETURNING %s
		`, jobSelectColumns), id, message))
	}
	if err != nil {
		return Job{}, err
	}

	tag, err := tx.Exec(ctx, `
		UPDATE job_attempts
		SET status = 'failed',
		    finished_at = now(),
		    error_message = $3,
		    duration_ms = GREATEST(0, (EXTRACT(EPOCH FROM (now() - started_at)) * 1000)::bigint)
		WHERE job_id = $1
		  AND lease_token = $2
		  AND status = 'running'
	`, id, leaseToken, message)
	if err != nil {
		return Job{}, err
	}
	if tag.RowsAffected() != 1 {
		return Job{}, errors.New("job attempt failure rejected by lease fencing")
	}

	if err := tx.Commit(ctx); err != nil {
		return Job{}, err
	}
	return updated, nil
}

func (p RetryPolicy) Normalize() RetryPolicy {
	if p.InitialDelay <= 0 {
		p.InitialDelay = 2 * time.Second
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = 30 * time.Second
	}
	if p.MaxDelay < p.InitialDelay {
		p.MaxDelay = p.InitialDelay
	}
	return p
}

func (p RetryPolicy) DelayForAttempt(attemptNumber int32) time.Duration {
	p = p.Normalize()
	if attemptNumber <= 1 {
		return p.InitialDelay
	}
	delay := p.InitialDelay
	for i := int32(1); i < attemptNumber; i++ {
		if delay >= p.MaxDelay/2 {
			return p.MaxDelay
		}
		delay *= 2
	}
	if delay > p.MaxDelay {
		return p.MaxDelay
	}
	return delay
}

type scanner interface {
	Scan(dest ...any) error
}

type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func scanJob(row scanner) (Job, error) {
	var job Job
	err := row.Scan(
		&job.ID,
		&job.Queue,
		&job.Type,
		&job.Payload,
		&job.Status,
		&job.Priority,
		&job.MaxAttempts,
		&job.AttemptCount,
		&job.IdempotencyKey,
		&job.LockedBy,
		&job.LockedUntil,
		&job.LeaseToken,
		&job.TimeoutSeconds,
		&job.TraceParent,
		&job.TraceState,
		&job.AvailableAt,
		&job.DeadLetteredAt,
		&job.CreatedAt,
		&job.UpdatedAt,
		&job.CompletedAt,
		&job.LastError,
	)
	return job, err
}

func scanJobAttempt(row scanner) (JobAttempt, error) {
	var attempt JobAttempt
	err := row.Scan(
		&attempt.ID,
		&attempt.JobID,
		&attempt.WorkerID,
		&attempt.LeaseToken,
		&attempt.AttemptNumber,
		&attempt.Status,
		&attempt.StartedAt,
		&attempt.FinishedAt,
		&attempt.ErrorMessage,
		&attempt.DurationMS,
	)
	return attempt, err
}

func scanWorker(row scanner) (Worker, error) {
	var worker Worker
	err := row.Scan(
		&worker.ID,
		&worker.Hostname,
		&worker.Queues,
		&worker.Status,
		&worker.Concurrency,
		&worker.Metadata,
		&worker.StartedAt,
		&worker.LastHeartbeatAt,
		&worker.UpdatedAt,
	)
	return worker, err
}

func scanJobLog(row scanner) (JobLog, error) {
	var log JobLog
	err := row.Scan(
		&log.ID,
		&log.JobID,
		&log.AttemptID,
		&log.Timestamp,
		&log.Level,
		&log.Message,
		&log.Fields,
	)
	return log, err
}

func scanCronJob(row scanner) (CronJob, error) {
	var cronJob CronJob
	err := row.Scan(
		&cronJob.ID,
		&cronJob.Name,
		&cronJob.Queue,
		&cronJob.Type,
		&cronJob.Payload,
		&cronJob.Schedule,
		&cronJob.Enabled,
		&cronJob.Priority,
		&cronJob.MaxAttempts,
		&cronJob.TimeoutSeconds,
		&cronJob.NextRunAt,
		&cronJob.LastRunAt,
		&cronJob.CreatedAt,
		&cronJob.UpdatedAt,
	)
	return cronJob, err
}

func scanCronRun(row scanner) (CronRun, error) {
	var run CronRun
	err := row.Scan(
		&run.ID,
		&run.CronJobID,
		&run.ScheduledFor,
		&run.JobID,
		&run.SchedulerID,
		&run.CreatedAt,
	)
	return run, err
}

func appendJobLog(ctx context.Context, q queryRower, p AppendJobLogParams) (JobLog, error) {
	p.Level = strings.ToLower(strings.TrimSpace(p.Level))
	if p.Level == "" {
		p.Level = "info"
	}
	if p.Level != "debug" && p.Level != "info" && p.Level != "warn" && p.Level != "error" {
		return JobLog{}, errors.New("job log level must be debug, info, warn, or error")
	}
	p.Message = strings.TrimSpace(p.Message)
	if p.Message == "" {
		return JobLog{}, errors.New("job log message is required")
	}
	if len(p.Fields) == 0 {
		p.Fields = []byte(`{}`)
	}
	if !json.Valid(p.Fields) {
		return JobLog{}, errors.New("job log fields must be valid JSON")
	}
	return scanJobLog(q.QueryRow(ctx, fmt.Sprintf(`
		INSERT INTO job_logs (job_id, attempt_id, level, message, fields)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING %s
	`, jobLogSelectColumns), p.JobID, p.AttemptID, p.Level, p.Message, p.Fields))
}

func findCronJobForUpdate(ctx context.Context, tx pgx.Tx, ref string) (CronJob, error) {
	if id, err := uuid.Parse(ref); err == nil {
		return scanCronJob(tx.QueryRow(ctx, fmt.Sprintf(`
			SELECT %s
			FROM cron_jobs
			WHERE id = $1
			FOR UPDATE
		`, cronJobSelectColumns), id))
	}
	return scanCronJob(tx.QueryRow(ctx, fmt.Sprintf(`
		SELECT %s
		FROM cron_jobs
		WHERE name = $1
		FOR UPDATE
	`, cronJobSelectColumns), ref))
}

func insertCronRun(ctx context.Context, tx pgx.Tx, cronJobID uuid.UUID, scheduledFor time.Time, jobID uuid.UUID, schedulerID string) (CronRun, bool, error) {
	run, err := scanCronRun(tx.QueryRow(ctx, fmt.Sprintf(`
		INSERT INTO cron_runs (cron_job_id, scheduled_for, job_id, scheduler_id)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (cron_job_id, scheduled_for) DO NOTHING
		RETURNING %s
	`, cronRunSelectColumns), cronJobID, scheduledFor, jobID, schedulerID))
	if err == nil {
		return run, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return CronRun{}, false, err
	}
	existing, err := scanCronRun(tx.QueryRow(ctx, fmt.Sprintf(`
		SELECT %s
		FROM cron_runs
		WHERE cron_job_id = $1
		  AND scheduled_for = $2
	`, cronRunSelectColumns), cronJobID, scheduledFor))
	if err != nil {
		return CronRun{}, false, err
	}
	return existing, false, nil
}

func cronIdempotencyKey(cronJobID uuid.UUID, scheduledFor time.Time) string {
	return fmt.Sprintf("cron:%s:%s", cronJobID, scheduledFor.UTC().Format(time.RFC3339))
}

func mustJSON(value any) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}

func nullableStringValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func normalizeQueues(queues []string) []string {
	seen := map[string]struct{}{}
	var normalized []string
	for _, queue := range queues {
		queue = strings.TrimSpace(queue)
		if queue == "" {
			continue
		}
		if _, ok := seen[queue]; ok {
			continue
		}
		seen[queue] = struct{}{}
		normalized = append(normalized, queue)
	}
	if len(normalized) == 0 {
		normalized = []string{"default"}
	}
	return normalized
}

func intervalLiteral(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	return fmt.Sprintf("%f seconds", d.Seconds())
}

func normalizedMaxAttempts(maxAttempts int32) int32 {
	if maxAttempts <= 0 {
		return 1
	}
	return maxAttempts
}
