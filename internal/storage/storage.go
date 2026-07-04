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
)

const (
	StatusQueued         = "queued"
	StatusRunning        = "running"
	StatusSucceeded      = "succeeded"
	StatusFailed         = "failed"
	StatusRetryScheduled = "retry_scheduled"
	StatusDeadLetter     = "dead_letter"

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
	available_at, dead_lettered_at, created_at, updated_at, completed_at, last_error
`

const jobAttemptSelectColumns = `
	id, job_id, worker_id, lease_token, attempt_number, status,
	started_at, finished_at, error_message, duration_ms
`

const workerSelectColumns = `
	id, hostname, queues, status, concurrency, metadata,
	started_at, last_heartbeat_at, updated_at
`

type Store struct {
	pool *pgxpool.Pool
}

func IsNotFound(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
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
		INSERT INTO jobs (queue, type, payload, status, priority, max_attempts, timeout_seconds, idempotency_key, available_at)
		VALUES ($1, $2, $3, 'queued', $4, $5, NULLIF($6, 0), NULLIF($7, ''), now() + $8::interval)
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
		RETURNING %s
	`, jobSelectColumns), p.Queue, p.Type, p.Payload, p.Priority, p.MaxAttempts, p.TimeoutSeconds, nullableStringValue(p.IdempotencyKey), intervalLiteral(time.Duration(p.DelaySeconds)*time.Second)))
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
