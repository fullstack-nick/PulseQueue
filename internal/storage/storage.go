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
	StatusQueued    = "queued"
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
)

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
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	CompletedAt    *time.Time      `json:"completed_at,omitempty"`
	LastError      *string         `json:"last_error,omitempty"`
}

type CreateJobParams struct {
	Queue          string
	Type           string
	Payload        json.RawMessage
	Priority       int32
	MaxAttempts    int32
	IdempotencyKey *string
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
	if p.Priority == 0 {
		p.Priority = 0
	}
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 1
	}
	if len(p.Payload) == 0 {
		p.Payload = []byte(`{}`)
	}
	if !json.Valid(p.Payload) {
		return Job{}, false, errors.New("payload must be valid JSON")
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Job{}, false, err
	}
	defer tx.Rollback(ctx)

	if p.IdempotencyKey != nil && *p.IdempotencyKey != "" {
		existing, err := scanJob(tx.QueryRow(ctx, `SELECT id, queue, type, payload, status, priority, max_attempts, attempt_count, idempotency_key, locked_by, locked_until, lease_token, created_at, updated_at, completed_at, last_error FROM jobs WHERE idempotency_key = $1`, *p.IdempotencyKey))
		if err == nil {
			if err := tx.Commit(ctx); err != nil {
				return Job{}, false, err
			}
			return existing, true, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return Job{}, false, err
		}
	}

	job, err := scanJob(tx.QueryRow(ctx, `
		INSERT INTO jobs (queue, type, payload, status, priority, max_attempts, idempotency_key)
		VALUES ($1, $2, $3, 'queued', $4, $5, NULLIF($6, ''))
		RETURNING id, queue, type, payload, status, priority, max_attempts, attempt_count, idempotency_key, locked_by, locked_until, lease_token, created_at, updated_at, completed_at, last_error
	`, p.Queue, p.Type, p.Payload, p.Priority, p.MaxAttempts, nullableStringValue(p.IdempotencyKey)))
	if err != nil {
		return Job{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, false, err
	}
	return job, false, nil
}

func (s *Store) GetJob(ctx context.Context, id uuid.UUID) (Job, error) {
	return scanJob(s.pool.QueryRow(ctx, `SELECT id, queue, type, payload, status, priority, max_attempts, attempt_count, idempotency_key, locked_by, locked_until, lease_token, created_at, updated_at, completed_at, last_error FROM jobs WHERE id = $1`, id))
}

func (s *Store) ListJobs(ctx context.Context, filter ListJobsFilter) ([]Job, error) {
	limit := filter.Limit
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, queue, type, payload, status, priority, max_attempts, attempt_count, idempotency_key, locked_by, locked_until, lease_token, created_at, updated_at, completed_at, last_error
		FROM jobs
		WHERE ($1 = '' OR status = $1)
		  AND ($2 = '' OR queue = $2)
		ORDER BY created_at DESC
		LIMIT $3
	`, filter.Status, filter.Queue, limit)
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

func (s *Store) ClaimJob(ctx context.Context, queue, workerID string, leaseDuration time.Duration) (Job, bool, error) {
	leaseToken := uuid.New()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Job{}, false, err
	}
	defer tx.Rollback(ctx)

	job, err := scanJob(tx.QueryRow(ctx, `
		WITH picked AS (
			SELECT id
			FROM jobs
			WHERE status = 'queued'
			  AND queue = $1
			ORDER BY priority DESC, created_at ASC
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
		RETURNING id, queue, type, payload, status, priority, max_attempts, attempt_count, idempotency_key, locked_by, locked_until, lease_token, created_at, updated_at, completed_at, last_error
	`, queue, workerID, fmt.Sprintf("%f seconds", leaseDuration.Seconds()), leaseToken))
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return Job{}, false, err
		}
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, false, err
	}
	return job, true, nil
}

func (s *Store) CompleteJob(ctx context.Context, id uuid.UUID, leaseToken uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'succeeded',
		    completed_at = now(),
		    updated_at = now(),
		    locked_by = NULL,
		    locked_until = NULL
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
	return nil
}

func (s *Store) FailJob(ctx context.Context, id uuid.UUID, leaseToken uuid.UUID, message string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'failed',
		    completed_at = now(),
		    updated_at = now(),
		    locked_by = NULL,
		    locked_until = NULL,
		    last_error = $3
		WHERE id = $1
		  AND lease_token = $2
		  AND status = 'running'
	`, id, leaseToken, message)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("job failure rejected by lease fencing")
	}
	return nil
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
		&job.CreatedAt,
		&job.UpdatedAt,
		&job.CompletedAt,
		&job.LastError,
	)
	return job, err
}

func nullableStringValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
