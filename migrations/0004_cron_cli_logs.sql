-- +goose Up
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_status_check;

ALTER TABLE jobs
  ADD CONSTRAINT jobs_status_check
  CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'retry_scheduled', 'dead_letter', 'cancelled'));

CREATE TABLE IF NOT EXISTS job_logs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  attempt_id uuid REFERENCES job_attempts(id) ON DELETE SET NULL,
  logged_at timestamptz NOT NULL DEFAULT now(),
  level text NOT NULL CHECK (level IN ('debug', 'info', 'warn', 'error')),
  message text NOT NULL,
  fields jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS job_logs_job_logged_at_idx
  ON job_logs (job_id, logged_at ASC, id ASC);

CREATE TABLE IF NOT EXISTS cron_jobs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL UNIQUE,
  queue text NOT NULL DEFAULT 'default',
  type text NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  schedule text NOT NULL,
  enabled boolean NOT NULL DEFAULT true,
  priority integer NOT NULL DEFAULT 0,
  max_attempts integer NOT NULL DEFAULT 1,
  timeout_seconds integer,
  next_run_at timestamptz NOT NULL,
  last_run_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cron_jobs_due_idx
  ON cron_jobs (enabled, next_run_at ASC)
  WHERE enabled = true;

CREATE TABLE IF NOT EXISTS cron_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  cron_job_id uuid NOT NULL REFERENCES cron_jobs(id) ON DELETE CASCADE,
  scheduled_for timestamptz NOT NULL,
  job_id uuid REFERENCES jobs(id) ON DELETE SET NULL,
  scheduler_id text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (cron_job_id, scheduled_for)
);

CREATE INDEX IF NOT EXISTS cron_runs_job_idx
  ON cron_runs (job_id);

-- +goose Down
DROP INDEX IF EXISTS cron_runs_job_idx;
DROP TABLE IF EXISTS cron_runs;
DROP INDEX IF EXISTS cron_jobs_due_idx;
DROP TABLE IF EXISTS cron_jobs;
DROP INDEX IF EXISTS job_logs_job_logged_at_idx;
DROP TABLE IF EXISTS job_logs;

UPDATE jobs
SET status = 'failed',
    completed_at = COALESCE(completed_at, now()),
    updated_at = now(),
    last_error = COALESCE(last_error, 'cancelled before rollback')
WHERE status = 'cancelled';

ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_status_check;

ALTER TABLE jobs
  ADD CONSTRAINT jobs_status_check
  CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'retry_scheduled', 'dead_letter'));
