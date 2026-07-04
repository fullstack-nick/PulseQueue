-- +goose Up
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_status_check;

ALTER TABLE jobs
  ADD CONSTRAINT jobs_status_check
  CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'retry_scheduled', 'dead_letter'));

ALTER TABLE jobs
  ADD COLUMN IF NOT EXISTS timeout_seconds integer,
  ADD COLUMN IF NOT EXISTS available_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS dead_lettered_at timestamptz;

CREATE INDEX IF NOT EXISTS jobs_due_queue_status_available_idx
  ON jobs (queue, status, available_at ASC, priority DESC, created_at ASC)
  WHERE status IN ('queued', 'retry_scheduled');

CREATE TABLE IF NOT EXISTS job_attempts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  worker_id text NOT NULL,
  lease_token uuid NOT NULL,
  attempt_number integer NOT NULL,
  status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  error_message text,
  duration_ms bigint,
  UNIQUE (job_id, attempt_number),
  UNIQUE (job_id, lease_token)
);

CREATE INDEX IF NOT EXISTS job_attempts_job_id_started_idx
  ON job_attempts (job_id, started_at ASC);

-- +goose Down
DROP TABLE IF EXISTS job_attempts;
DROP INDEX IF EXISTS jobs_due_queue_status_available_idx;

UPDATE jobs
SET status = 'failed',
    completed_at = COALESCE(completed_at, now()),
    updated_at = now()
WHERE status IN ('retry_scheduled', 'dead_letter');

ALTER TABLE jobs
  DROP COLUMN IF EXISTS dead_lettered_at,
  DROP COLUMN IF EXISTS available_at,
  DROP COLUMN IF EXISTS timeout_seconds;

ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_status_check;

ALTER TABLE jobs
  ADD CONSTRAINT jobs_status_check
  CHECK (status IN ('queued', 'running', 'succeeded', 'failed'));
