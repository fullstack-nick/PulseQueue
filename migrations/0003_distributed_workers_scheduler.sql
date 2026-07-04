-- +goose Up
CREATE TABLE IF NOT EXISTS workers (
  id text PRIMARY KEY,
  hostname text NOT NULL,
  queues text[] NOT NULL DEFAULT '{}',
  status text NOT NULL CHECK (status IN ('starting', 'running', 'draining', 'stopped')),
  concurrency integer NOT NULL DEFAULT 1 CHECK (concurrency > 0),
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  started_at timestamptz NOT NULL DEFAULT now(),
  last_heartbeat_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS workers_status_heartbeat_idx
  ON workers (status, last_heartbeat_at DESC);

CREATE INDEX IF NOT EXISTS jobs_running_locked_until_idx
  ON jobs (locked_until ASC)
  WHERE status = 'running';

CREATE INDEX IF NOT EXISTS jobs_due_signal_idx
  ON jobs (available_at ASC, queue)
  WHERE status IN ('queued', 'retry_scheduled');

-- +goose Down
DROP INDEX IF EXISTS jobs_due_signal_idx;
DROP INDEX IF EXISTS jobs_running_locked_until_idx;
DROP INDEX IF EXISTS workers_status_heartbeat_idx;
DROP TABLE IF EXISTS workers;
