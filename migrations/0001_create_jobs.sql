-- +goose Up
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS jobs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  queue text NOT NULL,
  type text NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
  priority integer NOT NULL DEFAULT 0,
  max_attempts integer NOT NULL DEFAULT 1,
  attempt_count integer NOT NULL DEFAULT 0,
  idempotency_key text,
  locked_by text,
  locked_until timestamptz,
  lease_token uuid,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  last_error text
);

CREATE UNIQUE INDEX IF NOT EXISTS jobs_idempotency_key_unique
  ON jobs (idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS jobs_status_queue_priority_created_idx
  ON jobs (status, queue, priority DESC, created_at ASC);

CREATE INDEX IF NOT EXISTS jobs_created_at_idx ON jobs (created_at DESC);

-- +goose Down
DROP TABLE IF EXISTS jobs;
