-- +goose Up
ALTER TABLE jobs
  ADD COLUMN IF NOT EXISTS traceparent text,
  ADD COLUMN IF NOT EXISTS tracestate text;

-- +goose Down
ALTER TABLE jobs
  DROP COLUMN IF EXISTS tracestate,
  DROP COLUMN IF EXISTS traceparent;
