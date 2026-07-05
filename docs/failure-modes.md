# PulseQueue Failure-Mode Demos

These demos prove the live GCP deployment can explain and recover from realistic job failures. Run them after deploying with `-EnableObservability`.

Set local client environment first:

```powershell
$env:PULSEQUEUE_API_URL="http://VM_PUBLIC_IP:8080"
$env:PULSEQUEUE_OPERATOR_TOKEN="replace-with-secret"
```

## Demo 1 - Worker Dies Mid-Job

Ensure two workers are running:

```powershell
gcloud compute ssh pulsequeue-phase1 `
  --project pulsequeue-r7m5o9ld `
  --zone us-central1-a `
  --command "cd /opt/pulsequeue/app && docker compose -f deployments/docker-compose.yml --env-file .env --profile observability up -d --scale worker=2"
```

Submit a long-running job:

```powershell
go run ./cmd/pulsequeue jobs submit `
  --type demo.sleep `
  --timeout-seconds 120 `
  --max-attempts 2 `
  --payload '{"duration_ms":90000}' `
  --output json
```

Confirm the first attempt has started:

```powershell
go run ./cmd/pulsequeue jobs attempts JOB_ID
```

Kill one worker container on the VM:

```powershell
gcloud compute ssh pulsequeue-phase1 `
  --project pulsequeue-r7m5o9ld `
  --zone us-central1-a `
  --command "cd /opt/pulsequeue/app && docker compose -f deployments/docker-compose.yml --env-file .env --profile observability ps worker"
```

```powershell
gcloud compute ssh pulsequeue-phase1 `
  --project pulsequeue-r7m5o9ld `
  --zone us-central1-a `
  --command "docker kill WORKER_CONTAINER_NAME"
```

Wait for the lease to expire, then verify recovery:

```powershell
go run ./cmd/pulsequeue jobs status JOB_ID
go run ./cmd/pulsequeue jobs attempts JOB_ID
go run ./cmd/pulsequeue jobs logs JOB_ID
```

Expected evidence:

- attempt 1 failed with `job lease expired`
- attempt 2 started on another worker
- final status is `succeeded`
- `pulsequeue_scheduler_recovered_jobs_total` increments
- `otel-collector` logs include scheduler and worker spans

## Demo 2 - Job Keeps Failing

Submit a planned failure:

```powershell
go run ./cmd/pulsequeue jobs submit `
  --type demo.fail `
  --max-attempts 3 `
  --payload '{"message":"phase5 planned failure"}' `
  --output json
```

Verify retries and dead-letter state:

```powershell
go run ./cmd/pulsequeue jobs status JOB_ID
go run ./cmd/pulsequeue jobs attempts JOB_ID
go run ./cmd/pulsequeue jobs logs JOB_ID
```

Manually retry it:

```powershell
go run ./cmd/pulsequeue jobs retry JOB_ID
```

Expected evidence:

- three failed attempts before manual retry
- status becomes `dead_letter`
- retry command moves the job back to `queued`
- `pulsequeue_jobs_failed_total`, `pulsequeue_jobs_dead_lettered_total`, and `pulsequeue_jobs_retried_total` increment

## Demo 3 - Duplicate Submission

Submit the same idempotency key twice:

```powershell
$key="phase5-duplicate-proof-$(Get-Date -Format yyyyMMddHHmmss)"
go run ./cmd/pulsequeue jobs submit --type demo.echo --idempotency-key $key --payload '{"message":"first"}' --output json
go run ./cmd/pulsequeue jobs submit --type demo.echo --idempotency-key $key --payload '{"message":"second"}' --output json
```

Expected evidence:

- both responses return the same job ID
- second response has `existing=true`
- PostgreSQL has one row for the idempotency key
- only one execution attempt is created
- `pulsequeue_jobs_submitted_total{existing="true"}` increments

## Demo 4 - Graceful Shutdown

Submit a medium sleep job:

```powershell
go run ./cmd/pulsequeue jobs submit `
  --type demo.sleep `
  --timeout-seconds 60 `
  --max-attempts 2 `
  --payload '{"duration_ms":20000}' `
  --output json
```

Stop one worker through Compose, which sends SIGTERM:

```powershell
gcloud compute ssh pulsequeue-phase1 `
  --project pulsequeue-r7m5o9ld `
  --zone us-central1-a `
  --command "cd /opt/pulsequeue/app && docker compose -f deployments/docker-compose.yml --env-file .env --profile observability stop worker"
```

Verify:

```powershell
go run ./cmd/pulsequeue workers list
go run ./cmd/pulsequeue jobs status JOB_ID
go run ./cmd/pulsequeue jobs attempts JOB_ID
```

Expected evidence:

- worker status transitions through draining/stopped
- active job either completes cleanly or is recovered by the scheduler
- no job is lost
- logs explain the outcome
