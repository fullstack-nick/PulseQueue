# PulseQueue Observability

Phase 5 adds a lean observability stack for the Docker Compose deployment:

- `GET /metrics` on the API service
- optional worker and scheduler metrics servers on `/metrics`
- Prometheus scrape config
- Grafana provisioning and dashboard
- OpenTelemetry OTLP export to a collector with debug stdout output

The GCP deployment keeps Prometheus and Grafana private. They bind only to VM loopback and are reached with SSH tunnels.

## Local Compose

Enable the observability profile and set worker/scheduler scrape ports:

```powershell
$env:PULSEQUEUE_WORKER_METRICS_ADDR=":2112"
$env:PULSEQUEUE_SCHEDULER_METRICS_ADDR=":2112"
$env:PULSEQUEUE_OTEL_EXPORTER_OTLP_ENDPOINT="otel-collector:4317"
docker compose -f deployments/docker-compose.yml --profile observability up --build
```

Open:

```text
Grafana:    http://localhost:13000
Prometheus: http://localhost:19090
API metrics: http://localhost:8080/metrics
```

Default Grafana credentials come from `.env`:

```text
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=pulsequeue
```

## GCP Deploy

Deploy the Phase 5 observability profile:

```powershell
.\deployments\gcp\scripts\deploy.ps1 `
  -ProjectId pulsequeue-r7m5o9ld `
  -Zone us-central1-a `
  -OperatorToken "replace-with-secret" `
  -BuildImageLocally `
  -EnableObservability `
  -GrafanaAdminPassword "replace-with-grafana-password"
```

No Terraform firewall change is required. Prometheus and Grafana stay private on the VM:

```text
127.0.0.1:19090 -> prometheus:9090
127.0.0.1:13000 -> grafana:3000
```

Open SSH tunnels from the operator machine:

```powershell
gcloud compute ssh pulsequeue-phase1 `
  --project pulsequeue-r7m5o9ld `
  --zone us-central1-a `
  -- -L 13000:127.0.0.1:13000 -L 19090:127.0.0.1:19090
```

Then open:

```text
Grafana:    http://localhost:13000
Prometheus: http://localhost:19090
```

## Trace Evidence

The apps export OTLP traces to `otel-collector:4317`. The collector writes detailed trace samples to container stdout.

Inspect live traces:

```powershell
gcloud compute ssh pulsequeue-phase1 `
  --project pulsequeue-r7m5o9ld `
  --zone us-central1-a `
  --command "cd /opt/pulsequeue/app && docker compose -f deployments/docker-compose.yml --env-file .env --profile observability logs --tail=200 otel-collector"
```

Expected trace evidence includes spans named:

```text
http POST
api.create_job
scheduler.tick
worker.execute_job
```

## Metrics To Check

Prometheus should expose:

```text
pulsequeue_jobs_submitted_total
pulsequeue_jobs_started_total
pulsequeue_jobs_succeeded_total
pulsequeue_jobs_failed_total
pulsequeue_jobs_retried_total
pulsequeue_jobs_dead_lettered_total
pulsequeue_queue_depth
pulsequeue_active_workers
pulsequeue_active_jobs
pulsequeue_job_duration_seconds
pulsequeue_job_latency_seconds
pulsequeue_worker_heartbeat_total
pulsequeue_scheduler_ticks_total
pulsequeue_scheduler_recovered_jobs_total
pulsequeue_cron_jobs_fired_total
pulsequeue_http_requests_total
pulsequeue_http_request_duration_seconds
```

Labels intentionally avoid `job_id`, `attempt_id`, idempotency keys, and raw error text.

## k6 Smoke

Run a free-tier-safe live smoke from the operator machine:

```powershell
$env:BASE_URL="http://VM_PUBLIC_IP:8080"
$env:TOKEN="replace-with-secret"
k6 run load/k6/phase5-smoke.js
```

If `k6` is not installed locally, run it through Docker:

```powershell
$k6Dir=(Resolve-Path load\k6).Path
docker run --rm `
  -v "${k6Dir}:/scripts" `
  grafana/k6:latest run `
  -e BASE_URL=$env:BASE_URL `
  -e TOKEN=$env:TOKEN `
  /scripts/phase5-smoke.js
```

Default load:

```text
2 VUs
60 seconds
p95 HTTP threshold below 2 seconds
checks above 95 percent
```

Record the k6 summary, queue depth returning to normal, and Grafana panels for README proof.
