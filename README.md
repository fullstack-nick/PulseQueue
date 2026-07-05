# PulseQueue

PulseQueue is a durable background job queue and workflow runtime written in Go. It exposes a small operator-authenticated REST API, a CLI, and gRPC health; stores job state in PostgreSQL; uses NATS for wake-up signaling; and runs as separate API, worker, and scheduler processes from the same binary.

It is built to demonstrate production-grade queue behavior without relying on managed services: retries, dead-lettering, idempotency, delayed execution, cron scheduling, worker heartbeats, lease recovery, logs, metrics, traces, Docker Compose deployment, and a live-proven Kubernetes/Helm path.

## What It Does

- Accepts durable jobs through REST or CLI and stores them in PostgreSQL.
- Executes jobs with worker leases using PostgreSQL row locking.
- Supports deterministic retries with exponential backoff and a `dead_letter` terminal state.
- Records per-attempt history and durable per-job logs.
- Handles execution timeouts with Go `context.Context`.
- Supports idempotency keys for duplicate-safe submissions.
- Schedules delayed jobs and 5-field UTC cron jobs.
- Tracks workers, heartbeats, queues, and scheduler recovery.
- Exposes Prometheus metrics, Grafana dashboards, and OpenTelemetry traces.
- Ships Docker Compose, Terraform, raw Kubernetes manifests, and a Helm chart.
- Publishes public images to `ghcr.io/fullstack-nick/pulsequeue`.

## Architecture

```text
CLI / REST clients
        |
        v
PulseQueue API  ---- health, jobs, cron, workers, queues, metrics
        |
        +---- PostgreSQL: jobs, attempts, logs, workers, cron, migrations
        |
        +---- NATS: wake-up signal path
                 |
                 +---- workers: leased execution, heartbeats, handler logs
                 |
                 +---- scheduler: delayed jobs, retries, cron fires, lease recovery
```

The same `pulsequeue` binary runs in three roles:

```text
pulsequeue server
pulsequeue worker
pulsequeue scheduler
```

PostgreSQL is the durability boundary. NATS can be unavailable temporarily without losing jobs, but readiness fails while NATS is unavailable because workers and schedulers depend on it for prompt wake-ups.

## Tech Stack

| Area | Technology |
| --- | --- |
| Language | Go 1.26 |
| HTTP API | chi |
| CLI | Cobra |
| Database | PostgreSQL 18, pgx |
| Signaling | NATS 2.12 |
| Metrics | Prometheus client, Prometheus server, Grafana |
| Tracing | OpenTelemetry collector |
| Local runtime | Docker Compose |
| Cloud runtime | Terraform-managed GCP Compute Engine VM with Docker Compose |
| Kubernetes proof | k3s, raw manifests, Helm |
| CI/CD | GitHub Actions, Docker Buildx, GHCR |
| Validation | Go test/vet, Docker build, Helm lint/template, kubeconform, Terraform validate, cost guardrail |

## Local Demo

Create a local environment file:

```powershell
Copy-Item .env.example .env
```

Set a non-default operator token in `.env`:

```text
PULSEQUEUE_OPERATOR_TOKEN=change-this
```

Start the stack:

```powershell
docker compose -f deployments/docker-compose.yml up --build
```

Use the CLI against the local API:

```powershell
$env:PULSEQUEUE_OPERATOR_TOKEN="change-this"
$env:PULSEQUEUE_API_URL="http://localhost:8080"
go run ./cmd/pulsequeue health
```

Submit and inspect a normal job:

```powershell
go run ./cmd/pulsequeue jobs submit --queue default --type demo.echo --payload '{"message":"hello"}'
go run ./cmd/pulsequeue jobs list
```

Exercise reliable execution:

```powershell
go run ./cmd/pulsequeue jobs submit --type demo.fail --max-attempts 3 --payload '{"message":"planned failure"}'
go run ./cmd/pulsequeue jobs list --status dead_letter
go run ./cmd/pulsequeue jobs attempts JOB_ID
```

Exercise timeout handling:

```powershell
go run ./cmd/pulsequeue jobs submit --type demo.sleep --timeout-seconds 1 --max-attempts 2 --payload '{"duration_ms":3000}'
go run ./cmd/pulsequeue jobs status JOB_ID
go run ./cmd/pulsequeue jobs attempts JOB_ID
```

Exercise scheduling and operations:

```powershell
go run ./cmd/pulsequeue jobs submit --type demo.echo --delay-seconds 10 --payload '{"message":"delayed"}'
go run ./cmd/pulsequeue workers list
go run ./cmd/pulsequeue queues list
go run ./cmd/pulsequeue cron create --name every-minute-demo --schedule "* * * * *" --type demo.echo --payload '{"message":"from cron"}'
go run ./cmd/pulsequeue cron list
go run ./cmd/pulsequeue jobs logs JOB_ID
go run ./cmd/pulsequeue jobs cancel QUEUED_JOB_ID
go run ./cmd/pulsequeue jobs retry DEAD_LETTER_OR_CANCELLED_JOB_ID
```

Start local observability:

```powershell
$env:PULSEQUEUE_WORKER_METRICS_ADDR=":2112"
$env:PULSEQUEUE_SCHEDULER_METRICS_ADDR=":2112"
$env:PULSEQUEUE_OTEL_EXPORTER_OTLP_ENDPOINT="otel-collector:4317"
docker compose -f deployments/docker-compose.yml --profile observability up --build
```

Then open:

```text
Grafana:     http://localhost:13000
Prometheus:  http://localhost:19090
API metrics: http://localhost:8080/metrics
NATS monitor: http://localhost:8222/varz
```

## API Surface

Unauthenticated:

```text
GET /health/live
GET /health/ready
GET /metrics
```

Authenticated with `Authorization: Bearer $PULSEQUEUE_OPERATOR_TOKEN`:

```text
POST /jobs
GET  /jobs
GET  /jobs/{id}
GET  /jobs/{id}/attempts
GET  /jobs/{id}/logs
POST /jobs/{id}/retry
POST /jobs/{id}/cancel
GET  /workers
GET  /queues
POST /cron
GET  /cron
POST /cron/{id-or-name}/enable
POST /cron/{id-or-name}/disable
```

Example REST submission:

```powershell
$headers = @{ Authorization = "Bearer change-this" }
$body = @{
  queue = "default"
  type = "demo.echo"
  payload = @{ message = "hello from api" }
  max_attempts = 3
  timeout_seconds = 10
  delay_seconds = 0
} | ConvertTo-Json

Invoke-RestMethod `
  -Method Post `
  -Uri http://localhost:8080/jobs `
  -Headers $headers `
  -Body $body `
  -ContentType "application/json"
```

## GCP Deployment

The normal cloud runtime is Docker Compose on a Terraform-managed GCP Compute Engine VM. The current deployment target used by the runbooks is:

```text
Project: pulsequeue-r7m5o9ld
Region: us-central1
Zone: us-central1-a
Instance: pulsequeue-phase1
Default machine: e2-micro
```

Only API/gRPC and SSH are reachable through GCP firewall rules, and only from the operator CIDR. PostgreSQL, NATS, Prometheus, Grafana, workers, and scheduler stay private.

Create `deployments/gcp/terraform/terraform.tfvars` from the example:

```hcl
project_id    = "pulsequeue-r7m5o9ld"
region        = "us-central1"
zone          = "us-central1-a"
operator_cidr = "YOUR_PUBLIC_IP/32"
```

Provision infrastructure:

```powershell
terraform -chdir=deployments/gcp/terraform init
terraform -chdir=deployments/gcp/terraform apply
```

Deploy with the proven local-image path:

```powershell
.\deployments\gcp\scripts\deploy.ps1 `
  -ProjectId pulsequeue-r7m5o9ld `
  -Zone us-central1-a `
  -OperatorToken "replace-with-secret" `
  -PostgresPassword "replace-with-secret" `
  -BuildImageLocally
```

`-BuildImageLocally` builds the Linux image locally, transfers it to the VM, loads it with Docker, and recreates Compose services without doing a resource-heavy remote image build.

Verify the live deployment:

```powershell
$env:PULSEQUEUE_API_URL="http://VM_PUBLIC_IP:8080"
$env:PULSEQUEUE_OPERATOR_TOKEN="replace-with-secret"
go run ./cmd/pulsequeue health
go run ./cmd/pulsequeue jobs submit --type demo.echo --payload '{"message":"live gcp"}'
go run ./cmd/pulsequeue jobs list
```

Deploy with observability:

```powershell
.\deployments\gcp\scripts\deploy.ps1 `
  -ProjectId pulsequeue-r7m5o9ld `
  -Zone us-central1-a `
  -OperatorToken "replace-with-secret" `
  -PostgresPassword "replace-with-secret" `
  -BuildImageLocally `
  -EnableObservability `
  -GrafanaAdminPassword "replace-with-grafana-password"
```

Open private observability tunnels:

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

## Kubernetes And Helm Proof

Docker Compose remains the default always-on GCP runtime. Kubernetes is provided as a live-proven proof path using k3s on the same GCP VM during a planned maintenance window.

During the k3s window, k3s runs the real PulseQueue stack:

```text
PostgreSQL StatefulSet
NATS StatefulSet
API Deployment and ClusterIP Service
Worker Deployment and HPA
Scheduler Deployment
runtime Secret and ConfigMap
```

Services are ClusterIP only. No LoadBalancer, NodePort, GKE, Cloud SQL, Artifact Registry, Memorystore, or public PostgreSQL/NATS endpoint is introduced.

Use the GHCR image published by GitHub Actions:

```powershell
.\deployments\gcp\scripts\deploy-k3s.ps1 `
  -ProjectId pulsequeue-r7m5o9ld `
  -Zone us-central1-a `
  -Mode manifests `
  -ImageRef ghcr.io/fullstack-nick/pulsequeue:sha-<shortsha> `
  -OperatorToken "replace-with-secret" `
  -PostgresPassword "replace-with-secret" `
  -StopCompose `
  -CleanupAfterProof

.\deployments\gcp\scripts\deploy-k3s.ps1 `
  -ProjectId pulsequeue-r7m5o9ld `
  -Zone us-central1-a `
  -Mode helm `
  -ImageRef ghcr.io/fullstack-nick/pulsequeue:sha-<shortsha> `
  -OperatorToken "replace-with-secret" `
  -PostgresPassword "replace-with-secret" `
  -StopCompose `
  -CleanupAfterProof
```

Full cluster and stress verification can temporarily resize the same VM if `e2-micro` is too small. The required end state is cleanup of k3s workloads, k3s stopped, and Docker Compose restored on `e2-micro`.

## CI And Images

GitHub Actions validates:

```text
go test ./...
go vet ./...
docker build
helm lint
helm template
kubeconform for raw manifests and rendered Helm output
terraform fmt/init/validate
managed-service cost guardrail
```

On pushes to `main`, the workflow publishes:

```text
ghcr.io/fullstack-nick/pulsequeue:main
ghcr.io/fullstack-nick/pulsequeue:sha-<shortsha>
```

## Supporting Docs

```text
docs/architecture.md      Architecture and runtime boundaries
docs/gcp-runbook.md       GCP Compose, k3s, Helm, cleanup, rollback
docs/scaling.md           Worker replicas, concurrency, HPA, free-tier limits
docs/observability.md     Metrics, dashboards, traces
docs/failure-modes.md     Failure demos and expected evidence
```

## Live Proof History

Newest proof is listed first. These are live GCP records, not local-only checks.

### 2026-07-05 Cloud-Native And Full-App Proof

```text
GitHub repo: https://github.com/fullstack-nick/PulseQueue
Verified image: ghcr.io/fullstack-nick/pulsequeue:sha-824ad57
GitHub Actions: ci run 28747756264 succeeded
GCP project: pulsequeue-r7m5o9ld
GCP VM: pulsequeue-phase1
GCP zone: us-central1-a
Temporary proof machine: n2-standard-8
Restored machine: e2-micro
Live API after restore: http://35.254.165.175:8080
Default runtime after proof: Docker Compose
```

Baseline Compose proof before the k3s window:

```text
/health/live 200 {"status":"live"}
/health/ready 200 {"status":"ready"}
demo.echo job e7c646db-1ed2-4ab7-8703-75fe26d4dac0 succeeded with attempt_count 1
PostgreSQL readback returned demo.echo | succeeded | 1
```

Raw k3s manifest proof:

```text
k3s: v1.36.2+k3s1
Namespace: pulsequeue-k3s
Image: ghcr.io/fullstack-nick/pulsequeue:sha-824ad57
API, worker, scheduler, PostgreSQL, NATS: rollout complete
/health/live 200
/health/ready 200
demo.echo job 2b3ef676-4495-44dd-a1c8-5fbdf3fdeb40 succeeded with attempt_count 1
PostgreSQL readback returned demo.echo | succeeded | 1
Services: ClusterIP only
Worker HPA: min 1, max 2
Pods: zero restarts
CleanupAfterProof: namespace removed
```

Helm and full app feature proof:

```text
Helm deployment: succeeded on the same k3s runtime
Full proof run: phase6-20260705194018
gRPC health: SERVING
NATS readiness: /varz returned server state on monitor port 8222
REST, CLI, retries, failures, DLQ, idempotency, delays, cron, job logs,
worker heartbeats, scheduler recovery, metrics, and DB readbacks: passed
Stress batch: 200 submitted, 200 succeeded, 0 bad, 0 active
Worker scale proof: HPA held 4 worker pods with concurrency 4 during stress
Final k3s pods: zero restarts
```

Final restored Compose proof:

```text
VM state: RUNNING e2-micro
k3s service: inactive
/health/live 200 {"status":"live"}
/health/ready 200 {"status":"ready"}
demo.echo job 25d2b636-a47a-4931-9cb6-ae1d45407d4d succeeded with attempt_count 1
PostgreSQL readback returned demo.echo | succeeded | 1
NATS monitor endpoint: http://127.0.0.1:8222/varz returned server state
Public exposure: API 8080 and gRPC 9090 only; PostgreSQL, NATS, Prometheus, and Grafana stayed private
```

### 2026-07-05 Observability And Failure Proof

```text
GitHub repo: https://github.com/fullstack-nick/PulseQueue
Implementation commit: 8abccb4c4e7644f8d4d499915597643db85176af
GitHub Actions: ci run 28734344561 succeeded
GCP project: pulsequeue-r7m5o9ld
GCP VM: pulsequeue-phase1
GCP zone: us-central1-a
Live API: http://35.254.165.175:8080
Deployment path: local image build + docker load + Docker Compose observability profile
Terraform drift check after proof: no changes
```

Live observability checks:

```text
/health/live 200
/health/ready 200
/metrics exposes pulsequeue_* metrics without operator auth
Prometheus ready: Prometheus Server is Ready.
Prometheus targets: pulsequeue-api up, pulsequeue-worker up, pulsequeue-scheduler up
Grafana health through VM loopback: database=ok, version=13.1.0
OTel collector debug logs include pulsequeue-api, pulsequeue-scheduler, and pulsequeue-worker spans
Queue drained proof after load/failure demos: pulsequeue_queue_depth{queue="default"} 0
Scheduler recovery metric after crash demo: sum(pulsequeue_scheduler_recovered_jobs_total) 1
```

k6 benchmark result:

```text
VUs: 2
Duration: 60s
Iterations/jobs submitted: 42
Checks: 295/295 succeeded
HTTP failed: 0/211 (0.00%)
HTTP p95: 202.99ms
Final queue depth: 0
```

Failure-mode evidence:

```text
Worker crash recovery:
  job 260c6d20-78f6-4999-9c04-70992f9507b9
  killed worker container 5026961f36f4 during attempt 1
  attempt 1 failed with "job lease expired" after 60,988ms
  scheduler recovered the lease
  worker-39f119f07317 completed attempt 2 successfully in 30,008ms
  trace_id 988f3c060a08e845aebb7efc76fb962c links api.create_job and worker.execute_job

Repeated failure/dead-letter/manual retry:
  job 2cc80b1e-c1a7-4d30-915a-f2aebab720ba
  demo.fail reached dead_letter after attempts 1-3
  manual retry returned status=queued and increased max_attempts to 4
  attempt 4 failed and job returned to dead_letter
  traceparent 00-676668c44505f4e5704f327607cb4ecd-65ad3442e94bd6b7-01

Duplicate submission:
  idempotency key phase5-duplicate-proof-20260705104338
  first submit created job 61f51367-56d2-4a9d-a2f3-7427b9cf42b5 with existing=false
  second submit returned the same job ID with existing=true
  job executed once; one succeeded attempt was recorded

Graceful shutdown:
  job 06184cc5-6ed0-4569-859b-c61db5de8d6e
  docker stop -t 30 sent to worker-39f119f07317 while attempt 1 was active
  job completed successfully in 10,006ms before worker stopped
  worker registry showed worker-39f119f07317 status=stopped
```

Container exposure on the VM:

```text
api             0.0.0.0:8080->8080/tcp, 0.0.0.0:9090->9090/tcp
grafana         127.0.0.1:13000->3000/tcp
prometheus      127.0.0.1:19090->9090/tcp
postgres        127.0.0.1:5432->5432/tcp
nats            127.0.0.1:4222->4222/tcp, 127.0.0.1:8222->8222/tcp
otel-collector  no public ports
worker          no public ports
scheduler       no public ports
firewall        tcp:8080,tcp:9090 and tcp:22 only from <operator-public-ip>/32
```

### 2026-07-04 Distributed Worker And Scheduler Proof

```text
GitHub repo: https://github.com/fullstack-nick/PulseQueue
GitHub Actions: passed on main for commit 08fd1d3715671a475f3dfccce2c6f189beb41b95
GCP project: pulsequeue-r7m5o9ld
GCP VM: pulsequeue-phase1
GCP region/zone: us-central1 / us-central1-a
Live API: http://35.254.165.175:8080
Deployment path: local image build + docker load + Docker Compose recreate
Live runtime: 2 worker replicas and 2 scheduler replicas
```

Live health:

```text
/health/live 200 {"status":"live"}
/health/ready 200 {"status":"ready"}
```

Distributed behavior:

```text
Mixed-priority batch:
0a0d4d61-8b0d-4dd9-8f2d-12940cf83f18  priority 1   succeeded  attempt_count 1
4c9a29b1-0cf9-4eb1-b8f2-9ce07a92a1d8  priority 5   succeeded  attempt_count 1
8588e4e9-da52-45ce-bb93-0ab30d38011f  priority 10  succeeded  attempt_count 1
149619b7-36ac-460c-b1d2-b05b137196da  priority 3   succeeded  attempt_count 1
bbb50d91-17e8-40e6-ab1b-9b756f2b09c7  priority 8   succeeded  attempt_count 1

Delayed job:
fb0e79f4-d770-4733-b2e2-2fb43b419b00  queued with attempt_count 0 before available_at
fb0e79f4-d770-4733-b2e2-2fb43b419b00  succeeded with attempt_count 1 after scheduler wakeup

Worker-crash recovery:
d818cd4a-7fb8-4279-beb1-fa97b472ef3d  attempt 1 started on worker-4ccae78418d0
deployments-worker-1 was killed while the job was running
scheduler-d1f0b028a2a8 recovered the expired lease with error "job lease expired"
d818cd4a-7fb8-4279-beb1-fa97b472ef3d  attempt 2 succeeded on worker-55d990e000cd
```

PostgreSQL readback on the GCP VM:

```text
version
0001_create_jobs
0002_reliable_execution
0003_distributed_workers_scheduler

workers
worker-4ccae78418d0  running  concurrency=1  queues={default}
worker-55d990e000cd  running  concurrency=1  queues={default}

jobs
0a0d4d61-8b0d-4dd9-8f2d-12940cf83f18  demo.echo   succeeded  attempt_count 1
4c9a29b1-0cf9-4eb1-b8f2-9ce07a92a1d8  demo.echo   succeeded  attempt_count 1
8588e4e9-da52-45ce-bb93-0ab30d38011f  demo.echo   succeeded  attempt_count 1
149619b7-36ac-460c-b1d2-b05b137196da  demo.echo   succeeded  attempt_count 1
bbb50d91-17e8-40e6-ab1b-9b756f2b09c7  demo.echo   succeeded  attempt_count 1
fb0e79f4-d770-4733-b2e2-2fb43b419b00  demo.echo   succeeded  attempt_count 1
d818cd4a-7fb8-4279-beb1-fa97b472ef3d  demo.sleep  succeeded  attempt_count 2

job_id                                attempt_number  worker_id            status     error_message
d818cd4a-7fb8-4279-beb1-fa97b472ef3d  1               worker-4ccae78418d0  failed     job lease expired
d818cd4a-7fb8-4279-beb1-fa97b472ef3d  2               worker-55d990e000cd  succeeded
```

Container exposure on the VM:

```text
api          0.0.0.0:8080->8080/tcp, 0.0.0.0:9090->9090/tcp
postgres     127.0.0.1:5432->5432/tcp
nats         127.0.0.1:4222->4222/tcp, 127.0.0.1:8222->8222/tcp
scheduler-1  no public ports
scheduler-2  no public ports
worker-1     no public ports
worker-2     no public ports
firewall     tcp:8080,tcp:9090 and tcp:22 only from <operator-public-ip>/32
```

### 2026-07-04 Reliable Execution Proof

```text
GitHub repo: https://github.com/fullstack-nick/PulseQueue
GCP project: pulsequeue-r7m5o9ld
GCP VM: pulsequeue-phase1
GCP region/zone: us-central1 / us-central1-a
Live API: http://35.254.165.175:8080
Deployment path: local image build + docker load + Docker Compose recreate
```

Live health:

```text
/health/live 200 {"status":"live"}
/health/ready 200 {"status":"ready"}
```

Reliable execution behavior:

```text
demo.fail job: 79f33cde-7652-4e0e-bc91-e0f4532eff27
Final status: dead_letter
Attempt count: 3
Attempt rows: 3

idempotency key: live-duplicate-proof-20260705003151
Returned job id both times: 320217ce-3709-4544-a547-5aed1da30831
Second submission existing: true
PostgreSQL rows for key: 1

demo.sleep job: ea62c9c9-0134-4567-aad1-86ae28424415
Timeout seconds: 1
Final status: dead_letter
Attempt count: 2
Attempt rows: 2
```

PostgreSQL readback on the GCP VM:

```text
id                                    type        status       attempt_count  max_attempts  timeout_seconds  last_error
79f33cde-7652-4e0e-bc91-e0f4532eff27  demo.fail   dead_letter  3              3                             live retry proof
320217ce-3709-4544-a547-5aed1da30831  demo.echo   succeeded    1              1
ea62c9c9-0134-4567-aad1-86ae28424415  demo.sleep  dead_letter  2              2             1                job timed out

job_id                                attempt_number  status  error_message
79f33cde-7652-4e0e-bc91-e0f4532eff27  1               failed  live retry proof
79f33cde-7652-4e0e-bc91-e0f4532eff27  2               failed  live retry proof
79f33cde-7652-4e0e-bc91-e0f4532eff27  3               failed  live retry proof
ea62c9c9-0134-4567-aad1-86ae28424415  1               failed  job timed out
ea62c9c9-0134-4567-aad1-86ae28424415  2               failed  job timed out
```

Container exposure on the VM:

```text
api       0.0.0.0:8080->8080/tcp, 0.0.0.0:9090->9090/tcp
postgres 127.0.0.1:5432->5432/tcp
nats      127.0.0.1:4222->4222/tcp, 127.0.0.1:8222->8222/tcp
worker    no public ports
firewall  tcp:8080,tcp:9090 and tcp:22 only from <operator-public-ip>/32
```

### 2026-07-04 Initial Durable Queue Proof

```text
GitHub repo: https://github.com/fullstack-nick/PulseQueue
GitHub Actions: passed on main
GCP project: pulsequeue-r7m5o9ld
GCP VM: pulsequeue-phase1
GCP region/zone: us-central1 / us-central1-a
Live API: http://35.254.165.175:8080
```

Live health:

```text
/health/live 200 {"status":"live"}
/health/ready 200 {"status":"ready"}
```

Live job proof:

```text
Submitted job: b00619a3-6f82-4ae0-bdec-74ac15762047
Type: demo.echo
Final status: succeeded
Attempt count: 1
```

PostgreSQL readback on the GCP VM:

```text
id                                    queue    type       status     attempt_count
b00619a3-6f82-4ae0-bdec-74ac15762047  default  demo.echo  succeeded  1
```

GCP firewall proof for the PulseQueue network:

```text
pulsequeue-allow-operator-api  <operator-public-ip>/32  tcp:8080,tcp:9090
pulsequeue-allow-operator-ssh  <operator-public-ip>/32  tcp:22
```
