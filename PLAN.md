# PulseQueue Project Plan

PulseQueue is a production-style distributed background job queue and workflow orchestrator written in Go.

It lets developers submit jobs through a CLI, REST API, and gRPC API. Jobs are stored durably, scheduled reliably, dispatched through a NATS-backed signal plane, executed by distributed worker agents, retried on failure, moved to a dead-letter queue when exhausted, and observed through logs, metrics, traces, and dashboards.

The product direction is a smaller, polished version of systems such as Temporal, Celery, Sidekiq, BullMQ, Kubernetes Jobs, and GitHub Actions runners, but scoped tightly enough to finish and strong enough to demonstrate real backend and cloud engineering skill.

PulseQueue is also a GCP cloud-native project under a strict free-tier discipline. Every implementation phase must end with the code pushed to GitHub, deployed to GCP, and verified live. Local success is useful during development, but it does not make a phase complete. Nothing important should remain local-only: every feature, deployment artifact, operational script, and demo path must have a live-GCP verification path.

## 1. Core Product Idea

A user should be able to run commands like:

```bash
pulsequeue server
pulsequeue worker --queue emails
pulsequeue submit --queue emails --type email.send --payload payload.json
pulsequeue jobs list
pulsequeue jobs status <job-id>
pulsequeue jobs logs <job-id>
pulsequeue jobs retry <job-id>
pulsequeue jobs cancel <job-id>
pulsequeue workers list
```

Example use case:

```text
User signup event
   -> validate email job
   -> enrich profile job
   -> AI/content moderation job
   -> send welcome email job
   -> webhook notification job
```

The project should feel like infrastructure a backend or platform team would actually deploy.

## 2. Main Architecture

```text
CLI / REST Client / gRPC Client
          |
          v
      API Server
          |
          v
   PostgreSQL Job Store
          |
          v
      Scheduler
          |
          v
  NATS Signal and Control Plane
          |
          v
    Worker Agents
          |
          v
 Job Execution + Logs + Metrics
```

Important architectural decision:

**PostgreSQL is the source of truth.**

NATS is the committed signal and control plane for dispatch wakeups, worker coordination events, and operational fanout. Durable job state lives in PostgreSQL. This gives PulseQueue a strong story around durability, consistency, low-latency dispatch, crash recovery, and operational clarity.

## 3. Locked Design Recommendations

These are the design decisions to lock in before implementation. They are chosen for functionality, real usefulness, and long-term production robustness.

### 3.1 PostgreSQL + NATS vs Redis-Backed Queueing

**Locked decision: PostgreSQL is the durable source of truth and NATS is the signal/control plane. Redis is not part of the core architecture.**

Use PostgreSQL for:

```text
job state
attempt history
worker leases
heartbeats
scheduled jobs
cron definitions
dead-letter state
idempotency keys
logs metadata
```

Use NATS for:

```text
queue wake-up signals
worker coordination events
scheduler coordination events
job log fanout
metrics and operational event fanout
gRPC worker-control flows where request/reply semantics are useful
```

Why this is the right long-run architecture:

```text
PostgreSQL gives durable storage, transactions, row locks, indexes, constraints, and straightforward recovery.
The system can crash and recover from the database without reconstructing state from an external broker.
NATS gives low-latency fanout, queue groups, simple worker notification, and a cleaner distributed control plane than database-only polling.
Redis is useful for caching and simple pub/sub, but NATS fits this product better because PulseQueue is a distributed coordination and dispatch system.
```

NATS must not become the source of truth for durable jobs:

```text
New job submitted -> durable row inserted in PostgreSQL -> NATS signal published.
Signal delivered -> workers wake up and attempt a PostgreSQL lease.
Signal missed -> reconciliation polling still finds the durable PostgreSQL row.
Signal duplicated -> PostgreSQL lease, status, and fencing checks prevent unsafe duplicate completion.
```

### 3.2 At-Least-Once vs Exactly-Once Execution

**Locked decision: PulseQueue guarantees at-least-once execution and makes retries safe through idempotency, leases, fencing, and explicit attempt tracking.**

PulseQueue should not promise exactly-once execution. Exactly-once execution is difficult in distributed systems because workers, networks, databases, and external services can fail at awkward moments.

The robust production promise is:

```text
PulseQueue will not lose accepted jobs.
The system will durably store accepted jobs.
Eligible jobs will be executed at least once unless cancelled.
Failed or timed-out jobs will retry according to policy.
Duplicate attempts are possible after crashes, lease expiry, or network failures.
Handlers must be idempotent for externally visible side effects.
```

Implementation rules:

```text
Use idempotency keys to deduplicate job submission.
Use job attempts to record every execution try.
Use lease tokens or fencing values so stale workers cannot incorrectly mark jobs complete.
Require handler-level idempotency for side effects such as email, payments, webhooks, or file writes.
Persist external delivery identifiers where handlers call external systems.
Expose attempt IDs in logs and traces so duplicate attempts are explainable.
```

This is the mature design. It is honest, implementable, and aligned with how reliable production job systems are usually built.

### 3.3 Polling vs Push Dispatch

**Locked decision: PulseQueue uses push-assisted durable dispatch: NATS for low-latency wakeups, PostgreSQL leases for correctness, and reconciliation polling for recovery.**

Primary dispatch flow:

```text
1. API or scheduler writes the job state to PostgreSQL in a transaction.
2. API or scheduler publishes a NATS event such as jobs.available.<queue>.
3. Workers subscribed to that queue wake immediately.
4. Workers claim jobs from PostgreSQL using transactional leases.
5. Workers execute jobs and write attempt results back to PostgreSQL.
6. Scheduler/reconciler repairs expired leases, timed-out jobs, missed signals, and retry eligibility.
```

Correctness rules:

```text
NATS decides when workers wake up.
PostgreSQL decides which worker owns a job.
PostgreSQL decides final job state.
Polling exists as reconciliation, not as the main product posture.
Leasing uses transactions and row-level locking, preferably FOR UPDATE SKIP LOCKED.
```

This gives the strongest long-run behavior:

```text
Low latency from NATS.
Durability and recovery from PostgreSQL.
Clear behavior under missed, duplicated, or delayed messages.
Realistic distributed-systems design worth discussing in interviews.
```

### 3.4 One Scheduler vs Multiple Schedulers

**Locked decision: PulseQueue supports active-active schedulers as the production target. A single scheduler is only a local development convenience.**

Scheduler correctness requirements:

```text
Use PostgreSQL row locks or advisory locks for scheduled job claiming.
Use unique constraints for cron fire instances so the same cron tick cannot enqueue twice.
Use transactional state transitions.
Use lease tokens or fencing values where stale ownership could matter.
Make scheduler ticks idempotent.
Partition or batch scheduler work so multiple scheduler replicas can share load.
Emit NATS events after durable scheduler state changes.
```

Production scheduler posture:

```text
Run at least two scheduler replicas in Kubernetes.
Prove duplicate scheduler execution is harmless through tests and demos.
Use database constraints as the final guard against duplicate cron fire creation.
Avoid leader election as the primary correctness mechanism.
```

This is more ambitious and more robust than a single-scheduler design. It demonstrates that PulseQueue can survive scheduler restarts and replica overlap without corrupting job state.

### 3.5 Additional Implementation Choices To Lock

Locked stack choices:

```text
Language: Go
Database: PostgreSQL
Signal/control plane: NATS
Database access: sqlc
Migrations: goose
API router: chi
CLI: Cobra
Logging: slog with structured JSON output
Metrics: Prometheus client_golang
Tracing: OpenTelemetry
Load testing: k6
Container orchestration: Docker Compose for local development and the strict-free-tier GCP always-on deployment path; Kubernetes/Helm verified live on GCP through a lightweight VM-hosted k3s path
GCP infrastructure: Terraform-managed Compute Engine VM, firewall rules, service account, persistent disk, and startup/bootstrap scripts
Deployment control: direct SSH to the VM for deployment, inspection, logs, database checks, NATS checks, and failure demos
External APIs: REST and gRPC are both first-class project APIs
Worker execution: in-process Go handlers and Docker/shell execution are both part of the full product target
Workflow model: durable single jobs first, cron jobs and DAG workflows in the full product target
```

Implementation order stays phased, but the product commitments are locked:

```text
Build the durable job core first so every higher-level feature inherits correct persistence, leases, attempts, retries, and observability.
Add NATS dispatch as part of the core distributed architecture, not as an undecided enhancement.
Keep active-active scheduler safety in the database design from the first scheduler implementation.
Deliver REST, gRPC, CLI, cron, DAG workflows, Docker/shell execution, GCP deployment, metrics, traces, dashboards, and load tests as the full PulseQueue plan.
Preserve maximum operational control: prefer Terraform, SSH, explicit scripts, visible logs, and direct service inspection over opaque managed abstractions.
Do not leave production-facing artifacts as local-only. If the project contains a script, deployment mode, manifest, Helm chart, demo, or feature, it needs a documented live-GCP execution and verification path.
```

### 3.6 GCP Free-Tier Deployment and Stage Gates

**Locked decision: PulseQueue is developed as a GCP-deployed system from the first phase, under strict free-tier constraints, with maximum direct operational control.**

Primary deployment target:

```text
GCP project provisioned by Terraform.
Default GCP region: us-central1.
Single free-tier-eligible Compute Engine VM as the primary runtime host.
Docker Compose on the VM for api, scheduler, worker, postgres, nats, prometheus, and grafana.
Lightweight k3s on the VM for live GCP verification of Kubernetes manifests and Helm charts.
Persistent disk mounted for PostgreSQL data, NATS data if needed, and durable operational artifacts.
Firewall rules kept minimal and explicit.
SSH access used for deployment, inspection, debugging, log review, and failure demos.
```

Free-tier discipline:

```text
Do not require Cloud SQL, GKE, Memorystore, managed NATS, Pub/Sub, Cloud Load Balancing, or other paid managed services for the main proof path.
Managed services are outside the mandatory architecture unless a separate explicit cost-approved change is made.
Keep the default deployment small enough to run continuously within free-tier expectations.
Prefer one controlled VM over a scattered set of managed services while the project is being built.
Kubernetes proof uses VM-hosted k3s on GCP, not a required GKE cluster.
```

Terraform owns infrastructure:

```text
region and zone variables, defaulting to us-central1
VPC/network basics
firewall rules
service account
Compute Engine VM
persistent disk
SSH metadata or OS Login configuration
startup/bootstrap script hooks
minimal outputs needed for deployment
```

SSH owns live operational control:

```text
install and verify Docker runtime
copy or pull release artifacts
run docker compose deployments
inspect service logs
run database migrations
check PostgreSQL state directly
check NATS health directly
run failure demos
collect live proof for each phase
verify any Kubernetes or Helm artifacts on the GCP VM through k3s before claiming they are complete
```

Stage completion rule:

```text
A phase is not implemented just because it builds locally.
A phase is implemented only when:
1. Code is committed and pushed to GitHub.
2. CI passes on GitHub.
3. Terraform-managed GCP infrastructure is current.
4. The phase is deployed on GCP.
5. Live GCP health checks and behavior checks pass.
6. Logs, database state, NATS state, and CLI/API output prove the phase works.
7. Any temporary firewall, secret, or debug workaround is removed or explicitly documented.
```

Live feature verification rule:

```text
Every feature and every production-facing artifact in every phase must be tested against the live GCP deployment whenever it is feasible.
The verification should call the live GCP-hosted API, gRPC endpoint, CLI path, SSH script, Docker Compose path, or k3s/Kubernetes path rather than only exercising localhost.
After the live call, verify the expected result through user-visible output and direct operational evidence.
Direct operational evidence should include SSH into the GCP VM when useful, service logs, PostgreSQL readback, NATS state, worker/scheduler logs, metrics, or trace output.
Do not mark a feature complete from unit tests, local Docker Compose, or static code inspection alone.
Prefer proof that shows the full path: client call -> live GCP service -> PostgreSQL/NATS/worker behavior -> expected final state.
Do not merge local-only deployment artifacts. Kubernetes manifests, Helm charts, scripts, and demos must be proven on GCP before they are treated as implemented.
```

This rule applies to every phase.

## 4. Services and Components

### 4.1 API Server

The API server exposes REST endpoints and gRPC endpoints as first-class interfaces. REST is the primary human/debug-friendly API, while gRPC is the typed integration API for high-throughput clients and worker-control flows.

Core REST endpoints:

```text
POST   /jobs
GET    /jobs
GET    /jobs/:id
POST   /jobs/:id/cancel
POST   /jobs/:id/retry
GET    /jobs/:id/logs
GET    /workers
GET    /queues
GET    /metrics
```

Job submission should support:

```text
queue name
job type
JSON payload
priority
timeout
retry policy
idempotency key
delay / scheduled time
cron expression for recurring jobs
```

Example job submission body:

```json
{
  "queue": "emails",
  "type": "email.send",
  "payload": {
    "to": "user@example.com",
    "template": "welcome"
  },
  "priority": 5,
  "timeout_seconds": 60,
  "max_attempts": 5,
  "idempotency_key": "signup-user-123-welcome-email"
}
```

The API server should validate input, persist jobs, expose job status, and return structured errors.

### 4.2 Scheduler

The scheduler is the brain of the system.

It manages job state transitions:

```text
queued
  -> running
  -> succeeded
  -> failed
  -> retry_scheduled
  -> dead_letter
  -> cancelled
```

It should handle:

```text
job priorities
delayed jobs
cron jobs
timeouts
worker leases
worker heartbeats
retry scheduling
exponential backoff
dead-letter queue
stuck job recovery
duplicate execution prevention
multi-scheduler safety
```

Core behavior:

```text
1. Pick eligible queued jobs.
2. Respect priority and queue ordering.
3. Lease jobs to workers.
4. Monitor worker heartbeats.
5. Re-queue jobs if a worker dies.
6. Retry failed jobs using exponential backoff.
7. Move exhausted jobs to dead_letter.
8. Mark timed-out jobs as failed or retryable.
```

This is one of the most important parts of the project because it demonstrates distributed systems thinking.

### 4.3 Worker Agent

The worker is a Go process that registers itself and executes jobs.

Example:

```bash
pulsequeue worker --queue emails --concurrency 10
pulsequeue worker --queue image-processing --concurrency 3
```

Worker features:

```text
goroutine-based worker pool
queue-specific workers
concurrency limits per queue or job type
context.Context cancellation
timeout handling
graceful shutdown
heartbeat reporting
job log streaming or retrieval
rate limiting
backpressure
crash recovery
```

Core execution mode:

```text
Run registered Go handlers inside the worker process.
```

Example:

```go
worker.Register("email.send", SendEmailHandler)
worker.Register("image.resize", ResizeImageHandler)
```

Container and shell execution mode:

```text
Run jobs as shell commands or Docker containers.
```

Example:

```bash
pulsequeue submit --image alpine --cmd "echo hello && sleep 5"
```

This borrows the best part of a distributed runner design while keeping durable job semantics as the center of the product.

### 4.4 CLI

The CLI makes the project feel real and polished.

Use Cobra.

Suggested commands:

```bash
pulsequeue server
pulsequeue scheduler
pulsequeue worker --queue emails --concurrency 5

pulsequeue submit email.send --payload payload.json
pulsequeue submit --queue emails --payload '{"to":"user@example.com"}'

pulsequeue jobs list
pulsequeue jobs list --status failed
pulsequeue jobs status <job-id>
pulsequeue jobs logs <job-id>
pulsequeue jobs retry <job-id>
pulsequeue jobs retry failed
pulsequeue jobs cancel <job-id>
pulsequeue jobs drain --queue emails

pulsequeue workers list
pulsequeue queues list
pulsequeue cron list
```

The CLI should support both human-readable output and JSON output:

```bash
pulsequeue jobs list --output json
```

## 5. Data Model

Suggested PostgreSQL tables:

```text
jobs
job_attempts
job_logs
workers
queues
scheduled_jobs
cron_jobs
dead_letter_jobs or a dedicated dead-letter projection
```

### 5.1 jobs

```text
id
queue
type
payload
status
priority
max_attempts
attempt_count
timeout_seconds
idempotency_key
scheduled_at
locked_by
locked_until
lease_token
created_at
updated_at
completed_at
last_error
```

### 5.2 job_attempts

```text
id
job_id
worker_id
lease_token
attempt_number
status
started_at
finished_at
error_message
duration_ms
```

### 5.3 workers

```text
id
hostname
queues
status
last_heartbeat_at
started_at
concurrency
metadata
```

### 5.4 job_logs

```text
id
job_id
attempt_id
timestamp
level
message
```

This schema gives enough depth to explain reliability, auditing, retries, and observability.

## 6. Reliability Features

These are core reliability features.

### 6.1 Retries

Support retry policies:

```text
max attempts
initial delay
max delay
exponential backoff
jitter
```

Example:

```json
{
  "max_attempts": 5,
  "backoff": "exponential",
  "initial_delay_seconds": 5,
  "max_delay_seconds": 300
}
```

### 6.2 Dead-Letter Queue

When a job fails too many times, move it to `dead_letter`.

CLI examples:

```bash
pulsequeue jobs list --status dead_letter
pulsequeue jobs retry <job-id>
pulsequeue jobs retry --status dead_letter
```

### 6.3 Idempotency

Support idempotency keys so duplicate submissions do not create duplicate jobs.

Example:

```text
idempotency_key = "user-123-welcome-email"
```

If the same key is submitted again, return the existing job instead of creating a new one.

### 6.4 Leasing and Heartbeats

Workers should not permanently own jobs.

Flow:

```text
1. Worker leases job for 60 seconds.
2. Worker sends heartbeat.
3. Scheduler extends lease.
4. If heartbeat stops, lease expires.
5. Job becomes eligible for another worker.
```

This enables the failure demo where a worker is killed mid-job and the job is recovered.

### 6.5 Cancellation

Support job cancellation.

Cases:

```text
queued job cancelled before execution
running job cancelled through context cancellation
cancelled job does not retry
```

### 6.6 Rate Limiting and Backpressure

Support simple limits:

```text
max concurrent jobs per worker
max concurrent jobs per queue
max concurrent jobs per job type
rate limit per queue
```

This shows practical production thinking.

## 7. Scheduling Features

### 7.1 Delayed Jobs

Example:

```bash
pulsequeue submit email.send --payload payload.json --delay 10m
```

The job remains scheduled until it becomes eligible.

### 7.2 Cron Jobs

Example:

```bash
pulsequeue cron create \
  --name daily-report \
  --queue reports \
  --type report.generate \
  --schedule "0 8 * * *"
```

Cron jobs should create normal jobs when triggered.

### 7.3 Duplicate Scheduler Protection

If multiple schedulers are running, only one should enqueue a given scheduled job.

Use:

```text
PostgreSQL row locking
advisory locks where useful
unique constraints for cron fire instances
transactional state transitions
```

Avoid an external leader-election service as the primary correctness mechanism.

## 8. Observability

This is what makes the project feel production-grade.

### 8.1 Metrics

Expose Prometheus metrics:

```text
jobs_submitted_total
jobs_started_total
jobs_succeeded_total
jobs_failed_total
jobs_retried_total
jobs_dead_lettered_total
queue_depth
job_duration_seconds
job_latency_seconds
worker_heartbeat_total
active_workers
active_jobs
```

Track useful percentiles:

```text
p50 latency
p95 latency
p99 latency
```

### 8.2 Logs

Use structured logging with fields like:

```text
request_id
job_id
worker_id
queue
job_type
attempt
status
duration_ms
```

Use `slog` initially.

### 8.3 Tracing

Use OpenTelemetry to trace:

```text
API request
  -> job creation
  -> scheduler lease
  -> worker execution
  -> job completion
```

### 8.4 Dashboard

Create a simple Grafana dashboard showing:

```text
queue depth
worker count
success rate
failure rate
retry rate
dead-letter jobs
p95 job latency
slowest job types
```

## 9. Deployment and DevOps

Include:

```text
Dockerfile
docker-compose.yml
Terraform for GCP infrastructure
SSH deployment scripts
GCP VM bootstrap scripts
Kubernetes manifests verified on GCP-hosted k3s
Helm chart verified on GCP-hosted k3s
GitHub Actions CI
database migrations
load testing
```

### 9.1 Docker Compose

Local stack:

```text
pulsequeue-api
pulsequeue-scheduler
pulsequeue-worker
postgres
prometheus
grafana
nats
```

The same Compose topology is used on the GCP VM for the strict-free-tier deployment path. This keeps local and live environments close while preserving direct operational control.

### 9.2 GCP Terraform Deployment

The primary live deployment target is GCP through Terraform and SSH.

Provision with Terraform:

```text
GCP project configuration assumptions
VPC/network basics
minimal firewall rules
service account
free-tier-eligible Compute Engine VM
persistent disk for durable data
SSH access configuration
startup/bootstrap hooks
```

Deploy with SSH:

```text
connect to the VM
install or verify Docker runtime
pull or copy release artifacts
run migrations
start services with docker compose
verify api, scheduler, worker, postgres, nats, prometheus, and grafana
inspect logs and state directly
```

The GCP deployment path should be understandable from first principles. Avoid hiding important behavior behind opaque scripts when a direct command, log, or state check makes the system clearer.

### 9.3 Kubernetes

Include manifests for:

```text
API Deployment
Scheduler Deployment
Worker Deployment
PostgreSQL dependency note
NATS dependency
Service
ConfigMap
Secret
HorizontalPodAutoscaler
```

Kubernetes and Helm are not local-only artifacts. They must be deployable and verified on GCP.

Locked Kubernetes verification path:

```text
Install or bootstrap lightweight k3s on the Terraform-managed GCP VM.
Deploy PulseQueue Kubernetes manifests to the GCP-hosted k3s runtime.
Deploy the Helm chart to the same GCP-hosted k3s runtime.
Verify API, scheduler, worker, PostgreSQL connectivity, NATS connectivity, logs, and health checks live on GCP.
Use SSH to inspect pods, events, logs, services, and state after live calls.
Keep Docker Compose as the default always-on free-tier runtime unless k3s resource usage is explicitly acceptable.
```

A GKE path requires a separate explicit cost-approved change. The default Kubernetes proof path is GCP-hosted k3s because it keeps the project live-verifiable on GCP without making GKE a required cost center.

### 9.4 GitHub Actions

CI should run:

```text
go test ./...
go vet ./...
golangci-lint
migration check
Docker image build
integration tests
Terraform fmt/validate
deployment script checks
```

CI success is required before a phase can be considered ready for live GCP deployment.

## 10. Example Demo App

Build one demo pipeline.

### Email + AI Moderation Pipeline

```text
User signup event
   -> validate email
   -> enrich profile
   -> moderate content
   -> send welcome email
   -> send webhook notification
```

This gives a concrete story instead of only showing abstract jobs.

Example queues:

```text
emails
profiles
moderation
webhooks
```

Example worker startup:

```bash
pulsequeue worker --queue emails --concurrency 10
pulsequeue worker --queue moderation --concurrency 2
pulsequeue worker --queue webhooks --concurrency 5
```

This also gives a natural reason to demonstrate retries, timeouts, rate limits, and dead-letter handling.

## 11. Workflow/DAG Feature

This is part of the full PulseQueue product target. It should be implemented after the durable single-job core is correct, because workflows inherit the same retry, lease, idempotency, logging, and observability model.

Support simple dependent jobs:

```text
job B runs after job A succeeds
job C runs after jobs A and B succeed
```

Example:

```json
{
  "workflow": "user-signup",
  "jobs": [
    {
      "id": "validate-email",
      "type": "email.validate"
    },
    {
      "id": "enrich-profile",
      "type": "profile.enrich",
      "depends_on": ["validate-email"]
    },
    {
      "id": "send-welcome-email",
      "type": "email.send",
      "depends_on": ["enrich-profile"]
    }
  ]
}
```

This gives a lightweight "mini Temporal" angle without overbuilding.

## 12. Suggested Repo Structure

```text
pulsequeue/
  cmd/
    api/
      main.go
    scheduler/
      main.go
    worker/
      main.go
    pulsequeue/
      main.go

  internal/
    api/
    jobs/
    queue/
    scheduler/
    worker/
    storage/
    telemetry/
    config/
    cron/
    leases/
    logs/
    retry/
    idempotency/

  proto/
    pulsequeue.proto

  migrations/

  deployments/
    docker-compose.yml
    gcp/
      terraform/
      scripts/
    k8s/
    helm/

  examples/
    signup-pipeline/
    email-worker/
    image-processing-worker/

  docs/
    architecture.md
    database-schema.md
    failure-modes.md
    gcp-deployment.md
    scaling.md
    tradeoffs.md
    benchmarks.md

  tests/
    integration/

  .github/
    workflows/
      ci.yml

  README.md
```

For now, keep this `PLAN.md` as the only markdown planning document. When implementation begins, the relevant sections can be split into `docs/architecture.md`, `docs/failure-modes.md`, `docs/tradeoffs.md`, and other focused docs.

## 13. Build Phases

Every phase has the same completion gate:

```text
push to GitHub
pass GitHub Actions
apply or confirm Terraform-managed GCP infrastructure
deploy to the GCP VM over SSH
run live API/CLI behavior checks
exercise each feature through the live GCP-hosted path when feasible
verify PostgreSQL and NATS state directly
SSH into the VM for service logs, worker/scheduler logs, database readback, NATS checks, metrics, or traces when useful
verify Kubernetes and Helm work on GCP-hosted k3s when those artifacts are part of the phase
capture logs or command output proving the phase works
remove or document temporary operational workarounds
```

### Phase 1 - Durable Job Core

Build:

```text
REST API
gRPC API skeleton
PostgreSQL schema
NATS connection and health path
job submission
basic job listing
basic worker loop
job states: queued, running, succeeded, failed
Docker Compose
Terraform GCP VM baseline
SSH deployment script
```

Goal:

```text
Submit a job on the live GCP deployment, process it, and see its final status through API/CLI plus PostgreSQL readback.
```

### Phase 2 - Reliable Execution

Add:

```text
retries
exponential backoff
dead-letter queue
idempotency keys
timeouts
context cancellation
job attempts table
structured logs
live GCP retry/dead-letter proof
```

Goal:

```text
Failed jobs retry correctly and move to dead_letter after max attempts on the live GCP deployment.
```

### Phase 3 - Distributed Workers and Scheduler

Add:

```text
worker registration
heartbeats
job leases
stuck job recovery
graceful shutdown
concurrency limits
priority queues
delayed jobs
NATS wake-up dispatch
reconciliation polling
active-active scheduler safety tests
```

Goal:

```text
Multiple workers and scheduler replicas can safely process jobs on GCP without unsafe duplicate completion.
```

### Phase 4 - Cron, CLI, and Logs

Add:

```text
cron jobs
full CLI
job log streaming or log retrieval
worker listing
queue inspection
cancel/retry commands
GCP-hosted CLI/API proof
```

Goal:

```text
The GCP-deployed system feels usable from the command line.
```

### Phase 5 - Observability and Failure Demo

Add:

```text
Prometheus metrics
Grafana dashboard
OpenTelemetry traces
failure-mode demos
load testing with k6
benchmark results in README
GCP live failure-mode evidence
```

Goal:

```text
You can prove the GCP-deployed system works under failure and controlled free-tier-safe load.
```

### Phase 6 - Cloud-Native Hardening

Add:

```text
Kubernetes manifests verified on GCP-hosted k3s
Helm chart verified on GCP-hosted k3s
GitHub Actions deployment gates
Docker image publishing
scaling docs
architecture docs
GCP runbook
Terraform hardening
cost guardrails
live GCP proof that both Docker Compose and k3s deployment paths work
```

Goal:

```text
The project is demonstrably deployable and operable on GCP through the default Docker Compose path and the Kubernetes/Helm k3s path, not just runnable locally.
```

## 14. Failure Scenarios to Demonstrate

Include these in future `docs/failure-modes.md` and the README.

### Demo 1: Worker Dies Mid-Job

```text
1. Submit a long-running job.
2. Worker picks it up.
3. Kill the worker process.
4. Heartbeat expires.
5. Scheduler re-queues the job.
6. Another worker completes it.
```

### Demo 2: Job Keeps Failing

```text
1. Submit a job designed to fail.
2. Watch retries happen with exponential backoff.
3. After max attempts, job moves to dead_letter.
4. Retry it manually from CLI.
```

### Demo 3: Duplicate Submission

```text
1. Submit the same job twice with the same idempotency key.
2. System returns the existing job.
3. No duplicate execution occurs.
```

### Demo 4: Graceful Shutdown

```text
1. Start worker with active jobs.
2. Send SIGTERM.
3. Worker stops accepting new jobs.
4. Current jobs finish or release leases safely.
```

These demos will make the project much more memorable than a normal API project.

## 15. Tech Stack

Use:

```text
Go
PostgreSQL
REST
NATS
Protocol Buffers
gRPC
Docker Compose
Terraform
GCP Compute Engine
direct SSH deployment
Kubernetes
Prometheus
Grafana
OpenTelemetry
Cobra CLI
sqlc
goose
golangci-lint
GitHub Actions
k6
```

Recommended choices:

```text
Database access: sqlc
CLI: Cobra
Logging: slog
Migrations: goose
Metrics: Prometheus client_golang
Tracing: OpenTelemetry
API router: chi
Signal/control plane: NATS
gRPC: google.golang.org/grpc
GCP infrastructure: Terraform-managed Compute Engine VM
Live deployment: SSH plus Docker Compose
Kubernetes verification: GCP-hosted k3s plus Helm
```

For this project, use `sqlc` over a heavy ORM because it shows strong SQL and Go skills while keeping database access type-safe.

## 16. Core vs Full Product

### Core Foundation

```text
REST API
gRPC API
PostgreSQL durable jobs
NATS signal/control plane
worker pool
job states
retries
dead-letter queue
idempotency keys
leases
heartbeats
timeouts
CLI
Docker Compose
Terraform-managed GCP deployment
SSH deployment scripts
tests
README
```

### Production Features

```text
Prometheus metrics
OpenTelemetry traces
Grafana dashboard
cron jobs
delayed jobs
failure demos
load test results
Kubernetes manifests verified on GCP-hosted k3s
GitHub Actions CI
active-active schedulers
Docker/shell execution
DAG workflows
web dashboard
worker autoscaling demo
Helm chart verified on GCP-hosted k3s
pprof performance report
GCP live proof runbooks
free-tier cost guardrails
```

### Advanced Product Features

```text
workflow versioning
workflow pause/resume
manual approval steps
multi-tenant quotas
signed worker registration
job payload encryption
retention policies
archive/export support
fine-grained RBAC
```

Delivery rule:

```text
The product target is ambitious and locked.
The implementation still proceeds in phases so the durable execution model is correct before higher-level orchestration features depend on it.
```

## 17. README Structure

The README should be designed for recruiters and engineers who skim.

Suggested structure:

```markdown
# PulseQueue - Reliable Background Job Orchestration in Go

PulseQueue is a production-style distributed job queue and workflow orchestrator written in Go.

## Highlights

- Durable PostgreSQL-backed job storage
- Concurrent worker engine using goroutines and context cancellation
- Retries, exponential backoff, idempotency keys, and dead-letter queues
- Job leases, worker heartbeats, and crash recovery
- Delayed jobs and cron scheduling
- REST/gRPC API plus polished CLI
- Prometheus metrics, OpenTelemetry tracing, and structured logs
- Terraform-managed GCP deployment with direct SSH operational control
- Docker Compose runtime on the GCP free-tier deployment path
- Kubernetes and Helm verified live on GCP-hosted k3s
- Load-tested with k6

## Architecture

Include diagram.

## Quickstart

Include docker compose instructions.

## GCP Deployment

Include Terraform, SSH deployment, free-tier constraints, live health checks, and rollback instructions.

## CLI Examples

Include submit, list, retry, cancel, logs.

## API Examples

Include curl examples.

## Failure Mode Demos

Show worker crash recovery, retry exhaustion, duplicate submission, graceful shutdown.

## Benchmarks

Show throughput and latency results.

## Tradeoffs

Explain PostgreSQL as source of truth, NATS as the signal/control plane, Redis as out of core scope, and exactly-once vs at-least-once execution.

## Roadmap

Mention DAG workflows, autoscaling, web dashboard, and Docker execution.
```

## 18. Tradeoffs Content for Docs

When this plan is split into focused docs, `docs/tradeoffs.md` should cover the locked decisions from section 3:

```text
PostgreSQL + NATS vs Redis-backed queueing
At-least-once vs exactly-once execution
Push-assisted durable dispatch vs polling-only dispatch
Active-active schedulers vs single scheduler
```

The message should be:

```text
PostgreSQL gives durability, transactions, row locking, simpler recovery, and a reliable source of truth.
NATS gives low-latency wakeups, queue groups, request/reply support, and distributed worker coordination.
Redis is not the core queueing or coordination dependency for PulseQueue.
NATS must not be the only source of truth for durable jobs.

PulseQueue guarantees at-least-once execution.
Exactly-once execution is not promised.
Idempotency keys, lease fencing, attempt records, and handler-level idempotency make retries safe.

NATS is the primary wake-up and coordination path.
PostgreSQL leases and state transitions are the correctness layer.
Reconciliation polling repairs missed signals, expired leases, timed-out jobs, and retry eligibility.

PulseQueue targets active-active schedulers.
PostgreSQL locking, advisory locks, transactional updates, and unique constraints make repeated scheduler work safe.
Leader election is not the primary correctness mechanism.
```

These notes show maturity and help explain the system design in interviews.

## 19. Portfolio Headline

Use this for GitHub:

> **PulseQueue - Reliable background job orchestration in Go**

Short description:

> A production-style distributed job queue with durable PostgreSQL-backed scheduling, NATS-based dispatch signaling, concurrent Go workers, retries, idempotency keys, dead-letter queues, cron jobs, worker heartbeats, Prometheus metrics, OpenTelemetry tracing, Terraform-managed GCP free-tier deployment, direct SSH operational control, GCP-verified Kubernetes/Helm deployment artifacts, and load-tested performance benchmarks.

## 20. Resume Bullet

Use this:

> Built **PulseQueue**, a distributed job queue and workflow orchestrator in Go, featuring concurrent worker pools, durable PostgreSQL-backed scheduling, NATS dispatch signaling, retries, idempotency, dead-letter queues, worker heartbeats, cron jobs, Prometheus metrics, OpenTelemetry tracing, Terraform-managed GCP deployment, direct SSH operations, and load-tested performance benchmarks.

A more technical version:

> Designed and implemented **PulseQueue**, a GCP-deployed distributed job orchestration platform in Go with REST/gRPC APIs, PostgreSQL persistence, NATS signaling, lease-based worker coordination, exponential-backoff retries, dead-letter handling, idempotent job submission, Prometheus/Grafana observability, OpenTelemetry tracing, Terraform-managed infrastructure, and GCP-verified Kubernetes/Helm deployment artifacts.

## 21. Final Combined Direction

Build PulseQueue as a distributed job queue first, then evolve it into a small workflow orchestrator.

The strongest version is not just:

```text
a Go REST API for background jobs
```

It is:

```text
A reliable distributed job orchestration platform with durable storage, worker coordination, retries, dead-letter handling, scheduling, observability, CLI tooling, failure recovery, and cloud-native deployment.
```

That combines the best parts of all three source plans:

```text
From Plan 1:
- Distributed job runner idea
- REST/gRPC API
- Worker agents
- Log streaming
- Kubernetes/GitHub Actions/load testing
- Docker execution

From Plan 2:
- Production-grade job queue framing
- PostgreSQL as source of truth
- Idempotency
- Dead-letter queue
- Cron scheduling
- Repo structure
- Phased product plan
- Failure demo

From Plan 3:
- Workflow orchestrator angle
- Rate limiting and backpressure
- Demo pipeline
- CLI polish
- Operational realism
- Benchmarks and tradeoff notes
```

This is the version to build.
