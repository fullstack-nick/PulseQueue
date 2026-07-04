# PulseQueue

PulseQueue is a production-style durable job queue and workflow orchestration platform in Go.

Phase 1 implements the durable job core:

- PostgreSQL-backed job persistence
- NATS wake-up signaling
- REST API with operator-token auth
- gRPC health skeleton and committed API proto
- Worker loop with `FOR UPDATE SKIP LOCKED` leasing
- Built-in `demo.echo` handler
- CLI for health checks and basic job operations
- Docker Compose runtime for local and GCP VM deployment
- Terraform baseline for a GCP free-tier Compute Engine VM

Phase 2 adds reliable execution:

- Per-attempt history in PostgreSQL
- Lease-fenced success and failure transitions
- Deterministic exponential-backoff retries
- `dead_letter` terminal state after attempts are exhausted
- Timeout-aware worker execution with `context.Context`
- Built-in `demo.fail` and `demo.sleep` handlers for failure proof
- CLI/API attempt inspection

## Quickstart

Create a local env file:

```powershell
Copy-Item .env.example .env
```

Set a non-default token in `.env`:

```text
PULSEQUEUE_OPERATOR_TOKEN=change-this
```

Start the stack:

```powershell
docker compose -f deployments/docker-compose.yml up --build
```

Check health:

```powershell
$env:PULSEQUEUE_OPERATOR_TOKEN="change-this"
$env:PULSEQUEUE_API_URL="http://localhost:8080"
go run ./cmd/pulsequeue health
```

Submit and inspect a job:

```powershell
go run ./cmd/pulsequeue jobs submit --queue default --type demo.echo --payload '{"message":"hello"}'
go run ./cmd/pulsequeue jobs list
```

Exercise retries and dead-letter handling:

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

## API

Unauthenticated:

```text
GET /health/live
GET /health/ready
```

Authenticated with `Authorization: Bearer $PULSEQUEUE_OPERATOR_TOKEN`:

```text
POST /jobs
GET  /jobs
GET  /jobs/{id}
GET  /jobs/{id}/attempts
```

Example:

```powershell
$headers = @{ Authorization = "Bearer change-this" }
$body = @{
  queue = "default"
  type = "demo.echo"
  payload = @{ message = "hello from api" }
  max_attempts = 3
  timeout_seconds = 10
} | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://localhost:8080/jobs -Headers $headers -Body $body -ContentType "application/json"
```

## GCP Phase 1 Deployment

The required live proof path is a Terraform-managed GCP Compute Engine VM with Docker Compose over SSH.

Prepare an isolated GCP project, then create `deployments/gcp/terraform/terraform.tfvars` from the example:

```hcl
project_id    = "pulsequeue-r7m5o9ld"
region        = "us-central1"
zone          = "us-central1-a"
operator_cidr = "YOUR_PUBLIC_IP/32"
```

Provision:

```powershell
terraform -chdir=deployments/gcp/terraform init
terraform -chdir=deployments/gcp/terraform apply
```

Deploy:

```powershell
.\deployments\gcp\scripts\deploy.ps1 `
  -ProjectId pulsequeue-r7m5o9ld `
  -Zone us-central1-a `
  -OperatorToken "replace-with-secret" `
  -BuildImageLocally
```

`-BuildImageLocally` is recommended for the free-tier VM because it builds the Linux image locally, loads it onto the VM, and recreates Compose services without doing a resource-heavy remote build.

Then verify live:

```powershell
$env:PULSEQUEUE_API_URL="http://VM_PUBLIC_IP:8080"
$env:PULSEQUEUE_OPERATOR_TOKEN="replace-with-secret"
go run ./cmd/pulsequeue health
go run ./cmd/pulsequeue jobs submit --type demo.echo --payload '{"message":"live gcp"}'
go run ./cmd/pulsequeue jobs list
```

Then verify Phase 2 behavior:

```powershell
go run ./cmd/pulsequeue jobs submit --type demo.fail --max-attempts 3 --payload '{"message":"live retry proof"}' --output json
go run ./cmd/pulsequeue jobs status JOB_ID
go run ./cmd/pulsequeue jobs attempts JOB_ID
go run ./cmd/pulsequeue jobs submit --type demo.echo --idempotency-key live-duplicate-proof --payload '{"message":"first"}'
go run ./cmd/pulsequeue jobs submit --type demo.echo --idempotency-key live-duplicate-proof --payload '{"message":"second"}'
go run ./cmd/pulsequeue jobs submit --type demo.sleep --timeout-seconds 1 --max-attempts 2 --payload '{"duration_ms":3000}' --output json
```

Also SSH into the VM and inspect service state:

```powershell
gcloud compute ssh pulsequeue-phase1 --project pulsequeue-r7m5o9ld --zone us-central1-a --command "cd /opt/pulsequeue/app && docker compose -f deployments/docker-compose.yml --env-file .env logs --tail=80 api worker"
```

## Phase Completion Gate

Each phase is complete only when:

- Code is pushed to public `fullstack-nick/PulseQueue`.
- GitHub Actions passes.
- GCP VM infrastructure is applied through Terraform.
- The stack is deployed through SSH.
- The phase behavior is exercised against the live GCP API and worker.
- PostgreSQL row state and API/worker logs are verified on the VM.
- PostgreSQL and NATS are not exposed through public firewall rules.

## Phase 1 Live Proof

Recorded on 2026-07-04.

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
pulsequeue-allow-operator-api  213.225.10.35/32  tcp:8080,tcp:9090
pulsequeue-allow-operator-ssh  213.225.10.35/32  tcp:22
```

Container exposure on the VM:

```text
api       0.0.0.0:8080->8080/tcp, 0.0.0.0:9090->9090/tcp
postgres 127.0.0.1:5432->5432/tcp
nats      127.0.0.1:4222->4222/tcp, 127.0.0.1:8222->8222/tcp
worker    no public ports
```

## Phase 2 Live Proof

Recorded on 2026-07-04 UTC / 2026-07-05 Europe/Berlin.

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

Live Phase 2 behavior:

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

GCP firewall proof for the PulseQueue network:

```text
pulsequeue-allow-operator-api  213.225.10.35/32  tcp:8080,tcp:9090
pulsequeue-allow-operator-ssh  213.225.10.35/32  tcp:22
```

Container exposure on the VM:

```text
api       0.0.0.0:8080->8080/tcp, 0.0.0.0:9090->9090/tcp
postgres 127.0.0.1:5432->5432/tcp
nats      127.0.0.1:4222->4222/tcp, 127.0.0.1:8222->8222/tcp
worker    no public ports
```
