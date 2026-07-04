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
```

Example:

```powershell
$headers = @{ Authorization = "Bearer change-this" }
$body = @{
  queue = "default"
  type = "demo.echo"
  payload = @{ message = "hello from api" }
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
  -OperatorToken "replace-with-secret"
```

Then verify live:

```powershell
$env:PULSEQUEUE_API_URL="http://VM_PUBLIC_IP:8080"
$env:PULSEQUEUE_OPERATOR_TOKEN="replace-with-secret"
go run ./cmd/pulsequeue health
go run ./cmd/pulsequeue jobs submit --type demo.echo --payload '{"message":"live gcp"}'
go run ./cmd/pulsequeue jobs list
```

Also SSH into the VM and inspect service state:

```powershell
gcloud compute ssh pulsequeue-phase1 --project pulsequeue-r7m5o9ld --zone us-central1-a --command "cd /opt/pulsequeue/app && docker compose -f deployments/docker-compose.yml --env-file .env logs --tail=80 api worker"
```

## Phase 1 Completion Gate

Phase 1 is complete only when:

- Code is pushed to public `fullstack-nick/PulseQueue`.
- GitHub Actions passes.
- GCP VM infrastructure is applied through Terraform.
- The stack is deployed through SSH.
- A live GCP `demo.echo` job reaches `succeeded`.
- PostgreSQL row state and API/worker logs are verified on the VM.
- PostgreSQL and NATS are not exposed through public firewall rules.
