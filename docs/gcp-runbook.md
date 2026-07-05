# GCP Runbook

## Environment

Default Phase 6 target:

```text
Project: pulsequeue-r7m5o9ld
Zone: us-central1-a
Instance: pulsequeue-phase1
Default runtime: Docker Compose
k3s namespace: pulsequeue-k3s
```

Use `C:\Program Files\Go\bin\go.exe` for local Go verification on this Windows host.

## Compose Deploy

Use the local-image path for the free-tier VM:

```powershell
.\deployments\gcp\scripts\deploy.ps1 `
  -ProjectId pulsequeue-r7m5o9ld `
  -Zone us-central1-a `
  -OperatorToken $env:PULSEQUEUE_OPERATOR_TOKEN `
  -PostgresPassword $env:POSTGRES_PASSWORD `
  -BuildImageLocally `
  -EnableObservability
```

Verify:

```powershell
$env:PULSEQUEUE_API_URL="http://VM_PUBLIC_IP:8080"
& "C:\Program Files\Go\bin\go.exe" run ./cmd/pulsequeue health
& "C:\Program Files\Go\bin\go.exe" run ./cmd/pulsequeue jobs submit --type demo.echo --payload '{"message":"compose proof"}'
```

Inspect services:

```powershell
gcloud compute ssh pulsequeue-phase1 --project pulsequeue-r7m5o9ld --zone us-central1-a --command "cd /opt/pulsequeue/app && docker compose -f deployments/docker-compose.yml --env-file .env ps"
```

## k3s Manifest Proof

Use the published GHCR image tag from the successful GitHub Actions publish job:

```powershell
.\deployments\gcp\scripts\deploy-k3s.ps1 `
  -ProjectId pulsequeue-r7m5o9ld `
  -Zone us-central1-a `
  -Mode manifests `
  -ImageRef ghcr.io/fullstack-nick/pulsequeue:sha-<shortsha> `
  -OperatorToken $env:PULSEQUEUE_OPERATOR_TOKEN `
  -PostgresPassword $env:POSTGRES_PASSWORD `
  -StopCompose `
  -CleanupAfterProof
```

The script installs or verifies k3s/Helm, creates the runtime secret, deploys the manifests, waits for StatefulSets and Deployments, runs `pulsequeue health` from a pod, submits a real `demo.echo` job, verifies completion, reads PostgreSQL from `postgres-0`, shows pods/services/HPA, and cleans up the namespace when requested.

## Helm Proof

```powershell
.\deployments\gcp\scripts\deploy-k3s.ps1 `
  -ProjectId pulsequeue-r7m5o9ld `
  -Zone us-central1-a `
  -Mode helm `
  -ImageRef ghcr.io/fullstack-nick/pulsequeue:sha-<shortsha> `
  -OperatorToken $env:PULSEQUEUE_OPERATOR_TOKEN `
  -PostgresPassword $env:POSTGRES_PASSWORD `
  -StopCompose `
  -CleanupAfterProof
```

Helm uses the same runtime secret as the raw manifest proof. No token, password, kubeconfig, or generated values file is committed.

## Full Phase 6 Proof

```powershell
.\deployments\gcp\scripts\phase6-proof.ps1 `
  -ProjectId pulsequeue-r7m5o9ld `
  -Zone us-central1-a `
  -OperatorToken $env:PULSEQUEUE_OPERATOR_TOKEN `
  -PostgresPassword $env:POSTGRES_PASSWORD `
  -ImageRef ghcr.io/fullstack-nick/pulsequeue:sha-<shortsha> `
  -EnableObservability
```

This script checks Terraform drift, deploys and proves Compose, proves raw k3s manifests, proves Helm, cleans up k3s workloads, restores Compose, and runs a final live smoke job.

## Rollback to Compose

If k3s proof fails or the VM is memory-constrained:

```powershell
.\deployments\gcp\scripts\deploy-k3s.ps1 `
  -ProjectId pulsequeue-r7m5o9ld `
  -Zone us-central1-a `
  -Mode cleanup

.\deployments\gcp\scripts\deploy.ps1 `
  -ProjectId pulsequeue-r7m5o9ld `
  -Zone us-central1-a `
  -OperatorToken $env:PULSEQUEUE_OPERATOR_TOKEN `
  -PostgresPassword $env:POSTGRES_PASSWORD `
  -BuildImageLocally `
  -EnableObservability
```

Then verify `/health/live`, `/health/ready`, a smoke job, and `docker compose ps`.

## Cost Guardrails

Phase 6 does not introduce GKE, Cloud SQL, load balancers, Artifact Registry, Memorystore, Cloud Run, or Pub/Sub. The Terraform validation locks the proof path to:

```text
region: us-central1
zone: us-central1-a
machine: e2-micro
boot disk: <= 30 GB
operator CIDR: /32
```
