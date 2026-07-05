# PulseQueue Architecture

PulseQueue is a Go job queue built around a small set of durable runtime components:

- REST and gRPC API process for job submission, inspection, health, and operator actions.
- Worker process for leased job execution.
- Scheduler process for delayed jobs, retries, cron fires, and expired lease recovery.
- PostgreSQL as the source of truth for jobs, attempts, logs, workers, and cron state.
- NATS as the wake-up signal path between API, scheduler, and workers.

## Runtime Model

The same `pulsequeue` binary runs in three roles:

```text
pulsequeue server
pulsequeue worker
pulsequeue scheduler
```

The role is selected with command arguments. Configuration is passed through environment variables, so Docker Compose, k3s manifests, and Helm all use the same runtime contract.

## Default GCP Runtime

The default always-on GCP deployment remains Docker Compose on a Terraform-managed `e2-micro` VM in `us-central1-a`.

Compose runs:

```text
api
worker
scheduler
postgres
nats
optional prometheus/grafana/otel collector
```

Only API/gRPC and SSH are reachable through GCP firewall rules, and only from the operator CIDR. PostgreSQL, NATS, Prometheus, and Grafana bind to loopback or cluster-internal addresses.

## k3s Proof Runtime

Phase 6 adds a second deployment path for Kubernetes and Helm proof. It uses lightweight k3s on the same GCP VM. This is not a fake demo: during the proof window, k3s runs the full PulseQueue stack:

```text
PostgreSQL StatefulSet
NATS StatefulSet
API Deployment and ClusterIP Service
Worker Deployment and HPA
Scheduler Deployment
runtime Secret
runtime ConfigMap
```

k3s services are ClusterIP only. The proof uses a short maintenance window: stop Compose, run the full app on k3s, submit and complete real jobs, read PostgreSQL state from the in-cluster database, clean up k3s workloads, and restore Compose as the always-on runtime.

## Image Flow

GitHub Actions publishes the app image to:

```text
ghcr.io/fullstack-nick/pulsequeue:main
ghcr.io/fullstack-nick/pulsequeue:sha-<shortsha>
```

The Docker Compose GCP path still prefers local image build and `docker load` because that path has been reliable on the free-tier VM. The k3s and Helm proof paths use the published GHCR image.

## Failure Boundaries

PostgreSQL remains the durability boundary. NATS can be unavailable temporarily without losing job state, but readiness fails while NATS is unavailable because workers and schedulers depend on it for prompt wakeups.

The API applies migrations at startup. In k3s, the API must become ready before workers and schedulers can complete useful work, and proof checks `/health/ready` before submitting jobs.
