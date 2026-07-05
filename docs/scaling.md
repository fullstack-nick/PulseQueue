# Scaling Notes

PulseQueue scaling is intentionally conservative on the current free-tier VM. The goal is to prove the operating model without hiding resource limits behind managed services.

## Worker Scaling

Workers scale on two axes:

- Replicas: more worker processes or pods.
- Concurrency: more goroutines per worker process.

The k3s Helm chart defaults to:

```text
worker replicas: 1
worker concurrency: 1
HPA min replicas: 1
HPA max replicas: 2
target CPU utilization: 75%
```

For the free-tier VM, prefer increasing replicas to `2` before increasing per-worker concurrency. That keeps lease ownership and worker heartbeat behavior easier to inspect.

## Scheduler Scaling

The scheduler is designed for active-active safety through database claims. The Phase 6 k3s proof keeps one scheduler replica by default to reduce VM pressure. A later scale proof can raise scheduler replicas to `2` and verify that delayed jobs, retry jobs, and cron fires do not duplicate unsafely.

## API Scaling

The API is stateless except for PostgreSQL and NATS dependencies. In k3s, additional API replicas can be added when a stable ingress or service exposure strategy exists. Phase 6 keeps the API ClusterIP-only and proves it from inside the cluster to avoid new firewall or load-balancer costs.

## PostgreSQL and NATS Limits

PostgreSQL and NATS are single-instance dependencies in the free-tier proof. They are intentionally not converted to Cloud SQL, managed NATS, or GKE add-ons in Phase 6.

Scaling beyond the current VM should be treated as a new phase because it changes the cost model and operational surface:

- database backup and restore policy
- disk growth and retention policy
- external ingress and TLS
- image pull authentication or public package policy
- metrics retention
- multi-node Kubernetes behavior

## Free-Tier Defaults

Keep these defaults unless a specific proof requires a short-lived change:

```text
GCP machine: e2-micro
GCP zone: us-central1-a
Compose: default always-on runtime
k3s: temporary proof runtime
PostgreSQL/NATS: internal only
API/gRPC: operator CIDR only
worker HPA: max 2 pods
```
