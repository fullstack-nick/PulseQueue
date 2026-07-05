#!/usr/bin/env bash
set -euo pipefail

MODE="${1:?mode is required}"
IMAGE_REF="${2:?image ref is required}"
NAMESPACE="${3:?namespace is required}"
STOP_COMPOSE="${4:-false}"
CLEANUP_AFTER="${5:-false}"

ROOT="/opt/pulsequeue/phase6"
SECRET_FILE="/tmp/pulsequeue-k3s-secrets.env"

cleanup_k3s() {
  helm uninstall pulsequeue -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true
  kubectl delete namespace "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  for _ in $(seq 1 60); do
    if ! kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "namespace $NAMESPACE still exists after cleanup wait" >&2
  kubectl get namespace "$NAMESPACE" -o yaml >&2 || true
  return 1
}

wait_for_rollouts() {
  kubectl -n "$NAMESPACE" rollout status statefulset/postgres --timeout=240s
  kubectl -n "$NAMESPACE" rollout status statefulset/nats --timeout=180s
  kubectl -n "$NAMESPACE" rollout status deployment/pulsequeue-api --timeout=240s
  kubectl -n "$NAMESPACE" rollout status deployment/pulsequeue-worker --timeout=180s
  kubectl -n "$NAMESPACE" rollout status deployment/pulsequeue-scheduler --timeout=180s
}

run_cli() {
  local name
  name="pulsequeue-proof-$(date +%s%N)"
  kubectl -n "$NAMESPACE" run "$name" \
    --rm -i \
    --restart=Never \
    --image="$IMAGE_REF" \
    --env="PULSEQUEUE_API_URL=http://pulsequeue-api:8080" \
    --env="PULSEQUEUE_OPERATOR_TOKEN=$OPERATOR_TOKEN" \
    --command -- /usr/local/bin/pulsequeue "$@"
}

prove_app() {
  local label="$1"
  local job_json
  local job_id
  local status_json
  local status
  local readback

  run_cli health
  job_json="$(run_cli jobs submit --type demo.echo --payload "{\"message\":\"$label\"}" --output json)"
  echo "$job_json"
  job_id="$(printf '%s\n' "$job_json" | sed -n 's/.*"id": "\([^"]*\)".*/\1/p' | head -n 1)"
  if [ -z "$job_id" ]; then
    echo "could not parse submitted job id" >&2
    return 1
  fi

  status=""
  for _ in $(seq 1 45); do
    status_json="$(run_cli jobs status "$job_id" --output json)"
    status="$(printf '%s\n' "$status_json" | sed -n 's/.*"status": "\([^"]*\)".*/\1/p' | head -n 1)"
    if [ "$status" = "succeeded" ]; then
      echo "$status_json"
      break
    fi
    sleep 2
  done

  if [ "$status" != "succeeded" ]; then
    echo "job $job_id did not succeed; last status was ${status:-unknown}" >&2
    kubectl -n "$NAMESPACE" logs deployment/pulsequeue-api --tail=80 >&2 || true
    kubectl -n "$NAMESPACE" logs deployment/pulsequeue-worker --tail=80 >&2 || true
    kubectl -n "$NAMESPACE" logs deployment/pulsequeue-scheduler --tail=80 >&2 || true
    return 1
  fi

  run_cli workers list
  readback="$(kubectl -n "$NAMESPACE" exec postgres-0 -- psql -U pulsequeue -d pulsequeue -tAc "select id::text || '|' || type || '|' || status || '|' || attempt_count::text from jobs where id = '$job_id'")"
  echo "postgres_readback=$readback"
  if ! printf '%s' "$readback" | grep -q "|demo.echo|succeeded|1"; then
    echo "unexpected PostgreSQL readback for job $job_id" >&2
    return 1
  fi

  kubectl -n "$NAMESPACE" get pods -o wide
  kubectl -n "$NAMESPACE" get svc -o wide
  kubectl -n "$NAMESPACE" get hpa -o wide || true
}

if [ "$MODE" = "cleanup" ]; then
  cleanup_k3s
  exit 0
fi

if [ ! -f "$SECRET_FILE" ]; then
  echo "missing $SECRET_FILE" >&2
  exit 1
fi

# shellcheck disable=SC1090
. "$SECRET_FILE"
rm -f "$SECRET_FILE"

if [ -n "${OPERATOR_TOKEN_B64:-}" ]; then
  OPERATOR_TOKEN="$(printf '%s' "$OPERATOR_TOKEN_B64" | base64 -d)"
fi
if [ -n "${POSTGRES_PASSWORD_B64:-}" ]; then
  POSTGRES_PASSWORD="$(printf '%s' "$POSTGRES_PASSWORD_B64" | base64 -d)"
fi

if [ -z "${OPERATOR_TOKEN:-}" ] || [ -z "${POSTGRES_PASSWORD:-}" ]; then
  echo "OPERATOR_TOKEN and POSTGRES_PASSWORD are required" >&2
  exit 1
fi

if [ "$STOP_COMPOSE" = "true" ] && [ -d /opt/pulsequeue/app ]; then
  cd /opt/pulsequeue/app
  docker compose -f deployments/docker-compose.yml --env-file .env down || true
fi

cd "$ROOT"
cleanup_k3s
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NAMESPACE" create secret generic pulsequeue-secrets \
  --from-literal=operator-token="$OPERATOR_TOKEN" \
  --from-literal=postgres-password="$POSTGRES_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -

case "$MODE" in
  manifests)
    kubectl apply -f deployments/k8s
    kubectl -n "$NAMESPACE" set image deployment/pulsequeue-api api="$IMAGE_REF"
    kubectl -n "$NAMESPACE" set image deployment/pulsequeue-worker worker="$IMAGE_REF"
    kubectl -n "$NAMESPACE" set image deployment/pulsequeue-scheduler scheduler="$IMAGE_REF"
    wait_for_rollouts
    prove_app "phase6 k3s manifests proof"
    ;;
  helm)
    image_repo="${IMAGE_REF%:*}"
    image_tag="${IMAGE_REF##*:}"
    helm upgrade --install pulsequeue deployments/helm/pulsequeue \
      --namespace "$NAMESPACE" \
      --set namespace="$NAMESPACE" \
      --set image.repository="$image_repo" \
      --set image.tag="$image_tag" \
      --set secrets.existingSecret=pulsequeue-secrets \
      --wait \
      --timeout 5m
    wait_for_rollouts
    prove_app "phase6 helm proof"
    ;;
  *)
    echo "unsupported mode: $MODE" >&2
    exit 1
    ;;
esac

if [ "$CLEANUP_AFTER" = "true" ]; then
  cleanup_k3s
fi
