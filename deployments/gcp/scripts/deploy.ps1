param(
  [Parameter(Mandatory = $true)][string]$ProjectId,
  [string]$Zone = "us-central1-a",
  [string]$Instance = "pulsequeue-phase1",
  [Parameter(Mandatory = $true)][string]$OperatorToken,
  [string]$PostgresPassword = "pulsequeue",
  [switch]$BuildImageLocally,
  [switch]$EnableObservability,
  [string]$GrafanaAdminUser = "admin",
  [string]$GrafanaAdminPassword = "pulsequeue"
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..\..")
$remote = "/opt/pulsequeue"
$archive = Join-Path $env:TEMP "pulsequeue-deploy.tar"
$imageArchive = Join-Path $env:TEMP "pulsequeue-images.tar"

Push-Location $root
try {
  if (Test-Path $archive) { Remove-Item -LiteralPath $archive -Force }
  tar --exclude .git --exclude .idea --exclude .terraform --exclude terraform.tfstate --exclude terraform.tfvars -cf $archive .
  if ($BuildImageLocally) {
    if (Test-Path $imageArchive) { Remove-Item -LiteralPath $imageArchive -Force }
    docker build -t pulsequeue:deploy .
    docker tag pulsequeue:deploy deployments-api:latest
    docker tag pulsequeue:deploy deployments-worker:latest
    docker tag pulsequeue:deploy deployments-scheduler:latest
    docker save deployments-api:latest deployments-worker:latest deployments-scheduler:latest -o $imageArchive
  }
} finally {
  Pop-Location
}

gcloud compute ssh $Instance --project $ProjectId --zone $Zone --command "mkdir -p $remote"
gcloud compute scp "$PSScriptRoot\bootstrap-vm.sh" "${Instance}:/tmp/bootstrap-vm.sh" --project $ProjectId --zone $Zone
gcloud compute ssh $Instance --project $ProjectId --zone $Zone --command "bash /tmp/bootstrap-vm.sh"
gcloud compute scp $archive "${Instance}:/tmp/pulsequeue-deploy.tar" --project $ProjectId --zone $Zone
if ($BuildImageLocally) {
  gcloud compute scp $imageArchive "${Instance}:/tmp/pulsequeue-images.tar" --project $ProjectId --zone $Zone
}

$workerMetricsAddr = ""
$schedulerMetricsAddr = ""
$otelEndpoint = ""
if ($EnableObservability) {
  $workerMetricsAddr = ":2112"
  $schedulerMetricsAddr = ":2112"
  $otelEndpoint = "otel-collector:4317"
}

$envContent = @"
POSTGRES_USER=pulsequeue
POSTGRES_PASSWORD=$PostgresPassword
POSTGRES_DB=pulsequeue
PULSEQUEUE_OPERATOR_TOKEN=$OperatorToken
PULSEQUEUE_API_URL=http://localhost:8080
PULSEQUEUE_PUBLIC_HTTP_BIND=0.0.0.0
PULSEQUEUE_PUBLIC_GRPC_BIND=0.0.0.0
PULSEQUEUE_WORKER_QUEUE=default
PULSEQUEUE_WORKER_CONCURRENCY=1
PULSEQUEUE_WORKER_POLL_INTERVAL=5s
PULSEQUEUE_WORKER_HEARTBEAT_INTERVAL=10s
PULSEQUEUE_LEASE_DURATION=60s
PULSEQUEUE_SCHEDULER_INTERVAL=2s
PULSEQUEUE_SCHEDULER_BATCH_SIZE=50
PULSEQUEUE_RETRY_INITIAL_DELAY=2s
PULSEQUEUE_RETRY_MAX_DELAY=30s
PULSEQUEUE_WORKER_METRICS_ADDR=$workerMetricsAddr
PULSEQUEUE_SCHEDULER_METRICS_ADDR=$schedulerMetricsAddr
PULSEQUEUE_OTEL_EXPORTER_OTLP_ENDPOINT=$otelEndpoint
GRAFANA_ADMIN_USER=$GrafanaAdminUser
GRAFANA_ADMIN_PASSWORD=$GrafanaAdminPassword
"@
$envFile = Join-Path $env:TEMP "pulsequeue.env"
Set-Content -LiteralPath $envFile -Value $envContent -NoNewline
gcloud compute scp $envFile "${Instance}:$remote/.env" --project $ProjectId --zone $Zone

$profileArg = ""
if ($EnableObservability) {
  $profileArg = "--profile observability "
}

$composeUp = "docker compose -f deployments/docker-compose.yml --env-file .env $profileArg up -d --build"
if ($BuildImageLocally) {
  $composeUp = "docker load -i /tmp/pulsequeue-images.tar && docker compose -f deployments/docker-compose.yml --env-file .env $profileArg up -d --no-build --force-recreate"
}

gcloud compute ssh $Instance --project $ProjectId --zone $Zone --command "cd $remote && rm -rf app && mkdir app && tar -xf /tmp/pulsequeue-deploy.tar -C app && cp .env app/.env && cd app && $composeUp"
gcloud compute ssh $Instance --project $ProjectId --zone $Zone --command "cd $remote/app && docker compose -f deployments/docker-compose.yml --env-file ../.env ps"
