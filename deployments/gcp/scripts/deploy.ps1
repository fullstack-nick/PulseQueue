param(
  [Parameter(Mandatory = $true)][string]$ProjectId,
  [string]$Zone = "us-central1-a",
  [string]$Instance = "pulsequeue-phase1",
  [Parameter(Mandatory = $true)][string]$OperatorToken,
  [string]$PostgresPassword = "pulsequeue",
  [switch]$BuildImageLocally
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..\..")
$remote = "/opt/pulsequeue"
$archive = Join-Path $env:TEMP "pulsequeue-deploy.tar"
$imageArchive = Join-Path $env:TEMP "pulsequeue-images.tar"

Push-Location $root
try {
  if (Test-Path $archive) { Remove-Item -LiteralPath $archive -Force }
  tar --exclude .git --exclude .terraform --exclude terraform.tfstate --exclude terraform.tfvars -cf $archive .
  if ($BuildImageLocally) {
    if (Test-Path $imageArchive) { Remove-Item -LiteralPath $imageArchive -Force }
    docker build -t pulsequeue:deploy .
    docker tag pulsequeue:deploy deployments-api:latest
    docker tag pulsequeue:deploy deployments-worker:latest
    docker save deployments-api:latest deployments-worker:latest -o $imageArchive
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

$envContent = @"
POSTGRES_USER=pulsequeue
POSTGRES_PASSWORD=$PostgresPassword
POSTGRES_DB=pulsequeue
PULSEQUEUE_OPERATOR_TOKEN=$OperatorToken
PULSEQUEUE_API_URL=http://localhost:8080
PULSEQUEUE_PUBLIC_HTTP_BIND=0.0.0.0
PULSEQUEUE_PUBLIC_GRPC_BIND=0.0.0.0
PULSEQUEUE_RETRY_INITIAL_DELAY=2s
PULSEQUEUE_RETRY_MAX_DELAY=30s
"@
$envFile = Join-Path $env:TEMP "pulsequeue.env"
Set-Content -LiteralPath $envFile -Value $envContent -NoNewline
gcloud compute scp $envFile "${Instance}:$remote/.env" --project $ProjectId --zone $Zone

$composeUp = "docker compose -f deployments/docker-compose.yml --env-file .env up -d --build"
if ($BuildImageLocally) {
  $composeUp = "docker load -i /tmp/pulsequeue-images.tar && docker compose -f deployments/docker-compose.yml --env-file .env up -d --no-build --force-recreate"
}

gcloud compute ssh $Instance --project $ProjectId --zone $Zone --command "cd $remote && rm -rf app && mkdir app && tar -xf /tmp/pulsequeue-deploy.tar -C app && cp .env app/.env && cd app && $composeUp"
gcloud compute ssh $Instance --project $ProjectId --zone $Zone --command "cd $remote/app && docker compose -f deployments/docker-compose.yml --env-file ../.env ps"
