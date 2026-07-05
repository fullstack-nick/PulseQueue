param(
  [Parameter(Mandatory = $true)][string]$ProjectId,
  [string]$Zone = "us-central1-a",
  [string]$Instance = "pulsequeue-phase1",
  [ValidateSet("manifests", "helm", "cleanup")][string]$Mode = "manifests",
  [string]$ImageRef = "ghcr.io/fullstack-nick/pulsequeue:main",
  [string]$Namespace = "pulsequeue-k3s",
  [string]$OperatorToken,
  [string]$PostgresPassword = "pulsequeue",
  [switch]$StopCompose,
  [switch]$CleanupAfterProof
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..\..")
$archive = Join-Path $env:TEMP "pulsequeue-k3s-artifacts.tar"
$secretFile = Join-Path $env:TEMP "pulsequeue-k3s-secrets.env"

if ($Mode -ne "cleanup" -and [string]::IsNullOrWhiteSpace($OperatorToken)) {
  throw "OperatorToken is required unless Mode is cleanup."
}

& (Join-Path $PSScriptRoot "install-k3s.ps1") -ProjectId $ProjectId -Zone $Zone -Instance $Instance

Push-Location $root
try {
  if (Test-Path $archive) { Remove-Item -LiteralPath $archive -Force }
  tar -cf $archive deployments/k8s deployments/helm
} finally {
  Pop-Location
}

gcloud compute ssh $Instance --project $ProjectId --zone $Zone --command "mkdir -p /opt/pulsequeue/phase6"
gcloud compute scp $archive "${Instance}:/tmp/pulsequeue-k3s-artifacts.tar" --project $ProjectId --zone $Zone
gcloud compute scp "$PSScriptRoot\deploy-k3s-remote.sh" "${Instance}:/tmp/pulsequeue-deploy-k3s-remote.sh" --project $ProjectId --zone $Zone
gcloud compute ssh $Instance --project $ProjectId --zone $Zone --command "rm -rf /opt/pulsequeue/phase6/* && tar -xf /tmp/pulsequeue-k3s-artifacts.tar -C /opt/pulsequeue/phase6 && chmod +x /tmp/pulsequeue-deploy-k3s-remote.sh"

if ($Mode -ne "cleanup") {
  $operatorTokenB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($OperatorToken))
  $postgresPasswordB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($PostgresPassword))
  $secretContent = @(
    "OPERATOR_TOKEN_B64='$operatorTokenB64'",
    "POSTGRES_PASSWORD_B64='$postgresPasswordB64'"
  ) -join "`n"
  Set-Content -LiteralPath $secretFile -Value $secretContent -NoNewline
  try {
    gcloud compute scp $secretFile "${Instance}:/tmp/pulsequeue-k3s-secrets.env" --project $ProjectId --zone $Zone
  } finally {
    Remove-Item -LiteralPath $secretFile -Force -ErrorAction SilentlyContinue
  }
}

$stopComposeArg = if ($StopCompose) { "true" } else { "false" }
$cleanupArg = if ($CleanupAfterProof) { "true" } else { "false" }
$command = "bash /tmp/pulsequeue-deploy-k3s-remote.sh '$Mode' '$ImageRef' '$Namespace' '$stopComposeArg' '$cleanupArg'"
gcloud compute ssh $Instance --project $ProjectId --zone $Zone --command $command
