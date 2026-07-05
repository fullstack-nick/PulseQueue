param(
  [Parameter(Mandatory = $true)][string]$ProjectId,
  [string]$Zone = "us-central1-a",
  [string]$Instance = "pulsequeue-phase1",
  [Parameter(Mandatory = $true)][string]$OperatorToken,
  [string]$PostgresPassword = "pulsequeue",
  [Parameter(Mandatory = $true)][string]$ImageRef,
  [string]$Namespace = "pulsequeue-k3s",
  [switch]$EnableObservability,
  [string]$GrafanaAdminUser = "admin",
  [string]$GrafanaAdminPassword = "pulsequeue"
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..\..")
$go = "C:\Program Files\Go\bin\go.exe"

function Invoke-ComposeDeploy {
  $args = @(
    "-ProjectId", $ProjectId,
    "-Zone", $Zone,
    "-Instance", $Instance,
    "-OperatorToken", $OperatorToken,
    "-PostgresPassword", $PostgresPassword,
    "-BuildImageLocally"
  )
  if ($EnableObservability) {
    $args += @("-EnableObservability", "-GrafanaAdminUser", $GrafanaAdminUser, "-GrafanaAdminPassword", $GrafanaAdminPassword)
  }
  & (Join-Path $PSScriptRoot "deploy.ps1") @args
}

function Get-ApiUrl {
  $ip = gcloud compute instances describe $Instance --project $ProjectId --zone $Zone --format "get(networkInterfaces[0].accessConfigs[0].natIP)"
  if ([string]::IsNullOrWhiteSpace($ip)) {
    throw "Could not determine VM public IP."
  }
  return "http://$ip`:8080"
}

function Invoke-LiveSmoke([string]$Label) {
  $apiUrl = Get-ApiUrl
  $env:PULSEQUEUE_API_URL = $apiUrl
  $env:PULSEQUEUE_OPERATOR_TOKEN = $OperatorToken
  Push-Location $root
  try {
    & $go run ./cmd/pulsequeue health
    $json = & $go run ./cmd/pulsequeue jobs submit --type demo.echo --payload "{`"message`":`"$Label`"}" --output json
    $json
    $jobId = ($json | Select-String -Pattern '"id": "([^"]+)"' | Select-Object -First 1).Matches.Groups[1].Value
    if ([string]::IsNullOrWhiteSpace($jobId)) {
      throw "Could not parse smoke job id."
    }
    for ($i = 0; $i -lt 30; $i++) {
      $statusJson = & $go run ./cmd/pulsequeue jobs status $jobId --output json
      $status = ($statusJson | Select-String -Pattern '"status": "([^"]+)"' | Select-Object -First 1).Matches.Groups[1].Value
      if ($status -eq "succeeded") {
        $statusJson
        return $jobId
      }
      Start-Sleep -Seconds 2
    }
    throw "Smoke job $jobId did not reach succeeded."
  } finally {
    Pop-Location
  }
}

Push-Location $root
try {
  $publicIp = gcloud compute instances describe $Instance --project $ProjectId --zone $Zone --format "get(networkInterfaces[0].accessConfigs[0].natIP)"
  Write-Host "Phase 6 proof target: project=$ProjectId zone=$Zone instance=$Instance ip=$publicIp image=$ImageRef"

  terraform -chdir=deployments/gcp/terraform plan -detailed-exitcode
  $planExit = $LASTEXITCODE
  if ($planExit -eq 1) {
    throw "terraform plan failed."
  }
  if ($planExit -eq 2) {
    throw "terraform plan has drift. Review before Phase 6 proof."
  }

  Invoke-ComposeDeploy
  $composeJob = Invoke-LiveSmoke "phase6 compose pre-k3s proof"
  Write-Host "compose_pre_k3s_job=$composeJob"

  & (Join-Path $PSScriptRoot "deploy-k3s.ps1") `
    -ProjectId $ProjectId `
    -Zone $Zone `
    -Instance $Instance `
    -Mode manifests `
    -ImageRef $ImageRef `
    -Namespace $Namespace `
    -OperatorToken $OperatorToken `
    -PostgresPassword $PostgresPassword `
    -StopCompose `
    -CleanupAfterProof

  & (Join-Path $PSScriptRoot "deploy-k3s.ps1") `
    -ProjectId $ProjectId `
    -Zone $Zone `
    -Instance $Instance `
    -Mode helm `
    -ImageRef $ImageRef `
    -Namespace $Namespace `
    -OperatorToken $OperatorToken `
    -PostgresPassword $PostgresPassword `
    -StopCompose `
    -CleanupAfterProof

  Invoke-ComposeDeploy
  $finalJob = Invoke-LiveSmoke "phase6 compose restored proof"
  Write-Host "compose_restored_job=$finalJob"

  gcloud compute ssh $Instance --project $ProjectId --zone $Zone --command "cd /opt/pulsequeue/app && docker compose -f deployments/docker-compose.yml --env-file .env ps"
} finally {
  Pop-Location
}
