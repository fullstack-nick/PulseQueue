param(
  [Parameter(Mandatory = $true)][string]$ProjectId,
  [string]$Zone = "us-central1-a",
  [string]$Instance = "pulsequeue-phase1"
)

$ErrorActionPreference = "Stop"
$scriptPath = Join-Path $PSScriptRoot "install-k3s.sh"

gcloud compute scp $scriptPath "${Instance}:/tmp/pulsequeue-install-k3s.sh" --project $ProjectId --zone $Zone
if ($LASTEXITCODE -ne 0) {
  throw "failed to copy install-k3s.sh to $Instance."
}
gcloud compute ssh $Instance --project $ProjectId --zone $Zone --command "bash /tmp/pulsequeue-install-k3s.sh"
if ($LASTEXITCODE -ne 0) {
  throw "install-k3s.sh failed on $Instance."
}
