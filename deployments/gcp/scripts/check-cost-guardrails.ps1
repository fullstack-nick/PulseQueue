param(
  [string]$TerraformDir = "deployments/gcp/terraform"
)

$ErrorActionPreference = "Stop"
$forbidden = @(
  "google_container_cluster",
  "google_container_node_pool",
  "google_sql_",
  "google_redis_",
  "google_compute_forwarding_rule",
  "google_compute_global_forwarding_rule",
  "google_compute_backend_service",
  "google_artifact_registry_repository",
  "google_pubsub_",
  "google_cloud_run_"
)

$files = Get-ChildItem -LiteralPath $TerraformDir -Filter *.tf -Recurse
$hits = @()
foreach ($file in $files) {
  foreach ($pattern in $forbidden) {
    $matches = Select-String -LiteralPath $file.FullName -Pattern $pattern -SimpleMatch
    foreach ($match in $matches) {
      $hits += [pscustomobject]@{
        File = $file.FullName
        Line = $match.LineNumber
        Pattern = $pattern
        Text = $match.Line.Trim()
      }
    }
  }
}

if ($hits.Count -gt 0) {
  $hits | Format-Table -AutoSize | Out-String | Write-Error
  throw "Cost guardrail failed: paid managed-service Terraform resource detected."
}

Write-Host "Cost guardrail passed for $TerraformDir"
