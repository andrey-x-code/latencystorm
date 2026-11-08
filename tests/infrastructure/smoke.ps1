[CmdletBinding()]
param([string]$ComposeFile = 'deploy/compose/compose.yaml', [string]$EnvFile = 'deploy/compose/.env.example')
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent | Split-Path -Parent
Push-Location $root
$kafkaStopped = $false
try {
  function Status([string]$Url) {
    try { return (Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 8).StatusCode }
    catch { if ($_.Exception.Response) { return [int]$_.Exception.Response.StatusCode }; throw }
  }
  $endpoints = @(
    'http://127.0.0.1:8080/health', 'http://127.0.0.1:8080/ready',
    'http://127.0.0.1:8081/health', 'http://127.0.0.1:8081/ready',
    'http://127.0.0.1:8082/health', 'http://127.0.0.1:8082/ready',
    'http://127.0.0.1:8083/health', 'http://127.0.0.1:8083/ready',
    'http://127.0.0.1:9090/-/ready'
  )
  foreach ($url in $endpoints) { if ((Status $url) -ne 200) { throw "Endpoint is not ready: $url" } }
  $demo = Invoke-WebRequest -Uri http://127.0.0.1:8083/ -UseBasicParsing -TimeoutSec 8 | Select-Object -ExpandProperty Content | ConvertFrom-Json
  if ($demo.status -ne 200) { throw 'Demo Target success response failed.' }
  & docker compose --env-file $EnvFile -f $ComposeFile stop kafka
  if ($LASTEXITCODE) { throw 'Kafka stop failed.' }
  $kafkaStopped = $true
  $deadline = (Get-Date).AddSeconds(30)
  do { Start-Sleep -Seconds 1; $control = Status 'http://127.0.0.1:8080/ready'; $results = Status 'http://127.0.0.1:8081/ready' } while (($control -ne 503 -or $results -ne 503) -and (Get-Date) -lt $deadline)
  if ($control -ne 503 -or $results -ne 503) { throw 'Readiness did not detect Kafka failure.' }
  & docker compose --env-file $EnvFile -f $ComposeFile start kafka
  if ($LASTEXITCODE) { throw 'Kafka start failed.' }
  $kafkaStopped = $false
  $deadline = (Get-Date).AddSeconds(45)
  do { Start-Sleep -Seconds 3; $control = Status 'http://127.0.0.1:8080/ready'; $results = Status 'http://127.0.0.1:8081/ready' } while (($control -ne 200 -or $results -ne 200) -and (Get-Date) -lt $deadline)
  if ($control -ne 200 -or $results -ne 200) { throw 'Readiness did not recover.' }
  Write-Host 'PASS: infrastructure smoke test.'
} finally {
  if ($kafkaStopped) { & docker compose --env-file $EnvFile -f $ComposeFile start kafka | Out-Host }
  Pop-Location
}
