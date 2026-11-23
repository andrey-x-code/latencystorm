param([string]$Project = 'latencystorm-stage05')

$ErrorActionPreference = 'Stop'
$compose = @('compose', '-p', $Project, '--env-file', 'deploy/compose/.env.example', '-f', 'deploy/compose/compose.yaml')
$control = 'http://127.0.0.1:8080'

function Wait-RunState($Run, $Expected) {
    $clock = [Diagnostics.Stopwatch]::StartNew()
    while ($clock.Elapsed.TotalSeconds -lt 45) {
        $state = (Invoke-RestMethod "$control/runs/$Run" -TimeoutSec 5).state
        if ($state -eq $Expected) { return }
        if ($state -in @('failed', 'completed', 'cancelled')) { throw "Run $Run reached $state instead of $Expected" }
        Start-Sleep -Milliseconds 200
    }
    throw "Run $Run did not reach $Expected (last state: $state)"
}

function New-Run($Duration) {
    $scenario = Invoke-RestMethod "$control/scenarios" -Method Post -ContentType 'application/json' -TimeoutSec 5 -Body (@{
        name = 'stage05-smoke'; method = 'GET'; url = 'http://demo-target:8080/'; headers = @{'X-Smoke' = 'stage05'}
        targetRps = 10; durationSeconds = $Duration; requestTimeoutMs = 1000; concurrencyLimit = 4
    } | ConvertTo-Json -Compress)
    Invoke-RestMethod "$control/runs" -Method Post -ContentType 'application/json' -TimeoutSec 5 -Body (@{
        scenarioId = $scenario.id
    } | ConvertTo-Json -Compress)
}

function Assert-Batches($Run, $FinalState, $RequireHttp, $ExpectZero = $false) {
    $sql = @"
SELECT count(*) || '|' || count(*) FILTER (WHERE payload->>'isFinal' = 'true' AND
    payload #>> '{summary,state}' = '$FinalState') || '|' ||
    coalesce(sum((payload #>> '{counters,httpResponses}')::bigint), 0) || '|' ||
    (count(*) = max(sequence))::int || '|' ||
    coalesce(sum((payload #>> '{counters,started}')::bigint), 0) || '|' ||
    (SELECT count(*) FROM run_manifests WHERE run_id = '$Run' AND finalization IS NOT NULL)
FROM result_batches WHERE run_id = '$Run';
"@
    $clock = [Diagnostics.Stopwatch]::StartNew()
    while ($clock.Elapsed.TotalSeconds -lt 15) {
        $row = $sql | docker @compose exec -T postgres psql -U postgres -d results -v ON_ERROR_STOP=1 -At
        if ($LASTEXITCODE -ne 0) { throw 'Could not query the Results Service database' }
        $parts = $row -split '\|'
        if ($parts[1] -eq '1' -and $parts[3] -eq '1' -and $parts[5] -eq '1' -and (!$RequireHttp -or [int]$parts[2] -gt 0)) {
            if ($RequireHttp -and [int]$parts[0] -lt 2) { throw 'Expected interval and final batches' }
            if ($ExpectZero -and [int]$parts[4] -ne 0) { throw 'A request started before the scheduled start' }
            if (!$ExpectZero -and [int]$parts[4] -ge 600) { throw 'Stop did not interrupt generation' }
            return
        }
        Start-Sleep -Milliseconds 200
    }
    throw "No contiguous $FinalState batches for $Run (last row: $row)"
}

$natural = New-Run 3
Wait-RunState $natural.id 'completed'
Assert-Batches $natural.id 'ASSIGNMENT_STATE_COMPLETED' $true

$stopped = New-Run 60
Wait-RunState $stopped.id 'running'
Invoke-RestMethod "$control/runs/$($stopped.id)/stop" -Method Post -TimeoutSec 5 | Out-Null
Invoke-RestMethod "$control/runs/$($stopped.id)/stop" -Method Post -TimeoutSec 5 | Out-Null
Wait-RunState $stopped.id 'cancelled'
Assert-Batches $stopped.id 'ASSIGNMENT_STATE_STOPPED' $false

$early = New-Run 60
Invoke-RestMethod "$control/runs/$($early.id)/stop" -Method Post -TimeoutSec 5 | Out-Null
Wait-RunState $early.id 'cancelled'
Assert-Batches $early.id 'ASSIGNMENT_STATE_STOPPED' $false $true

Write-Output 'PASS: stage 5 completed, stopped and pre-start stop.'
