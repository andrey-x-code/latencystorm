[CmdletBinding()]
param([switch]$UpdateGenerated)
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path $PSScriptRoot -Parent
Push-Location $repoRoot
try {
    $buildArgs = @()
    foreach ($line in Get-Content contracts/toolchain.env) {
        if ($line -and -not $line.StartsWith('#')) { $buildArgs += @('--build-arg', $line) }
    }
    $updateValue = if ($UpdateGenerated) { 'true' } else { 'false' }
    $buildArgs += @('--build-arg', "UPDATE_LOCKS=$updateValue")
    & docker build @buildArgs --file contracts/Dockerfile.verify --target export --output type=local,dest=.artifacts/contracts .
    if ($LASTEXITCODE -ne 0) { throw 'Contract generation or verification failed.' }
    $copies = @{
        '.artifacts/contracts/go/go.mod' = 'contracts/go/go.mod'
        '.artifacts/contracts/go/go.sum' = 'contracts/go/go.sum'
        '.artifacts/contracts/csharp/packages.lock.json' = 'contracts/csharp/packages.lock.json'
        '.artifacts/contracts/checks/packages.lock.json' = 'tests/contracts/csharp/packages.lock.json'
        '.artifacts/contracts/checks/compat_test.go' = 'tests/contracts/go/compat_test.go'
    }
    $generatedRoot = (Resolve-Path .artifacts/contracts/go/gen).Path
    foreach ($file in Get-ChildItem $generatedRoot -Recurse -File) {
        $relative = $file.FullName.Substring($generatedRoot.Length + 1)
        $copies[$file.FullName] = Join-Path 'contracts/go/gen' $relative
    }
    foreach ($source in $copies.Keys) {
        $target = $copies[$source]
        if ($UpdateGenerated) {
            $null = New-Item -ItemType Directory -Force -Path (Split-Path $target -Parent)
            Copy-Item -LiteralPath $source -Destination $target -Force
        } else {
            if (-not (Test-Path -LiteralPath $target)) { throw "Missing generated/locked file: $target" }
            $actual = [IO.File]::ReadAllText((Resolve-Path $source)).Replace("`r`n", "`n")
            $expected = [IO.File]::ReadAllText((Resolve-Path $target)).Replace("`r`n", "`n")
            if ($actual -cne $expected) { throw "Generated/locked file differs: $target. Run with -UpdateGenerated and review the diff." }
        }
    }
    Write-Host 'PASS: C#/Go compatibility, generation, formatting and dependency locks.'
} finally {
    Pop-Location
}
