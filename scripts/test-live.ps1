<#
.SYNOPSIS
  Run the appliance-backed (LIVE) test suites against a real Netezza.

.DESCRIPTION
  GitHub CI never runs these. Configuration comes from the environment only
  (never commit credentials):

    NZ_DEV_HOST, NZ_DEV_USER, NZ_DEV_PASSWORD   required
    NZ_DEV_DB or NZ_DEV_DATABASE                required
    NZ_DEV_PORT                                 optional (default 5480)

  Modes:
    (default)       live_qualification, live_driver, live_integration
    -Qualification  only live_qualification (self-contained)
    -Stress         live_stress (slow; NZ_STRESS_QUERIES / NZ_STRESS_CYCLES)
    -Admin          live_admin: needs administrative rights (DROP SESSION on a
                    session the test created); never part of the default run
    -Capture        regenerate tests/fixtures/wire/*.bin
    -All            functional + stress

  Every suite runs serially (--test-threads=1).
#>
[CmdletBinding()]
param(
    [switch]$Qualification,
    [switch]$Stress,
    [switch]$Admin,
    [switch]$Capture,
    [switch]$All,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ExtraArgs = @()
)

$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')

$missing = @()
if (-not $env:NZ_DEV_HOST) { $missing += 'NZ_DEV_HOST' }
if (-not $env:NZ_DEV_USER) { $missing += 'NZ_DEV_USER' }
if (-not $env:NZ_DEV_PASSWORD) { $missing += 'NZ_DEV_PASSWORD' }
if (-not ($env:NZ_DEV_DB -or $env:NZ_DEV_DATABASE)) { $missing += 'NZ_DEV_DB or NZ_DEV_DATABASE' }
if ($missing.Count -gt 0) {
    Write-Error "test-live: missing configuration: $($missing -join ', ')"
    exit 2
}

# Older tooling still checks this flag.
$env:NZ_RUN_LIVE_TESTS = '1'
$features = 'compat,chrono'

function Invoke-Suite([string]$Name) {
    Write-Host "==> cargo test --test $Name"
    cargo test -p nz_rust --features $features --test $Name -- --ignored --nocapture --test-threads=1 @ExtraArgs
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

if ($Capture) {
    cargo run -p nz_rust --features $features --example capture_wire_fixtures -- --out tests/fixtures/wire @ExtraArgs
    exit $LASTEXITCODE
}
if ($Qualification) {
    Invoke-Suite 'live_qualification'
    exit 0
}
if ($Stress) {
    Invoke-Suite 'live_stress'
    exit 0
}
if ($Admin) {
    Invoke-Suite 'live_admin'
    exit 0
}
Invoke-Suite 'live_qualification'
Invoke-Suite 'live_driver'
Invoke-Suite 'live_integration'
if ($All) { Invoke-Suite 'live_stress' }
