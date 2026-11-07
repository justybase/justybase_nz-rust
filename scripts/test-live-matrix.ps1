# Run the self-contained LIVE qualification suite against one or two appliances.
#
# Profiles (environment only; credentials are never written to disk):
#   NZ_DEV_*    primary appliance (required)
#   NZ_DEV2_*   second appliance or server version (optional)
#
# Each profile needs HOST, USER, PASSWORD and DB or DATABASE. PORT and SCHEMA
# are optional and are forwarded unchanged.
#
# Usage:
#   scripts/test-live-matrix.ps1 [-RequireTwo] [-ExtraArgs ...]
#
# Results: target/live-matrix/report.txt (never committed).
[CmdletBinding()]
param(
    [switch]$RequireTwo,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ExtraArgs = @()
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$out = Join-Path $root 'target/live-matrix'
$report = Join-Path $out 'report.txt'
New-Item -ItemType Directory -Force -Path $out | Out-Null

@(
    '# nz_rust live matrix'
    "# date: $((Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ'))"
    "# host: $env:COMPUTERNAME"
    ''
) | Set-Content -Path $report

function Get-EnvValue([string]$Name) {
    $value = [Environment]::GetEnvironmentVariable($Name)
    if ($null -eq $value) { '' } else { $value }
}

function Add-Report([string]$Line) {
    Write-Host $Line
    Add-Content -Path $report -Value $Line
}

function Get-MissingProfile([string]$Prefix) {
    $missing = @()
    if (-not (Get-EnvValue "${Prefix}HOST")) { $missing += "${Prefix}HOST" }
    if (-not (Get-EnvValue "${Prefix}USER")) { $missing += "${Prefix}USER" }
    if (-not (Get-EnvValue "${Prefix}PASSWORD")) { $missing += "${Prefix}PASSWORD" }
    if (-not ((Get-EnvValue "${Prefix}DB") + (Get-EnvValue "${Prefix}DATABASE"))) {
        $missing += "${Prefix}DB or ${Prefix}DATABASE"
    }
    return $missing
}

function Invoke-Profile([string]$Prefix, [string]$Label, [string[]]$Extras) {
    $missing = Get-MissingProfile $Prefix
    if ($missing.Count -gt 0) {
        Add-Report ("SKIP  {0}: missing {1}" -f $Label, ($missing -join ', '))
        return 2
    }

    $hostName = Get-EnvValue "${Prefix}HOST"
    $port = Get-EnvValue "${Prefix}PORT"
    $db = Get-EnvValue "${Prefix}DB"
    $database = Get-EnvValue "${Prefix}DATABASE"
    $schema = Get-EnvValue "${Prefix}SCHEMA"

    $env:NZ_DEV_HOST = $hostName
    $env:NZ_DEV_PORT = if ($port) { $port } else { '5480' }
    $env:NZ_DEV_USER = Get-EnvValue "${Prefix}USER"
    $env:NZ_DEV_PASSWORD = Get-EnvValue "${Prefix}PASSWORD"
    if ($db) {
        $env:NZ_DEV_DB = $db
        Remove-Item Env:NZ_DEV_DATABASE -ErrorAction SilentlyContinue
    } else {
        $env:NZ_DEV_DATABASE = $database
        Remove-Item Env:NZ_DEV_DB -ErrorAction SilentlyContinue
    }
    if ($schema) {
        $env:NZ_DEV_SCHEMA = $schema
    } else {
        Remove-Item Env:NZ_DEV_SCHEMA -ErrorAction SilentlyContinue
    }

    $target = if ($db) { $db } else { $database }
    Add-Report ("==> {0} ({1}:{2}/{3})" -f $Label, $hostName, $env:NZ_DEV_PORT, $target)
    $log = Join-Path $out "$Label.log"
    $env:NZ_RUN_LIVE_TESTS = '1'
    # Cargo writes progress to stderr; Windows PowerShell treats native stderr
    # as a terminating error under ErrorActionPreference=Stop, so relax it while
    # the test process runs and rely on $LASTEXITCODE instead.
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    Push-Location $root
    try {
        & cargo test -p nz_rust --features 'compat,chrono' --test live_qualification -- `
            --ignored --nocapture --test-threads=1 @Extras *> $log
        $code = $LASTEXITCODE
    } finally {
        Pop-Location
        $ErrorActionPreference = $previousPreference
    }
    if ($code -eq 0) {
        Add-Report "PASS  $Label"
        return 0
    }
    Add-Report "FAIL  $Label (see $log)"
    return 1
}

$status = 0
try {
    $primaryStatus = Invoke-Profile 'NZ_DEV_' 'primary' $ExtraArgs
} catch {
    Add-Report "FAIL  primary: $_"
    $primaryStatus = 1
}
if ($primaryStatus -ne 0) { $status = 1 }

$secondaryConfigured = @(
    Get-EnvValue 'NZ_DEV2_HOST'
    Get-EnvValue 'NZ_DEV2_USER'
    Get-EnvValue 'NZ_DEV2_PASSWORD'
    Get-EnvValue 'NZ_DEV2_DB'
    Get-EnvValue 'NZ_DEV2_DATABASE'
) -join ''

if ($secondaryConfigured) {
    try {
        $secondaryStatus = Invoke-Profile 'NZ_DEV2_' 'secondary' $ExtraArgs
    } catch {
        Add-Report "FAIL  secondary: $_"
        $secondaryStatus = 1
    }
    if ($secondaryStatus -ne 0) { $status = 1 }
} elseif ($RequireTwo) {
    Add-Report 'FAIL  secondary: NZ_DEV2_* is required but not configured'
    $status = 1
} else {
    Add-Report 'SKIP  secondary: NZ_DEV2_* is not configured'
}

Add-Report ''
if ($status -eq 0) {
    Add-Report 'matrix passed'
} else {
    Add-Report 'matrix failed'
}
exit $status
