param(
    [ValidateRange(1, 65535)]
    [int]$WebPort = 8199,
    # Run without a console window; stop with stop.ps1
    [switch]$Background
)

$ErrorActionPreference = 'Stop'
$venv = Join-Path $PSScriptRoot '.venv'
$venvPython = Join-Path $venv 'Scripts\python.exe'
$requirements = Join-Path $PSScriptRoot 'backend\requirements.txt'
$ready = Join-Path $venv 'requirements.sha256'
$runLocal = Join-Path $PSScriptRoot 'run_local.py'
$pidFile = Join-Path $PSScriptRoot 'data\run_local.pid'

if (-not (Test-Path -LiteralPath $venvPython -PathType Leaf)) {
    & python -c 'import sys; sys.exit(sys.version_info < (3, 10))'
    if ($LASTEXITCODE -ne 0) { throw 'Python 3.10 or later is required.' }
    & python -m venv $venv
    if ($LASTEXITCODE -ne 0) { throw 'Could not create the virtual environment.' }
}

$hasher = [System.Security.Cryptography.SHA256]::Create()
try {
    $requirementsHash = [BitConverter]::ToString($hasher.ComputeHash([IO.File]::ReadAllBytes($requirements)))
} finally {
    $hasher.Dispose()
}
if ((-not (Test-Path -LiteralPath $ready -PathType Leaf)) -or
    ((Get-Content -LiteralPath $ready -Raw).Trim() -ne $requirementsHash)) {
    & $venvPython -m pip --disable-pip-version-check --no-cache-dir install -r $requirements
    if ($LASTEXITCODE -ne 0) { throw 'Could not install Python dependencies.' }
    Set-Content -LiteralPath $ready -Value $requirementsHash -Encoding ASCII
}

if (Test-Path -LiteralPath $pidFile -PathType Leaf) {
    $runningPid = [int](Get-Content -LiteralPath $pidFile -Raw).Trim()
    $running = Get-Process -Id $runningPid -ErrorAction SilentlyContinue
    if ($running -and $running.ProcessName -like 'python*') {
        Write-Host "Already running (PID $runningPid). Run .\stop.ps1 first to restart."
        exit 0
    }
}

if (-not $Background) {
    & $venvPython $runLocal --web-port $WebPort
    exit $LASTEXITCODE
}

$venvPythonw = Join-Path $venv 'Scripts\pythonw.exe'
Start-Process -FilePath $venvPythonw -WorkingDirectory $PSScriptRoot `
    -ArgumentList @("`"$runLocal`"", '--background', '--web-port', $WebPort) | Out-Null

$healthUrl = "http://127.0.0.1:$WebPort/api/health"
$deadline = (Get-Date).AddSeconds(15)
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Milliseconds 500
    try {
        Invoke-RestMethod -Uri $healthUrl -TimeoutSec 2 | Out-Null
        Write-Host "Started successfully: http://127.0.0.1:$WebPort/"
        Write-Host "Logs: data\server.log (backend), data\console.log (process output). Stop: .\stop.ps1"
        exit 0
    } catch {
    }
}
Write-Host "Not ready after 15 seconds. Check data\console.log and data\server.log."
exit 1
