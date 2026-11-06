$ErrorActionPreference = 'Stop'
$pidFile = Join-Path $PSScriptRoot 'data\run_local.pid'
$backendPort = 18199

if (-not (Test-Path -LiteralPath $pidFile -PathType Leaf)) {
    Write-Host 'Not running.'
    exit 0
}
$runningPid = [int](Get-Content -LiteralPath $pidFile -Raw).Trim()
$running = Get-Process -Id $runningPid -ErrorAction SilentlyContinue
if (-not $running -or $running.ProcessName -notlike 'python*') {
    Remove-Item -LiteralPath $pidFile -Force
    Write-Host 'Not running (removed stale PID file).'
    exit 0
}

# Graceful path: the backend disconnects BLE, closes sessions and the history DB, then exits 0.
try {
    Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:$backendPort/api/shutdown" -TimeoutSec 5 | Out-Null
} catch {
    Write-Host "Shutdown request failed: $($_.Exception.Message)"
}

$exitCode = 0
if ($running.WaitForExit(20000)) {
    Write-Host "Stopped (PID $runningPid)."
} else {
    # A forced stop skips the BLE disconnect and session cleanup, so report it as a failure.
    $exitCode = 1
    & taskkill.exe /PID $runningPid /T /F | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Graceful stop timed out; force-killed PID $runningPid and its children."
    } else {
        Write-Host "Graceful stop timed out and taskkill failed (exit $LASTEXITCODE); PID $runningPid may still be running."
    }
}
if ($running.HasExited -and (Test-Path -LiteralPath $pidFile -PathType Leaf)) {
    Remove-Item -LiteralPath $pidFile -Force
}
exit $exitCode
