@echo off
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start.ps1" -Background
if errorlevel 1 (
    echo.
    echo Start failed. Press any key to close.
    pause >nul
    exit /b 1
)
rem ~1 second: ping waits 1s between its 2 echo requests
ping -n 2 127.0.0.1 >nul
