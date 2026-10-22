@echo off
setlocal

REM install_batch_scheduler.bat (Oct-23 user ask)
REM
REM Idempotent one-shot installer.  Registers two Windows Task
REM Scheduler entries so the batch orchestrator survives:
REM
REM   1. SPRF-Batch-Startup
REM        trigger: At system startup
REM        action : start_batch.bat
REM        → PC reboots overnight / Windows Update restart / power-cut
REM          recovery all auto-start the orchestrator again.
REM
REM   2. SPRF-Batch-Watchdog
REM        trigger: Every 10 minutes (indefinite)
REM        action : batch_watchdog.bat
REM        → kills + relaunches the orchestrator when it's not running,
REM          OR when its heartbeat (batch_status.heartbeat_at) is more
REM          than 10 minutes stale (= process is hung on a SAP /
REM          Excel popup).
REM
REM Run this ONCE from an elevated cmd.exe:
REM    install_batch_scheduler.bat
REM Re-running is safe — schtasks /CREATE /F replaces an existing
REM entry under the same name.

cd /d "%~dp0"
set STARTUP_BAT=%~dp0start_batch.bat
set WATCHDOG_BAT=%~dp0batch_watchdog.bat

echo [install] registering "SPRF-Batch-Startup" (at system startup)
schtasks /CREATE /F /TN "SPRF-Batch-Startup" /SC ONSTART ^
  /RL HIGHEST /RU "SYSTEM" ^
  /TR "cmd /c \"\"%STARTUP_BAT%\"\"" ^
  >nul
if errorlevel 1 (
  echo [install] startup task registration FAILED — are you running
  echo           this from an elevated ^(Run as administrator^) cmd.exe?
  endlocal
  exit /b 1
)

echo [install] registering "SPRF-Batch-Watchdog" (every 10 minutes)
schtasks /CREATE /F /TN "SPRF-Batch-Watchdog" /SC MINUTE /MO 10 ^
  /RL HIGHEST /RU "SYSTEM" ^
  /TR "cmd /c \"\"%WATCHDOG_BAT%\"\"" ^
  >nul
if errorlevel 1 (
  echo [install] watchdog task registration FAILED — are you running
  echo           this from an elevated ^(Run as administrator^) cmd.exe?
  endlocal
  exit /b 1
)

echo.
echo [install] Both tasks registered.  Verify with:
echo             schtasks /QUERY /TN SPRF-Batch-Startup
echo             schtasks /QUERY /TN SPRF-Batch-Watchdog
echo           Remove later with:
echo             schtasks /DELETE /F /TN SPRF-Batch-Startup
echo             schtasks /DELETE /F /TN SPRF-Batch-Watchdog
endlocal
