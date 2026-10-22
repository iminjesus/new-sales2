@echo off
setlocal enabledelayedexpansion

REM batch_watchdog.bat (Oct-23 user ask)
REM
REM Task Scheduler runs this every 10 minutes.  Three jobs:
REM  1. If no pythonw.exe is running batch_orchestrator.py, relaunch it.
REM  2. If pythonw.exe IS running but the heartbeat_at value in
REM     batch_status is > HEARTBEAT_STALE_MIN minutes old, the process
REM     is hung — kill it and relaunch.
REM  3. (Catch-all) if start_batch.bat exits with EXIT_STEP_TIMEOUT (2)
REM     or EXIT_UNCAUGHT (3), Windows watchdog's next tick re-runs
REM     start_batch.bat.
REM
REM Logs: batch_watchdog.log (same folder).

cd /d "%~dp0"

set STALE_MIN=10
set LOG=batch_watchdog.log

for /f "tokens=1-5 delims=/:. " %%a in ("%date% %time%") do set TS=%%c-%%b-%%a %%d:%%e

REM --- Step 1: is pythonw.exe running batch_orchestrator.py?
set FOUND=
for /f "skip=3 tokens=1,2" %%A in ('tasklist /FI "IMAGENAME eq pythonw.exe" /FO TABLE 2^>nul') do (
    if /i "%%A"=="pythonw.exe" set FOUND=%%B
)

if not defined FOUND (
    echo [%TS%] no pythonw.exe — launching start_batch.bat >> %LOG%
    call start_batch.bat >> %LOG% 2>&1
    exit /b 0
)

REM --- Step 2: process is up — check heartbeat freshness via a tiny
REM     Python one-liner so we don't need a separate MySQL client.
REM     Writes "FRESH" or "STALE <minutes>" to a scratch file so the
REM     batch can decide.
set HBCHK=%TEMP%\batch_hb_check.txt
if exist "%HBCHK%" del /q "%HBCHK%"
pythonw.exe -c "import os,sys; sys.path.insert(0, r'%~dp0'); from batch_orchestrator import status_get, _now_syd; from datetime import datetime, timedelta; v = status_get('heartbeat_at') or ''; now = _now_syd().replace(tzinfo=None); open(r'%HBCHK%','w').write('MISSING' if not v else ('FRESH' if (now - datetime.strptime(v[:19], '%%Y-%%m-%%d %%H:%%M:%%S')) < timedelta(minutes=%STALE_MIN%) else 'STALE'))" 2>>%LOG%

set HB=
if exist "%HBCHK%" set /p HB=<"%HBCHK%"

if "%HB%"=="FRESH" (
    REM Healthy — nothing to do.
    exit /b 0
)
if "%HB%"=="MISSING" (
    echo [%TS%] heartbeat_at missing — orchestrator may still be starting; leaving it alone >> %LOG%
    exit /b 0
)

REM Not fresh and not missing — treat as hung.
echo [%TS%] heartbeat STALE (> %STALE_MIN% min) — killing PID %FOUND% and relaunching >> %LOG%
taskkill /F /PID %FOUND% >> %LOG% 2>&1
timeout /t 3 /nobreak >nul
call start_batch.bat >> %LOG% 2>&1

endlocal
