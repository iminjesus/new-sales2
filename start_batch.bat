@echo off
REM start_batch.bat — launch batch_orchestrator.py in the background.
REM Survives this window closing because we use pythonw (no console)
REM and `start /B`.  Logs go to batch_orchestrator.log; the PID of the
REM background process is written into batch_orchestrator.pid for the
REM stop_batch.bat script.

cd /d "%~dp0"

REM Oct-16: force UTF-8 for stdout / stderr so Unicode characters in
REM any imported sub-script (sapcrawling_batch.py's '──' box drawing,
REM sapcrawling_sales1_batch.py's '→' arrow, etc.) can no longer
REM trigger UnicodeEncodeError on this Windows box's cp1252 default.
REM errors is set to 'replace' so stray chars become '?' instead of
REM crashing.
REM
REM Oct-20: batch_orchestrator now imports sapcrawling_batch /
REM sapcrawling_sales1_batch (batch-only forks) instead of the
REM manually-runnable sapcrawling / sapcrawling_sales1.  The .bat
REM itself still only launches batch_orchestrator.py — the fork
REM selection happens inside batch_orchestrator.py.
set PYTHONIOENCODING=utf-8:replace
set PYTHONUTF8=1

REM Pick pythonw.exe (no console) so no stray cmd window stays open.
REM If pythonw isn't on PATH, fall back to python but still detach.
where pythonw.exe >nul 2>&1
if %ERRORLEVEL%==0 (
    start "hkau-batch" /B pythonw.exe batch_orchestrator.py >> batch_orchestrator.log 2>&1
) else (
    start "hkau-batch" /B python.exe batch_orchestrator.py >> batch_orchestrator.log 2>&1
)

REM Give the orchestrator a second to write its PID file.
timeout /t 2 /nobreak >nul

if exist batch_orchestrator.pid (
    set /p PID=<batch_orchestrator.pid
    echo [start_batch] batch_orchestrator running as PID %PID%.
    echo [start_batch] Logs: %~dp0batch_orchestrator.log
    echo [start_batch] Stop with: stop_batch.bat
) else (
    echo [start_batch] WARNING: batch_orchestrator.pid not found — launch may have failed.
    echo [start_batch] Check %~dp0batch_orchestrator.log for errors.
)
