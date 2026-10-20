@echo off
REM stop_batch.bat — kill the background batch_orchestrator.
REM Reads batch_orchestrator.pid, taskkills it, deletes the pid file.
REM If the pid file is missing (manual start etc.), falls back to
REM killing every pythonw.exe whose command line contains
REM batch_orchestrator.py.
REM
REM Oct-20: batch_orchestrator now imports sapcrawling_batch /
REM sapcrawling_sales1_batch (batch-only forks).  Nothing to change
REM here — this .bat only kills batch_orchestrator.py, which is
REM still the top-level process.

cd /d "%~dp0"

if exist batch_orchestrator.pid (
    set /p PID=<batch_orchestrator.pid
    echo [stop_batch] killing PID %PID% ...
    taskkill /PID %PID% /F >nul 2>&1
    if %ERRORLEVEL%==0 (
        echo [stop_batch] stopped.
    ) else (
        echo [stop_batch] taskkill failed; falling back to name match.
        goto NAME_MATCH
    )
    del batch_orchestrator.pid >nul 2>&1
    goto :EOF
)

:NAME_MATCH
echo [stop_batch] no PID file — scanning for pythonw.exe running batch_orchestrator.py ...
wmic process where "name='pythonw.exe' and CommandLine like '%%batch_orchestrator.py%%'" delete >nul 2>&1
wmic process where "name='python.exe'  and CommandLine like '%%batch_orchestrator.py%%'" delete >nul 2>&1
echo [stop_batch] done.
