@echo off
setlocal enabledelayedexpansion

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
REM
REM Oct-20 part 2: enable delayed expansion so !PID! inside the
REM `if exist (...)` block reads the value that `set /p PID=` just
REM assigned.  Without this, %PID% expanded at PARSE time (before
REM `set /p` ran), echoed empty, and taskkill killed nothing while
REM reporting "stopped."

cd /d "%~dp0"

if exist batch_orchestrator.pid (
    set /p PID=<batch_orchestrator.pid
    if "!PID!" == "" (
        echo [stop_batch] pid file is empty — falling back to name match.
        goto NAME_MATCH
    )
    echo [stop_batch] killing PID !PID! ...
    taskkill /PID !PID! /F /T >nul 2>&1
    if !errorlevel! == 0 (
        echo [stop_batch] stopped.
    ) else (
        echo [stop_batch] taskkill failed; falling back to name match.
        goto NAME_MATCH
    )
    REM Also kill any orphaned Excel / Office Activation Wizard the
    REM orchestrator may have left on screen.  Harmless no-op if none.
    taskkill /F /IM EXCEL.EXE /T >nul 2>&1
    taskkill /F /IM OSPPREARM.EXE >nul 2>&1
    taskkill /F /IM msoia.exe >nul 2>&1
    del batch_orchestrator.pid >nul 2>&1
    endlocal
    goto :EOF
)

:NAME_MATCH
echo [stop_batch] no PID file — scanning for pythonw.exe running batch_orchestrator.py ...
wmic process where "name='pythonw.exe' and CommandLine like '%%batch_orchestrator.py%%'" delete >nul 2>&1
wmic process where "name='python.exe'  and CommandLine like '%%batch_orchestrator.py%%'" delete >nul 2>&1
taskkill /F /IM EXCEL.EXE /T >nul 2>&1
taskkill /F /IM OSPPREARM.EXE >nul 2>&1
taskkill /F /IM msoia.exe >nul 2>&1
del batch_orchestrator.pid >nul 2>&1
echo [stop_batch] done.
endlocal
