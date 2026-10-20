@echo off
setlocal enabledelayedexpansion

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

REM Oct-20 part 2: if a prior pythonw.exe just died but Windows hasn't
REM yet released its exclusive handle on batch_orchestrator.log, the
REM `>> batch_orchestrator.log` redirect below fails noisily with
REM "The process cannot access the file because it is being used by
REM another process".  Wait up to 10 seconds for the handle to clear
REM before launching.  (type nul >> file) is a cheap append test that
REM writes zero bytes; its errorlevel is 0 only when the file is
REM writable.  2>nul swallows the probe's own complaint while we wait.
set LOGREADY=
for /L %%I in (1,1,10) do (
    if not defined LOGREADY (
        (type nul >> batch_orchestrator.log) 2>nul
        if !errorlevel! == 0 set LOGREADY=1
        if not defined LOGREADY timeout /t 1 /nobreak >nul
    )
)
if not defined LOGREADY (
    echo [start_batch] batch_orchestrator.log is still locked after 10 s.
    echo [start_batch] Something else is holding it. Kill lingering
    echo [start_batch] pythonw.exe processes or close a Get-Content -Wait
    echo [start_batch] tail, then try again.
    endlocal
    exit /b 1
)

REM Pick pythonw.exe (no console) so no stray cmd window stays open.
REM If pythonw isn't on PATH, fall back to python but still detach.
where pythonw.exe >nul 2>&1
if %ERRORLEVEL%==0 (
    start "hkau-batch" /B pythonw.exe batch_orchestrator.py >> batch_orchestrator.log 2>&1
) else (
    start "hkau-batch" /B python.exe batch_orchestrator.py >> batch_orchestrator.log 2>&1
)

REM Give the orchestrator a few seconds to write its PID file.  The
REM previous 2-second wait was tight — Python's import of pywin32 +
REM mysql.connector can take a beat on cold start, so if we checked
REM too early the PID file wasn't there yet and we printed a false
REM "launch may have failed" warning.
timeout /t 4 /nobreak >nul

if exist batch_orchestrator.pid (
    REM Delayed expansion (!PID!) is REQUIRED here — inside a parens
    REM block %PID% expands at PARSE time, before `set /p PID=` has
    REM run, so the echo would always show the old (empty) value.
    set /p PID=<batch_orchestrator.pid
    echo [start_batch] batch_orchestrator running as PID !PID!.
    echo [start_batch] Logs: %~dp0batch_orchestrator.log
    echo [start_batch] Stop with: stop_batch.bat
) else (
    echo [start_batch] WARNING: batch_orchestrator.pid not found — launch may have failed.
    echo [start_batch] Check %~dp0batch_orchestrator.log for errors.
)

endlocal
