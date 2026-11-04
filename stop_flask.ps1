# stop_flask.ps1 -- force-kill the Flask dev server on port 5000.
#
# Nov-04 user ask: Ctrl+C on PowerShell sometimes fails to break
# Werkzeug's accept loop.  This script finds whichever process is
# bound to :5000 and kills it, so a stuck Flask can always be cleared
# with a single command even when the keyboard shortcut is swallowed.
#
# Usage:
#   .\stop_flask.ps1              # kill whatever is on :5000
#   .\stop_flask.ps1 -Port 5001   # kill a different port
#
# After stopping, relaunch with:
#   python app.py

param(
    [int]$Port = 5000
)

$ErrorActionPreference = 'SilentlyContinue'

$conns = Get-NetTCPConnection -LocalPort $Port -State Listen
if (-not $conns) {
    Write-Host "[stop_flask] nothing listening on :$Port"
    exit 0
}

$pids = $conns | Select-Object -ExpandProperty OwningProcess | Sort-Object -Unique
foreach ($pid in $pids) {
    $proc = Get-Process -Id $pid -ErrorAction SilentlyContinue
    if ($proc) {
        Write-Host "[stop_flask] killing PID $pid ($($proc.ProcessName))"
        Stop-Process -Id $pid -Force
    } else {
        Write-Host "[stop_flask] PID $pid already gone"
    }
}

# Short wait so the OS releases the port before the operator relaunches.
Start-Sleep -Milliseconds 500

$still = Get-NetTCPConnection -LocalPort $Port -State Listen
if ($still) {
    Write-Host "[stop_flask] WARNING: port $Port still LISTENING after kill"
    exit 1
}
Write-Host "[stop_flask] port $Port released, safe to restart"
