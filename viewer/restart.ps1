# restart.ps1 — kill any running viewer, then start fresh
# Usage: .\viewer\restart.ps1

$Port = 5050
$RepoDir = Split-Path -Parent $PSScriptRoot
$VenvPython = "E:\SEC_projects\.venv-sec\Scripts\python.exe"

# Kill any process already on the port
$conns = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue
if ($conns) {
    $pids = $conns.OwningProcess | Select-Object -Unique
    foreach ($pid in $pids) {
        Write-Host "Killing existing process on port $Port (PID $pid)"
        Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
    }
}

$Url = "http://localhost:$Port"
Write-Host "Starting viewer at $Url"

Push-Location $RepoDir
$logPath = Join-Path $RepoDir "viewer\viewer.log"
Start-Process -FilePath $VenvPython -ArgumentList "viewer/app.py" `
    -RedirectStandardOutput $logPath -RedirectStandardError "$logPath.err" -NoNewWindow -PassThru | ForEach-Object {
    Write-Host "Server PID: $($_.Id)  |  Logs: viewer\viewer.log"
}
Pop-Location

# Wait up to 5s for the port to open
for ($i = 0; $i -lt 10; $i++) {
    Start-Sleep -Milliseconds 500
    if (Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue) { break }
}

Write-Host "Open: $Url"
Start-Process $Url
