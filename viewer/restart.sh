#!/usr/bin/env bash
# restart.sh — kill any running viewer, then start fresh
# Usage: ./viewer/restart.sh [MART_DB=/path/to/db]

PORT=5050
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

# Kill any process already on the port
PIDS=$(lsof -ti :"$PORT" 2>/dev/null)
if [ -n "$PIDS" ]; then
    echo "Killing existing process(es) on port $PORT: $PIDS"
    kill -9 $PIDS
fi

# Determine WSL host IP for the browser URL
HOST_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
URL="http://${HOST_IP}:${PORT}"

echo "Starting viewer at $URL"

# Launch the server in background, log to /tmp/viewer.log
cd "$REPO_DIR"
conda run -n tf python viewer/app.py &>"$REPO_DIR/viewer/viewer.log" &
SERVER_PID=$!

# Wait up to 5s for the port to open
for i in $(seq 1 10); do
    sleep 0.5
    if lsof -i :"$PORT" &>/dev/null; then
        break
    fi
done

echo "Server PID: $SERVER_PID  |  Logs: viewer/viewer.log"
echo "Open: $URL"

# Try to open the browser from Windows side (WSL2)
if command -v cmd.exe &>/dev/null; then
    cmd.exe /c start "$URL" 2>/dev/null
elif command -v explorer.exe &>/dev/null; then
    explorer.exe "$URL" 2>/dev/null
fi
