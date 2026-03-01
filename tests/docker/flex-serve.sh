#!/usr/bin/env bash
# flex-serve — starts worker + MCP server in background.
# Replaces systemd for non-systemd environments (Docker, containers, CI).
#
# Usage: flex-serve [--stop]

set -euo pipefail

PIDFILE_WORKER="$HOME/.flex/worker.pid"
PIDFILE_MCP="$HOME/.flex/mcp.pid"
LOG_WORKER="$HOME/.flex/logs/worker.log"
LOG_MCP="$HOME/.flex/logs/mcp.log"

mkdir -p "$HOME/.flex/logs"

_stop() {
    for name in worker mcp; do
        pidfile="$HOME/.flex/${name}.pid"
        if [ -f "$pidfile" ]; then
            pid=$(cat "$pidfile")
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid" && echo "  stopped flex-$name (pid $pid)"
            fi
            rm -f "$pidfile"
        fi
    done
}

if [ "${1:-}" = "--stop" ]; then
    _stop
    exit 0
fi

# Stop any existing instances
_stop 2>/dev/null || true

# Start worker
python3 -m flex.modules.claude_code.compile.worker --daemon \
    >> "$LOG_WORKER" 2>&1 &
echo $! > "$PIDFILE_WORKER"
echo "  [ok] flex-worker started (pid $(cat $PIDFILE_WORKER))"
echo "       log: $LOG_WORKER"

# Give worker a moment to initialize
sleep 1

# Start MCP server
python3 -m flex.mcp_server --http --port 7134 \
    >> "$LOG_MCP" 2>&1 &
echo $! > "$PIDFILE_MCP"
echo "  [ok] flex-mcp started (pid $(cat $PIDFILE_MCP)) → http://localhost:7134/mcp"
echo "       log: $LOG_MCP"

echo ""
echo "  Ready. Run: claude"
