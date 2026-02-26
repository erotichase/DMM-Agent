#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AGENT="$SCRIPT_DIR/agent.py"
VENV="$SCRIPT_DIR/.venv"

# Auto-create venv if missing
if [ ! -d "$VENV" ]; then
    echo "[setup] Creating venv..."
    python3 -m venv "$VENV"
    "$VENV/bin/pip" install -q -r "$SCRIPT_DIR/requirements.txt"
    echo "[setup] Done"
fi

# Check if DEVICE_TOKEN is configured
TOKEN=$(grep '^DEVICE_TOKEN' "$AGENT" | head -1 | sed 's/.*= *"\(.*\)".*/\1/')

if [ -z "$TOKEN" ]; then
    echo "=========================================="
    echo "  首次运行 — 启动设备绑定流程"
    echo "  Agent 将生成 Telegram 链接"
    echo "  请在 Telegram 中点击链接完成绑定"
    echo "=========================================="
else
    MASKED="${TOKEN:0:4}***${TOKEN: -4}"
    echo "[agent] Token: $MASKED"
fi

# Kill existing agent if running (PID recorded in lock file)
LOCK_FILE="${DMM_AGENT_LOCK:-/tmp/dmm-agent.lock}"
if [ -f "$LOCK_FILE" ]; then
    OLD_PID=$(cat "$LOCK_FILE" 2>/dev/null | tr -d '[:space:]')
    if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
        echo "[agent] Stopping old agent (PID $OLD_PID)..."
        kill "$OLD_PID" 2>/dev/null || true
        # Wait up to 5 seconds for graceful shutdown
        for i in $(seq 1 50); do
            if ! kill -0 "$OLD_PID" 2>/dev/null; then
                break
            fi
            sleep 0.1
        done
        # Force kill if still alive
        if kill -0 "$OLD_PID" 2>/dev/null; then
            echo "[agent] Force killing old agent (PID $OLD_PID)"
            kill -9 "$OLD_PID" 2>/dev/null || true
            sleep 0.5
        fi
        echo "[agent] Old agent stopped"
    fi
    # Remove stale lock file so new agent can acquire it
    rm -f "$LOCK_FILE"
fi

# Prevent websockets from picking up system proxy
unset ALL_PROXY all_proxy HTTPS_PROXY https_proxy HTTP_PROXY http_proxy

exec "$VENV/bin/python" "$AGENT"
