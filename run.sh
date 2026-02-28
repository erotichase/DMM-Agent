#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AGENT="$SCRIPT_DIR/agent.py"
VENV="$SCRIPT_DIR/.venv"
CONFIG="$SCRIPT_DIR/config.json"

# ─── Dev 用户表 (./run.sh a|b|c|d|e) ───
_dev_lookup() {
    case "$1" in
        a) TG_ID=111111111; USER_NAME="Dev User A" ;;
        b) TG_ID=222222222; USER_NAME="Dev User B" ;;
        c) TG_ID=333333333; USER_NAME="Dev User C" ;;
        d) TG_ID=444444444; USER_NAME="Dev User D" ;;
        e) TG_ID=555555555; USER_NAME="Dev User E" ;;
        *) return 1 ;;
    esac
}

# 处理 dev 用户参数
DEV_USER="${1:-}"
if [ -n "$DEV_USER" ]; then
    DEV_USER=$(echo "$DEV_USER" | tr '[:upper:]' '[:lower:]')
    if ! _dev_lookup "$DEV_USER"; then
        echo "用法: ./run.sh [a|b|c|d|e]"
        echo ""
        for key in a b c d e; do
            _dev_lookup "$key"
            echo "  $key  →  $USER_NAME (tg_id=$TG_ID)"
        done
        exit 1
    fi

    # 用 python 更新 config.json（保留其他字段）
    python3 -c "
import json, sys
with open('$CONFIG', 'r') as f:
    cfg = json.load(f)
cfg['DEV'] = True
cfg['DEV_TG_ID'] = $TG_ID
cfg['DEV_USER_NAME'] = '$USER_NAME'
cfg['DEVICE_TOKEN'] = ''
with open('$CONFIG', 'w') as f:
    json.dump(cfg, f, indent=4, ensure_ascii=False)
    f.write('\n')
"
    echo "[dev] 切换到 $USER_NAME (tg_id=$TG_ID)，已清空 DEVICE_TOKEN"
fi

# Auto-create venv if missing
if [ ! -d "$VENV" ]; then
    echo "[setup] Creating venv..."
    python3 -m venv "$VENV"
    "$VENV/bin/pip" install -q -r "$SCRIPT_DIR/requirements.txt"
    echo "[setup] Done"
fi

# Kill existing agent if running (PID recorded in lock file)
LOCK_FILE="${DMM_AGENT_LOCK:-/tmp/dmm-agent.lock}"
if [ -f "$LOCK_FILE" ]; then
    OLD_PID=$(cat "$LOCK_FILE" 2>/dev/null | tr -d '[:space:]')
    if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
        echo "[agent] Stopping old agent (PID $OLD_PID)..."
        kill "$OLD_PID" 2>/dev/null || true
        for i in $(seq 1 50); do
            if ! kill -0 "$OLD_PID" 2>/dev/null; then
                break
            fi
            sleep 0.1
        done
        if kill -0 "$OLD_PID" 2>/dev/null; then
            echo "[agent] Force killing old agent (PID $OLD_PID)"
            kill -9 "$OLD_PID" 2>/dev/null || true
            sleep 0.5
        fi
        echo "[agent] Old agent stopped"
    fi
    rm -f "$LOCK_FILE"
fi

# Prevent websockets from picking up system proxy
unset ALL_PROXY all_proxy HTTPS_PROXY https_proxy HTTP_PROXY http_proxy

exec "$VENV/bin/python" "$AGENT"
