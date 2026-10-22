#!/usr/bin/env bash
# stop.sh — 停止 dev.sh 启动的进程组与 Redis 容器（只按记录的 PID 停止）。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RUN_DIR="${MYBOT_DEV_DIR:-$ROOT/.dev}"
PIDS="$RUN_DIR/pids"

if [ -f "$PIDS" ]; then
    # 逆序停止：Worker → Runtime → daprd。
    tac "$PIDS" | while read -r pid name; do
        [ -n "$pid" ] || continue
        if kill -0 "$pid" 2>/dev/null; then
            echo "   stop $name (pgid $pid)"
            kill -TERM -- "-$pid" 2>/dev/null || true
        fi
    done
    : > "$PIDS"
fi

docker stop mybot-redis > /dev/null 2>&1 || true
echo "==> dev services stopped"
