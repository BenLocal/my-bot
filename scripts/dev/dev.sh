#!/usr/bin/env bash
# dev.sh — one-click local dev: Redis + Dapr + Runtime + N fake Workers.
# Usage: ./scripts/dev/dev.sh [num_workers]
set -euo pipefail

NUM_WORKERS="${1:-2}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export GOCACHE=/tmp/go-cache
export GOMODCACHE=/tmp/go-mod-cache
export GOPATH=/tmp/go-path

WORKER_PIDS=()

cleanup() {
    echo "==> Cleaning up..."
    for pid in "${WORKER_PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    kill "$RUNTIME_PID" 2>/dev/null || true
    daprd_pid=$(pgrep -f "daprd" || true)
    if [ -n "$daprd_pid" ]; then
        kill "$daprd_pid" 2>/dev/null || true
    fi
    redis_pid=$(pgrep -f "redis-server" || true)
    if [ -n "$redis_pid" ]; then
        kill "$redis_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo "==> Starting Redis..."
if ! pgrep -f "redis-server" > /dev/null; then
    docker run -d --name mybot-redis -p 6379:6379 redis --appendonly yes
    sleep 1
fi

echo "==> Starting Dapr..."
if ! pgrep -f "daprd" > /dev/null; then
    daprd --dapr-http-port 3500 --dapr-grpc-port 50001 \
        --components-path deploy/dapr/local \
        --config-file deploy/dapr/local/config.yaml \
        --app-port 8080 \
        --app-protocol http \
        > /tmp/mybot-daprd.log 2>&1 &
    sleep 3
fi

echo "==> Building..."
go build -o /tmp/mybot-runtime ./cmd/mybot-runtime
go build -o /tmp/mybot-worker ./cmd/mybot-worker

echo "==> Starting Runtime..."
/tmp/mybot-runtime --port 8080 --actor-port 8081 --dapr-port 50001 --global-max 2 &
RUNTIME_PID=$!
sleep 2

echo "==> Starting $NUM_WORKERS fake Workers..."
for i in $(seq 1 "$NUM_WORKERS"); do
    /tmp/mybot-worker --addr ":9${i}01" --executor fake --account default --capacity 2 \
        > /tmp/mybot-worker-${i}.log 2>&1 &
    WORKER_PIDS+=("$!")
    echo "   Worker $i → :9${i}01"
done

echo ""
echo "======================================"
echo "  my-bot dev environment is ready"
echo "  API:      http://localhost:8080"
echo "  Dapr:     http://localhost:3500"
echo "  Workers:  :9101, :9201, ..."
echo "  Logs:     /tmp/mybot-*.log"
echo "======================================"
echo ""
echo "Press Ctrl-C to stop."

wait
