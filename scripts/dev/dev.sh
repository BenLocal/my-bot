#!/usr/bin/env bash
# dev.sh — 本地联调：Redis(docker) + Dapr sidecar + Runtime + N 个 fake Worker（plan T12）。
# 用法：./scripts/dev/dev.sh [worker 数，默认 2]；Ctrl-C 或 ./scripts/dev/stop.sh 停止。
# 只停止本脚本启动的进程（PID 记录在 $RUN_DIR/pids），不按名字模式 kill。
set -euo pipefail

NUM_WORKERS="${1:-2}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RUN_DIR="${MYBOT_DEV_DIR:-$ROOT/.dev}"
cd "$ROOT"
mkdir -p "$RUN_DIR/bin" "$RUN_DIR/artifacts"
: > "$RUN_DIR/pids"

cleanup() {
    echo "==> Stopping..."
    "$ROOT/scripts/dev/stop.sh" || true
}
trap cleanup EXIT INT TERM

# 后台启动并记录进程组（setsid 让子进程独立成组，stop.sh 按组停止）。
spawn() {
    local name="$1"; shift
    setsid "$@" > "$RUN_DIR/$name.log" 2>&1 &
    echo "$! $name" >> "$RUN_DIR/pids"
}

echo "==> Redis (docker: mybot-redis)"
if ! docker ps --format '{{.Names}}' | grep -qx mybot-redis; then
    docker run -d --rm --name mybot-redis -p 127.0.0.1:6379:6379 redis:7-alpine redis-server --appendonly yes > /dev/null
fi

echo "==> Building"
go build -o "$RUN_DIR/bin/mybot-runtime" ./cmd/mybot-runtime
go build -o "$RUN_DIR/bin/mybot-worker" ./cmd/mybot-worker

echo "==> Dapr sidecar + Runtime"
spawn daprd dapr run --app-id mybot-runtime --app-port 8080 --app-protocol http \
    --dapr-http-port 3500 --dapr-grpc-port 50001 \
    --resources-path deploy/dapr/local --config deploy/dapr/local/config.yaml
sleep 3
spawn runtime "$RUN_DIR/bin/mybot-runtime" --port 8080 --dapr-port 50001 \
    --roles config/roles.fake.yaml --roles-dir .
sleep 2

echo "==> $NUM_WORKERS fake Workers"
for i in $(seq 1 "$NUM_WORKERS"); do
    port=$((9100 + i))
    spawn "worker-w$i" "$RUN_DIR/bin/mybot-worker" --addr ":$port" --worker-id "w$i" \
        --executor fake --account default --capacity 2 \
        --store-addr localhost:50001 --store-name statestore --pubsub-name pubsub \
        --artifact-dir "$RUN_DIR/artifacts" \
        --fake-script '{"delay":"2s","result":"{\"summary\":\"ok\",\"done\":true}"}'
    echo "   w$i → :$port"
done

cat <<INFO

  my-bot dev environment is ready
  API:   http://localhost:8080/v1
  Logs:  $RUN_DIR/*.log

  curl -s -X POST localhost:8080/v1/tasks -H 'Idempotency-Key: demo-1' \\
       -d '{"objective":"hello","members":["assistant"]}'
  curl -s localhost:8080/v1/goals?status=all
  curl -s localhost:8080/v1/workers

Press Ctrl-C to stop.
INFO
wait
