#!/usr/bin/env bash
# stop.sh — stop all dev services.
set -euo pipefail

echo "==> Stopping dev services..."

# Kill runtime and workers.
pkill -f "mybot-runtime" 2>/dev/null || true
pkill -f "mybot-worker" 2>/dev/null || true

# Kill daprd.
pkill -f "daprd" 2>/dev/null || true

# Stop Redis container.
docker stop mybot-redis 2>/dev/null || true

echo "==> All dev services stopped."
