#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# dev-up.sh — starts the in-memory dev trio in the background:
#
#   mas-api          --dev-inmemory   (HTTP :8080, gRPC :50051)
#   mas-scheduler    --dev-inmemory   (ticks every MAS_SCHEDULER_INTERVAL_MS,
#                                      completion listener :8090)
#   mas-worker       --dev-inmemory   (echo consumer over InMemoryBroker)
#
# Every knob flows from config/development.toml (+ MAS_* env overrides).
# logs → .dev-logs/<process>.log; PIDs → .dev-logs/pids.
# Stops cleanly with scripts/dev-down.sh or Ctrl-C + `wait` in the caller.
#
# Usage: scripts/dev-up.sh [--fg]   (--fg runs all three in foreground jobs;
#                                   Ctrl-C kills the trio together)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f .cargo-env ]; then source .cargo-env; fi
export MAS_ENV="${MAS_ENV:-development}"

cargo build -p mas-api -p mas-scheduler-service -p mas-worker --quiet
mkdir -p .dev-logs
: > .dev-logs/pids

start() {
    local name="$1"; shift
    "$@" & local pid=$!
    echo "$pid $name" >> .dev-logs/pids
    echo "started $name (pid $pid) → .dev-logs/$name.log"
}

if [ "${1:-}" = "--fg" ]; then
    trap 'kill -TERM -- -$$ 2>/dev/null || true' INT TERM
fi

start mas-api \
    bash -c "exec target/debug/mas-api --dev-inmemory >> .dev-logs/mas-api.log 2>&1"
start mas-scheduler \
    bash -c "exec MAS_SCHEDULER_INTERVAL_MS=\${MAS_SCHEDULER_INTERVAL_MS:-1000} target/debug/mas-scheduler --dev-inmemory >> .dev-logs/mas-scheduler.log 2>&1"
start mas-worker \
    bash -c "exec target/debug/mas-worker --dev-inmemory >> .dev-logs/mas-worker.log 2>&1"

echo
echo "api health : curl -s http://127.0.0.1:8080/v1/health/ready"
echo "cli smoke  : target/debug/mas status"
echo
echo "stop with: scripts/dev-down.sh"
[ "${1:-}" = "--fg" ] && wait
