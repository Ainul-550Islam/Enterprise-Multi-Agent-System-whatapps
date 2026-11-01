#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# smoke.sh — post-deploy health gate against a RUNNING api (dev-up.sh or any
# real target). Uses the production mas-cli client against the wire contract,
# not curl: it proves the same path customers use.
#
#   BASE_URL (default http://127.0.0.1:8080)
#   MAS_TENANT_ID / MAS_ORGANIZATION_ID (expected to 401/400 when absent —
#   the readiness check is anonymous; scoped probes below are optional)
#
# Exits non-zero on any failed step.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f .cargo-env ]; then source .cargo-env; fi
cargo build -p mas-cli -p mas-api --quiet

BASE="${BASE_URL:-http://127.0.0.1:8080}"

echo "1) anonymous readiness (expects a healthy connectivity oracle)"
target/debug/mas --base-url "$BASE" status

echo
echo "2) scoped surface (skipped unless MAS_TENANT_ID + MAS_ORGANIZATION_ID set)"
if [ -n "${MAS_TENANT_ID:-}" ] && [ -n "${MAS_ORGANIZATION_ID:-}" ]; then
    target/debug/mas --base-url "$BASE" workflow list --limit 3
    target/debug/mas --base-url "$BASE" executions list --limit 3
    echo "scoped smoke PASSED"
else
    echo "set MAS_TENANT_ID + MAS_ORGANIZATION_ID (+ optional MAS_PROJECT_ID) to run scoped probes"
fi

echo
echo "smoke OK for $BASE"
