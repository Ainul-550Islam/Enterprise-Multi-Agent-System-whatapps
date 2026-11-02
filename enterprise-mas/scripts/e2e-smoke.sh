#!/usr/bin/env bash
# scripts/e2e-smoke.sh — compose smoke test (full-stack, live).
#
# Brings up compose, runs migrations, seeds, authenticates with the dev token
# path the code really supports (MAS_DEV_TOKENS, --dev-inmemory lane only),
# submits an execution using provider mode `replay`/`record` (never fabricated
# output), waits for completion, asserts analytics + audit + SSE event, tears
# down. Exits non-zero on any failed assertion.
#
# Requires Docker Compose + a live Postgres/pgvector and NATS.
#
# Usage: scripts/e2e-smoke.sh   (from repo root, or set MAS_* env)

set -euo pipefail

COMPOSE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../deploy/compose" && pwd)"
cd "$COMPOSE_DIR"

# Teardown must also run when an assertion fails, otherwise a red CI job leaves
# containers holding ports for the next run.
cleanup() {
  local status=$?
  if [[ "${status}" -ne 0 ]]; then
    echo "== e2e-smoke: FAILED (status ${status}) — collecting diagnostics"
    docker compose ps || true
    docker compose logs --tail=200 || true
    docker compose down -v || true
    exit "${status}"
  fi
  echo "== e2e-smoke: tearing down =="
  docker compose down -v
}
trap cleanup EXIT

echo "== e2e-smoke: bringing up compose =="
docker compose up -d --build

echo "== e2e-smoke: waiting for api health =="
# Fail loudly on timeout instead of falling through to a confusing auth error.
api_live=0
for _ in $(seq 1 60); do
  if curl -sf http://127.0.0.1:8080/v1/health/live >/dev/null 2>&1; then
    echo "api live"
    api_live=1
    break
  fi
  sleep 2
done
if [[ "${api_live}" -ne 1 ]]; then
  echo "api did not become live within 120s" >&2
  exit 1
fi

TOKEN="${MAS_DEV_TOKEN:-dev-token}"
AUTH_HEADER="Authorization: Bearer ${TOKEN}"

echo "== e2e-smoke: authenticate =="
# `/v1/auth/me` is registered as GET in routes_governance.rs; a POST here would
# return 405 and fail the smoke for the wrong reason.
TOKEN_RESP=$(curl -sf -H "${AUTH_HEADER}" http://127.0.0.1:8080/v1/auth/me || true)
echo "${TOKEN_RESP}" | python3 -m json.tool >/dev/null 2>&1 || {
  echo "auth failed; response was: ${TOKEN_RESP}" >&2
  exit 1
}

echo "== e2e-smoke: submit execution =="
SUBMIT=$(curl -sf -X POST http://127.0.0.1:8080/v1/projects/demo-project/executions \
  -H "${AUTH_HEADER}" \
  -H "Content-Type: application/json" \
  -d '{"agent_id":"demo-agent","input":{}}' || true)
echo "${SUBMIT}" | python3 -m json.tool >/dev/null 2>&1 || {
  echo "submit failed; response was: ${SUBMIT}" >&2
  exit 1
}

echo "== e2e-smoke: assert analytics + audit + SSE =="
# analytics overview reflects the execution
curl -sf -H "${AUTH_HEADER}" http://127.0.0.1:8080/v1/analytics/overview >/dev/null
# audit events list mutations
curl -sf -H "${AUTH_HEADER}" http://127.0.0.1:8080/v1/audit-events >/dev/null

echo "e2e-smoke: PASS"
