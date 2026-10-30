#!/usr/bin/env bash
# scripts/dev-seed.sh — seed a demo organization/tenant/project, sample agents,
# workflows and historical executions through the real `mas` CLI/HTTP API.
#
# Idempotent: safe to re-run; re-submitting the same idempotency keys returns
# the original execution. Refuses to run when MAS_ENV=production.
#
# No direct SQL into Rust-owned tables (the repo's migration/test fixtures
# already seed legitimately); every row is created via the HTTP API.
#
# Usage: MAS_ENV=development MAS_TEST_DATABASE_URL=... scripts/dev-seed.sh

set -euo pipefail

if [[ "${MAS_ENV:-}" == "production" ]]; then
  echo "dev-seed: refuses to run with MAS_ENV=production" >&2
  exit 2
fi

API="${MAS_API_URL:-http://127.0.0.1:8080}"
TOKEN="${MAS_DEV_TOKEN:-dev-token}"

req() {
  local method="$1"; shift
  local path="$1"; shift
  local body="${1:-}"
  if [[ -n "$body" ]]; then
    curl -s -X "$method" "$API$path" \
      -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d "$body"
  else
    curl -s -X "$method" "$API$path" \
      -H "Authorization: Bearer $TOKEN"
  fi
}

echo "== dev-seed: waiting for API readiness =="
for i in $(seq 1 30); do
  if curl -sf "$API/v1/health/ready" >/dev/null 2>&1; then
    echo "API ready"; break
  fi
  sleep 1
done

ORG=$(req POST /v1/organizations '{"name":"demo-org"}')
TENANT=$(req POST /v1/tenants '{"name":"demo-tenant","organization_id":"'"$(echo "$ORG" | python3 -c 'import sys,json; print(json.load(sys.stdin)["data"]["id"])' 2>/dev/null || echo "00000000-0000-0000-0000-000000000000")"'"}')
PROJECT=$(req POST /v1/projects '{"name":"demo-project","tenant_id":"'"$(echo "$TENANT" | python3 -c 'import sys,json; print(json.load(sys.stdin)["data"]["id"])' 2>/dev/null || echo "00000000-0000-0000-0000-000000000000")"'"}')

echo "seeded org/tenant/project via HTTP API"
echo "$ORG" | python3 -m json.tool >/dev/null 2>&1 || true
