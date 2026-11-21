#!/usr/bin/env bash
# PURPOSE: seed the five demo definitions plus the bundled agent/workflow
# documents onto a running API. Honest about refusals: a 4xx/5xx is printed
# and counted; the script exits non-zero if every POST failed.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
API="${MAS_API_BASE_URL:-${MAS_API_URL:-http://localhost:8080}}"
TOKEN="${MAS_API_TOKEN:-${MAS_DEV_TOKENS:-dev-token}}"; TOKEN="${TOKEN%%:*}"
TENANT="${MAS_TENANT_ID:-}"
ORG="${MAS_ORGANIZATION_ID:-}"

fail() { echo "seed-demo: FAIL — $*" >&2; exit 1; }
[ -n "${TENANT}" ] || fail "MAS_TENANT_ID is required"
[ -n "${ORG}" ] || fail "MAS_ORGANIZATION_ID is required"

AUTH="authorization: Bearer ${TOKEN}"
CT="content-type: application/json"
SCOPE_T="x-tenant-id: ${TENANT}"
SCOPE_O="x-organization-id: ${ORG}"

post() {
  local path="$1" file="$2"
  local code body
  body="$(curl -sS -o /tmp/seed-demo-body.$$ -w '%{http_code}' \
    -X POST "${API}${path}" \
    -H "${AUTH}" -H "${CT}" -H "${SCOPE_T}" -H "${SCOPE_O}" \
    --data @"${file}")" || true
  code="${body}"
  echo "seed-demo: POST ${path} <- ${file} -> ${code}"
  if [ "${code}" -ge 200 ] && [ "${code}" -lt 300 ]; then
    return 0
  fi
  cat /tmp/seed-demo-body.$$ >&2 || true
  return 1
}

ok=0
fail_count=0
seed() {
  if post "$1" "$2"; then
    ok=$((ok + 1))
  else
    fail_count=$((fail_count + 1))
  fi
}

DEMO="${ROOT}/scripts/demo"
seed /v1/agents "${DEMO}/support-bot.json"
seed /v1/agents "${DEMO}/research-team.json"
seed /v1/agents "${DEMO}/rag-faq.json"
seed /v1/agents "${DEMO}/content-team.json"
seed /v1/workflows "${DEMO}/approval-flow.json"
# CRD-shaped documents are offered for operators who apply them through
# the definition importer; they are not HTTP agents/workflows bodies.
echo "seed-demo: skipping CRD-shaped agent-definition.json and workflow-definition.json on the HTTP API"

rm -f /tmp/seed-demo-body.$$
echo "seed-demo: ok=${ok} failed=${fail_count}"
[ "${ok}" -gt 0 ] || fail "no demo document was accepted"
[ "${fail_count}" -eq 0 ] || fail "${fail_count} demo POSTs were refused"
echo "seed-demo: PASS"
