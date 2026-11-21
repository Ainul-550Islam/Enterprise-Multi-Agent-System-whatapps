#!/usr/bin/env bash
# PURPOSE: end-to-end smoke of the running MAS stack (api + workflow execution
# lane + auth enforcement). Provisions its own org/tenant/project over the
# API, then drives one execution round-trip (publish -> create -> start ->
# pause -> resume -> cancel) and asserts the liveness/readiness contract.
# Exits non-zero on the first failed assertion. Requires a --dev-inmemory api
# with MAS_DEV_TOKENS set (system actor).
set -euo pipefail

API="${MAS_API_BASE_URL:-http://localhost:8080}"
TOKEN="${MAS_DEV_TOKENS:-dev-token}"; TOKEN="${TOKEN%%:*}"
CT="content-type: application/json"
AUTH="authorization: Bearer ${TOKEN}"

fail() { echo "e2e-smoke: FAIL — $*" >&2; exit 1; }
jget() { python3 -c "import sys,json;d=json.load(sys.stdin);print(d$1)"; }

echo "e2e-smoke: api=${API}"

# 1. liveness is raw JSON; readiness aggregates dependencies.
live="$(curl -fsS "${API}/health" || true)"
echo "${live}" | grep -q '"status"' || fail "/health did not answer JSON: ${live}"
ready="$(curl -fsS "${API}/v1/health/ready" || true)"
echo "${ready}" | grep -q 'ready' || fail "/v1/health/ready missing 'ready': ${ready}"
echo "step 1 ok: health + ready"

# 2. unauthenticated calls to protected routes must be rejected.
code="$(curl -s -o /dev/null -w '%{http_code}' "${API}/v1/agents?project_id=00000000-0000-0000-0000-000000000000")"
[ "${code}" = "401" ] || fail "expected 401 without token, got ${code}"
echo "step 2 ok: unauthenticated 401"

# 3. an authenticated call without the scope pair is refused, never silently
#    defaulted into some tenant. Directory-resolved routes answer FORBIDDEN
#    ("no tenant scope was resolved"); scope-extractor routes answer
#    VALIDATION_FAILED naming x-tenant-id (covered by api unit tests).
code="$(curl -s -o /dev/null -w '%{http_code}' -X GET "${API}/v1/agents?project_id=00000000-0000-0000-0000-000000000000" -H "${AUTH}")"
body="$(curl -s -X GET "${API}/v1/agents?project_id=00000000-0000-0000-0000-000000000000" -H "${AUTH}")"
echo "${body}" | grep -q 'no tenant scope' || fail "expected 'no tenant scope' refusal: ${body}"
[ "${code}" = "403" ] || fail "expected 403 for the scope-less refusal, got ${code}"
echo "step 3 ok: scope-less call refused (403, no tenant scope)"

# 4. provision the tenant plane (system actor).
SUFFIX="$(python3 -c 'import uuid;print(uuid.uuid4().hex[:8])')"
ORG="$(curl -fsS -X POST "${API}/v1/organizations" -H "${AUTH}" -H "${CT}" \
  -d "{\"legal_name\":\"E2E Smoke ${SUFFIX}\",\"display_name\":\"E2E Smoke\",\"slug\":\"e2e-${SUFFIX}\"}")" \
  || fail "organization create failed: ${ORG}"
OID="$(echo "${ORG}" | jget "['data']['id']")"
TEN="$(curl -fsS -X POST "${API}/v1/tenants" -H "${AUTH}" -H "${CT}" -H "x-organization-id: ${OID}" \
  -d "{\"name\":\"E2E Tenant ${SUFFIX}\",\"slug\":\"e2e-${SUFFIX}\",\"environment\":\"development\",\"isolation\":\"shared_rls\"}")"
TID="$(echo "${TEN}" | jget "['data']['id']")"
curl -fsS -X POST "${API}/v1/tenants/${TID}/activate" -H "${AUTH}" -H "${CT}" -H "x-organization-id: ${OID}" -d '{}' >/dev/null \
  || fail "tenant activation failed"
SCOPE=(-H "x-tenant-id: ${TID}" -H "x-organization-id: ${OID}")
PRJ="$(curl -fsS -X POST "${API}/v1/projects" -H "${AUTH}" -H "${CT}" "${SCOPE[@]}" \
  -d "{\"name\":\"E2E Project ${SUFFIX}\",\"slug\":\"e2e-${SUFFIX}\"}")"
PID="$(echo "${PRJ}" | jget "['data']['id']")"
echo "step 4 ok: org=${OID} tenant=${TID} project=${PID}"

# 5. scoped reads work once the tenant plane is operational.
code="$(curl -s -o /dev/null -w '%{http_code}' "${API}/v1/agents?project_id=${PID}" -H "${AUTH}" "${SCOPE[@]}")"
[ "${code}" = "200" ] || fail "expected 200 for scoped agents list, got ${code}"
code="$(curl -s -o /dev/null -w '%{http_code}' "${API}/v1/agents?project_id=${PID}" -H "${AUTH}" \
  -H "x-tenant-id: 00000000-0000-0000-0000-000000000000" -H "x-organization-id: ${OID}")"
[ "${code}" = "404" ] || fail "expected 404 for unknown tenant, got ${code}"
echo "step 5 ok: scoped 200 / unknown-tenant 404"

# 6. one execution round-trip through the workflow lane.
WF="$(curl -fsS -X POST "${API}/v1/workflows" -H "${AUTH}" -H "${CT}" "${SCOPE[@]}" \
  -d "{\"project_id\":\"${PID}\",\"name\":\"E2E Flow ${SUFFIX}\",\"slug\":\"e2e-${SUFFIX}\"}")"
WID="$(echo "${WF}" | jget "['data']['id']")"
curl -fsS -X PUT "${API}/v1/workflows/${WID}/graph" -H "${AUTH}" -H "${CT}" "${SCOPE[@]}" \
  -d '{"nodes":[{"node_key":"start","node_type":"start","name":"start"},{"node_key":"finish","node_type":"end","name":"finish"}],"edges":[{"from":"start","to":"finish"}]}' >/dev/null
PUB="$(curl -fsS -X POST "${API}/v1/workflows/${WID}/publish" -H "${AUTH}" -H "${CT}" "${SCOPE[@]}" -d '{}')"
[ "$(echo "${PUB}" | jget "['data']['version_number']")" = "1" ] || fail "publish did not create version 1: ${PUB}"
EX="$(curl -fsS -X POST "${API}/v1/projects/${PID}/executions" -H "${AUTH}" -H "${CT}" "${SCOPE[@]}" \
  -d "{\"workflow_id\":\"${WID}\",\"input\":{\"msg\":\"smoke\"}}")"
EXID="$(echo "${EX}" | jget "['data']['id']")"
[ "$(echo "${EX}" | jget "['data']['status']")" = "queued" ] || fail "execution not queued: ${EX}"
for verb in start pause resume cancel; do
  curl -fsS -X POST "${API}/v1/executions/${EXID}/signal" -H "${AUTH}" -H "${CT}" "${SCOPE[@]}" \
    -d "{\"signal\":\"${verb}\"}" >/dev/null || fail "signal ${verb} failed"
done
FINAL="$(curl -fsS "${API}/v1/executions/${EXID}" -H "${AUTH}" "${SCOPE[@]}")"
CANCELLED="$(echo "${FINAL}" | jget "['data']['cancellation_requested']")"
[ "${CANCELLED}" = "True" ] || fail "cancel was not durable: ${FINAL}"
echo "step 6 ok: execution ${EXID} round-trip (queued -> start/pause/resume/cancel, cancellation_requested=true)"

echo "e2e-smoke: PASS"
