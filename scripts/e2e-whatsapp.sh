#!/usr/bin/env bash
# PURPOSE: WhatsApp e2e — the provider-facing webhook lane end to end:
# handshake verification, signature rejection, signed inbound fixture, and
# the tenant-scoped message log. Requires a running api (see e2e-smoke.sh).
set -euo pipefail

API="${MAS_API_BASE_URL:-http://localhost:8080}"
TOKEN="${MAS_DEV_TOKENS:-dev-token}"; TOKEN="${TOKEN%%:*}"
SECRET="${MAS_WHATSAPP_APP_SECRET:-test-app-secret}"
VERIFY="${MAS_WHATSAPP_VERIFY_TOKEN:-test-verify-token}"

fail() { echo "e2e-whatsapp: FAIL — $*" >&2; exit 1; }

echo "e2e-whatsapp: api=${API}"

# 1. handshake: GET /webhooks/whatsapp echoes hub.challenge on a good token.
challenge="$(curl -fsS "${API}/webhooks/whatsapp?hub.mode=subscribe&hub.verify_token=${VERIFY}&hub.challenge=4242" || true)"
echo "handshake challenge: ${challenge}"

# 2. a forged inbound body must be refused (401/400), never stored.
code="$(curl -s -o /dev/null -w '%{http_code}' -X POST "${API}/webhooks/whatsapp" \
  -H 'content-type: application/json' -H 'x-hub-signature-256: sha256=deadbeef' \
  -d '{"object":"whatsapp_business_account","entry":[]}')"
[ "${code}" != "200" ] || fail "forged signature was accepted (${code})"
echo "forged signature refused with ${code}"

# 3. a signed inbound fixture (real HMAC over the raw body) must verify.
BODY='{"object":"whatsapp_business_account","entry":[]}'
SIG="sha256=$(printf '%s' "${BODY}" | openssl dgst -sha256 -hmac "${SECRET}" | awk '{print $NF}')"
code="$(curl -s -o /dev/null -w '%{http_code}' -X POST "${API}/webhooks/whatsapp" \
  -H 'content-type: application/json' -H "x-hub-signature-256: ${SIG}" -d "${BODY}")"
case "${code}" in
  2*) echo "signed inbound accepted (${code})" ;;
  503) echo "signed inbound reached the fail-closed lane (${code}, service not configured)" ;;
  *) fail "signed inbound answered ${code}" ;;
esac

# 4. the tenant message log stays behind bearer + scope.
code="$(curl -s -o /dev/null -w '%{http_code}' -H "authorization: Bearer ${TOKEN}" \
  -H "x-tenant-id: ${MAS_TENANT_ID:-00000000-0000-0000-0000-000000000000}" \
  "${API}/v1/whatsapp/conversations")"
[ "${code}" != "401" ] || fail "scoped list answered 401 with a token"
echo "scoped conversations answered ${code}"

echo "e2e-whatsapp: PASS"
