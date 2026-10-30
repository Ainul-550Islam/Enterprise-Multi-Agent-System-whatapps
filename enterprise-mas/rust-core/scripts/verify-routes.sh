#!/usr/bin/env bash
# PURPOSE: Phase B gate — route completeness (static) plus a live production
# server boot against real PostgreSQL, curling every Batch 01/02 route with a
# valid and an invalid bearer token and logging the real HTTP status codes.
#
# Owning layer: CI verification (repo scripts/).
# Production responsibility: fail closed (non-zero) on any missing, duplicate,
# unauthenticated, or wrongly-authorized route; never fake a status code.
# MUST NOT: weaken checks, skip the boot, or report a code that was not
# observed from a real HTTP response.

set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
EV_DIR="../ci-evidence"
mkdir -p "$EV_DIR"

FAILS=0
note_fail() { echo "FAIL $*"; FAILS=$((FAILS + 1)); }
note_pass() { echo "PASS $*"; }

echo "== Phase B: route completeness (source) =="
echo "UTC_NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)"

python3 - <<'PYEOF'
import glob, os, re, sys

# (method, path) pairs added by Batch 01 / Batch 02, verbatim from the batch
# prompts' "NEW ROUTES YOU ADD" sections.
required_new = [
    ("GET", "/v1/auth/me"),
    ("GET", "/v1/audit-events"),
    ("GET", "/v1/policies"),
    ("POST", "/v1/policies"),
    ("GET", "/v1/policies/{id}"),
    ("PATCH", "/v1/policies/{id}"),
    ("POST", "/v1/policies/{id}/publish"),
    ("POST", "/v1/policies/{id}/rollback"),
    ("POST", "/v1/policies/{id}/evaluate"),
    ("GET", "/v1/quotas"),
    ("GET", "/v1/usage/summary"),
    ("POST", "/v1/tools"),
    ("GET", "/v1/tools"),
    ("GET", "/v1/tools/{id}"),
    ("PATCH", "/v1/tools/{id}"),
    ("PUT", "/v1/agents/{agent_id}/tool-permissions"),
    ("GET", "/v1/connector-manifests"),
    ("POST", "/v1/connectors/{connector_id}/probe"),
    ("POST", "/v1/connectors/{connector_id}/disable"),
    ("GET", "/v1/webhooks/{id}/deliveries"),
    ("POST", "/v1/webhooks/{id}/deliveries/{delivery_id}/replay"),
    ("GET", "/v1/stream"),
    ("POST", "/v1/assistant/messages"),
]

# Pre-existing routes the verify prompt requires to still be present
# (exact paths from router.rs / batch prompts).
required_existing = [
    ("GET", "/v1/usage"),
    ("GET", "/v1/billing/subscription"),
    ("GET", "/v1/billing/entitlements"),
    ("GET", "/v1/billing/usage-totals"),
    ("GET", "/v1/billing/receipts"),
    ("GET", "/v1/billing/receipts/{provider}/{event_id}"),
    ("POST", "/v1/connectors"),
    ("GET", "/v1/connectors"),
    ("POST", "/v1/connectors/test"),
    ("GET", "/v1/connectors/{connector_id}"),
    ("POST", "/v1/connectors/{connector_id}/oauth/authorize"),
    ("POST", "/v1/webhooks"),
    ("GET", "/v1/webhooks"),
    ("POST", "/v1/webhooks/{webhook_id}/enable"),
    ("POST", "/v1/webhooks/{webhook_id}/disable"),
    ("GET", "/v1/assistant/insights"),
    ("GET", "/v1/analytics/overview"),
    ("GET", "/v1/analytics/performance"),
    ("GET", "/v1/analytics/tasks-by-agent"),
    ("GET", "/v1/analytics/usage-cost"),
    ("GET", "/v1/rollbacks"),
    ("GET", "/v1/approvals"),
    ("POST", "/v1/approvals/{id}/approve"),
    ("POST", "/v1/approvals/{id}/deny"),
]

route_re = re.compile(
    r'\.route\(\s*"([^"]+)"\s*,\s*'
    r'((?:\s*(?:get|post|put|patch|delete)\s*\(\s*[A-Za-z0-9_:{}/]+\s*\)\s*(?:\.\s*)?)+)',
    re.S,
)
method_re = re.compile(r'(get|post|put|patch|delete)\s*\(')

registrations = {}  # (METHOD, path) -> [file names]
parse_errors = 0
for path in sorted(glob.glob("crates/api/src/**/*.rs", recursive=True)):
    src = open(path, encoding="utf-8").read()
    src = src.split("#[cfg(test)]")[0]  # production registrations only
    for match in route_re.finditer(src):
        route_path = match.group(1)
        for method in method_re.findall(match.group(2)):
            key = (method.upper(), route_path)
            registrations.setdefault(key, []).append(path)

failures = 0

def fail(msg):
    global failures
    print(f"FAIL {msg}")
    failures += 1

def ok(msg):
    print(f"PASS {msg}")

for key in required_new:
    hits = registrations.get(key, [])
    if len(hits) == 1:
        ok(f"route {key[0]} {key[1]} registered exactly once ({hits[0]})")
    elif not hits:
        fail(f"route {key[0]} {key[1]} is NOT registered anywhere")
    else:
        fail(f"route {key[0]} {key[1]} registered {len(hits)} times: {hits}")

for key in required_existing:
    hits = registrations.get(key, [])
    if len(hits) == 1:
        ok(f"pre-existing route {key[0]} {key[1]} still registered ({hits[0]})")
    elif not hits:
        fail(f"pre-existing route {key[0]} {key[1]} disappeared")
    else:
        fail(f"pre-existing route {key[0]} {key[1]} duplicated: {hits}")

# No duplicate registrations anywhere (axum panics on duplicates).
dupes = {k: v for k, v in registrations.items() if len(v) > 1}
if dupes:
    for key, hits in sorted(dupes.items()):
        fail(f"duplicate registration {key[0]} {key[1]} in {hits}")
else:
    ok("no duplicate (method, path) registrations anywhere in api src")

# Both additive route groups must sit inside the protected router, before its
# authenticated_layer route_layer, and neither group may add its own layer.
router_src = open("crates/api/src/router.rs", encoding="utf-8").read()
prod_router = router_src.split("#[cfg(test)]")[0]
p_start = prod_router.find("let protected = Router::new()")
auth_layer = prod_router.find(
    ".route_layer(axum::middleware::from_fn_with_state(state.clone(), authenticated_layer))"
)
p_end = prod_router.find("let internal = Router::new()")
g_merge = prod_router.find(".merge(crate::routes_governance::routes())")
t_merge = prod_router.find(".merge(crate::routes_tooling::routes())")
ctx_layer = prod_router.find("request_context_layer")

if p_start != -1 and auth_layer != -1 and p_end != -1 and p_start < auth_layer < p_end:
    ok("protected router closes with authenticated_layer before internal/public merge")
else:
    fail(
        f"protected router envelope not found intact "
        f"(start={p_start}, auth_layer={auth_layer}, internal={p_end})"
    )

for name, idx in (("routes_governance", g_merge), ("routes_tooling", t_merge)):
    if idx != -1 and p_start < idx < auth_layer:
        ok(f"{name} merged inside the protected (authenticated) router")
    else:
        fail(f"{name} merge not inside protected router (idx={idx})")

if ctx_layer != -1:
    ok("global request_context_layer applied to the full router")
else:
    fail("request_context_layer missing from router")

for group in ("routes_governance", "routes_tooling"):
    src_path = f"crates/api/src/{group}.rs"
    src = open(src_path, encoding="utf-8").read().split("#[cfg(test)]")[0]
    if ".layer(" in src or "route_layer" in src:
        fail(f"{group} applies its own middleware envelope (double layer)")
    else:
        ok(f"{group} registers routes with no second middleware envelope")

print(f"STATIC_FAIL_COUNT={failures}")
sys.exit(min(failures, 100))
PYEOF
static_rc=$?
FAILS=$((FAILS + static_rc))

if [ "$static_rc" -ne 0 ]; then
    echo "static route table check failed (rc=$static_rc); still attempting live boot"
fi

echo "== Phase B: live boot against real PostgreSQL + route curls =="

if ! command -v psql >/dev/null 2>&1; then
    note_fail "psql client available (command -v psql)"
    echo "NOT RUN: live boot — psql client missing"
    echo "EXIT_CODE=1"
    date -u +%Y-%m-%dT%H:%M:%SZ
    exit 1
fi
note_pass "psql client available"

# PostgreSQL cluster up?
if ! pg_isready -q 2>/dev/null; then
    sudo pg_ctlcluster 17 main start >/dev/null 2>&1 || true
    sleep 2
fi
if pg_isready -q 2>/dev/null; then
    note_pass "PostgreSQL cluster accepting connections"
else
    note_fail "PostgreSQL cluster accepting connections"
    echo "EXIT_CODE=$FAILS"
    date -u +%Y-%m-%dT%H:%M:%SZ
    exit 1
fi

# NATS up? (required by the production SSE composition; without it the server
# refuses to boot in production mode, which the log will show honestly.)
NATS_STARTED=0
if ! (echo > /dev/tcp/127.0.0.1/4222) 2>/dev/null; then
    nohup nats-server -p 4222 >"$EV_DIR/B_nats.log" 2>&1 &
    NATS_STARTED=1
    sleep 1
fi
if (echo > /dev/tcp/127.0.0.1/4222) 2>/dev/null; then
    note_pass "NATS server listening on 4222"
else
    note_fail "NATS server listening on 4222"
fi

# Build the real server binary (evidence kept in the routes log tail).
echo "-- cargo build -p mas-api --bin mas-api --"
if ! source .cargo-env; then
    note_fail "source .cargo-env (cargo toolchain)"
    echo "EXIT_CODE=$FAILS"
    date -u +%Y-%m-%dT%H:%M:%SZ
    exit 1
fi
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
build_log="$EV_DIR/B_build.log"
if ! cargo build -p mas-api --bin mas-api >"$build_log" 2>&1; then
    note_fail "cargo build -p mas-api --bin mas-api (see ci-evidence/B_build.log)"
    tail -20 "$build_log"
    echo "NOT_RUN_REASON=build failure"
    echo "EXIT_CODE=$FAILS"
    date -u +%Y-%m-%dT%H:%M:%SZ
    exit 1
fi
note_pass "cargo build -p mas-api --bin mas-api"
echo "BUILD_EXIT_CODE=0"

# Real Python AgentBridge server (the same repo harness the e2e test spawns):
# the production composition dials it at boot over HTTP/2 before serving.
STUB_ROOT="$(cd ../python-orchestrator && pwd)"
STUB_PID=""
STUB_PORT=""
if [ -f "$STUB_ROOT/tests/bridge/stub_server.py" ]; then
    PYTHONPATH="$STUB_ROOT/src" nohup python3 "$STUB_ROOT/tests/bridge/stub_server.py" \
        --behaviour success --delay 0 >"$EV_DIR/B_stub.log" 2>&1 &
    STUB_PID=$!
    for _ in $(seq 1 30); do
        STUB_PORT=$(grep -o 'PORT=[0-9]\+' "$EV_DIR/B_stub.log" 2>/dev/null | head -1 | cut -d= -f2 || true)
        [ -n "$STUB_PORT" ] && break
        sleep 1
    done
fi
if [ -n "$STUB_PORT" ]; then
    note_pass "Python AgentBridge stub listening on 127.0.0.1:$STUB_PORT (ci-evidence/B_stub.log)"
else
    note_fail "Python AgentBridge stub startup (tests/bridge/stub_server.py)"
    echo "NOT_RUN: route curls — AgentBridge stub did not start"
    echo "EXIT_CODE=$FAILS"
    date -u +%Y-%m-%dT%H:%M:%SZ
    exit 1
fi

DB_URL="postgres://migrator:migrator_dev_pw@127.0.0.1:5432/mas"
HTTP_ADDR="127.0.0.1:18080"
GRPC_ADDR="127.0.0.1:15051"
BASE="http://127.0.0.1:18080"

env \
    MAS_ENV=development \
    MAS_API_HTTP_ADDR="$HTTP_ADDR" \
    MAS_API_GRPC_ADDR="$GRPC_ADDR" \
    MAS_DATABASE_URL="$DB_URL" \
    MAS_DATABASE_DIRECTORY_URL="$DB_URL" \
    MAS_DATABASE_AUDIT_URL="$DB_URL" \
    MAS_DATABASE_INBOUND_URL="$DB_URL" \
    MAS_DATABASE_INTEGRATIONS_URL="$DB_URL" \
    MAS_DATABASE_NOTIFICATIONS_URL="$DB_URL" \
    MAS_DATABASE_OAUTH_URL="$DB_URL" \
    MAS_DATABASE_OAUTH_REFRESH_URL="$DB_URL" \
    MAS_DATABASE_MIGRATE_ON_BOOT=true \
    MAS_NATS_URL="nats://127.0.0.1:4222" \
    MAS_OIDC_ISSUER="https://mas-idp.invalid/" \
    MAS_OIDC_JWKS_URL="https://mas-idp.invalid/jwks" \
    MAS_VAULT_URL="https://vault.verify.invalid/" \
    MAS_VAULT_TOKEN="verify-phase-b-vault-token-abc123" \
    MAS_OAUTH_SLACK_CLIENT_ID="verify-phase-b-slack-client-id" \
    MAS_OAUTH_CALLBACK_BASE_URL="https://oauth.verify.invalid" \
    MAS_AI_BRIDGE_ENDPOINT="http://127.0.0.1:${STUB_PORT}" \
    MAS_AI_BRIDGE_TOKEN="verify-phase-b-ai-bridge-token" \
    MAS_INTERNAL_SERVICE_SECRET="verify-phase-b-internal-service-secret-0123456789abcdef" \
    MAS_AI_BRIDGE_INTERNAL_SIGNING_SECRET="verify-phase-b-ai-bridge-signing-secret-0123456789abcdef" \
    MAS_AUDIT_CURSOR_KEY="verify-phase-b-audit-cursor-key-32bytes-abcdef" \
    MAS_DEV_TOKENS="" \
    ./target/debug/mas-api >"$EV_DIR/B_boot.log" 2>&1 &
SERVER_PID=$!

cleanup() {
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    if [ -n "$STUB_PID" ]; then
        kill "$STUB_PID" 2>/dev/null || true
        wait "$STUB_PID" 2>/dev/null || true
    fi
    if [ "$NATS_STARTED" -eq 1 ]; then
        pkill -f "nats-server -p 4222" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# Wait for readiness (public health route, no auth).
READY=0
for _ in $(seq 1 90); do
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        break
    fi
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$BASE/v1/health/live" 2>/dev/null || true)
    if [ "$code" = "200" ]; then
        READY=1
        break
    fi
    sleep 1
done

if [ "$READY" -ne 1 ]; then
    note_fail "production server booted (GET /v1/health/live -> 200)"
    echo "-- boot log tail --"
    tail -40 "$EV_DIR/B_boot.log" 2>/dev/null || true
    if kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "NOT_RUN: route curls — server never became ready (log ci-evidence/B_boot.log)"
    else
        echo "NOT_RUN: route curls — server exited during boot (log ci-evidence/B_boot.log)"
    fi
    echo "EXIT_CODE=$FAILS"
    date -u +%Y-%m-%dT%H:%M:%SZ
    exit 1
fi
note_pass "production server booted (GET /v1/health/live -> 200)"

# Seed a tenant-bound API key through SQL (hash formula = SHA-256 hex, per
# mas_security::api_keys::hash_key, mirrored by tests/support/mod.rs).
SEED_OUT=$(python3 - "$DB_URL" <<'PYEOF'
import hashlib, secrets, subprocess, sys, uuid
db = sys.argv[1]
org = uuid.uuid4()
tenant = uuid.uuid4()
project = uuid.uuid4()
raw = "mas_" + secrets.token_hex(32)
prefix = raw[:12]
digest = hashlib.sha256(raw.encode()).hexdigest()
suffix = uuid.uuid4().hex[:8]
sql = f"""
INSERT INTO organizations (id, legal_name, display_name, slug)
VALUES ('{org}', 'Verify Org {suffix}', 'Verify Org {suffix}', 'verify-org-{suffix}');
INSERT INTO tenants (id, organization_id, name, slug, status, environment, isolation_mode)
VALUES ('{tenant}', '{org}', 'Verify Tenant {suffix}', 'verify-tenant-{suffix}', 'active', 'development', 'shared_rls');
INSERT INTO projects (id, tenant_id, organization_id, name, slug)
VALUES ('{project}', '{tenant}', '{org}', 'Verify Project {suffix}', 'verify-project-{suffix}');
INSERT INTO api_keys (id, tenant_id, organization_id, name, prefix, secret_hash, scopes, status, role)
VALUES ('{uuid.uuid4()}', '{tenant}', '{org}', 'verify-key-{suffix}', '{prefix}', '{digest}', '"*"'::jsonb, 'active', 'admin');
"""
proc = subprocess.run(["psql", db, "-v", "ON_ERROR_STOP=1", "-q", "-c", sql],
                      capture_output=True, text=True)
if proc.returncode != 0:
    sys.stderr.write(proc.stderr)
    sys.exit(1)
print(raw)
print(tenant)
print(org)
PYEOF
)
seed_rc=$?
if [ "$seed_rc" -ne 0 ]; then
    note_fail "seed org/tenant/project/api-key row in live PostgreSQL"
    echo "NOT_RUN: route curls — seed failed"
    echo "EXIT_CODE=$FAILS"
    date -u +%Y-%m-%dT%H:%M:%SZ
    exit 1
fi
VALID_TOKEN=$(echo "$SEED_OUT" | sed -n '1p')
TENANT_ID=$(echo "$SEED_OUT" | sed -n '2p')
ORG_ID=$(echo "$SEED_OUT" | sed -n '3p')
note_pass "seeded org/tenant/project + API key (prefix ${VALID_TOKEN:0:12}…)"

INVALID_TOKEN="mas_$(printf '0%.0s' {1..64})"
DUMMY_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')

# curl one route with a token; echo the observed HTTP status code.
# Optional $4 = JSON body for non-GET requests (defaults to `{}`).
status() {
    local method="$1" path="$2" token="$3"
    local body="{}"
    if [ $# -ge 4 ]; then body="$4"; fi
    local args=(-s -o /dev/null -w '%{http_code}' --max-time 6 -X "$method"
                -H "Authorization: Bearer $token"
                -H "x-tenant-id: $TENANT_ID"
                -H "x-organization-id: $ORG_ID")
    if [ "$method" != "GET" ]; then
        args+=(-H "Content-Type: application/json" -d "$body")
    fi
    if [ "$method" = "GET" ] && [ "$path" = "/v1/stream" ]; then
        args=(-s -o /dev/null -w '%{http_code}' --max-time 4 -N -X "$method"
              -H "Authorization: Bearer $token"
              -H "x-tenant-id: $TENANT_ID"
              -H "x-organization-id: $ORG_ID")
    fi
    curl "${args[@]}" "$BASE$path" 2>/dev/null || true
}

# check_route METHOD PATH [EXPECTED_VALID_CODES] [JSON_BODY]
#
# EXPECTED_VALID_CODES is a whitespace-separated allow-list for the
# valid-token response. Without it the default rule applies: anything except
# 401/403/500/502/503/504/000 (auth failures, server failures, dead route).
# An explicit list is only used where the contract itself dictates a
# non-2xx answer (e.g. the assistant's user-principal gate for API keys —
# pinned by application::assistant_service::submission_gate_refuses_api_key_principals).
check_route() {
    local method="$1" path="$2" expected="${3:-}"
    local body="{}"
    if [ $# -ge 4 ]; then body="$4"; fi
    local p=${path//\{id\}/$DUMMY_ID}
    p=${p//\{agent_id\}/$DUMMY_ID}
    p=${p//\{connector_id\}/$DUMMY_ID}
    p=${p//\{delivery_id\}/$DUMMY_ID}
    p=${p//\{event_id\}/$DUMMY_ID}
    p=${p//\{provider\}/stripe}
    local bad good
    bad=$(status "$method" "$p" "$INVALID_TOKEN" "$body")
    good=$(status "$method" "$p" "$VALID_TOKEN" "$body")
    if [ "$bad" != "401" ]; then
        note_fail "$method $path invalid-token -> $bad (expected 401)"
        return
    fi
    if [ -n "$expected" ]; then
        local ok_code=0 code
        for code in $expected; do
            [ "$good" = "$code" ] && ok_code=1 && break
        done
        if [ "$ok_code" -eq 1 ]; then
            echo "PASS $method $path invalid=$bad valid=$good (expected: $expected)"
        else
            note_fail "$method $path valid-token -> $good (expected one of: $expected)"
        fi
        return
    fi
    if [ "$good" = "401" ] || [ "$good" = "403" ] || [ "$good" = "500" ] || [ "$good" = "502" ] || [ "$good" = "503" ] || [ "$good" = "504" ] || [ "$good" = "000" ]; then
        note_fail "$method $path valid-token -> $good (invalid/blocked status)"
    else
        echo "PASS $method $path invalid=$bad valid=$good"
    fi
}

echo "-- batch route curl matrix (observed codes) --"
# Batch 01
check_route GET /v1/auth/me
check_route GET /v1/audit-events
check_route GET /v1/policies
check_route POST /v1/policies
check_route GET /v1/policies/\{id\}
check_route PATCH /v1/policies/\{id\}
check_route POST /v1/policies/\{id\}/publish
check_route POST /v1/policies/\{id\}/rollback
check_route POST /v1/policies/\{id\}/evaluate
check_route GET /v1/quotas
# Windowed reads: `from`/`to` are required query fields (validated, bounded).
check_route GET "/v1/usage/summary?from=2026-10-01T00:00:00Z&to=2026-10-30T00:00:00Z"
# Batch 02
check_route POST /v1/tools
check_route GET /v1/tools
check_route GET /v1/tools/\{id\}
check_route PATCH /v1/tools/\{id\}
check_route PUT /v1/agents/\{agent_id\}/tool-permissions
check_route GET /v1/connector-manifests
check_route POST /v1/connectors/\{connector_id\}/probe
check_route POST /v1/connectors/\{connector_id\}/disable
check_route GET /v1/webhooks/\{id\}/deliveries
check_route POST /v1/webhooks/\{id\}/deliveries/\{delivery_id\}/replay
check_route GET /v1/stream
# The assistant contract requires a USER principal (application-layer gate);
# an API key therefore gets the designed 403 — pinned by the application unit
# test submission_gate_refuses_api_key_principals_and_accepts_users. The body
# must be a real message so the request passes validation and reaches the gate.
check_route POST /v1/assistant/messages "403" '{"message":"verify-phase-b ping"}'

echo "-- pre-existing route liveness with valid token --"
check_route GET /v1/usage
check_route GET /v1/billing/subscription
check_route GET /v1/rollbacks
check_route GET /v1/approvals
check_route GET /v1/assistant/insights
check_route GET "/v1/analytics/overview?from=2026-10-01T00:00:00Z&to=2026-10-30T00:00:00Z"
check_route GET /v1/connectors
check_route GET /v1/webhooks

if [ "$FAILS" -eq 0 ]; then
    echo "ROUTES: ALL PASS"
else
    echo "ROUTES: $FAILS FAILURE(S)"
fi
echo "EXIT_CODE=$FAILS"
date -u +%Y-%m-%dT%H:%M:%SZ
exit "$FAILS"
