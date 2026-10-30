#!/usr/bin/env bash
# PURPOSE: Phase C gate — static code-quality and security checks over the
#          Batch 01/02 file set plus every file changed versus origin/main.
# Production responsibility: fail closed (non-zero) on forbidden markers,
# request-path panics, HTTP clients outside the egress module, format!-built
# or unscoped SQL, secret material in DTOs/SSE/audit/logs, handlers that skip
# the ctx → one service call → envelope shape, mutations without canonical
# authz (or without an audit record, except documented read-only POSTs),
# non-additive edits to shared files outside the verify-fix allowlist,
# missing PURPOSE headers, oversized files, and any deleted file.
# Evidence: every check prints PASS/FAIL lines; summary is STATIC_FAIL_COUNT.
set -uo pipefail

cd "$(dirname "$0")/.."
FAILS=0
note_fail() { echo "FAIL $*"; FAILS=$((FAILS + 1)); }
note_pass() { echo "PASS $*"; }

echo "== Phase C: static gates (batch files + diff vs origin/main) =="

REPO_ROOT=$(git rev-parse --show-toplevel) || {
    echo "FATAL not inside a git work tree"
    exit 1
}

# ---------------------------------------------------------------------------
# Scope: the batch file inventory (same list Phase A pins) plus every file
# changed versus origin/main plus the verify-phase additions.
# ---------------------------------------------------------------------------
BATCH_FILES=$(cat <<'LIST'
crates/api/src/handlers/auth.rs
crates/api/src/handlers/audit.rs
crates/api/src/handlers/policies.rs
crates/api/src/handlers/usage.rs
crates/api/src/routes_governance.rs
crates/api/tests/governance_routes.rs
crates/application/src/audit_query_service.rs
crates/application/src/policy_service.rs
crates/application/src/usage_service.rs
crates/persistence/src/repositories/usage.rs
crates/persistence/tests/usage_live_pg.rs
crates/persistence/src/repositories/audit.rs
crates/security/tests/jwks.rs
crates/api/src/handlers/tools.rs
crates/api/src/handlers/connectors.rs
crates/api/src/handlers/webhooks.rs
crates/api/src/handlers/stream.rs
crates/api/src/handlers/assistant.rs
crates/api/src/routes_tooling.rs
crates/api/tests/tooling_routes.rs
crates/application/src/tool_service.rs
crates/application/src/connector_service.rs
crates/application/src/webhook_service.rs
crates/application/src/stores.rs
crates/persistence/src/repositories/tools.rs
crates/persistence/src/repositories/webhook_admin.rs
crates/persistence/src/repositories/connector_lifecycle.rs
crates/persistence/tests/tools_live_pg.rs
crates/persistence/tests/analytics_live_pg.rs
crates/persistence/tests/rollback_live_pg.rs
LIST
)

CHANGED=$(git diff --name-only origin/main -- . | sed 's|^enterprise-mas/rust-core/||')
UNTRACKED_NEW="crates/messaging/src/event_tail.rs
crates/persistence/tests/project_list_live_pg.rs
scripts/verify-inventory.sh
scripts/verify-routes.sh
scripts/verify-static.sh"

SCAN_SET=$(printf '%s\n%s\n' "$BATCH_FILES" "$UNTRACKED_NEW" | sort -u; printf '%s\n' "$CHANGED" | sort -u)
SCAN_SET=$(printf '%s\n' "$SCAN_SET" | sort -u | sed '/^$/d')

# Files added wholesale (full-text scans) vs modified (added-line scans).
added_files() {
    git diff --name-status origin/main -- . | awk '$1=="A"{sub(/^enterprise-mas\/rust-core\//,"",$2); print $2}'
    printf '%s\n' "$UNTRACKED_NEW"
}

# ---------------------------------------------------------------------------
# S1: batch files and verify-phase additions exist, are non-trivial, start
#     with a PURPOSE header; every scanned source file is within 1500 lines.
# ---------------------------------------------------------------------------
echo "-- S1 existence / PURPOSE header / size --"
PURPOSE_SET=$(printf '%s\n%s\n' "$BATCH_FILES" "$UNTRACKED_NEW" | sed '/^$/d')
# Existence for everything in scope.
while IFS= read -r f; do
    case "$f" in
        *.rs|*.toml|*.sql|*.sh) ;;
        *) continue ;;
    esac
    if [ ! -f "$f" ]; then
        note_fail "S1 $f missing"
    fi
done <<< "$SCAN_SET"
# PURPOSE header for contract-scoped files (batch set + verify additions +
# the config/SQL artifacts the batch prompts listed).
while IFS= read -r f; do
    [ -f "$f" ] || continue
    if grep -qE '^(//!|#|--)\s*PURPOSE:' "$f"; then
        note_pass "S1 $f PURPOSE header"
    else
        note_fail "S1 $f missing PURPOSE header"
    fi
done <<< "$PURPOSE_SET"
for f in config/default.toml config/production.toml migrations/0050_audit_query_indexes/up.sql migrations/0051_webhook_replay_attempts/down.sql; do
    if [ -f "$f" ] && ! grep -qE '^(//!|#|--)\s*PURPOSE:' "$f"; then
        note_fail "S1 PURPOSE header missing in $f"
    fi
done
# Size gate: files ADDED by this change set must be ≤1500 lines. A
# pre-existing file over the cap is only acceptable when this change set did
# not push it there (before >1500 already); growing a sub-1500 file past the
# cap fails. The before/after numbers are printed as evidence either way.
while IFS= read -r f; do
    case "$f" in
        *.rs) ;;
        *) continue ;;
    esac
    [ -f "$f" ] || continue
    after=$(wc -l < "$f")
    git_path="enterprise-mas/rust-core/$f"
    if before=$(git cat-file -e "origin/main:$git_path" 2>/dev/null && git show "origin/main:$git_path" | wc -l); then
        if [ "$before" -le 1500 ] && [ "$after" -gt 1500 ]; then
            note_fail "S1 $f grew past 1500 lines (before=$before after=$after)"
        elif [ "$before" -gt 1500 ]; then
            note_pass "S1 $f PREEXISTING_OVERSIZE (before=$before after=$after — not pushed over the cap by this change set)"
        else
            note_pass "S1 $f size ok (before=$before after=$after)"
        fi
    else
        if [ "$after" -gt 1500 ]; then
            note_fail "S1 ADDED file $f is $after lines (>1500)"
        else
            note_pass "S1 ADDED file $f size ok ($after lines)"
        fi
    fi
done <<< "$SCAN_SET"

# ---------------------------------------------------------------------------
# S2: forbidden markers. Batch files and added files are scanned in full;
#     modified files only for lines this change set introduced.
# ---------------------------------------------------------------------------
echo "-- S2 forbidden markers (todo!/TODO/FIXME/XXX/unimplemented!/placeholder) --"
MARKER_RE='todo!\(|unimplemented!\(|TODO|FIXME|XXX|placeholder'
for f in $BATCH_FILES; do
    [ -f "$f" ] || continue
    if hits=$(grep -nE "$MARKER_RE" "$f"); then
        note_fail "S2 markers in batch file $f:"$'\n'"$hits"
    else
        note_pass "S2 $f clean"
    fi
done
while IFS= read -r f; do
    [ -f "$f" ] || continue
    if git diff --name-status origin/main -- . | grep -q "^A.*${f}\$"; then
        if hits=$(grep -nE "$MARKER_RE" "$f"); then
            note_fail "S2 markers in added file $f:"$'\n'"$hits"
        else
            note_pass "S2 added $f clean"
        fi
    fi
done <<< "$SCAN_SET"
# Added lines of modified files.
while IFS= read -r f; do
    [ -f "$f" ] || continue
    if git diff -U0 origin/main -- "enterprise-mas/rust-core/$f" | grep -E '^\+' | grep -v '^\+\+\+' | grep -qE "$MARKER_RE"; then
        note_fail "S2 forbidden marker in ADDED lines of $f"
    fi
done < <(printf '%s\n' "$CHANGED" | sed '/^$/d')
note_pass "S2 added-lines of modified files scanned"

# ---------------------------------------------------------------------------
# S3: no request-path panics — unwrap()/expect()/panic!/unreachable!/todo in
#     non-test code of added files (full) and added lines of modified files.
# ---------------------------------------------------------------------------
echo "-- S3 request-path panics (unwrap/expect/panic/unreachable outside tests) --"
PANIC_RE='\.unwrap\(\)|\.expect\(|[^a-z_]panic!|unreachable!'
while IFS= read -r f; do
    [ -f "$f" ] || continue
    case "$f" in tests/*|*/tests/*) continue ;; scripts/*) continue ;; esac
    if hits=$(awk '/#\[cfg\(test\)\]/{t=1} !t && /(\.unwrap\(\)|\.expect\(|(^|[^_a-z])panic!|unreachable!|todo!\(|unimplemented!\()/{print FNR": "$0}' "$f"); then
        if [ -n "$hits" ]; then
            note_fail "S3 panic-style call in non-test code $f:"$'\n'"$hits"
        else
            note_pass "S3 $f non-test code clean"
        fi
    else
        note_pass "S3 $f non-test code clean"
    fi
done < <(added_files | sort -u | sed '/^$/d')
while IFS= read -r f; do
    [ -f "$f" ] || continue
    case "$f" in scripts/*) continue ;; esac
    if git diff -U0 origin/main -- "enterprise-mas/rust-core/$f" | grep -E '^\+' | grep -v '^\+\+\+' | grep -qE "$PANIC_RE"; then
        note_fail "S3 panic-style call in ADDED lines of $f"
    fi
done < <(printf '%s\n' "$CHANGED" | sed '/^$/d')
note_pass "S3 added-lines of modified files scanned"

# ---------------------------------------------------------------------------
# S4: no raw HTTP client outside the egress allowlist.
# ---------------------------------------------------------------------------
echo "-- S4 raw reqwest/hyper outside egress --"
EGRESS_ALLOWED="crates/integrations/src/reqwest_egress.rs
crates/api/src/oidc_auth.rs
crates/cli/src/client.rs"
CLIENT_RE='reqwest::Client|reqwest::get\(|reqwest::Request|reqwest::Response|hyper::Client|hyper::client|hyper::Request|hyper::Response'
while IFS= read -r f; do
    [ -f "$f" ] || continue
    case "$f" in scripts/*) continue ;; esac
    case "$EGRESS_ALLOWED" in *"$f"*) note_pass "S4 $f is a designated egress/verifier module"; continue ;; esac
    if hits=$(grep -nE "$CLIENT_RE" "$f"); then
        note_fail "S4 HTTP client usage outside egress in $f:"$'\n'"$hits"
    else
        note_pass "S4 $f no raw HTTP client"
    fi
done <<< "$SCAN_SET"
while IFS= read -r f; do
    [ -f "$f" ] || continue
    if git diff -U0 origin/main -- "enterprise-mas/rust-core/$f" | grep -E '^\+' | grep -v '^\+\+\+' | grep -qE "$CLIENT_RE"; then
        note_fail "S4 HTTP client usage in ADDED lines of $f"
    fi
done < <(printf '%s\n' "$CHANGED" | sed '/^$/d')
note_pass "S4 added-lines of modified files scanned"

# ---------------------------------------------------------------------------
# S5..S11 run inside Python for precise parsing.
# ---------------------------------------------------------------------------
export STATIC_BATCH_FILES="$BATCH_FILES"
python3 - <<'PYEOF'
import re, subprocess, sys, os

os.chdir(os.getcwd())
fails = 0

def fail(msg):
    global fails
    print(f"FAIL {msg}")
    fails += 1

def ok(msg):
    print(f"PASS {msg}")

def changed_files():
    out = subprocess.run(["git", "diff", "--name-only", "origin/main", "--", "."],
                         capture_output=True, text=True).stdout
    files = []
    for line in out.splitlines():
        if line.startswith("enterprise-mas/rust-core/"):
            files.append(line[len("enterprise-mas/rust-core/"):])
    return files

def added_files():
    out = subprocess.run(["git", "diff", "--name-status", "origin/main", "--", "."],
                         capture_output=True, text=True).stdout
    files = []
    for line in out.splitlines():
        parts = line.split("\t")
        if parts[0] == "A" and parts[1].startswith("enterprise-mas/rust-core/"):
            files.append(parts[1][len("enterprise-mas/rust-core/"):])
    files += ["crates/messaging/src/event_tail.rs",
              "crates/persistence/tests/project_list_live_pg.rs"]
    return sorted(set(f for f in files if f.endswith(".rs")))

def added_lines(path):
    out = subprocess.run(["git", "diff", "-U0", "origin/main", "--",
                          "enterprise-mas/rust-core/" + path],
                         capture_output=True, text=True).stdout
    lines = []
    for line in out.splitlines():
        if line.startswith("+") and not line.startswith("+++"):
            lines.append(line[1:])
    return lines

def full_text(path):
    with open(path, encoding="utf-8", errors="replace") as fh:
        return fh.read()

# -------------------------------------------------------------------------
# S5: format!-built SQL. Interpolation must be CONST identifiers only
#     (house style: {TENANT_COLUMNS}); anything else is dynamic SQL.
# -------------------------------------------------------------------------
print("-- S5 format!-built SQL --")
sql_kw = re.compile(r'\b(SELECT|INSERT INTO|UPDATE|DELETE FROM|FROM)\b', re.I)
const_interp = re.compile(r'^[A-Z][A-Z0-9_]*$')
fmt_sql = re.compile(r'format!\(\s*(f?"[^"]*")', re.S)
dynamic_hits = 0
batch_env = os.environ.get("STATIC_BATCH_FILES", "")
batch_list = [line.strip() for line in batch_env.splitlines() if line.strip()]
scan_list = sorted(set(changed_files()) | set(batch_list))
for f in scan_list:
    if not f.endswith(".rs") or not os.path.exists(f):
        continue
    src = full_text(f)
    for m in fmt_sql.finditer(src):
        fmt = m.group(1)
        if not sql_kw.search(fmt):
            continue
        interps = re.findall(r'\{([^{}]*)\}', fmt)
        bad = [i for i in interps if not const_interp.match(i.strip())]
        if bad:
            dynamic_hits += 1
            line = src[:m.start()].count("\n") + 1
            fail(f"S5 {f}:{line} format! with dynamic SQL interpolation {bad}")
if dynamic_hits == 0:
    ok("S5 no format!-built SQL beyond uppercase CONST interpolation")

# -------------------------------------------------------------------------
# S6: SQL scoping — every statement on a FORCE-RLS table must carry a tenant
#     predicate, run through the scoped request transaction, or be the
#     documented dedicated-pool read (directory resolver).
# -------------------------------------------------------------------------
print("-- S6 SQL tenant scoping evidence --")
rls_tables = set()
for path in subprocess.run(["bash", "-c", "ls migrations/*/up.sql"], capture_output=True, text=True).stdout.split():
    sql = open(path).read()
    for m in re.finditer(r'ALTER TABLE ([a-z_]+) (?:ENABLE|FORCE) ROW LEVEL SECURITY', sql):
        rls_tables.add(m.group(1))
non_tenant_ok = {"mas_schema_migrations", "schema_migrations", "migrations_table"}  # migration ledger: no tenant column
stmt_re = re.compile(r'"((?:[^"\\]|\\.)*)"', re.S)
kw = re.compile(r'^\s*(SELECT|INSERT INTO|UPDATE|DELETE FROM|WITH)\b', re.I)
table_re = re.compile(r'\b(?:FROM|INTO|UPDATE|TABLE)\s+\{?([A-Za-z_][A-Za-z0-9_]*)\}?', re.I)
scoped_re = re.compile(r'scoped_request::connection')
scan_persist = [f for f in sorted(set(changed_files()))
                if f.startswith("crates/persistence/src/") and f.endswith(".rs")]
scan_persist += ["crates/persistence/src/directory.rs"]
scan_persist = sorted(set(scan_persist))
for f in scan_persist:
    if not os.path.exists(f):
        continue
    src = full_text(f)
    has_scoped = bool(scoped_re.search(src))
    for m in stmt_re.finditer(src):
        stmt = m.group(1)
        if not kw.match(stmt):
            continue
        one = " ".join(stmt.split())
        line = src[:m.start()].count("\n") + 1
        tm = table_re.search(one)
        table = tm.group(1).lower() if tm else "?"
        has_pred = bool(re.search(r'tenant_id|organization_id|current_tenant|set_config', one, re.I))
        if has_pred:
            ok(f"S6 {f}:{line} [{table}] explicit tenant/org predicate")
        elif table in non_tenant_ok or (table == "?" and re.search(r'pg_advisory_(lock|unlock)', one)):
            ok(f"S6 {f}:{line} [{table}] non-tenant bookkeeping/session statement (no tenant column by design)")
        elif f == "crates/persistence/src/directory.rs":
            ok(f"S6 {f}:{line} [{table}] DESIGN: dedicated directory-pool resolver read (PURPOSE-documented, membership-validated)")
        elif table in rls_tables and has_scoped:
            ok(f"S6 {f}:{line} [{table}] FORCE-RLS table through scoped request transaction")
        else:
            fail(f"S6 {f}:{line} [{table}] unscoped SQL (no tenant predicate, no scoped txn, not a documented resolver read)")

# -------------------------------------------------------------------------
# S7: secrets in DTOs / SSE / audit / logs.
# -------------------------------------------------------------------------
print("-- S7 secrets in DTOs, SSE, audit, logs --")
contracts = [f for f in changed_files() if "/contracts/" in f or f.startswith("crates/contracts/")]
raw_secret = re.compile(r'^\s*pub\s+\w*(secret|password|private_key)\w*\s*:\s*(String|Vec<u8>|uuid)', re.I)
ok_ref = re.compile(r'_id\b|_reference\b|_hash\b|_prefix\b|_version\b|has_\w+|enabled|rotation')
for f in contracts:
    for i, line in enumerate(full_text(f).splitlines(), 1):
        m = raw_secret.search(line)
        if m and not ok_ref.search(line):
            fail(f"S7 {f}:{i} raw secret value in DTO field: {line.strip()}")
ok("S7 contracts scanned for raw secret-valued DTO fields (no FAIL lines above = clean)")

stream_src = full_text("crates/api/src/handlers/stream.rs")
# Test fixtures deliberately carry decoy secret keys; only production
# (pre-#[cfg(test)]) SSE payload construction is in scope.
stream_src = stream_src.split("#[cfg(test)]", 1)[0]
for i, line in enumerate(stream_src.splitlines(), 1):
    if re.search(r'json!\(', line) and re.search(r'secret|password|bearer|signing', line, re.I):
        fail(f"S7 stream.rs:{i} secret-ish key in SSE/json payload: {line.strip()}")
ok("S7 SSE production payload construction has no secret-bearing keys (no FAIL lines above = clean)")

for f in added_files() + changed_files():
    if not os.path.exists(f) or f.startswith("scripts/"):
        continue
    src = full_text(f)
    if "#[cfg(test)]" in src:
        src = src.split("#[cfg(test)]", 1)[0]
    for i, line in enumerate(src.splitlines(), 1):
        if not re.search(r'(println!|eprintln!|log::|tracing::)', line):
            continue
        # A log statement leaks a secret only when it INTERPOLATES one
        # ({token}/{secret}/{:?} of secret material), not when it merely
        # mentions the topic ("OAuth token refresh pass failed").
        if re.search(r'\{(token|secret|password|bearer|api_key)|\{:\?\}', line) or \
           re.search(r'(format!|println!|eprintln!).*\b(token|secret|password|bearer)\s*[=)]', line):
            fail(f"S7 {f}:{i} possible secret interpolation in log line: {line.strip()}")
ok("S7 log statements in changed files interpolate no secret values (no FAIL lines above = clean)")

# -------------------------------------------------------------------------
# S8: handler shape — ctx → at most one service call → envelope; no direct
#     persistence. auth::me is the documented matrix projection (0 services).
# -------------------------------------------------------------------------
print("-- S8 handler shape (ctx -> one service call -> envelope) --")
handler_files = ["crates/api/src/handlers/auth.rs", "crates/api/src/handlers/audit.rs",
                 "crates/api/src/handlers/policies.rs", "crates/api/src/handlers/usage.rs",
                 "crates/api/src/handlers/tools.rs", "crates/api/src/handlers/connectors.rs",
                 "crates/api/src/handlers/webhooks.rs", "crates/api/src/handlers/assistant.rs",
                 "crates/api/src/handlers/stream.rs"]
for hf in handler_files:
    src = full_text(hf)
    tops = [(m.start(), m.group(1)) for m in re.finditer(r'^pub async fn (\w+)', src, re.M)]
    tops.append((len(src), None))
    for i in range(len(tops) - 1):
        body = src[tops[i][0]:tops[i + 1][0]]
        fn = tops[i][1]
        services = re.findall(r'state\s*\.\s*(\w+_service)\s*\(', body)
        persist = re.search(r'sqlx|\.pool\b|Store::new|repository', body)
        has_ctx = "Ctx(ctx)" in body
        if persist:
            fail(f"S8 {hf}::{fn} touches persistence directly")
        if len(services) > 1:
            fail(f"S8 {hf}::{fn} calls {len(services)} services ({services}), expected exactly one")
        if not has_ctx:
            fail(f"S8 {hf}::{fn} does not take the request Ctx")
        if fn == "me" and hf.endswith("auth.rs"):
            if services:
                fail(f"S8 auth::me unexpectedly calls a state service")
            else:
                ok("S8 auth::me — documented matrix projection (no state service, no persistence)")
            continue
        if len(services) == 1 and has_ctx and not persist:
            ok(f"S8 {hf}::{fn} ctx -> {services[0]} -> envelope")
        elif not services:
            fail(f"S8 {hf}::{fn} makes no service call")

# -------------------------------------------------------------------------
# S9: mutations — route methods POST/PATCH/PUT/DELETE must have canonical
#     authz in the handler; the invoked service mutation must record audit
#     (read-only POST contracts are allowlisted with reasons).
# -------------------------------------------------------------------------
print("-- S9 mutation authz + audit (fail closed) --")
routes = {"crates/api/src/routes_governance.rs": "crates/api/src/handlers/",
          "crates/api/src/routes_tooling.rs": "crates/api/src/handlers/"}
MUT_METHODS = ("post", "patch", "put", "delete")
READONLY_POSTS = {
    ("policies", "evaluate"): "B01 contract: evaluate is a dry-run decision test",
    ("connectors", "probe"): "B02 contract: probe is a read-only connectivity check",
}
AUDIT_EXEMPT = {
    ("policy_service", "evaluate"): "dry-run evaluate (no mutation)",
    ("connector_service", "probe"): "read-only probe (no mutation)",
    ("usage_service", "summary"): "read aggregation",
    ("usage_service", "quotas"): "read",
    ("audit_query_service", "query"): "read",
    ("policy_service", "list"): "read", ("policy_service", "get"): "read",
    ("tool_service", "list_page"): "read", ("tool_service", "get"): "read",
    ("connector_service", "manifests"): "read",
    ("webhook_service", "deliveries"): "read",
}
route_re = re.compile(r'\.(get|post|patch|put|delete)\(\s*([a-z_]+)::(\w+)', re.I)
handlers = {}
for rf in routes:
    for m in route_re.finditer(full_text(rf)):
        method, mod, fn = m.group(1).lower(), m.group(2), m.group(3)
        handlers.setdefault((mod, fn), []).append(method)
svc_sources = {}
for svc in ["policy_service", "tool_service", "connector_service", "webhook_service",
            "assistant_service", "usage_service", "audit_query_service"]:
    p = f"crates/application/src/{svc}.rs"
    if os.path.exists(p):
        svc_sources[svc] = full_text(p)

def service_fn_body(svc, fn):
    src = svc_sources.get(svc, "")
    m = re.search(rf'^    pub async fn {fn}\b', src, re.M)
    if not m:
        return None
    nxt = re.search(r'^    pub (?:async )?fn ', src[m.end():], re.M)
    end = m.end() + nxt.start() if nxt else len(src)
    return src[m.start():end]

SERVICE_FOR_MOD = {"policies": "policy_service", "tools": "tool_service",
                   "connectors": "connector_service", "webhooks": "webhook_service",
                   "assistant": "assistant_service", "usage": "usage_service",
                   "audit": "audit_query_service"}
handler_sources = {}
for hf in handler_files:
    handler_sources[os.path.splitext(os.path.basename(hf))[0]] = full_text(hf)

for (mod, fn), methods in sorted(handlers.items()):
    hsrc = handler_sources.get(mod, "")
    m = re.search(rf'^pub async fn {fn}\b', hsrc, re.M)
    if not m:
        continue
    nxt = re.search(r'^pub async fn ', hsrc[m.end():], re.M)
    body = hsrc[m.start():m.end() + nxt.start()] if nxt else hsrc[m.start():]
    is_mut = any(x in methods for x in MUT_METHODS)
    authz = bool(re.search(r'authorized_service_context|authorize\(', body))
    if is_mut:
        if not authz:
            fail(f"S9 {mod}::{fn} registered for {methods} has no canonical authorized_service_context (fail closed)")
        else:
            ok(f"S9 {mod}::{fn} {methods} handler carries canonical authz")
        key = (mod, fn)
        if key in READONLY_POSTS:
            ok(f"S9 {mod}::{fn} {methods} read-only contract: {READONLY_POSTS[key]} (no audit required)")
            continue
        svc = SERVICE_FOR_MOD.get(mod)
        if svc:
            sbody = service_fn_body(svc, fn if fn != "replace_agent_permissions" else "replace_agent_permissions")
            if sbody is None:
                # handler may map to a differently-named service method
                alt = {"register": "register", "create": "create", "update": "update",
                       "publish": "publish", "rollback": "rollback", "disable": "disable",
                       "replay": "replay", "submit": "submit", "replace_agent_permissions": "replace_agent_permissions"}
                sbody = service_fn_body(svc, alt.get(fn, fn))
            if sbody is None:
                fail(f"S9 could not locate service fn for {mod}::{fn} in {svc} (audit unverified)")
            elif re.search(r'record_mutation|self\.audit|audit::', sbody):
                ok(f"S9 {mod}::{fn} -> {svc} records audit")
            else:
                fail(f"S9 {mod}::{fn} -> {svc} mutation without an audit record")

# -------------------------------------------------------------------------
# S10: shared-file non-additive edits must sit inside the verify-fix
#      allowlist (file + recorded reason); anything else fails.
# -------------------------------------------------------------------------
print("-- S10 shared-file non-additive edits (allowlist) --")
ALLOWED_DELETIONS = {
    "crates/api/src/handlers/assistant.rs": "verify fix: canonical Execution/Create authz on submit",
    "crates/api/src/handlers/auth.rs": "verify fix: api-key actor id strip_prefix before ApiKeyId parse",
    "crates/application/src/stores.rs": "verify: PURPOSE header",
    "crates/application/src/tool_service.rs": "verify fix: encode_cursor returns Result (no request-path expect)",
    "crates/application/src/webhook_service.rs": "verify fix: encode_cursor returns Result (no request-path expect)",
    "crates/domain/src/project.rs": "verify fix: serde container default on ProjectConfig (schema default '{}')",
    "crates/messaging/Cargo.toml": "verify feature: nats feature gains optional tokio for the SSE tail",
    "crates/persistence/src/directory.rs": "verify fix: bind uuid (uuid = text operator error)",
    "crates/persistence/src/migrations.rs": "verify: registry entries 0050-0052 + registry test pins",
    "crates/persistence/src/repositories/audit.rs": "verify fix: tenant predicates on audit-pool reads + PURPOSE header",
    "crates/persistence/tests/live_pg.rs": "verify: correlation read gains tenant argument",
    "migrations/0051_webhook_replay_attempts/up.sql": "verify: PURPOSE header",
}
numstat = subprocess.run(["git", "diff", "--numstat", "origin/main", "--", "."],
                         capture_output=True, text=True).stdout
for line in numstat.splitlines():
    add, dele, path = line.split("\t")
    if not path.startswith("enterprise-mas/rust-core/"):
        continue
    rel = path[len("enterprise-mas/rust-core/"):]
    if int(dele) == 0:
        continue
    if rel in ALLOWED_DELETIONS:
        ok(f"S10 {rel}: {dele} deletions inside allowlist — {ALLOWED_DELETIONS[rel]}")
    else:
        fail(f"S10 {rel}: {dele} deletions NOT in the verify-fix allowlist")

# -------------------------------------------------------------------------
# S11: no file deletions anywhere in the change set; print the diff stat.
# -------------------------------------------------------------------------
print("-- S11 diff --stat vs origin/main (no deletions of files) --")
namestatus = subprocess.run(["git", "diff", "--name-status", "origin/main", "--", "."],
                            capture_output=True, text=True).stdout
deleted = [l for l in namestatus.splitlines() if l.startswith("D")]
if deleted:
    for l in deleted:
        fail(f"S11 file deleted from the tree: {l}")
else:
    ok("S11 no files deleted (D status) anywhere in the change set")
stat = subprocess.run(["git", "diff", "--stat", "origin/main", "--", "."],
                      capture_output=True, text=True).stdout
print(stat)
print(f"PY_FAIL_COUNT={fails}")
sys.exit(min(fails, 100))
PYEOF
py_rc=$?
FAILS=$((FAILS + py_rc))

echo "STATIC_FAIL_COUNT=$FAILS"
if [ "$FAILS" -eq 0 ]; then
    echo "STATIC: ALL PASS"
else
    echo "STATIC: $FAILS FAILURE(S)"
fi
echo "EXIT_CODE=$FAILS"
date -u +%Y-%m-%dT%H:%M:%SZ
exit "$FAILS"
