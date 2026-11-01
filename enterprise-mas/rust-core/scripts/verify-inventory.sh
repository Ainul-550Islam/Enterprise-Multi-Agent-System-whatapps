#!/usr/bin/env bash
# PURPOSE: Phase A inventory gate — every Batch 01/02 file must exist, be
# non-trivial, and start with the `//! PURPOSE:` header; wiring must be present.
# Emits raw PASS/FAIL lines; exit 1 on any FAIL.
set -uo pipefail
cd "$(dirname "$0")/.."
fail=0
chk() { # chk <path> <min-lines>
  local f="$1" min="$2"
  if [ ! -f "$f" ]; then echo "FAIL missing: $f"; fail=1; return; fi
  local n; n=$(wc -l < "$f")
  if [ "$n" -lt "$min" ]; then echo "FAIL trivial($n<$min lines): $f"; fail=1; return; fi
  if ! head -1 "$f" | grep -qE '^(//!|--|#) *PURPOSE:'; then
    echo "FAIL header: $f first line -> $(head -1 "$f" | cut -c1-60)"; fail=1; return
  fi
  echo "PASS $f ($n lines, PURPOSE header)"
}
echo "== Batch 01 inventory =="
chk crates/api/src/handlers/auth.rs 50
chk crates/api/src/handlers/audit.rs 40
chk crates/api/src/handlers/policies.rs 50
chk crates/api/src/handlers/usage.rs 40
chk crates/api/src/routes_governance.rs 30
chk crates/api/tests/governance_routes.rs 50
chk crates/application/src/audit_query_service.rs 80
chk crates/application/src/policy_service.rs 80
chk crates/application/src/usage_service.rs 60
chk crates/persistence/src/repositories/usage.rs 80
chk crates/persistence/tests/usage_live_pg.rs 50
chk crates/persistence/src/repositories/audit.rs 80
chk crates/security/tests/jwks.rs 50
echo "== Batch 02 inventory =="
chk crates/api/src/handlers/tools.rs 80
chk crates/api/src/handlers/connectors.rs 60
chk crates/api/src/handlers/webhooks.rs 60
chk crates/api/src/handlers/stream.rs 150
chk crates/api/src/handlers/assistant.rs 40
chk crates/api/src/routes_tooling.rs 30
chk crates/api/tests/tooling_routes.rs 50
chk crates/application/src/tool_service.rs 150
chk crates/application/src/connector_service.rs 80
chk crates/application/src/webhook_service.rs 80
chk crates/application/src/stores.rs 300
chk crates/persistence/src/repositories/tools.rs 150
chk crates/persistence/tests/tools_live_pg.rs 50
chk crates/persistence/tests/analytics_live_pg.rs 50
chk crates/persistence/tests/rollback_live_pg.rs 50
chk config/default.toml 100
chk config/production.toml 100
chk migrations/0050_audit_query_indexes/up.sql 5
chk migrations/0050_audit_query_indexes/down.sql 3
chk migrations/0051_webhook_replay_attempts/up.sql 10
chk migrations/0051_webhook_replay_attempts/down.sql 5
echo "== Wiring present =="
w() { # w <desc> <grep-pattern> <file>
  if grep -q "$2" "$3"; then echo "PASS wiring: $1"; else echo "FAIL wiring: $1 ($2 not in $3)"; fail=1; fi
}
w "pub mod auth/audit/policies/usage"   "^pub mod auth;$" crates/api/src/handlers/mod.rs
w "pub mod audit handler"               "^pub mod audit;$" crates/api/src/handlers/mod.rs
w "pub mod policies handler"            "^pub mod policies;$" crates/api/src/handlers/mod.rs
w "pub mod usage handler"               "^pub mod usage;$" crates/api/src/handlers/mod.rs
w "pub mod tools"                       "^pub mod tools;$" crates/api/src/handlers/mod.rs
w "pub mod connectors"                  "^pub mod connectors;$" crates/api/src/handlers/mod.rs
w "pub mod webhooks"                    "^pub mod webhooks;$" crates/api/src/handlers/mod.rs
w "pub mod stream"                      "^pub mod stream;$" crates/api/src/handlers/mod.rs
w "pub mod assistant"                   "^pub mod assistant;$" crates/api/src/handlers/mod.rs
w "routes_governance module"            "^pub mod routes_governance;$" crates/api/src/lib.rs
w "routes_tooling module"               "^pub mod routes_tooling;$" crates/api/src/lib.rs
w "merge routes_governance"             "routes_governance::routes()" crates/api/src/router.rs
w "merge routes_tooling"                "routes_tooling::routes()" crates/api/src/router.rs
w "AppState audit_query field"          "pub audit_query: Option<Arc<AuditQueryService>>" crates/api/src/state.rs
w "AppState policies field"             "pub policies: Option<Arc<PolicyService>>" crates/api/src/state.rs
w "AppState usage field"                "pub usage: Option<Arc<UsageService>>" crates/api/src/state.rs
w "AppState tools field"                "pub tools: Option<Arc<ToolService>>" crates/api/src/state.rs
w "AppState connector_admin field"      "pub connector_admin: Option<Arc<ConnectorService>>" crates/api/src/state.rs
w "AppState webhook_admin field"        "pub webhook_admin: Option<Arc<WebhookService>>" crates/api/src/state.rs
w "AppState stream_hub field"           "pub stream_hub: Option<Arc<StreamHub>>" crates/api/src/state.rs
w "with_audit_query builder"            "pub fn with_audit_query" crates/api/src/state.rs
w "with_policies builder"               "pub fn with_policies" crates/api/src/state.rs
w "with_usage builder"                  "pub fn with_usage" crates/api/src/state.rs
w "with_tools builder"                  "pub fn with_tools" crates/api/src/state.rs
w "with_connector_admin builder"        "pub fn with_connector_admin" crates/api/src/state.rs
w "with_webhook_admin builder"          "pub fn with_webhook_admin" crates/api/src/state.rs
w "with_stream_hub builder"             "pub fn with_stream_hub" crates/api/src/state.rs
w "app lib: audit_query_service mod"    "pub mod audit_query_service" crates/application/src/lib.rs
w "app lib: policy_service mod"         "pub mod policy_service" crates/application/src/lib.rs
w "app lib: usage_service mod"          "pub mod usage_service" crates/application/src/lib.rs
w "app lib: tool_service mod"           "pub mod tool_service" crates/application/src/lib.rs
w "app lib: connector_service mod"      "pub mod connector_service" crates/application/src/lib.rs
w "app lib: webhook_service mod"        "pub mod webhook_service" crates/application/src/lib.rs
w "persistence lib re-export toolstore" "PostgresToolStore" crates/persistence/src/lib.rs
w "persistence lib re-export usagestore" "PostgresUsageStore" crates/persistence/src/lib.rs
w "repositories mod usage"              "^pub mod usage;$" crates/persistence/src/repositories/mod.rs
w "repositories mod tools"              "^pub mod tools;$" crates/persistence/src/repositories/mod.rs
w "composition: ToolService built"      "ToolService::new" crates/api/src/main.rs
w "composition: ConnectorService built" "ConnectorService::new" crates/api/src/main.rs
w "composition: WebhookService built"   "WebhookService::new" crates/api/src/main.rs
w "composition: StreamHub built"        "StreamHub::new" crates/api/src/main.rs
w "composition: AuditQueryService built" "AuditQueryService::new" crates/api/src/main.rs
w "composition: PolicyService built"    "PolicyService::new" crates/api/src/main.rs
w "composition: UsageService built"     "UsageService::new" crates/api/src/main.rs
w "config default [stream] heartbeat_secs" "^heartbeat_secs" config/default.toml
w "config production [stream] channel_buffer" "^channel_buffer" config/production.toml
w "config default [stream] max_connections_per_principal" "^max_connections_per_principal" config/default.toml
echo "== forbidden markers in batch files =="
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
if grep -nE "todo!\(|unimplemented!\(|TODO|FIXME|XXX|placeholder" $BATCH_FILES 2>/dev/null; then
  echo "FAIL forbidden markers found (above)"; fail=1
else
  echo "PASS no todo!/unimplemented!/TODO/FIXME/XXX/placeholder in batch files"
fi
if [ $fail -eq 0 ]; then echo "INVENTORY: ALL PASS"; else echo "INVENTORY: FAILURES PRESENT"; fi
exit $fail
