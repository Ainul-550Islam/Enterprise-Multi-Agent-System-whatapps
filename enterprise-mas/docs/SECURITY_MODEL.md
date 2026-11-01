# Security Model (English whitepaper)

Verified against `reports/SECURITY_NARRATIVE_BN.md` (Bangla) and the CODE.
Anything the code does not enforce is dropped here; nothing is implied.

## Enforced properties (each with implementing file)

1. **Bearer-token verification boundary** — `crates/security/src/tokens.rs`,
   `crates/security/src/jwks.rs`. Unauthenticated protected calls return 401
   with zero token material in errors (`response.rs` `stable_error`).
2. **Internal service HMAC / body-bound signatures** — Rust
   `crates/worker-service/src/agent_execution.rs` (`AgentBridgeSettings`,
   `require_internal_signing_secret`); Python
   `src/mas_orchestrator/main.py` (`build_bridge_server`,
   `require_internal_signature`). Production refuses unsigned calls.
3. **Tenant-scoped reads (foreign tenant → 404, never 403)** —
   `crates/application/src/execution_service.rs`, handler layer.
4. **Row-level security (RLS) armed per request** —
   `crates/persistence/src/transaction.rs`, `crates/persistence/src/rls.rs`.
5. **Secret references, never inline secrets** —
   `crates/security/src/secret_string.rs`, `crates/security/src/secret_gate.rs`;
   Python `config/secrets.py`.
6. **Audit-every-mutation (sink errors propagate)** —
   `crates/application/src/audit.rs`, `crates/application/src/audit_query_service.rs`.
7. **SSRF guard on outbound egress** — `crates/integrations` (guarded HTTP
   client, `EgressPolicy`: scheme/port/hostname allowlists, DNS pinning,
   redirect re-validation, response caps).
8. **Production fail-closed guardrails** —
   `crates/api/src/main.rs` (`enforce_production_guardrails`, production OIDC
   HTTPS validation); Python `main.py` production composition.

## Not enforced (dropped from BN narrative where code doesn't implement)

* SOC2 / ISO certification — not implemented; docs say "not implemented".
* Frontend / dashboard UI — not in code.
* WhatsApp channel — not in code.
* Stripe checkout — not in code.

## Supported versions

0.1.0 (current development line). Earlier releases unsupported.

## Reporting

See `SECURITY.md` at the repo root.
