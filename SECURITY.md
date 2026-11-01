# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.0   | :white_check_mark: (current development line) |

Earlier releases are not supported.

## Reporting a Vulnerability

Security researchers: please report suspected vulnerabilities privately to the
platform security contact listed in the repository (do not file a public GitHub
issue for a potential security problem). Include:

* a description of the issue and the affected component(s);
* steps to reproduce, with minimal request/response examples where applicable;
* the version / commit affected;
* any proposed mitigation, if you have one.

Reports are acknowledged within 5 business days. Validated reports receive a
remediation timeline and credit (unless you ask to remain anonymous).

## Security properties actually enforced in code

Each property below names the implementing file so it can be audited against the
source rather than taken on faith. Anything not listed here is **not** enforced.

| Property | File(s) that implement it |
| -------- | ------------------------- |
| Bearer-token verification boundary | `enterprise-mas/rust-core/crates/security/src/tokens.rs`, `crates/security/src/jwks.rs` |
| Internal service HMAC / body-bound signatures | `enterprise-mas/rust-core/crates/worker-service/src/agent_execution.rs` (`AgentBridgeSettings`, `require_internal_signing_secret`), `crates/security/src/internal_service.rs` |
| Tenant-scoped reads (foreign tenant -> 404, never 403) | `enterprise-mas/rust-core/crates/application/src/execution_service.rs`, handler layer |
| Row-level security (RLS) armed per request | `enterprise-mas/rust-core/crates/persistence/src/transaction.rs`, `crates/persistence/src/rls.rs` |
| Secret references, never inline secrets | `enterprise-mas/rust-core/crates/security/src/secret_string.rs`, `crates/security/src/secret_gate.rs`; Python `config/secrets.py` |
| Audit-every-mutation (sink errors propagate) | `enterprise-mas/rust-core/crates/application/src/audit.rs`, `crates/application/src/audit_query_service.rs` |
| SSRF guard on outbound egress | `enterprise-mas/rust-core/crates/integrations` (guarded HTTP client, `EgressPolicy`) |
| Production fail-closed guardrails | `enterprise-mas/rust-core/crates/api/src/main.rs` (`enforce_production_guardrails`, production OIDC HTTPS validation) |
| Python production composition fail-closed | `enterprise-mas/python-orchestrator/src/mas_orchestrator/main.py` (`build_bridge_server`, `require_internal_signature`) |
| Key rotation entry points | `enterprise-mas/rust-core/crates/api/src/handlers/api_keys.rs` (`/v1/api-keys/{id}/rotate`), `crates/security/src/jwks.rs` |

Not implemented (stated plainly, not implied): SOC2/ISO certification, a frontend/dashboard, a WhatsApp channel, Stripe checkout.
