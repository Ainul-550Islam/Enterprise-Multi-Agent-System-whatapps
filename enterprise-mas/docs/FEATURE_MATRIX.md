# Feature matrix

This matrix maps selected implemented capabilities to source and tests in this repository. A file reference indicates where to inspect the behavior; it does not by itself establish production readiness or certification.

| Capability | Implementation | Test evidence |
|---|---|---|
| HTTP API routing and OpenAPI parity | `rust-core/crates/api/src/router.rs`, `rust-core/crates/api/src/openapi_bundle.rs` | `rust-core/crates/api/tests/openapi_parity.rs` |
| Tenant-scoped identity and access | `rust-core/crates/security/`, `rust-core/crates/tenancy/` | `rust-core/crates/api/tests/` and `rust-core/crates/security/tests/` |
| Migration registration and execution | `rust-core/crates/persistence/src/migrations.rs` | `rust-core/crates/persistence/src/migrations.rs` unit tests; registry directories are also checked by `scripts/dev/verify_manifest.py` |
| WhatsApp channel payloads and signature checks | `rust-core/crates/channel-whatsapp/src/` | `rust-core/crates/channel-whatsapp/tests/` |
| Stripe billing integration | `rust-core/crates/billing-stripe/src/`, `rust-core/crates/application/src/billing_service.rs` | `rust-core/crates/billing-stripe/tests/`, plus persistence and API tests |
| Python orchestration runtime | `python-orchestrator/src/mas_orchestrator/` | `python-orchestrator/tests/` |
| Python SDK | `sdk/python/mas_sdk/` | `sdk/python/tests/` |
| TypeScript SDK | `sdk/typescript/src/` | `sdk/typescript/tests/` |
| Operations dashboard | `web/src/app/`, `web/src/components/` | `web/tests/unit/`, `web/tests/e2e/` |
| Sandboxed code execution | `sandbox-runner/runner.py` | `sandbox-runner/tests/` |

## Verification caveat

The full Rust workspace, live-Postgres tests, and web test/build gates have not been run in the current environment. The feature references above must be checked against a successful run of the relevant CI jobs before a release claim is made. The repository manifest check may report unregistered migration directories; only registered migrations are applied by the runner.
