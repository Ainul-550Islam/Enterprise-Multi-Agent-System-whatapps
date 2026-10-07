# Selected `[GAP]` Implementation Report

**Workspace:** `/home/user/enterprise-mas-audit/enterprise-mas/`  
**Target specification:** `/home/user/uploads/ENTERPRISE_MAS_FULL_TREE.md`  
**Audit date:** 2026-10-07  
**Scope:** the attachment's 44 `[GAP]` entries. The 341 `[NEW]` entries and the full 918-file future tree were not treated as implementation scope.

## Result

The selected existing-file gap work is implemented and checked. Existing domain/application/persistence contracts were reused; changes avoid placeholder handlers and do not create the unrelated `[NEW]` product surface. The earlier six-spec audit and full Module 1 source delivery remain available separately; this report records the selected tree-gap delivery and its verification boundary.

## Security and versioning closure

- Added route-level authorization-matrix coverage for the agent/workflow version, rollback/history, and execution-step timeline routes. The shared in-memory fixture now creates, validates, and publishes a real start-to-end workflow graph before submitting an execution against it.
- The matrix asserts unauthenticated requests are rejected with 401, foreign-tenant resources are hidden with 404, Viewer reads are allowed, and Viewer mutation/history actions are denied with 403.
- Wired canonical authorization checks on the new mutation/read-history route paths. Removed the obsolete, unused rollback `scoped()` helper.
- Disambiguated the membership and execution-step store `save` calls in the router tests now that both traits are in scope. Corrected the headerless request assertion: a principal with exactly one authoritative tenant binding resolves that tenant without caller-supplied scope metadata.
- Workflow publish snapshots and rollback records remain append-only and tenant scoped. Execution-step upsert preserves the first persisted row ID for an execution ordinal, and the HTTP timeline reads persisted steps in deterministic order.

## API-key prefix lookup under forced RLS

- Added migration `0032_api_key_prefix_lookup` with a narrow, SELECT-only policy keyed by the public fixed-width prefix. It does not disable RLS, trust tenant headers, or grant `BYPASSRLS`.
- The repository binds the prefix in a dedicated transaction-local setting and verifies the full digest in the application before establishing identity. Last-used updates are tenant-filtered and use the request's scoped transaction when available; the pre-scope verifier path arms a tenant setting from the stored key row before writing.
- SQL contract tests assert the prefix-only policy, real query binding/transaction use, and tenant predicate on the last-used update.

## Python bridge/Data Hub quality and deliverable checks

- The S3-compatible Data Hub listing parser now uses `defusedxml`; its dependency is declared and included in the bootstrap toolchain. Signed HTTP response typing is explicit, and type-only imports no longer become runtime requirements.
- Python bootstrap/check scripts use paths relative to the checked-out workspace instead of stale `/home/user/...` locations. The check script rebuilds and verifies the complete Module 1 source document.
- Regenerated `/home/user/enterprise-mas-audit/RUST_PYTHON_BRIDGE_PHASE16_FILES.md`: 36 complete source files, 15,329 source lines, verified byte-for-byte against the working tree. No abbreviated file bodies are used.

## Verification

Passed:

- `source .cargo-env && cargo fmt --all --check`
- `source .cargo-env && cargo test --workspace --locked`
- `source .cargo-env && cargo clippy --workspace --all-targets --locked -- -D warnings`
- `source .cargo-env && cargo test -p mas-ai-bridge --test e2e_python --locked -- --ignored` — 12 passed, real Rust-to-Python gRPC test server.
- `source .cargo-env && cargo test -p mas-api --test security_matrix --features testutils --locked` — 29 passed.
- `source .cargo-env && cargo test -p mas-api --lib --locked` — 45 passed.
- `source .cargo-env && cargo test -p mas-persistence --lib --locked` — 98 passed.
- `source .cargo-env && cargo test -p mas-persistence --test tenant_sql_contract --locked` — 12 passed.
- `bash scripts/check.sh` in `python-orchestrator/` — Ruff format/lint passed, strict mypy passed for 170 source files, pytest passed (1,970), compileall passed, full-deliverable build and byte-for-byte check passed (36/36).

## Verification boundary

`MAS_TEST_DATABASE_URL` was unset. PostgreSQL `live_pg` tests compile and are present, but their database-backed assertions self-skip without that variable; actual PostgreSQL role/RLS enforcement was therefore **not** live-certified in this run. Rust's default workspace run also ignores Python-dependent bridge tests by design; the separate command above ran all 12 cross-language AgentBridge E2E cases successfully. No live external model provider or production database was used.

## Scope boundary

The tree's `[NEW]` feature families were not added. In particular, this delivery does not claim to provide the full new analytics, approvals, tools, connectors, webhook, assistant, search, notification, billing-provider, or frontend product surfaces. The six-spec audit report remains the separate source of record for broader production risks and limitations outside this selected gap scope.
