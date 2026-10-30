# Changelog

All notable changes to enterprise-mas are documented in this file. Format follows
Keep a Changelog (https://keepachangelog.com/en/1.0.0/).

## [1.0.0] — 2026-10-30

Derived from migrations 0001–0053 and the implemented crates/modules in
`enterprise-mas/rust-core` plus `enterprise-mas/python-orchestrator`.

### Added
- Rust core workspace: 22 crates (common, domain, contracts, orchestration,
  execution, policy, security, tenancy, events, messaging, persistence,
  scheduling, quota, integrations, observability, application, api, worker,
  scheduler-service, worker-service, cli, ai-bridge)
- HTTP v1 API surface in `crates/api` with stable `ApiEnvelope` response
  contract and `StableApiError` error contract
- Tenancy: organizations, tenants, projects, users, memberships (register + list)
- Agent lifecycle: register, get, list, versions, publish, rollback
- Workflows: create, get, graph update, publish, disable, versions, rollback
- Executions: submit (idempotent), list, get, steps, start/pause/resume/cancel
- Schedules: register, get, pause, resume, disable
- API keys: create, list, revoke, rotate (metadata only; raw material once)
- Policies: CRUD, publish, rollback, dry-run evaluate
- Quotas & usage summary (tenant-scoped, read-only)
- Analytics overview / performance / tasks-by-agent / usage-cost / insights
- Audit events (keyset-paginated, redacted)
- Connectors: register, list, get, probe, disable; connector manifests
- Webhooks: register, list, get, enable/disable, deliveries, replay
- OAuth authorization callback + connector-bound authorize
- Inbound webhook endpoints (public HMAC-verified ingestion)
- Tools: register, get, list, update, agent tool-permissions
- SSE stream (`/v1/stream`) with per-tenant/per-principal connection caps
- Governance: `/v1/auth/me`, `/v1/audit-events`, `/v1/policies`, `/v1/quotas`,
  `/v1/usage/summary`
- SSE stream (`/v1/stream`) with per-tenant/per-principal connection caps
- Governance: `/v1/auth/me`, `/v1/audit-events`, `/v1/policies`, `/v1/quotas`,
  `/v1/usage/summary`
- Persistence: SQLx pool, RLS context, 53 migrations (0001–0053), migration
  runner with FNV-1a checksum ledger and advisory-lock apply
- Worker: lease-fenced consumer, retry with full jitter, delivery-ceiling DLQ,
  graceful shutdown
- Scheduler-service: due-schedule scanner, lease-based exactly-once run ledger
- gRPC: `mas.api.v1` services (tenancy, agents, workflows, executions, schedules)
- Python AI orchestrator: agent runtime, providers, tools, memory, RAG plane,
  guardrails, gRPC AgentBridge server
- Cross-language Rust <-> Python AgentBridge end-to-end tests (12)
- CI workflows: `ci.yml` (rust, bridge-e2e, python, openapi-parity,
  docker-build, compose-smoke) and `security.yml` (deny, audit, pip-audit,
  gitleaks, trivy)
- Deployment: Dockerfiles (api, worker, scheduler, migrate, orchestrator),
  compose (postgres/pgvector + nats + migrate + api + worker + scheduler +
  orchestrator), Helm chart
- Release artifacts: LICENSE (proprietary template), SECURITY.md, THIRD_PARTY_NOTICES.md,
  CHANGELOG.md, README.md, repo-cleanup.sh, gen-third-party-notices.sh

### Not included
- Frontend / dashboard UI (not in code)
- WhatsApp channel (not in code)
- Stripe checkout (not in code)

### Test results (from ci-evidence logs)
- `cargo fmt --all -- --check`: PASS
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: PASS
- `cargo test --workspace --locked`: 1148 passed, 0 failed
- `cargo test --workspace --locked --features nats`: 1148 passed, 0 failed
- `cargo test -p mas-ai-bridge --test e2e_python --locked -- --ignored`: 12 passed, 0 failed
- `cargo deny check`: PASS
- `python-orchestrator/scripts/check.sh`: 2003 passed, 2 skipped, 0 failed
