# Architecture (verified against code)

This document describes the architecture exactly as implemented in
`enterprise-mas/rust-core` and `enterprise-mas/python-orchestrator`. Every
claim names the implementing file so it can be audited against source.

## Crate / module responsibilities

| Layer | Crate / module | Responsibility | File |
| ----- | -------------- | -------------- | ---- |
| Process edge | `crates/api` | Axum HTTP v1 + tonic gRPC servers, middleware, handlers, graceful shutdown | `crates/api/src/{main.rs,server.rs,router.rs,state.rs,handlers/*}` |
| Route table | `crates/api/src/router.rs` | Single HTTP route table; public / protected / internal envelopes | `crates/api/src/router.rs` |
| Governance routes | `crates/api/src/routes_governance.rs` | `/v1/auth/me`, `/v1/audit-events`, `/v1/policies`, `/v1/quotas`, `/v1/usage/summary` | `crates/api/src/routes_governance.rs` |
| Tooling routes (Batch 02) | `crates/api/src/routes_tooling.rs` | `/v1/tools`, `/v1/connector-manifests`, `/v1/stream`, `/v1/assistant/messages`, webhook deliveries | `crates/api/src/routes_tooling.rs` |
| Use cases | `crates/application` | Tenancy, agents, workflows, executions, schedules, api-keys, policies, quotas, usage, analytics, audit, connectors, webhooks, tools, oauth | `crates/application/src/*_service.rs` |
| Security plane | `crates/security` | Token verifiers, JWKS, API keys, internal-service HMAC, authorization, RLS context | `crates/security/src/*.rs` |
| Tenancy | `crates/tenancy` | Org → tenant → project hierarchy, isolation | `crates/tenancy/src/*.rs` |
| Persistence | `crates/persistence` | SQLx pool, transactions, migration runner (FNV-1a ledger, advisory lock), 19 repositories, RLS | `crates/persistence/src/*.rs` |
| Migrations | `migrations/` | 53 additive migrations (0001–0053), no DROP/TRUNCATE | `migrations/NNNN_*/up.sql` |
| Messaging | `crates/messaging` | BrokerPort; NATS/JetStream adapter behind `nats` feature | `crates/messaging/src/*.rs` |
| Integrations | `crates/integrations` | Connector registry, SSRF-guarded HTTP, webhooks, OAuth | `crates/integrations/src/*.rs` |
| Observability | `crates/observability` | JSON logging w/ redaction, metrics, tracing, health registry | `crates/observability/src/*.rs` |
| Domain | `crates/domain` | Pure aggregates + value objects | `crates/domain/src/*.rs` |
| Contracts | `crates/contracts` | Wire DTOs, envelopes, `StableApiError` | `crates/contracts/src/*.rs` |
| Common | `crates/common` | `AppError`/`Result`, 39 typed UUIDv7 IDs, Timestamp, pagination | `crates/common/src/*.rs` |
| Python orchestrator | `python-orchestrator` | Agent runtime, providers, tools, memory, RAG, guardrails, gRPC AgentBridge server | `src/mas_orchestrator/{agents,providers,tools,memory,guardrails,grpc}/*.py` |

## Rust-vs-Python ownership

**Rust (authoritative):** user/service identity, tenant scope, authorization,
policy, quota, execution/task state, audit, platform-owned persistence, gRPC
server boundary. Python never reads or writes Rust-owned tables.

**Python (AI execution only):** agent runtime, prompt assembly, model-provider
adapters, tools, memory, guardrails, RAG plane. Reached exclusively through the
versioned gRPC `AgentBridgeApi` protocol.

## Data flow of one execution

1. Client submits `POST /v1/projects/{project_id}/executions` (api crate).
2. `ExecutionService` authorizes (resolved tenant from scope, never request
   header), checks idempotency key, persists execution, audits.
3. Policy + quota enforced **before** consumption (`ControlPlane` →
   `PolicyService` → `QuotaEngine`).
4. Execution queued to outbox / task broker (NATS JetStream `MAS_TASKS` or
   Postgres polling lane, runtime-selected via `broker.kind`).
5. Worker leases task, calls Python bridge `RunAgent` over gRPC with
   body-bound HMAC signature + bearer token.
6. Python executes agent, returns typed verdict + usage.
7. Worker settles (ack + row-complete on success; DLQ on delivery ceiling).
8. Usage metered and recorded **after** consumption; audit event appended.
9. Analytics rollups reflect the execution; SSE `execution.completed` delivered
   to the owning tenant's stream channel only.

## Tenancy / RLS design

Every request resolves a tenant from the authenticated scope (middleware +
application layer), never from path/body. `mas_persistence` arms row-level
security inside a single transaction per request (`crates/persistence/src/
transaction.rs`, `rls.rs`). Foreign-tenant reads answer 404, never 403
(`handlers` + `authorized_service_context`).

## Files that deviate

* `--dev-inmemory` lane exists for local bring-up only (explicit flag);
  production refuses to start stateless (`crates/api/src/main.rs`).
* Python providers.mode `record`/`replay` rejected in production
  (`config/validation.py`).
