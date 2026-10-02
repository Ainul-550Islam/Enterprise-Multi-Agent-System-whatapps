# rust-core

The high-performance, security-critical core of **enterprise-mas** (Enterprise Multi-Agent System). It owns orchestration, execution, security, tenancy, policy, quota, eventing, and persistence.

> **Hard rule — no LLM/provider business logic in rust-core.**
> Model selection, prompt assembly, provider SDK calls, streaming token parsing, and LLM cost logic live in the **Python orchestrator**, which communicates with rust-core exclusively over the versioned gRPC protocol. Rust never imports provider SDKs and never interprets model semantics.

---

## 1. Architecture boundaries

```
┌────────────────────────────── rust-core (this workspace) ──────────────────────────────┐
│                                                                                         │
│   api ─────────────┐        ┌────────────── application services (use cases) ────────┐  │
│   worker ──────────┼──────▶ │  agent · workflow · task · execution · tool · policy   │  │
│   scheduler-service┘        │  quota · usage · audit · schedule · webhook · health   │  │
│                             └───────┬───────────────────────────────┬───────────────┘  │
│        ┌────────────────────────────▼───┐                    ┌──────▼───────┐          │
│        │ orchestration │ execution      │                    │ domain (pure)│          │
│        │ engine · dispatcher · runtime  │                    │ aggregates + │          │
│        │ retry · checkpoint · locks     │                    │ invariants   │          │
│        └───────┬───────────────┬────────┘                    └──────▲───────┘          │
│                │               │                                     │                  │
│   policy · security · tenancy · quota ──► enforcement planes  ◄──────┘                  │
│                │               │                                                        │
│   persistence (PostgreSQL/RLS) │   events+outbox   messaging (NATS/gRPC)                │
│                                                                                         │
└───────────────┬───────────────────────────────┬─────────────────────────────────────────┘
                │ versioned gRPC (protobuf)     │ signed events / webhooks
        ┌───────▼────────┐              ┌───────▼────────┐
        │ Python         │              │ External       │
        │ orchestrator   │              │ systems        │
        │ (agents/LLM)   │              │ (connectors)   │
        └────────────────┘              └────────────────┘
```

**Dependency rules (compile-enforced):**

* `common` depends on nothing internal.
* `domain` depends only on `common` (+ pure data libs). It is forbidden from touching HTTP, SQL, brokers, or the network.
* `contracts` depends only on `common`. It defines the wire surface (DTOs, envelopes, `StableApiError`).
* Infrastructure crates (`persistence`, `messaging`, `integrations`) depend on `domain`/`contracts`, never the reverse.
* `application` composes domain + infrastructure into use cases; `api`/`worker`/`scheduler-service` are thin processes around it.

## 2. Crate responsibilities

| Crate | Responsibility |
|---|---|
| `crates/common` | `AppError`/`Result`, 27 typed UUIDv7 IDs, UTC `Timestamp`, shared enums, cursor pagination, validation, constants, secret/PII redaction. |
| `crates/domain` | Pure aggregates (organization → session) + value objects (`Email`, `Slug`, `SafeUrl`, `SemanticVersion`, `TokenBudget`, `ResourceLimits`). Business invariants only. |
| `crates/contracts` | Versioned API DTOs, request/response envelopes, auth claims, health/usage DTOs, stable error contract. |
| `crates/orchestration` | `OrchestrationEngine`, dispatcher, scheduler, workflow runtime (DAG), retry/timeout/cancellation/idempotency/checkpoint/DLQ/locks, `RuntimeContext`. |
| `crates/execution` | Executor, step/action/tool runners, agent bridge (Python gRPC), result processing, output validation, resource guard, sandbox, failure handling. |
| `crates/policy` | Policy engine, rule evaluator, conditions, decisions, context, loader, cache, decision audit. |
| `crates/security` | Authenticators, RBAC/ABAC, API keys, JWT, encryption, secrets, hashing, signatures, nonces, tenant guard, PII, secure headers. |
| `crates/tenancy` | TenantContext/resolver, org context, isolation enforcer, runtime limits, feature flags, environments, HTTP middleware. |
| `crates/events` | Event envelope/types, publisher/subscriber, dispatcher, consumer runner, dedup, transactional outbox, DLQ. |
| `crates/messaging` | NATS/JetStream, gRPC helpers/interceptors, codecs, header propagation, connection lifecycle, broker health. |
| `crates/persistence` | SQLx pool, transactions, migrations runner, 19 repositories, row models, RLS context. |
| `crates/scheduling` | Scheduler loop, fair priority queue, cron, delayed queue, leases, recovery. |
| `crates/quota` | Rate limiter, concurrency limiter, token meter, quota engine, atomic counter store, overage policy. |
| `crates/integrations` | Connector registry, hardened HTTP client (SSRF), webhooks, OAuth, DB/storage/messaging/CRM/ticketing/email connectors. |
| `crates/observability` | JSON logging w/ redaction, metrics, OpenTelemetry, correlation, audit telemetry, health registry. |
| `crates/application` | Use-case services for every aggregate + internal DTO mapping. |
| `crates/api` | Axum HTTP + tonic gRPC servers, middleware stack, handlers, graceful shutdown. |
| `crates/worker` | Task consumer process: leases, retries, heartbeats, graceful shutdown. |
| `crates/scheduler-service` | Due-schedule scanner producing tasks with idempotency keys. |
| `crates/cli` | Admin CLI: migrate, tenant, agent, workflow, policy, replay, health. |

> **Status:** `common`, `domain`, `contracts`, `orchestration`, `execution`, `policy`, `security`, `tenancy`, `events`, `messaging`, `persistence`, `scheduling`, `quota`, `integrations`, `observability`, `application` and `api` are implemented, formatted, clippy-clean and unit-tested (372 tests passing; `cargo test --workspace --locked` green). The persistence crate ships the **12 SQL migrations** (`migrations/NNNN_name/up.sql`: extensions → tenancy → identity → agents → workflows → execution → policy → billing → schedules → integrations → audit → outbox) with RLS policies on every tenant-owned table, fail-closed composite policies, and append-only enforcement triggers for `audit_events`/`policy_decisions`; the `MigrationRunner` embeds the registry at compile time (FNV-1a checksums), applies pending migrations transactionally in strict order, and refuses DB-ahead / gap / checksum-drift states; the redacted-`DatabaseUrl` pool config, `UnitOfWork`/`atomic` transactions, the fail-closed `RlsContext` (`app.current_tenant`/`app.current_org`/`app.current_user` GUCs as bound `set_config` calls), the row↔domain serde bridge re-running domain invariants on hydration (`Organization`/`Tenant`/`Project`/`User`/`Membership`/`OutboxRecord`/`AuditEvent`), and the first repositories — `PostgresOutboxStore` (transactional `enqueue_in`, `SKIP LOCKED` dispatcher fetch), `PostgresAuditLog` (append-only-by-API + keyset pagination), `ScopedStore` (whitelisted document tables + scoped transactions), and the tenant-facing CRUD stores — `OrganizationStore`, `TenantStore` (slug routing lookup, org catalog), `ProjectStore`, `UserStore` (self-service profile reads, privileged login-path email lookup), `MembershipStore` (resolver queries) — with transactional `*_in` writes that join the enclosing unit of work and `get_*_scoped` reads that refuse mismatched RLS scopes before any SQL. The messaging crate is the wire floor everything else stands on: the size-capped `JsonEventCodec` (malformed/oversize frames are non-retriable by construction), validated `HeaderSet`s and correlation/causation/tenant `PropagationContext`, NATS-shaped `Subject`/`SubjectFilter` with exact wildcard semantics, the I/O-free `ConnectionController` lifecycle state machine with bounded backoff reconnection, the JetStream-shaped `BrokerPort` with a full-semantics `InMemoryBroker` (idempotent publish via message ids, ack/nak/term/in-progress ack windows, `max_deliver` → dead-letter), dependency-free gRPC helpers (canonical code mapping from `AppError`, metadata rules, `grpc-timeout` codec, W3C traceparent, credential-excluding header forwarding), and a sanitized `BrokerHealthProbe` for readiness endpoints. The tenancy crate is the multi-tenant isolation spine: the **Organization → Tenant → Project** hierarchy with write-time ownership coherence (cross-linked projects are impossible), the `TenantResolver` turning (principal, requested ids) into a proven `ResolvedTenantContext` (cross-tenant existence hidden as NotFound, suspended tenants/memberships fail closed, effective roles = union of active org/tenant memberships), the `IsolationService` re-checking every scoped object at trust boundaries with violation counters/audit reports, and the entitlement layer (plan matrix + contract overrides, numeric caps, suspended/cancelled subscriptions deny before quota ever runs). The events crate provides the domain-event plumbing: the immutable `EventEnvelope` (validated dotted event type, required correlation id, optional causation/actor), the `InMemoryEventBus` reference pub/sub with exact/prefix/any filters and per-consumer delivery stats, the transactional **outbox** (`OutboxStorePort` + `OutboxDispatcher` with capped exponential backoff and max-attempts → dead-letter), and `BusEventPublisher` bridging the orchestration `EventPublisher` trait onto the bus. The scheduling crate turns `domain::schedule` definitions into dispatched runs: the fair due scanner (`DueScanner` + per-tenant round-robin interleave), pure catch-up (Skip/Latest/Bounded, backlog-hardened enumeration caps) and overlap (Forbid/Allow) policies, the fencing-token `LeaseStorePort` (+ in-memory reference: expiry, renew-by-token, reap), the dedup'd capacity-bounded `DelayedQueue`, the `ScheduleStorePort` with exactly-once-per-tick run ledgers, and the deterministic `SchedulerRuntime` — scan → lease → catch-up → overlap → enqueue → dispatch with explicit `tick(now)`/startup `recover` (expired-lease reaping), all composable by `scheduler-service`. The quota crate is the usage-metering floor the execution loop's `quota (reserve) → … → release quota` bracket stands on: the bucket-addressed `CounterStorePort` (window-bucketed counters with saturating gauges, GC for expired windows, in-memory reference), the increment-first fixed-window `RateLimiter` whose rejections count against the limit and surface `retry_after` hints, the permit-based `ConcurrencyLimiter` (TTL'd permits in a ledger, exactly-once release semantics, expired-permit sweeping that frees capacity without double-subtracting), the lazy-refill token-bucket `TokenMeter` (no partial spends, deficit-time hints, accrued refill persisted on rejection so refill time is never silently burned), and the `QuotaEngine` turning registered per-tenant `Quota` definitions into enforcement-aware consumption decisions — over-limit consumption under `Enforce` is rolled back with a retry hint, soft modes are flagged with `overage_units`, unregistered dimensions metered-but-unlimited — plus the `EngineQuotaBridge` wiring it into orchestration's `QuotaPort`. The integrations crate is the safe-external-systems plane: versioned provider manifests (8 builtins across database/storage/messaging/CRM/ticketing/email/webhook) driving tenant connector installation through the `ConnectorFactory` (manifest lookup → auth-mode check → config key contract → category rules, with a blanket no-secret-material-in-config rule), the tenant-scoped connector registry hiding cross-tenant existence, SSRF-hardened egress (`EgressPolicy` scheme/port/hostname allowlists, conservative DNS-resolution pinning, redirect-hop re-validation of every `Location`, response body caps, `GuardedHttpClient` over the `HttpClientPort` real stacks bind to), HMAC-SHA256 webhooks both ways — fail-closed inbound verification (two-sided replay window, constant-time compare) and a store-and-forward outbound dispatcher (idempotent enqueue per endpoint+event, signing at dispatch, exponential backoff re-anchored on the caller's clock, `Retry-After` honouring, exhaustion dead-letter, endpoint circuit breaker) — the OAuth 2.0 authorization-code + PKCE core (one-shot TTL-bound state sessions, zeroized redacted token bundles, proactive expiry skew), and connection probes advancing the connector lifecycle with sanitized errors. The observability crate is the telemetry plane everything else reports through: JSON log events with key-aware mandatory redaction (sensitive keys replaced wholesale via the shared SecretRedactor, email-shaped values neutralized, AppError enrichment limited to the public surface), a cardinality-guarded metrics registry (validated families/labels, 10k series fuse per family with self-observable drop counter, Prometheus text exposition + JSON snapshot), W3C `traceparent`-compatible trace/span ids with a deterministic tail sampler (same trace sampled identically at every hop), span lifecycle enforcement (sensitive attribute keys rejected outright, single-line error statuses, double-finish rejected), a bounded batching span processor with requeue-on-failure export semantics, the liveness/readiness health registry aggregating worst-of with sanitized operator-safe JSON output, and the audit telemetry bridge mapping append-only `AuditEvent`s onto redacted log lines and `mas_audit_events_total{outcome,class}` / denial counters without ever touching raw audit payloads. The application crate is the use-case layer every api/worker process composes: a context-propagation `ServiceContext` (validated correlation ids, explicit tenant/org scope, monotonic clock stamps), object-safe store ports (`Box<dyn …Port>`, `Debug + Send + Sync`) so infrastructure arrives as decade-stable traits instead of dependencies (an `InMemoryServices` reference implementation backs every test), an audit sink port with the rule that **a mutation that cannot be audited is a failed mutation** (sink errors propagate), wire-DTO mappers keeping domain aggregates from ever crossing process boundaries verbatim, and the five services — `TenancyService` (hierarchy registration with re-verified org→tenant→project coherence, global email/slug uniqueness, invite-with-conflict), `AgentService` (project-scoped registration, per-project slug uniqueness, monotonic immutable publish with checksum round-trips), `WorkflowService` (validated graph updates: structural rules → explicit acyclicity check → deterministic Kahn topological order returned to the runtime), `ExecutionService` (existence pre-checks before creation, idempotency-key replay returning the original execution, flag-driven cancellation, all transitions audited, foreign-tenant reads answered `NotFound`) and `ScheduleService` (register-activates, idempotent resume, existence-hiding lookups). The api crate is the thin transport layer over `application`: an axum HTTP v1 surface (every route answers `ApiEnvelope { data | StableApiError, meta(request_id + correlation echo) }`, RFC-9457-aligned stable error contract; correlation middleware that honors/validates/generates `X-Correlation-Id` and always echoes it; bearer auth through an injected `TokenVerifierPort` seam — unauthenticated protected calls 401 with zero token material in errors; `X-Tenant-Id`/`X-Organization-Id` scope headers re-checked at the application layer so foreign ids resolve as 404, never 403; `X-Idempotent-Replay: true` on idempotency-hit execution submissions, generic-id and explicit-up-cast error codes only), a tonic gRPC facsimile of the same semantics (vendored-`protoc` build, `mas.api.v1` services for tenancy/agents/workflows/executions/schedules, metadata keys mirroring headers, canonical AppError→gRPC-code mapping), health probes (always-ok liveness, dependency-aggregated readiness), per-request 256KiB body caps, deterministic topological-order feedback on workflow graph pushes, and concurrent HTTP+gRPC serving with SIGINT/SIGTERM graceful drain. The worker crate is the queue-consumption engine: broker-facing `ConsumerRunner` with `max_in_flight`-bounded JoinSet scheduling, lease-fenced mediation (Conflict → bounded Nak), deadline/delivery pre-checks, heartbeat (lease-renew + ack-window extension at `ack_wait/2`, aborted exactly once at handler return), and the full settlement protocol — success ⇒ Ack + row-complete; cancelled ⇒ Ack + cancel; `serialization` ⇒ Term; `validation`/`conflict`/`forbidden`/`unauthorized` ⇒ fail + Term (poison; delivery-ceiling DLQ); retryable ⇒ fail + retry + full-jitter Nak or `Exhausted` Term at the delivery cap, with lifecycle writes never blocking settlement (log + trace; redelivery heals). The scheduler-service crate is the only wall-clock boundary to the deterministic scheduler runtime: a recover-once → interval-tick → mid-tick-drain `TickDriver` with cumulative stats and capped exponential backoff after consecutive whole-tick failures, the in-flight honesty ledger (start/finish idempotent, `reap_older_than` for stranded entries), the HTTP dispatch adapter (idempotency-keyed schedule-run POST; 2xx→Started + ledger insert, 409/429→Deferred, else requeue-and-count) plus its `/v1/scheduler/completions` listener route answering 204 idempotently, all loopback-driven in `--dev-inmemory`. The cli crate ships the `mas` operator binary: a typed reqwest API client (envelope-decode with two-phase Value→typed conversion persisting serde(default) generics, tolerant 2xx fallback for plain-text health probes, stable-error surface), a clap command tree (`status`; execution submit/get/list and start/pause/resume/cancel transitions with `cli-{uuid}` idempotency keys and `--wait` polling to terminal states; schedule list/get and pause/resume/disable; agent/workflow list-get), scope guard failing locally before the network, brief|json rendering, and a strict exit-code contract (0 ok / 1 API error / 2 usage-or-transport), exercised end-to-end against a fake axum server and smoke-rehearsed live against `mas-api --dev-inmemory`. All 20 workspace crates are implemented, formatted, clippy-clean, rustdoc-clean and green (`cargo test --workspace --locked`: 372 tests). Workspace support dirs (`config/`, `tests/`, `benches/`, `scripts/`) remain.

## 3. Build & verify

```bash
# Toolchain (pinned by rust-toolchain.toml; rustup resolves automatically)
rustup component add rustfmt clippy

# Formatting / linting / tests (same order as scripts/test.sh)
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace

# Build (dev / release)
cargo build --workspace
cargo build --workspace --release

# Benchmarks (once benches land)
cargo bench --workspace
```

A `Cargo.lock` is committed and required: all application builds use `--locked` in CI.

## 4. Environment setup

Local development needs PostgreSQL 16+, Redis 7+ and NATS 2.10+ with JetStream. `scripts/dev.sh` (added with the infrastructure crates) boots the full local stack. Configuration is layered:

```
config/default.toml        # typed defaults — NO secrets
config/development.toml    # local overrides
config/test.toml           # deterministic test settings
config/production.toml     # prod schema; secrets only from env/secret manager
```

## 5. Migrations

SQL migrations live in `migrations/NNNN_name/up.sql` and run strictly in order; `persistence::migrations::MigrationRunner` verifies the recorded version, applies pending migrations transactionally, and refuses incompatible states. Use `./scripts/migrate.sh` (or `cli migrate up`) — never hand-edit applied migrations. RLS policies for every tenant-owned table are part of the migration set, not app code.

## 6. Rust ↔ Python communication rules

1. All Rust↔Python traffic is **gRPC over the shared protobuf definitions** (`/contracts/proto`), generated by `scripts/generate-proto.sh` for both sides.
2. Every message carries `MessageHeaders`: `tenant_id`, `actor_id`, `request_id`, `correlation_id`, `causation_id`, `protocol_version`. Mismatched major protocol versions are rejected.
3. The Python orchestrator **requests deterministic decisions** from rust-core (policy, quota, authz, tool invocation). It never executes tools or policy locally.
4. Payloads are size-capped and validated by `messaging::codec`; malformed frames are NAK'ed, never retried blindly.
5. Rust never calls Python over ad-hoc HTTP or shared files.

## 7. Security model

* **Authentication**: JWT (issuer/audience/algorithm pinned), API keys (only hashes stored; prefix lookup), mTLS/JWT service identity.
* **Authorization**: RBAC + ABAC via `mas-security`; every sensitive action additionally produces a `PolicyContext` decision with an audit record.
* **Tenancy defense in depth**: `TenantGuard` asserts identity tenant == resource tenant on every operation; PostgreSQL **row-level security** enforces the same invariant below the app layer; cross-tenant operations require an explicit privileged scope.
* **Secrets**: domain objects store `SecretReference`s only. Raw secrets exist solely inside `security::secrets` provider adapters and are redacted from logs/events by `common::redaction`.
* **Cryptography**: authenticated encryption with versioned keys; tampered ciphertext yields a typed error, never plaintext. Webhook/event signatures are HMAC-SHA256 with timestamp tolerance.
* **Errors at boundaries** are mapped to `StableApiError { code, message, request_id, details }`. Internal context never crosses the boundary.

## 8. Multi-tenancy model

* Hierarchy: `Organization → Tenant → Project → {agents, workflows, tools, executions}`.
* Every request resolves a `TenantContext` at the edge (JWT/API-key/service + optional header/subdomain; mismatches are rejected) and injects it into `RuntimeContext` for the entire pipeline.
* Isolation modes: shared schema + RLS (default), schema-per-tenant, dedicated database (enterprise). Isolation is a tenant attribute enforced by `tenancy::isolation` plus DB-level RLS.
* Fairness: per-tenant rate limits, concurrency limits, and weighted scheduling prevent noisy-neighbor starvation.

## 9. Execution lifecycle

`submit → validate → authorize → policy → quota (reserve) → queue → dispatch → claim (lease) → execute (steps, checkpoints) → finalize (usage, events) → release quota`

* **Tasks** are idempotency-keyed and attempt-bounded; retries use exponential backoff with jitter; exhausted tasks go to DLQ with full failure metadata (replayable via CLI).
* **Executions** are workflow/agent runs with parent/root links, cooperative cancellation, pause/resume, deadlines, and resource accounting (steps, tool calls, tokens, active time).
* **Checkpoints** make executions restart-safe; `recovery` replays from the last valid checkpoint after crashes.
* All transitions emit signed events through the **transactional outbox** (state change and event commit atomically; publish happens after commit).

## 10. Error-handling rules

1. One error type — `AppError` — with 13 categories, stable `error_code()`s, `http_status()` mapping, and retryability classification. Stringly-typed or per-crate error enums are not allowed at boundaries.
2. Retryability is a property of the **category**; attempt budgets/deadlines are policy, not error metadata.
3. Internal messages never cross boundaries: `public_message()` / `StableApiError` are the only outward texts; `request_id` correlates to traces.
4. Panics are bugs: no `unwrap`/`expect` in library code paths that can fail at runtime (enforced via lint policy; tests are exempt).

## 11. Testing strategy

* **Unit** (in-crate, e.g. `common/src/tests.rs`): invariants, codecs, redaction, error-code stability.
* **Integration** (`tests/integration/*.rs`, anchored into `crates/api/tests/root_integration.rs`): real axum HTTP server + production router + real `mas-cli` client — full tenancy → agent → workflow → graph → execution → replay → transitions → foreign-scope flow, plus strict exact-envelope assertions and scope-header contract.
* **Contract** (`tests/contract/*.rs`, anchored into `crates/api/tests/root_contract.rs`): golden envelope/graph/dispatch fixtures round-trip through the wire DTOs; `StableApiError` code stability is pinned so later evolution notices.
* **Fixtures** (`tests/fixtures/*.json`): committed wire golden files — the envelope shapes, workflow graph, and scheduler dispatch payload.
* Benches (`benches/*.rs`) guard the worker backoff policy, messaging codec round-trip, and application topological-order hot path; anchored via `[[bench]]` sections in `mas-worker`, `mas-messaging`, `mas-application`.
* Crate-level `#[cfg(test)]` suites hold the per-component invariants (378 lib/unit/integration tests at last full gate).

## 12. Production deployment requirements

* Release builds: `lto = "thin"`, `codegen-units = 1`, stripped symbols (`[profile.release]`).
* Processes: `api` (HTTP+gRPC), `worker`, `scheduler-service` run as separate replicas with readiness/liveness gates; all support graceful shutdown (stop intake → drain → report unfinished work).
* Required runtime guarantees: TLS everywhere, connection pools sized via `config/production.toml`, OpenTelemetry export enabled, RLS enforced, secrets from the secret manager only, and `--locked` builds from committed `Cargo.lock`.

## 13. Quickstart (what works today)

```bash
# 1. Toolchain (idempotent; rust-toolchain.toml pins the exact channel)
./scripts/bootstrap-toolchain.sh && source .cargo-env

# 2. The gate — fmt / clippy -D / full tests / rustdoc
./scripts/check.sh               # add --quick to skip docs

# 3. In-memory dev trio (config/development.toml drives every knob)
./scripts/dev-up.sh              # logs → .dev-logs/, stop with dev-down.sh
target/debug/mas status              # cli smoke against the live api

# 4. Migration manifest (dry) — what DBAs export before deploys
DATABASE_URL=postgres://… ./scripts/migrate.sh plan|status|up

# 5. Benchmarks
./scripts/bench.sh --quick
```

Feature flags, binaries, and config keys:

| Flag / knob | Where | Effect |
|---|---|---|
| `--dev-inmemory` | `mas-api`, `mas-scheduler`, `mas-worker` | Replaces PostgreSQL/NATS adapters with in-process implementations; production runs omit the flag and choose real adapters. |
| `testutils` (default-on) | `mas-api` | Exposes `state::test_support` so `tests/integration` can boot real servers. |
| `MAS_ENV` | all processes | Selects `config/$MAS_ENV.toml` overlay over `config/default.toml`. |
| `MAS_*` env vars | all processes | Leaf-level overrides (`MAS_SERVER_HTTP_ADDR=…`, `MAS_SCHEDULER_INTERVAL_MS=…`, `MAS_WORKER_POLL_MS=…`). |
| `server.dev_tokens` | api config | Dev bootstrap tokens; **refused to boot under `MAS_ENV=production`**. |

Every service composes dependencies explicitly in `main` (DI by hand —
`api::server`, `scheduler_service::driver`, `worker::runtime`); crossing
boundary payloads live only in `mas-contracts`; `mas_domain` stays free of
infrastructure dependencies.
