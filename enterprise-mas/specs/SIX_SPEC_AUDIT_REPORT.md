# Six-spec implementation audit

**Repository requested:** `Ainul-550Islam/Enterprise-Multi-Agent-System-whatapps`, branch `main`, project root `enterprise-mas/`  
**Audited workspace:** `/home/user/enterprise-mas-audit/enterprise-mas/`  
**Audit date:** 2026-10-06  
**Role:** Principal multi-agent systems architecture and Rust/Python implementation review

## Executive verdict

The repository contains substantial, typed Rust and Python implementations, durable execution/control-plane migrations, security boundaries, and extensive unit/contract coverage. The codebase is **not yet production-release complete**. Three source-level blockers/risk areas are especially consequential:

1. **Bridge Integration — P0:** production Python composition only registers two textual built-in agent IDs, while Rust's `AgentId` is a UUID and the worker sends the database agent ID. The Python runtime looks up that exact ID and has no authenticated Rust-owned agent/version snapshot resolver. A normal database-backed agent run therefore cannot resolve its agent. The bridge also records the version in context without selecting that version in the runtime.
2. **Billing & Quota — P1:** `ExecutionService.submit()` consumes execution quota before the durable transaction resolves idempotency. A replay can be charged again, and a later persistence failure can leave a charge without an execution.
3. **Execution Pipeline — P0 for the default worker configuration:** the default PostgreSQL broker marks `tasks.status='running'` before the durable attempt coordinator tries to claim only `pending|queued` tasks. The second claim cannot create the attempt; no live PostgreSQL reproduction was run, but the source SQL/state mismatch is direct. Successful result output is also dropped before durable task/execution settlement.

The Rust↔Python transport work is materially implemented and verified (including signed calls, scope/error mapping, cancellation/deadline handling, and task-attempt/request/correlation propagation). That does not close the agent-catalog/version gap above. The bridge integration status spec's “complete” wording is stronger than the runtime composition currently supports.

## Priority 1 — Bridge Integration

### B1 — P0: Python cannot resolve real Rust agent IDs or pinned versions

**Evidence**

- Rust IDs are UUID-backed (`rust-core/crates/common/src/ids.rs`); the durable worker requires an `AgentId` on an `execution.run` task and forwards it through `AgentRunRequest` (`rust-core/crates/worker-service/src/agent_execution.rs`, `rust-core/crates/execution/src/agent_bridge.rs`).
- The production Python composition binds `TenantScopedAgentRegistry` to `DEFAULT_AGENT_CATALOG` (`python-orchestrator/src/mas_orchestrator/services/composition.py:290-322`). That catalog contains only the literal IDs `platform.default` and `platform.analyst` (`.../composition.py:424-467`).
- The gRPC servicer forwards the supplied `request.agent_id` to `ExecutionService` (`python-orchestrator/src/mas_orchestrator/grpc/agent_bridge_servicer.py:798-807`). `AgentRuntime._select_agent()` performs an exact tenant/agent lookup and does not substitute a default when an explicit ID is missing (`.../agents/runtime.py:210-226`).
- The protobuf `agent_version` is stored in `ExecutionContext`, but the servicer builds `ExecutionRequest` with only `agent_id` and protocol version; runtime selection is not version-aware. The catalog materializer defaults a missing version to the string `"1"`.

**Impact:** UUID agent IDs created by the Rust API do not match either built-in Python catalog ID. An ordinary database-backed request is returned as `agent_not_found` before it reaches a model provider. Even if IDs happened to match, an immutable Rust-published version is not resolved/selected from the request.

**Required closure:** add a tenant-scoped, authenticated Rust-owned agent/version resolution contract (or pass a validated immutable version snapshot in the signed bridge request); have Python validate and execute that exact snapshot; test with real UUID IDs, tenant boundaries, missing/disabled agents, and two published versions. Do not map arbitrary IDs to the static default agent.

### B2 — P1: idempotency decision code is not wired to an authoritative reader in production

`ExecutionService` calls `evaluate()` before execution, but its default is `UnknownStateReader`, which always reports `UNKNOWN` and intentionally means “proceed” (`python-orchestrator/src/mas_orchestrator/services/execution_service.py:150-163`; `.../services/idempotency.py:91-101, 178-190`). Production composition calls `build_execution_service(runtime)` without a `state_reader` (`.../services/composition.py:322`). Thus this decision path does not suppress duplicates. The request idempotency key and durable attempt ID are propagated across the wire, but propagation is not deduplication. Ordinary durable worker claims provide a separate guard when functioning; a lost RPC response/retry or direct replay can still repeat paid Python work. The Python tests exercise the pure decision logic, not a production Rust state reader wired into composition.

**Required closure:** either implement and wire an authoritative, tenant-scoped state/attempt query or explicitly define a safe at-least-once contract with provider-side idempotency where available; add a lost-response/retry test that proves the same attempt cannot launch paid work twice. Keep Rust as the source of truth.

### B3 — P2: production AI event publisher is absent

The production `ExecutionService` uses the default `EventEmitter`, whose default sink is `LoggingSink`; `build_runtime_bundle()` does not inject a broker/event publisher. The event publishing module's default transport is likewise explicitly local-only (`python-orchestrator/src/mas_orchestrator/events/emitter.py`, `.../events/publisher.py:56-72`). Rust does write canonical execution/task/usage events through its transactional outbox, so this is not a claim that *all* platform events are missing. It means Python AI-specific events are logs, not durable broker events as the master spec describes.

### Bridge items that are implemented

- Protobuf `AgentBridgeApi` contract and generated Rust/Python bindings; Rust `GrpcAgentBridge`; authenticated internal metadata; tenant/organization identity reconciliation; bounded request/deadline; cancellation; structured success/failure and retry classification.
- Durable task attempt ID, request ID, and correlation propagation through the Rust worker/runtime/bridge and Python execution context.
- Python composition with real OpenAI/Anthropic adapters and no production echo/in-memory bridge fallback.
- Cross-language contract, identity/scope, deadline/cancellation, error mapping, telemetry, and composition tests. These prove the tested Rust/Python transport scenarios; they do **not** prove a real database-backed agent config or live provider execution.

## Priority 2 — Security & Tenancy

### S1 — P2: internal HMAC freshness is not one-time replay prevention

Rust and Python verify a body-bound HMAC and a ±60-second freshness window. There is no nonce or replay cache. A captured, valid request body/signature can be replayed during that window; the current “replayed call” security test signs a request two hours in the past and therefore tests staleness, not same-window replay. Production HTTPS and the shared secret reduce exposure, but the authentication layer itself does not make a fresh signature single-use. Combined with B2, a replay can trigger duplicate AI work if it reaches the servicer.

**Required closure:** add a replay nonce/idempotency claim with atomic expiry (or prove the transport is otherwise single-delivery and test that guarantee), and test an identical signed request twice inside the freshness window.

### S2 — P2: no production secret-manager resolver is supplied for referenced tenant credentials

The domain and migrations correctly store `SecretReference` metadata rather than raw credentials. `SecretResolverPort` is present, but the concrete resolver found in `rust-core/crates/security/src/secret_gate.rs` is `InMemorySecretResolver`, explicitly dev/test-only. No Vault/cloud/KMS resolver was found in the inspected Rust crates. Python provider keys can be injected through environment secret references, but this does not resolve the Rust-owned connector/credential references. Until a production resolver is injected, integrations that require stored tenant credentials cannot be deployed end-to-end.

### Security controls found

- Canonical principal/scope and authorization modules; tenant/organization/project relationship checks; resource authorization and stable authentication/authorization errors.
- Scoped persistence APIs; tenant-scoped repositories; transaction-local RLS context; migrations enabling and forcing RLS; database-role separation/grants for worker, outbox and scheduler paths.
- Body-bound internal service signatures; API-key hashing; secret-safe wrapper types; SSRF-guarded provider/tool HTTP transport; explicit refusal of unconfigured/unsafe code sandbox paths.

### Security verification boundary

Static SQL/role checks and Rust/Python tests pass. **No live PostgreSQL role/RLS exercise was run**, so actual production role grants, pooled-connection behavior, and RLS enforcement were not empirically verified. Do not describe this as live database security certification.

## Priority 3 — Billing & Quota

### Q1 — P1: submission quota is charged before idempotency resolution

`ExecutionService.submit()` computes the fingerprint, calls `ControlPlane::authorize_execution()` with `reservation=None`, then calls `durable.submit(intent)` (`rust-core/crates/application/src/execution_service.rs:217-280`). `authorize_execution()` uses `require_quota()` in that branch, and `require_quota()` calls the counter's consuming operation (`.../control_plane/mod.rs:790-838` and `.../control_plane/mod.rs:330-347`). The durable transaction is where the idempotency replay/conflict is resolved.

**Impact:** a same-key replay consumes another execution unit; if durable submission fails after the charge, the quota remains consumed without a durable execution. The current service comment says no compensation is needed, but the charge is not part of the durable idempotency transaction.

**Required closure:** make submission metering idempotent by the same tenant/request key and commit it atomically with durable submission, or use a recoverable reservation/settlement design. Add tests for replay and database failure after authorization, asserting the counter and execution rows remain consistent.

### Q2 — P1: failed-attempt usage is dropped; provider/model/cost attribution is incomplete

- The protobuf and Rust `AgentRunVerdict::Failed` carry reported usage, and the node adapter charges token budget on failure. But `AgentRunHandler` converts a failed outcome into `Err` and returns before putting usage in the task result (`rust-core/crates/worker-service/src/agent_execution.rs:519-547`).
- `TaskOutcome::Failed` contains only a failure (`rust-core/crates/application/src/durable.rs:305-321`); persistence creates `observed_usage` only for `Succeeded` (`rust-core/crates/persistence/src/durable.rs:1257-1287`). A provider failure that consumed reported tokens is therefore not written to the usage ledger.
- The success payload's `usage_json()` contains tokens/tool calls/steps, but not provider, model, cost, or attempt identity (`.../worker-service/src/agent_execution.rs:575+`). The persistence writer does not populate the new `task_attempt_id` usage column. The unique task/resource index is keyed by `(task_id, resource)`, so it cannot distinguish actual metered usage across attempts.

**Required closure:** carry safe usage on failure and success into durable settlement, retain `task_attempt_id`, provider/model/cost where actually reported, and design the unique metering key around an actual logical event/attempt so retries neither double-meter nor erase real failed-attempt consumption. Add partial-provider-failure and retry tests.

### Q3 — P2: billing webhook intake/provider integration is only a receipt-store foundation

The API exposes receipt **read** routes (`rust-core/crates/api/src/router.rs:78-83`; handlers are GET-only in `.../handlers/control_plane.rs:253-314`). `BillingWebhookStore::claim()` accepts a caller-supplied `signature_valid: bool` and persists it; it does not verify a signature (`rust-core/crates/persistence/src/repositories/control_plane/billing.rs:149-205`). No webhook intake route, provider signature verifier, event-to-subscription processor, or billing-provider abstraction was found. Receipt uniqueness/forward-only state and read APIs exist, but this is not an end-to-end secure billing webhook.

### Billing/quota items that are implemented

PostgreSQL-backed atomic quota counters, quota definitions/overrides, time-window limits, quota reservations and settlement paths, plan/subscription/entitlement models, control-plane administration/read APIs, RLS/role SQL, usage ledger, receipt tables, and audit/outbox plumbing are present. The workspace has substantial in-memory and SQL-contract tests. Database-dependent PostgreSQL tests were not run against a live database in this audit.

## Priority 4 — Execution Pipeline

### E1 — P0 for the default `broker.kind=pg` path: task is claimed twice using incompatible state transitions

- `worker-service` defaults `broker.kind` to `pg` (`rust-core/crates/worker-service/src/main.rs:358-369`).
- `PgTaskBroker::CLAIM_SQL` selects `queued` and changes the task to `running`, but does not set the durable task owner/lease/attempt (`rust-core/crates/persistence/src/repositories/broker.rs:63-77`).
- The worker then calls durable `claim_in()`, which creates the attempt only for a task in `pending|queued` (`rust-core/crates/persistence/src/durable.rs:1052-1110`). It can no longer claim the row the broker just changed to `running`.

**Impact:** source logic indicates the default PostgreSQL worker path cannot execute a newly broker-claimed task through the durable attempt claim. This is a source-level finding; no live PostgreSQL reproduction was available. NATS delivery still uses the durable attempt path, but its live service was not exercised here.

**Required closure:** make one component the sole owner of the durable claim (preferably have the broker deliver without mutating task state, then let durable claim install owner/lease/attempt atomically), or unify the two claim operations in one transaction. Add a live PostgreSQL integration test for the configured default lane, including lease expiry/redelivery and attempt creation.

### E2 — P1: successful output is discarded before durable settlement

`AgentRunHandler` returns a JSON payload containing `output` and `usage` on success (`rust-core/crates/worker-service/src/agent_execution.rs:528-560`). `TaskOutcome::Succeeded` carries usage only, not the result (`rust-core/crates/application/src/durable.rs:305-321`); the consumer extracts usage and calls `Attempts::succeed()` without passing the output (`rust-core/crates/worker/src/consumer.rs:678-682`). Durable finalization calls `Execution::complete(..., None)` and the task-fence update does not write `tasks.result` (`rust-core/crates/persistence/src/durable.rs:1308-1313, 1330-1345, 590-618`).

**Impact:** the worker can acknowledge a task after terminal state commits while the canonical execution output and task result remain absent. Clients polling the durable execution cannot retrieve the successful AI result.

**Required closure:** add a typed result field to settlement/outcome, validate size/schema, persist task and execution result in the same transaction as task/attempt/usage/events, and test read-after-completion from the real repository.

### E3 — P2: step results are not persisted by the production agent handler

`Executor::for_agent_runs()` sets `steps: None`, and `AgentRunHandler` discards `AgentRunOutcome.steps` after execution. The local run scope can track steps, but there is no durable step-store write on this path. The canonical execution spec asks for step results where applicable. If direct single-agent runs are intentionally one opaque step, document that contract; otherwise wire a tenant-scoped durable `StepStorePort` and add restart/readback tests.

### Execution items that are implemented

Atomic durable submission/idempotency rows, outbox relay abstractions, task/attempt aggregates, lease/fencing fields, cancellation/retry classification, task/event/audit settlement, usage storage, and both PostgreSQL and NATS broker implementations exist. This source review does not certify the default PostgreSQL lane until E1 is fixed and live-tested.

## Master implementation spec (`specs/first.md`)

The Python package contains typed configuration/contracts/context; agent planner/router/runtime/supervisor/delegation; OpenAI, Anthropic and compatible model adapters; retry/failover/cost logic; prompt rendering/guardrails; tool schemas/permissions/SSRF-safe HTTP; memory and RAG abstractions; event/telemetry/security/evaluation modules. Python tests cover these libraries well.

Remaining production wiring gaps beyond B1-B3:

- `build_runtime_bundle()` creates `AgentFactory(tool_executor=None, tool_registry=None)` and its catalog explicitly contains only tool-free agents (`python-orchestrator/src/mas_orchestrator/services/composition.py:295-300, 430-464`). Tool code exists but the production composition does not execute tools.
- In-process memory/vector stores deliberately reject production; no durable Python memory backend or pgvector/vector-database adapter was found. This is a safe fail-closed boundary, not a complete production RAG/memory deployment.
- The code sandbox deliberately ships no host-execution backend and refuses to run without an injected container/microVM/managed sandbox.
- Production composition runs the agent/model path, but does not wire these unavailable tool/memory/RAG/sandbox services or a durable AI event transport. Do not claim these capabilities are production-operational merely because their interfaces and unit tests exist.
- No live model provider credentials or external provider test was used; the Rust/Python E2E tests use an explicit test server/runtime.

## Bridge status spec (`specs/BRIDGE_INTEGRATION_STATUS.md`)

The status report accurately records substantial transport and cross-language test work, but its “complete” conclusion should be revised to list B1/B2, the real-agent/version resolution gap, and the distinction between local AI event logging and Rust's durable outbox. It should also say that cross-language gRPC E2E coverage is not a live Rust→Python→external-provider→durable-result production test.

## Verification performed / relied upon

- `python3 -m pytest -q`: **1,945 passed**.
- Python compileall, Ruff check, Ruff format check, and mypy: **passed** (mypy reported 161 source files clean).
- `cargo fmt --all -- --check`: **passed**.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: **passed** in the completed repository verification recorded for this workspace.
- `cargo test --workspace --locked --features nats -j 2`: **exited successfully**. Its output marks the Python-dependent Rust E2E tests and live NATS/PostgreSQL tests ignored; the separate Rust↔Python E2E run recorded **12 passed**.
- `cargo doc --workspace --all-features --no-deps --locked`: **passed**.
- No live PostgreSQL or NATS server was used. No real external model provider was invoked.

The workspace mirror has no `.git` metadata, so the local branch/commit SHA and byte-for-byte equivalence with GitHub's current `main` cannot be independently attested. Findings refer to the supplied workspace snapshot.

## Supplemental review of the uploaded target tree

`ENTERPRISE_MAS_FULL_TREE.md` describes a much broader future scope than the original six-spec audit plus Module 1. It labels itself as a 918-file target (533 `[OK]`, 44 `[GAP]`, 341 `[NEW]`); those are the attachment's classifications, not an independent verification of every one of those files. The relevant additional gaps and consistency notes are:

- The target tree calls for `DataHubBridgeApi`, `execution/src/data_hub_bridge.rs`, a Rust gRPC adapter, a Python data-hub servicer/service, data-source adapters, `PgVectorIndex`, a pgvector extra, separate RAG-plane DB role, and live pgvector tests. Those Data Hub bridge/index files are absent from the inspected workspace; the existing RAG index intentionally fails closed when no vector backend is configured.
- The tree's `[GAP]` on `agents/registry.py` and its proposed built-in agent registration hook are consistent with B1: the current Python catalog is static and does not resolve Rust UUID agent IDs or immutable Rust-published configuration snapshots.
- The target lists Next.js frontend, Docker/Kubernetes, observability deployment, OpenAPI, CI/CD, and numerous API/application modules as `[NEW]`. These are outside the original six-spec/Module 1 implementation deliverable and are not present as a complete product in this workspace. The current audit does not claim those 341 proposed files have been delivered.
- **Migration-number collision:** the target proposes migrations `0020_workflow_versions` through `0026_approvals`, while this workspace already contains `0020_control_plane`, `0021_control_plane_grants`, and `0022_worker_metering_grant`. These migrations cannot be copied under the proposed numbers without colliding with existing history. Any future implementation must append after `0022` or provide a reviewed consolidation/renumbering plan that preserves checksum/order guarantees.
- The attachment's layout note says `python-orchestrator/` is outside the monorepo root; in the audited workspace it is already under `enterprise-mas/`. The Python `README.md` requested by the target tree is missing.

This supplemental tree therefore expands future scope; it does not replace or invalidate the original six-spec findings above.

## Full Module 1 source delivery

A separate, unabridged source bundle is provided as `Module1_Full_Code.md` and `Module1_Full_Source.zip`. The bundle contains complete current Rust/Python bridge files, generated contract bindings, task-attempt/correlation propagation, and bridge tests, preserving repository-relative paths. It contains whole files, not abbreviated excerpts. It is the implemented bridge transport source, **not a release sign-off**: B1, B2, and E1/E2 above remain blockers for a production end-to-end execution path.
