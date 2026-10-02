# Rust Core — Full Audit & Complete Remediation Specification

**Project:** Enterprise Multi-Agent System + SaaS  
**Component:** `rust-core`  
**Audit type:** Source/tree/static integration audit + remediation specification  
**Audit date:** 2026-09-30  
**Implementation policy:** Fix every item in this document before starting the Python Orchestrator implementation.

---

## 0. Executive status

The Rust Core source tree contains a substantial foundation: domain aggregates, typed IDs, policy primitives, tenancy/RLS primitives, execution/runtime modules, event/outbox abstractions, durable PostgreSQL task brokering, scheduling, quota primitives, security helpers, observability and a broad test suite.

The current problem is **not an absence of code**. The primary problem is that several major subsystems are not wired together into one canonical production execution path.

### Current decision

**DO NOT start Python Orchestrator implementation yet.**

First complete this remediation specification and pass all Rust Core acceptance gates.

### Most important blockers

1. API authentication does not bind the authenticated principal to a server-trusted tenant/organization/scope context.
2. API execution submission currently persists an `Execution` but does not enter the canonical task/worker/runtime execution pipeline.
3. `worker-service` still binds `execution.run` to `EchoHandler` in the production startup path.
4. `ExecutionService` and `OrchestrationEngine` currently form two partially overlapping execution lifecycle paths; one canonical path must be selected and used everywhere.
5. The Rust execution layer only has an in-memory `AgentBridge`; a production Rust → Python gRPC adapter is not implemented.
6. Orchestration engine ports have in-memory/no-op implementations but do not yet have a complete production adapter set.
7. Transactional consistency is incomplete: resource mutation, idempotency, audit and outbox work are not consistently committed as one atomic unit.
8. RLS infrastructure exists, but the normal authenticated API request path does not consistently apply `RlsContext` to the database transaction.
9. Persistence only has row/repository/application-port coverage for part of the migration schema. Runtime-critical tables are missing from the complete domain → row → repository → application-service → API chain.
10. Production scheduler/worker lease semantics still use in-memory lease state in important paths.
11. Distributed quota/concurrency enforcement is not yet backed by a production shared coordination mechanism.
12. Scheduler completion is not execution-specific and the completion HTTP endpoint is not strongly authenticated.
13. Worker delivery/attempt bookkeeping is not fully durable; PostgreSQL broker fetch currently reports a synthetic delivery count of `1`.
14. There is no verified production-grade outbox relay runtime wired into service startup.

---

# 1. Audit method and limitations

This audit was performed against the uploaded Rust Core workspace snapshot.

The audit included:

- workspace and crate tree inspection;
- source-level review of API/auth/context composition;
- source-level review of application services;
- execution/orchestration/worker flow review;
- persistence/repository/schema alignment review;
- scheduler review;
- security/RLS review;
- contract/protocol review;
- static test-source inventory;
- production/dev composition review.

### Important validation limitation

The audit runtime did **not** have an installed Rust toolchain available for a real build. A toolchain bootstrap attempt could not complete because network/DNS access failed. Therefore this document does **not** claim that `cargo check`, `cargo test`, `cargo clippy`, `cargo fmt --check`, or live PostgreSQL/NATS tests were executed successfully.

Those commands are mandatory acceptance gates in Section 20 and must be executed in a real Rust-capable CI/development environment after remediation.

### Audit snapshot facts

At the audited snapshot:

- the workspace contains 21 Rust crates;
- `rust-core` contains a large source tree with hundreds of Rust files;
- the SQL migration set contains the complete base schema and RLS/outbox migrations;
- source contains extensive tests, but the repository's hard-coded test-count documentation must be regenerated after an actual test run.

Do not preserve a hard-coded claim such as “372 tests passing” unless CI has generated and verified that number.

---

# 2. Severity model

## P0 — release blocker

Any issue that can cause:

- cross-tenant access,
- unauthorized action execution,
- duplicate or lost execution,
- inability to execute real agents/workflows,
- production-only correctness failure,
- broken Rust ↔ Python boundary,
- non-atomic security/audit state,
- unsafe distributed coordination.

**All P0 items must be fixed before Python Orchestrator work begins.**

## P1 — required production functionality

Issues that do not necessarily stop all execution but leave critical platform capabilities incomplete, unsafe under failure, or not enterprise-ready.

## P2 — hardening and operational quality

Documentation synchronization, optional enterprise identity extensions, developer-experience cleanup and further hardening after the core runtime is correct.

---

# 3. Canonical architecture that the final code MUST implement

The final Rust Core must have one authoritative execution path:

```text
Client / SaaS API
        |
        v
Authentication
        |
        v
Trusted Authorization Context
        |
        v
Tenant / Organization Resolution
        |
        v
Policy + Entitlement + Quota
        |
        v
Application Execution Facade
        |
        v
Canonical OrchestrationEngine
        |
        +-----------------------------+
        |                             |
        v                             v
Execution state                 Task record
        |                             |
        +-----------------------------+
                      |
                      v
               Transactional Outbox
                      |
                      v
                  Task Broker
                      |
                      v
                 Worker Service
                      |
                      v
              Execution Runtime
                      |
          +-----------+-----------+
          |                       |
          v                       v
     Tool Runtime          AgentBridge
                                  |
                                  v
                         Python Orchestrator
                                  |
                         LLM / Agent / RAG
```

The following rule is mandatory:

> **Rust Core decides whether an action is permitted and how it is safely executed. Python decides what AI reasoning/action to perform.**

Rust must not contain LLM/provider reasoning logic. Python must not become the source of truth for tenant authorization, quotas, durable task state, or platform security.

---

# 4. Root architectural correction: eliminate the split execution lifecycle

## Problem

There are two execution paths:

### Path A — application service

`crates/application/src/execution_service.rs`

Current behavior:

```text
validate project/workflow/agent
        -> create Execution
        -> optional idempotency record
        -> audit
        -> return
```

It does not enqueue a task or invoke the canonical orchestration engine.

### Path B — orchestration engine

`crates/orchestration/src/engine.rs`

This already contains richer runtime behavior, including policy/quota/concurrency/idempotency/event concepts.

The problem is that API composition does not use this engine as the single source of truth.

## Required fix

Refactor the application execution layer into a **thin application facade over `OrchestrationEngine`**.

Do not keep two separate execution state machines.

### Required target

`crates/application/src/execution_service.rs` must become responsible for:

- converting API/app commands into engine commands;
- enforcing high-level authorization through the trusted service context;
- invoking the canonical `OrchestrationEngine`;
- mapping engine results back to application/API DTOs.

The canonical state transitions and execution creation/dispatch logic must belong to the orchestration/runtime layer.

### Required new/updated files

```text
crates/application/src/execution_service.rs
crates/application/src/context.rs
crates/application/src/stores.rs
crates/application/src/ports.rs                 # add if needed for canonical runtime ports
crates/orchestration/src/engine.rs
crates/orchestration/src/ports.rs                # add production-facing port traits if cleaner than current module layout
crates/execution/src/executor.rs
```

### Acceptance criteria

- API execution submission enters exactly one canonical engine path.
- A successful submission creates durable execution state **and** a runnable task atomically.
- Idempotent replay returns the original execution and does not create another runnable task.
- Start/cancel/pause/resume/retry operate on the canonical runtime state.
- No code path creates an execution that can remain permanently pending solely because no task was dispatched.

---

# 5. P0 — Authentication and authorization remediation

## 5.1 Problem: API principal loses tenant/org/scope information

### Source area

```text
crates/api/src/prod_auth.rs
crates/api/src/auth.rs
crates/api/src/context.rs
crates/api/src/middleware.rs
```

The production API-key verifier loads a richer database row, but the API principal is reduced to minimal identity data while `RequestContext` can obtain tenant/organization from request headers.

That means caller-provided tenant/organization headers can become an authority input instead of merely a routing hint.

## Required fix

Never trust caller-supplied tenant/org headers as the authoritative tenant boundary.

### Implement a trusted principal model

Use the existing richer security principal concept from:

```text
crates/security/src/principal.rs
```

or create an API-facing equivalent containing at least:

```text
actor_id
principal_kind
organization_id
tenant_id
roles
scopes
expires_at
service_identity
```

### API-key authentication must perform

```text
raw API key
    -> prefix lookup
    -> hash verification
    -> revoked/expiry check
    -> load tenant_id
    -> load organization_id
    -> load scopes
    -> load owner/user identity when applicable
    -> build TrustedPrincipal
```

The request context must derive tenant/org scope from that principal.

### Header handling

`x-tenant-id` and `x-organization-id` may be accepted only as:

- explicit resource-routing selectors,
- cross-scope administrator hints,
- or diagnostic inputs.

They must never override a credential-bound tenant without an explicit authorization check.

### Mandatory checks

```text
credential tenant == requested tenant
credential org == requested organization
principal scope contains required scope
membership active
role active
tenant active
organization active
```

For elevated organization-level operations, use an explicit permission such as:

```text
organization:manage
tenant:manage
```

and validate the target tenant is inside the authorized organization hierarchy.

## 5.2 Add service-level authorization

Do not rely only on HTTP middleware.

Application services must receive a trusted authorization context and verify:

- actor;
- tenant;
- organization;
- role/scope;
- target resource ownership;
- action permission.

This prevents a future internal caller from bypassing HTTP middleware accidentally.

### Required files

```text
crates/security/src/principal.rs
crates/security/src/authorization.rs
crates/security/src/rbac.rs
crates/security/src/abac.rs
crates/api/src/auth.rs
crates/api/src/context.rs
crates/api/src/middleware.rs
crates/api/src/prod_auth.rs
crates/application/src/context.rs
crates/application/src/authorization.rs            # create if needed
```

### Acceptance criteria

- An API key cannot select another tenant by header.
- A tenant A key cannot read/write tenant B.
- A tenant-level key cannot call an organization-admin route unless it explicitly has the required scope.
- A revoked/expired key is rejected.
- Missing scope returns deterministic `403` with a stable error code.
- No secret, token or credential is logged.

---

# 6. P0 — Make PostgreSQL RLS part of the normal authenticated request path

## Existing foundation

`crates/persistence/src/rls.rs` already defines a fail-closed transaction-local `RlsContext`.

That foundation must become the actual enforcement mechanism for tenant-bound database work.

## Current gap

`PgServices` primarily works from a raw `PgPool`, while normal API composition does not consistently apply RLS GUCs in a transaction around tenant-owned operations.

## Required final design

For normal tenant-bound requests:

```text
TrustedPrincipal
      -> ServiceContext
      -> RlsContext
      -> DB transaction
      -> set_config(..., transaction local)
      -> repository operations
      -> commit
```

### Mandatory implementation

Add a reusable database access wrapper such as:

```text
crates/persistence/src/scoped_db.rs
```

with APIs conceptually equivalent to:

```rust
async fn transaction_scoped<T>(
    scope: &RlsContext,
    operation: impl FnOnce(&mut TransactionContext) -> ...,
) -> Result<T>
```

All tenant-owned mutations/read operations that require RLS must use this path.

### Explicit privileged paths

Background daemons such as scheduler/replay/migration workers may use a dedicated service role only where required.

That privilege must be:

- explicit;
- documented;
- isolated from user request credentials;
- never populated from HTTP headers;
- audited where security-sensitive.

### Database role requirements

Ensure the application user is subject to RLS and cannot simply bypass it via table-owner semantics or unrestricted roles.

### Acceptance criteria

- Unscoped tenant-owned transaction fails closed.
- Tenant A transaction cannot access tenant B rows even if the SQL accidentally omits an explicit tenant filter.
- API request context and DB RLS context always match.
- RLS tests run against live PostgreSQL, not only mocks.

---

# 7. P0 — Complete production adapters for OrchestrationEngine ports

## Problem

`crates/orchestration/src/engine.rs` defines important engine ports, but production implementations are incomplete. Current source contains in-memory/no-op implementations used by tests.

## Required production adapters

Create or extend:

```text
crates/persistence/src/orchestration_ports.rs
```

or split into dedicated files:

```text
crates/persistence/src/repositories/orchestration.rs
crates/persistence/src/repositories/idempotency.rs
crates/persistence/src/repositories/dead_letter.rs
crates/persistence/src/repositories/workflow_catalog.rs
```

Implement production versions for:

```text
TaskStorePort
ExecutionStorePort
WorkflowCatalogPort
PolicyPort
QuotaPort
EventPublisher
IdempotencyStore
DeadLetterStore
```

Do not replace production logic with `Noop` or `InMemory` implementations.

## Acceptance criteria

- Production startup contains no required runtime port backed by `Noop`.
- Every engine dependency has a durable or explicitly externalized production implementation.
- In-memory implementations remain test/dev only.
- The worker can create/update durable execution state through these real adapters.

---

# 8. P0 — Complete the real Worker execution handler

## Current problem

`crates/worker-service/src/main.rs` still binds `execution.run` to `EchoHandler` in the production startup path.

This means the worker can acknowledge a task while only returning an echo payload.

## Required fix

Implement a real handler:

```text
crates/worker-service/src/execution_handler.rs
```

with responsibility for:

```text
TaskQueueMessage
   -> validate payload
   -> load execution/task context
   -> build RuntimeContext
   -> resolve agent/workflow
   -> create Execution runtime
   -> invoke mas-execution::Executor
   -> persist step/execution outcome
   -> publish events
   -> settle task
```

### Required startup change

`crates/worker-service/src/main.rs` must bind:

```text
execution.run -> RealExecutionHandler
```

not `EchoHandler`.

The echo handler can remain test-only or under a clearly development-only feature.

### Production handler dependencies

The handler must have access to:

```text
PgServices / repositories
ExecutionExecutor
OrchestrationEngine
PolicyEngine
QuotaEngine
Tool runtime
AgentBridge
EventPublisher
Audit recorder
Distributed lock
```

### Acceptance criteria

A real `execution.run` message must execute the actual runtime and eventually end in one of:

```text
completed
failed
cancelled
timed_out
```

and never simply return `{"echo": ...}` in production.

---

# 9. P0 — Implement production Rust → Python AgentBridge

## Current state

`crates/execution/src/agent_bridge.rs` defines:

- `AgentBridge` trait;
- `AgentRunRequest`;
- `AgentRunVerdict`;
- `InMemoryAgentBridge`.

There is no complete production adapter.

## Required implementation

Create:

```text
crates/execution/src/grpc_agent_bridge.rs
```

or an equivalent production adapter crate.

The adapter must:

- use shared protobuf definitions;
- send authenticated service-to-service metadata;
- propagate `tenant_id`, `organization_id`, `actor_id`, `execution_id`, `correlation_id`;
- propagate deadline;
- propagate cancellation;
- enforce request/response size limits;
- handle transient gRPC errors;
- classify retryable vs terminal errors;
- expose health/readiness;
- never log prompt/secret content by default.

## Service-to-service security

Use one of:

- mTLS service identity;
- signed short-lived service token;
- or another explicit workload-auth mechanism.

Do not use a static unauthenticated internal URL.

## Acceptance criteria

```text
Rust Executor
    -> AgentBridge
    -> authenticated gRPC
    -> Python Orchestrator
    -> response/stream
    -> Rust Executor
```

works in an integration test.

---

# 10. P0 — Establish one canonical protobuf source

## Problem

There are contract/proto concepts in multiple locations, including API-local proto files and shared contract declarations. The Python/Rust boundary must not depend on duplicate manually maintained definitions.

## Required final structure

Use one canonical source:

```text
contracts/
└── proto/
    ├── common.proto
    ├── agent_runtime.proto
    ├── execution_runtime.proto
    ├── task_runtime.proto
    ├── tool_runtime.proto
    ├── policy_runtime.proto
    ├── health.proto
    └── error.proto
```

If API public protobuf is also required, version it separately but still keep exactly one source per contract.

## Required messages

### `agent_runtime.proto`

Define request/response/events containing:

```text
request_id
correlation_id
causation_id
tenant_id
organization_id
actor_id
execution_id
agent_id
agent_version
input
resource_limits
deadline
trace_context
```

Responses must support:

```text
success
failure
cancelled
timed_out
usage
```

### `execution_runtime.proto`

Define:

```text
execution started
step started
step completed
step failed
execution paused
execution resumed
execution cancelled
execution completed
execution failed
```

### `tool_runtime.proto`

Define:

```text
tool_id
connector_id
execution_id
input schema/value
permission context
deadline
result/error
usage
```

### Compatibility rules

- explicit protocol version;
- backwards-compatible field additions;
- no renumbering/reuse of protobuf fields;
- unknown fields must not crash consumers unnecessarily;
- generated bindings are reproducible.

### Acceptance criteria

Rust and Python generated bindings come from exactly the same proto source and contract tests pass.

---

# 11. P0 — Make execution submission atomic

## Current problem

Application services generally perform separate database operations such as:

```text
save execution
idempotency_put
write audit
```

without a single transaction around the complete operation.

## Required final transaction

For a new execution:

```text
BEGIN
  validate current authorization/quota/policy
  reserve idempotency key
  insert execution
  insert task
  insert audit event
  insert outbox event
COMMIT
```

After commit:

```text
outbox relay -> broker
```

## Required modifications

Extend:

```text
crates/persistence/src/transaction.rs
```

and add transaction-aware repository methods.

Do not use a repository API that secretly opens a second independent transaction while already inside the application transaction.

### Required API pattern

Repositories should have transaction-aware variants, conceptually:

```text
save_in(tx, resource)
insert_in(tx, ...)
reserve_idempotency_in(tx, ...)
append_audit_in(tx, ...)
append_outbox_in(tx, ...)
```

The exact Rust trait shape may vary, but atomicity must be provable.

### Failure test

Simulate failure after each mutation stage and prove the transaction rolls back completely.

---

# 12. P0 — Task creation and durable execution dispatch

## Current gap

Execution submission does not create a task in the canonical path.

## Required change

Define a single authoritative command for starting executable work:

```text
ExecutionSubmission
    -> Execution row
    -> Task row
    -> Outbox event
```

The task payload must include:

```text
operation
execution_id
project_id
workflow_id
agent_id
priority
not_before
tenant_id
organization_id
correlation_id
idempotency key
resource limits
```

### Broker relationship

The durable PostgreSQL task table remains the source of task state.

The messaging layer is the wake-up/delivery transport.

The outbox guarantees state-change + event publication consistency.

### Acceptance criteria

Creating an execution always produces either:

- a durable runnable task, or
- an explicit terminal rejection before execution creation.

There must never be an accepted execution with no path to a worker.

---

# 13. P0 — Durable task attempts, delivery count and worker fencing

## Current problem

`PgTaskBroker` currently returns a synthetic `deliveries: 1` value for fetched durable tasks.

This is incompatible with reliable redelivery/backoff/DLQ accounting if the delivery count itself is not persisted.

## Required fix

Make `task_attempts` the authoritative attempt ledger.

On each claim:

```text
BEGIN
  atomically claim task
  create task_attempt row
  assign worker_id
  assign lease_id/fencing_token
  assign lease_expiry
  increment attempt_count
COMMIT
```

On completion/failure:

```text
update task_attempt
update task state
publish outcome
```

### Required additions

`TaskAttempt` should contain at least:

```text
id
task_id
attempt_number
worker_id
fencing_token
started_at
lease_expires_at
finished_at
status
failure_code
failure_message_safe
result_reference
```

### Fencing

Stale workers must not be able to overwrite a newer attempt.

Use either:

- monotonically increasing fencing token;
- attempt number + worker lease validation;
- optimistic version check.

### Acceptance criteria

- delivery/attempt number survives process restart;
- retries increase the durable attempt number;
- stale worker completion is rejected;
- DLQ threshold is based on durable attempts, not a fabricated in-memory count.

---

# 14. P0 — Fix worker `max_in_flight` enforcement

## Current problem

The consumer fetch/launch logic checks `max_in_flight`, but the fetch path can already obtain more messages than remaining capacity.

## Required fix

Before fetching:

```text
capacity = max_in_flight - current_in_flight
```

Fetch no more than `capacity` messages.

Alternatively use a bounded semaphore around the actual task handler, but the broker claim/fetch semantics must also respect safe capacity so leases are not unnecessarily acquired.

### Acceptance criteria

With `max_in_flight = N`, active execution handlers never exceed N.

A worker shutdown must leave recoverable tasks rather than claiming a large batch and abandoning them.

---

# 15. P0 — Production distributed concurrency control

## Current problem

The orchestration concurrency limiter is primarily process-local.

A multi-replica deployment can therefore exceed a tenant/global concurrency limit when each worker independently believes it has capacity.

## Required fix

Introduce a production distributed concurrency primitive.

Preferred options:

1. PostgreSQL atomic semaphore/lease;
2. Redis atomic Lua-based semaphore with fencing;
3. another strongly consistent distributed lease service.

The implementation must support:

```text
acquire
renew
release
expire/recover
fencing
```

### Scope

Support at least:

```text
tenant
project
agent
execution
```

where configured.

### Acceptance criteria

Two worker replicas cannot collectively exceed the configured global tenant concurrency limit.

---

# 16. P0 — Production quota implementation

## Current problem

`mas-quota` contains useful engine/bridge abstractions but production shared counters are not fully wired.

## Required fix

Implement a shared durable counter backend.

Recommended:

```text
Redis for low-latency atomic counters
PostgreSQL for durable usage ledger
```

Use Redis atomic operations/Lua or equivalent for quota reservation.

### Required flow

```text
check
  -> reserve
  -> execute
  -> commit actual usage
  -> release unused reservation
```

For failed execution, reservation cleanup must not corrupt the usage ledger.

### Required dimensions

At least:

```text
API requests
executions
concurrent executions
tool invocations
tokens
storage bytes
```

### Acceptance criteria

Quota enforcement is correct across two or more worker replicas.

---

# 17. P0 — Production outbox relay

## Existing foundation

`crates/events/src/outbox.rs` already contains an `OutboxDispatcher` abstraction.

## Gap

There is no clearly wired, always-running production relay in the primary service composition.

## Required implementation

Create:

```text
crates/outbox-relay/
├── Cargo.toml
└── src/
    ├── main.rs
    ├── service.rs
    ├── relay.rs
    ├── lease.rs
    └── health.rs
```

or implement the same responsibilities in a dedicated background task of an already existing process if operationally justified.

## Required behavior

```text
claim pending outbox rows
    -> publish to broker
    -> mark delivered

publish failure
    -> retry with backoff
    -> dead-letter after policy threshold
```

Claiming must be concurrency-safe across replicas.

### Acceptance criteria

- state mutation and outbox insertion are atomic;
- a relay crash does not lose an event;
- duplicate publication is safely deduplicated downstream;
- a stuck relay is observable through health/metrics.

---

# 18. P1 — Complete persistence model coverage

## Current schema vs row/repository coverage

The migration schema contains more entities than the current complete `rows.rs` and `PgServices` application repository surface.

Runtime-critical missing coverage must be added.

### Required domain/row/repository/application coverage

At minimum:

```text
ApiKey
Session
Tool
ToolPermission
WorkflowNode
WorkflowEdge
TaskAttempt
Policy
PolicyDecision
Subscription
Entitlement
Quota
UsageRecord
ScheduleRun
Connector
Credential
Webhook
WebhookDelivery
Notification
```

### Required new/updated files

```text
crates/persistence/src/rows.rs
crates/persistence/src/repositories/api_keys.rs
crates/persistence/src/repositories/tasks.rs
crates/persistence/src/repositories/policies.rs
crates/persistence/src/repositories/quota.rs
crates/persistence/src/repositories/usage.rs
crates/persistence/src/repositories/tools.rs
crates/persistence/src/repositories/connectors.rs
crates/persistence/src/repositories/webhooks.rs
crates/persistence/src/repositories/subscriptions.rs
crates/persistence/src/repositories/entitlements.rs
crates/persistence/src/repositories/sessions.rs
crates/persistence/src/repositories/notifications.rs
crates/persistence/src/repositories/schedules.rs
crates/persistence/src/repositories/services.rs
```

Create whichever files are not present rather than stuffing unrelated repositories into one giant source file.

### Acceptance criteria

Every production runtime entity has a complete path:

```text
domain
 -> persistence row
 -> repository port
 -> production repository
 -> application service/use-case
 -> API/internal runtime consumer
```

Exceptions must be intentional and documented.

---

# 19. P1 — Extend application store ports

`crates/application/src/stores.rs` currently covers only a subset of the migration schema.

Add required application ports for:

```text
TaskStorePort
TaskAttemptStorePort
ToolStorePort
ToolPermissionStorePort
PolicyStorePort
PolicyDecisionStorePort
QuotaStorePort
UsageStorePort
ConnectorStorePort
CredentialStorePort
EntitlementStorePort
SubscriptionStorePort
WebhookStorePort
WebhookDeliveryStorePort
NotificationStorePort
ApiKeyStorePort
SessionStorePort
ScheduleRunStorePort
AuditQueryPort
```

Every port must define:

- tenant scope expectations;
- mutation/query methods;
- typed errors;
- pagination where needed;
- transaction-aware variants for atomic use-cases.

---

# 20. P1 — Database integrity and cross-scope foreign keys

The database stores both `tenant_id` and `organization_id` on many tables.

Application checks alone are not enough.

## Required database invariants

Where applicable, child rows must prove:

```text
tenant.organization_id == child.organization_id
```

Use composite foreign keys or equivalent constraints.

Examples requiring review:

```text
agents
agent_versions
tools
tool_permissions
workflows
workflow_nodes
workflow_edges
executions
tasks
policies
quotas
usage_records
connectors
webhooks
```

### Required migration

Create a new migration after the existing schema, for example:

```text
migrations/0013_cross_scope_integrity/up.sql
```

The exact migration number must follow the actual migration ledger if other migrations exist in the target branch.

### Acceptance criteria

A row with a tenant ID from one organization and an organization ID from another cannot be inserted even if application code is bypassed.

---

# 21. P1 — Task state transitions must be compare-and-swap safe

## Problem

A stale task object should not be able to overwrite a newer state in a concurrent worker scenario.

## Required fix

Use optimistic versioning or conditional state transitions.

Examples:

```sql
UPDATE tasks
SET status = $new_status,
    version = version + 1
WHERE id = $task_id
  AND version = $expected_version
  AND status = $expected_status;
```

Or use an equivalent `attempt_number/fencing_token` guarded update.

Never allow a stale worker to move a newer task from:

```text
running -> retrying
```

if another worker has already recovered/reclaimed it.

### Acceptance criteria

A stale completion/failure returns a typed conflict and does not mutate the current owner/attempt.

---

# 22. P1 — Scheduler production correctness

## 22.1 Replace production `InMemoryLeaseStore`

`crates/scheduler-service/src/main.rs` currently imports/uses `InMemoryLeaseStore` in production-oriented startup paths.

Replace it with a shared durable lease implementation.

Recommended file:

```text
crates/scheduling/src/postgres_lease.rs
```

or an equivalent persistent implementation.

## 22.2 Fix schedule completion identity

Current completion logic can update unfinished runs for a schedule rather than the exact execution.

### Required final API

Completion notice must include:

```text
schedule_id
schedule_run_id
execution_id
outcome
completed_at
```

Update exactly one run using a unique identity.

Never mark “all unfinished runs of schedule X” as finished because one execution completed.

## 22.3 Authenticate scheduler completion endpoint

`/v1/scheduler/completions` must not be a public unauthenticated state-changing endpoint.

Use authenticated service-to-service communication:

- mTLS;
- signed service token;
- or another strong service identity.

## 22.4 Durable overlap semantics

Use `schedule_runs` plus durable lease/unique constraints as the source of truth.

If a DB uniqueness constraint is the final duplicate-fire guard, the dispatcher must rely on it consistently and surface the conflict as a safe idempotent replay rather than silently creating extra work.

### Acceptance criteria

Two scheduler replicas fire one planned tick exactly once at the execution level.

A completion event for execution A cannot finish execution B.

An unauthenticated client cannot mark schedule runs completed.

---

# 23. P1 — Complete production Tool Runtime

The execution layer has a `ToolRunner`, but the production path must connect it to the enterprise integration registry and security controls.

## Required flow

```text
Python decides tool call
        |
        v
Rust ToolInvocationService
        |
        v
Tenant/Agent/Tool permission
        |
        v
Policy evaluation
        |
        v
Quota reservation
        |
        v
Connector resolution
        |
        v
SecretReference resolution
        |
        v
SSRF/network guard
        |
        v
External system
        |
        v
Result schema validation
        |
        v
Usage + audit + event
```

### Required production adapters

Ensure:

```text
HTTP
Database
Object Storage
Email
CRM
Ticketing
Messaging
Webhook
OAuth
```

are actual runtime-capable adapters or explicitly marked unavailable until implemented.

No “registered but not executable” tool must be exposed as production-capable.

---

# 24. P1 — SSRF and outbound network enforcement

`integrations/http_guard.rs` and related networking logic must be connected to the real outbound HTTP path.

Required protections:

- allow only configured schemes;
- reject loopback/private/link-local/metadata IP ranges unless explicitly permitted;
- defend against DNS rebinding;
- resolve and validate destination at connection time;
- enforce timeout;
- cap redirects;
- validate redirect targets again;
- cap response size;
- prevent credential leakage through logs/URLs;
- audit outbound high-risk requests.

The existence of a validator file is insufficient; the real HTTP client must use it.

---

# 25. P1 — Policy Engine production wiring

`mas-policy` already contains policy logic and an engine adapter foundation.

Complete production storage and application integration.

Required:

```text
PolicyRepository
PolicyService
PolicyEngine
PolicyDecision persistence
Policy audit
API routes
Execution integration
Tool integration
Agent execution integration
```

All security-sensitive actions must evaluate policy before execution.

### Deny-by-default rules

Undefined permission must be denied.

No `AllowAllPolicy` may appear in production composition.

---

# 26. P1 — Audit trail and tamper evidence

The audit database trigger already protects against ordinary UPDATE/DELETE in the audited table. Preserve that protection.

If the schema/documentation claims a tamper-evident hash chain, implement the actual chain semantics.

Required fields:

```text
event_id
previous_hash
current_hash
canonical_payload
tenant_id
organization_id
actor_id
action
resource_type
resource_id
outcome
occurred_at
correlation_id
```

Canonicalization must be deterministic.

Add a verifier tool:

```text
crates/cli/src/commands/audit.rs
```

with commands equivalent to:

```text
audit verify-chain
 audit verify-tenant
 audit export
```

Do not claim “tamper-proof” when only append-only storage exists.

---

# 27. P1 — Notifications/Webhooks

Complete:

```text
WebhookRepository
WebhookDeliveryRepository
WebhookService
NotificationRepository
NotificationService
```

Required webhook behavior:

- HMAC/signature verification;
- timestamp/replay window;
- delivery retry policy;
- exponential backoff + jitter;
- dead-letter after retry ceiling;
- delivery idempotency;
- secret rotation;
- response status/latency capture;
- no plaintext secret logging.

---

# 28. P1 — API surface completion

Current router exposes a subset of the platform.

Required runtime-critical additions:

```text
/v1/tasks
/v1/tasks/{task_id}
/v1/tools
/v1/tools/{tool_id}
/v1/policies
/v1/policies/{policy_id}
/v1/quotas
/v1/usage
/v1/connectors
/v1/connectors/{connector_id}
/v1/webhooks
/v1/webhooks/{webhook_id}
/v1/executions/{execution_id}/events
/v1/executions/{execution_id}/cancel
/v1/api-keys
/v1/audit
```

Add API endpoints only where they correspond to implemented service-layer contracts.

### API contract requirements

Every protected mutation must:

```text
authenticate
authorize
resolve tenant
apply RLS
validate payload
check idempotency where relevant
execute transaction
emit audit/outbox
return stable response
```

---

# 29. P1 — Human identity / enterprise authentication foundation

This can be a later user-facing increment than API-key execution, but the Rust Core must have clean extension points.

Prepare for:

```text
OIDC
OAuth2
JWKS
SSO
SCIM
session revocation
organization membership
role mapping
```

Do not implement insecure custom password/authentication if a trusted identity provider is intended.

At minimum ensure `User`, `Membership`, `Session`, role and scope models can support enterprise identity integration without redesigning tenancy.

---

# 30. P1 — Scheduler service and API use the same canonical execution pipeline

Do not let scheduler create executions through a separate simplified path.

Required:

```text
schedule tick
    -> canonical application command
    -> canonical OrchestrationEngine
    -> execution + task + outbox
```

Scheduler should not duplicate core execution business rules.

---

# 31. P1 — Usage metering must be authoritative

Usage must not be inferred only from logs.

Record authoritative usage for:

```text
execution
agent
model/provider
input tokens
output tokens
tool calls
wall-clock time
storage
API requests
```

Python will eventually report model usage, but Rust must validate and persist the platform-level usage record and bind it to tenant/execution IDs.

Do not allow a client-provided usage number to become billing truth without validation.

---

# 32. P1 — Entitlement/subscription runtime enforcement

The database/domain includes subscription/entitlement concepts. Complete the runtime chain:

```text
organization/tenant plan
        -> entitlement
        -> feature check
        -> quota
        -> execution
```

Examples:

```text
feature.agent.supervisor
feature.rag
feature.tool.database
feature.webhooks
feature.enterprise_sso
limit.max_agents
limit.max_concurrent_runs
limit.max_monthly_tokens
```

Any entitlement override must be tenant-scoped and auditable.

---

# 33. P1 — Health/readiness improvements

Separate:

```text
liveness
readiness
startup
```

Readiness should fail when critical dependencies required for safe execution are unavailable.

Examples:

```text
PostgreSQL
Task broker
Outbox relay
Python orchestrator
Redis (if quota/concurrency depends on it)
```

Do not report the system “ready” merely because the HTTP server is listening.

Health payloads must not expose secrets or internal credentials.

---

# 34. P1 — Observability requirements

Existing tracing/metrics foundation should be wired to every critical path.

Required metrics:

```text
http_requests_total
http_request_latency
api_auth_failures_total
authorization_denials_total
policy_denials_total
task_queue_depth
task_claim_latency
task_attempts_total
task_retries_total
task_dead_letters_total
execution_started_total
execution_completed_total
execution_failed_total
execution_cancelled_total
execution_latency
workflow_step_latency
agent_bridge_latency
agent_bridge_errors
agent_bridge_timeouts
tool_calls_total
tool_failures_total
quota_denials_total
quota_reservations
outbox_pending
outbox_publish_failures
scheduler_fire_total
scheduler_duplicate_guard_total
```

Every span should preserve:

```text
request_id
correlation_id
causation_id
tenant_id
organization_id
execution_id
task_id
attempt_id
```

Sensitive AI content is not a default log field.

---

# 35. P1 — Failure handling and recovery

Every external/runtime boundary needs a typed failure model.

Minimum categories:

```text
validation
unauthorized
forbidden
not_found
conflict
rate_limited
quota_exceeded
timeout
cancelled
transient_dependency
permanent_dependency
serialization
policy_denied
internal
```

Every error must declare whether it is:

```text
retryable
non_retryable
requires_compensation
terminal
```

Retry must never happen merely because an error exists.

---

# 36. P1 — Checkpoint/recovery must be production durable

`crates/execution/src/checkpoint` and orchestration checkpoint logic must use durable persistence for production.

Required:

```text
execution checkpoint
workflow node state
completed step list
pending branch list
context/version checksum
```

A worker process crash after node 3 of 7 must allow safe resume rather than restarting every node blindly.

Any external side effect must use idempotency/compensation semantics.

---

# 37. P1 — Approval/Human-in-the-loop control

The workflow domain supports an approval node concept.

Complete the production lifecycle:

```text
workflow reaches approval node
      -> execution Paused/PendingApproval
      -> approval record
      -> authorized human action
      -> policy re-check
      -> resume
```

Approval must be tenant-scoped and actor-scoped.

Do not let a generic resume endpoint bypass required approval.

---

# 38. P1 — Strong API idempotency semantics

Idempotency must include:

```text
tenant_id
operation
idempotency_key
request fingerprint
original response/reference
created_at
expiry
```

If the same key is reused with a different request payload, return a typed idempotency conflict.

Do not replay a previous execution for a materially different request.

---

# 39. P1 — Security of internal service endpoints

For every internal-only HTTP/gRPC service:

- authenticate the caller;
- authorize the operation;
- use mTLS or signed workload identity;
- reject untrusted forwarded headers;
- propagate correlation metadata safely;
- enforce deadline and body-size limits;
- rate-limit as appropriate;
- record security events.

Internal network placement alone is not an authorization mechanism.

---

# 40. P1 — Configuration and secret handling

Keep:

```text
config defaults
environment variables
secret manager references
```

separate.

Production config files must never contain real secrets.

Validate required production config at startup.

Provide explicit startup failure messages for missing mandatory dependencies.

---

# 41. P1 — Repository transaction boundaries and concurrency semantics

Every mutation repository must define whether it requires:

```text
plain single-statement atomicity
transaction
row lock
optimistic version check
advisory lock
lease
```

Do not rely on comments such as “caller ensures serialization”.

Make concurrency behavior executable in tests.

---

# 42. P2 — Rust toolchain alignment

The audited workspace has a Rust MSRV/configuration value and a separately pinned toolchain version.

Make the policy explicit.

Either:

```text
rust-version == actual CI/toolchain baseline
```

or document:

```text
rust-version = supported MSRV
rust-toolchain = developer/CI pinned toolchain
```

and ensure all features compile on the declared MSRV.

CI must validate the chosen policy.

---

# 43. P2 — Repository/documentation synchronization

Do not keep stale hard-coded claims such as:

```text
number of crates
number of tests
feature “complete” status
```

unless generated by CI or manually verified in the same release.

Update:

```text
README.md
docs/production-wiring.md
docs/architecture.md
docs/nats-wiring.md
```

so documentation describes the actual current runtime composition.

---

# 44. P2 — Repository artifact hygiene

Do not commit host/environment artifacts from the uploaded workspace.

Examples requiring exclusion/removal from the repository artifact:

```text
.profile
.sudo_as_admin_successful
user-local .cargo/bin toolchains
/home/... absolute developer paths
```

`.cargo-env` must not contain a hard-coded developer machine path in the production repository.

Use repository-relative tooling and environment variables.

---

# 45. Required production composition

The final production startup must compose the following explicitly:

```text
Config
  |
  +-- PostgreSQL
  +-- Redis/shared quota/concurrency (if selected)
  +-- NATS/JetStream or durable task wake-up transport
  +-- Outbox Relay
  +-- Policy Engine
  +-- Quota Engine
  +-- Authorization
  +-- Tenant/RLS context provider
  +-- Tool runtime
  +-- AgentBridge -> Python
  +-- OrchestrationEngine
  +-- ExecutionExecutor
  +-- Scheduler
  +-- Worker
  +-- Observability
  +-- Health registry
```

No production-required dependency may be silently replaced with:

```text
Noop
InMemory
Echo
AllowAll
```

unless the feature is explicitly disabled and startup refuses requests that require it.

---

# 46. Required new production files / likely additions

The exact file names may be adjusted to match the final crate layout, but equivalent functionality is mandatory.

```text
crates/application/src/authorization.rs
# Application-level authorization service/port used by all protected use-cases.

crates/persistence/src/scoped_db.rs
# Request-scoped PostgreSQL transaction wrapper that applies RLS context before tenant-owned operations.

crates/persistence/src/repositories/idempotency.rs
# Durable idempotency reservation/replay/conflict store.

crates/persistence/src/repositories/policies.rs
# PostgreSQL policy and policy-decision repositories.

crates/persistence/src/repositories/quota.rs
# Durable quota/usage repository and reservation ledger.

crates/persistence/src/repositories/tools.rs
# Tool/tool-permission repositories and transaction-aware mutations.

crates/persistence/src/repositories/connectors.rs
# Connector/credential repositories using secret references only.

crates/persistence/src/repositories/webhooks.rs
# Webhook endpoint and delivery repositories.

crates/persistence/src/repositories/subscriptions.rs
# Subscription and entitlement persistence.

crates/persistence/src/repositories/sessions.rs
# Session persistence and revocation.

crates/execution/src/grpc_agent_bridge.rs
# Production Rust-to-Python gRPC AgentBridge.

crates/worker-service/src/execution_handler.rs
# Real production execution task handler.

crates/outbox-relay/Cargo.toml
# Dedicated production outbox relay binary manifest.

crates/outbox-relay/src/main.rs
# Outbox relay service bootstrap.

crates/outbox-relay/src/relay.rs
# Durable claim/publish/ack/retry loop.

crates/outbox-relay/src/lease.rs
# Replica-safe outbox claim lease/fencing logic.

crates/scheduling/src/postgres_lease.rs
# Durable scheduler lease implementation.

crates/worker/src/task_fencing.rs
# Optional dedicated helper for task attempt/fencing validation if not kept in lifecycle.rs.

contracts/proto/agent_runtime.proto
# Canonical Rust/Python agent-runtime contract.

contracts/proto/execution_runtime.proto
# Canonical execution state/event contract.

contracts/proto/tool_runtime.proto
# Canonical tool invocation/result contract.

contracts/proto/policy_runtime.proto
# Canonical policy evaluation contract.
```

---

# 47. Required database migrations

Create the migrations required by the final implementation. Do not edit already-applied migration history in a deployed environment.

Potential migrations include:

```text
0013_cross_scope_integrity
0014_task_attempt_fencing
0015_execution_idempotency_fingerprint
0016_outbox_claim_leases
0017_scheduler_lease_fencing
0018_usage_reservations
0019_approval_records
0020_audit_hash_chain
```

The exact numbering must be computed from the actual migration ledger at implementation time.

Every migration must:

- be forward-only for production;
- have transactional safety where PostgreSQL supports it;
- have explicit indexes;
- include rollback considerations in documentation;
- be tested against an empty database and an upgraded existing database.

---

# 48. Required end-to-end tests

These are release-blocking tests.

## 48.1 Authentication

```text
valid API key -> accepted
expired API key -> rejected
revoked API key -> rejected
wrong tenant header -> rejected
wrong organization header -> rejected
missing scope -> forbidden
```

## 48.2 Tenant isolation

Create:

```text
Organization A / Tenant A
Organization B / Tenant B
```

Prove every tenant-owned resource rejects cross-tenant reads/writes.

Test through both:

```text
HTTP API
internal application service
raw repository path subject to RLS
```

## 48.3 Execution pipeline

```text
API request
 -> auth
 -> tenant resolution
 -> policy
 -> quota
 -> execution
 -> task
 -> outbox
 -> broker
 -> worker
 -> execution runtime
 -> Python bridge
 -> completion
```

All IDs must remain consistent.

## 48.4 Idempotency race

Send the same idempotency key concurrently from multiple clients.

Expected:

```text
1 execution
1 task
1 outbox command
N safe replay responses
```

## 48.5 Worker crash

Kill worker after claim and before completion.

Expected:

```text
lease expires
new attempt claims task
stale worker cannot overwrite
execution eventually reaches terminal state
```

## 48.6 Scheduler duplicate fire

Run two scheduler replicas against one due schedule.

Expected:

```text
one schedule_run
one execution
one task
```

## 48.7 Outbox crash

Fail relay after database commit but before broker acknowledgement.

Expected:

```text
event remains durable
relay retries
consumer deduplicates
```

## 48.8 Quota across replicas

Run two worker replicas and issue enough concurrent operations to exceed the tenant limit.

Expected:

```text
configured ceiling never exceeded
```

## 48.9 Policy enforcement

Test:

```text
allow
deny
approval-required
quota-blocked
```

for agent execution and tool invocation.

## 48.10 Encryption tamper test

Modify one byte of ciphertext.

Expected:

```text
typed authentication/integrity error
no plaintext returned
no panic
```

## 48.11 Rust ↔ Python contract

Run a real local/integration Python gRPC service and verify:

```text
request metadata
deadline
cancellation
success
retryable failure
terminal failure
usage
```

## 48.12 API security regression

Every protected route must pass an authorization matrix test.

---

# 49. Mandatory Rust validation commands

Once remediation is complete and a Rust toolchain is available, run exactly the relevant commands below.

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo check --workspace --all-targets --locked
cargo test --workspace --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
```

Then run live integration tests using real infrastructure.

Example environment names:

```text
MAS_TEST_DATABASE_URL
MAS_TEST_NATS_URL
MAS_TEST_REDIS_URL
MAS_TEST_PYTHON_GRPC_URL
```

Do not report success based only on mock/unit tests when the release criteria depend on PostgreSQL/NATS/Redis/Python connectivity.

---

# 50. Required CI gates

CI must reject merges if any of these fail:

```text
format check
clippy with warnings denied
workspace check
workspace tests
documentation build
migration verification
schema/RLS integration tests
cross-tenant isolation tests
idempotency race tests
scheduler duplicate-fire tests
worker crash/recovery tests
outbox atomicity tests
Rust/Python contract tests
secret scan
dependency audit
```

Use a reproducible toolchain and locked dependencies.

---

# 51. Production prohibition list

The following must not remain on a production execution path:

```text
EchoHandler
AllowAllPolicy
InMemoryLeaseStore for cross-replica ownership
InMemoryAgentBridge
InMemory quota counters
Noop event publisher
Noop policy implementation
caller-trusted tenant headers
unscoped database transaction for tenant data
plaintext secret persistence
unauthenticated scheduler completion
stale worker overwrite
synthetic delivery count used as authoritative attempt count
```

Tests may use these implementations where appropriate.

Production startup must either provide the real implementation or refuse to start/use the affected capability.

---

# 52. Definition of Done for Rust Core

Rust Core is **NOT complete** until every statement below is true.

## Security

- [ ] API principal is bound to tenant and organization from trusted credential data.
- [ ] Scope/role authorization is enforced at API and application-service boundaries.
- [ ] RLS is applied to normal tenant-owned DB requests.
- [ ] Cross-tenant access tests pass against live PostgreSQL.
- [ ] Internal service calls are authenticated.
- [ ] Secrets never appear in logs or ordinary rows.
- [ ] Encryption tamper tests pass.

## Execution

- [ ] One canonical execution lifecycle exists.
- [ ] Execution submission creates a durable runnable task.
- [ ] Outbox and state mutation are atomic.
- [ ] Worker executes real runtime code.
- [ ] No production EchoHandler exists.
- [ ] AgentBridge has a real production gRPC implementation.
- [ ] Execution completes/fails/cancels correctly.
- [ ] Checkpoint/recovery works after worker failure.

## Distributed runtime

- [ ] Worker leases are durable/fenced.
- [ ] Scheduler leases are durable/fenced.
- [ ] Global concurrency is distributed-safe.
- [ ] Quotas are distributed-safe.
- [ ] Task attempt count is durable.
- [ ] Stale workers cannot mutate newer attempts.
- [ ] Worker max-in-flight is actually enforced.

## Data

- [ ] Runtime-critical schema entities have row/repository/application coverage.
- [ ] Cross-tenant/org integrity is enforced in DB.
- [ ] Idempotency includes request fingerprint.
- [ ] Audit trail is append-only and any advertised hash-chain integrity is implemented and verifiable.

## Eventing

- [ ] Outbox relay is production-wired.
- [ ] Event publication is retryable.
- [ ] Consumers are idempotent.
- [ ] Dead-letter/replay works.

## Scheduler

- [ ] Scheduler uses canonical execution submission.
- [ ] Duplicate-fire protection is replica-safe.
- [ ] Completion is keyed by exact execution/run ID.
- [ ] Completion endpoint is authenticated.

## Contracts

- [ ] One canonical versioned proto source exists.
- [ ] Rust and Python bindings are generated from the same source.
- [ ] Contract tests pass.
- [ ] Deadline/cancellation/correlation metadata propagate.

## API

- [ ] Runtime-critical routes exist.
- [ ] Every protected mutation authorizes correctly.
- [ ] Errors use stable typed error contracts.
- [ ] Health/readiness reflect dependency truth.

## Quality

- [ ] `cargo fmt --check` passes.
- [ ] `cargo clippy ... -D warnings` passes.
- [ ] `cargo check` passes.
- [ ] `cargo test` passes.
- [ ] docs build passes.
- [ ] live integration tests pass.
- [ ] CI gates pass.

---

# 53. Recommended implementation order

Do the work in this exact dependency order.

```text
Phase R1 — Security identity/context
    -> trusted principal
    -> tenant/org binding
    -> service authorization
    -> RLS request integration

Phase R2 — Transaction/data foundations
    -> transaction-aware repositories
    -> idempotency
    -> rows/repos/ports for runtime entities
    -> cross-scope DB constraints

Phase R3 — Canonical execution path
    -> application facade over OrchestrationEngine
    -> production engine ports
    -> execution + task + outbox atomicity

Phase R4 — Worker runtime
    -> durable task attempts
    -> worker fencing
    -> RealExecutionHandler
    -> production ExecutionExecutor composition

Phase R5 — Python bridge boundary
    -> canonical protobuf source
    -> production gRPC AgentBridge
    -> internal service auth
    -> contract tests

Phase R6 — Distributed infrastructure
    -> distributed quota
    -> distributed concurrency
    -> durable scheduler lease
    -> durable outbox relay

Phase R7 — Tools / policy / integrations
    -> production tool runtime
    -> policy integration
    -> connector/secret integration
    -> SSRF enforcement

Phase R8 — Scheduler convergence
    -> canonical execution path
    -> exact completion identity
    -> authenticated completion
    -> multi-replica tests

Phase R9 — API/control-plane completion
    -> tasks
    -> tools
    -> policy
    -> quota/usage
    -> connectors
    -> webhooks
    -> audit
    -> API keys/sessions

Phase R10 — Full verification
    -> live integration suite
    -> security suite
    -> chaos/recovery suite
    -> performance smoke tests
    -> CI green
```

---

# 54. Coding-agent execution rules

This section is mandatory for any coding agent implementing the remediation.

## Rule 1 — Do not skip files

Every file explicitly required by the implementation must be fully written.

Do not use:

```text
...
existing code
implementation omitted
same as above
placeholder
TODO instead of implementation
```

## Rule 2 — Preserve working behavior

Do not remove existing functionality merely to make the code simpler.

If an existing component is partially correct:

```text
preserve it
upgrade it
wire it into production
add tests
```

## Rule 3 — No production stubs

Do not replace missing production logic with:

```text
Noop
InMemory
Echo
AllowAll
mock
```

except in test/dev-only modules.

## Rule 4 — Keep boundaries clean

Rust:

```text
security
authorization
tenant isolation
state
execution
policy
quota
tool control
persistence
transport
```

Python later:

```text
LLM
agent reasoning
planning
RAG
memory
model providers
AI evaluation
```

## Rule 5 — Security before convenience

Never bypass tenant/org/scope checks to simplify a service method.

Never accept request headers as authority without validating them against the trusted credential context.

## Rule 6 — Database invariants should be enforced in DB where practical

Do not depend exclusively on application code for tenant/org consistency.

## Rule 7 — Every new async path needs failure semantics

Document/implement:

```text
retry
timeout
cancellation
idempotency
lease
recovery
```

as relevant.

## Rule 8 — Update tests with every fix

Every bug fix should add or update a regression test.

## Rule 9 — Do not start Python before Rust gate passes

The Python Orchestrator depends on stable Rust execution contracts.

Do not invent Python-facing contracts ad hoc while Rust Core is still changing.

## Rule 10 — Final response from coding agent

At the end of Rust remediation, report:

```text
files added
files changed
migrations added
production stubs removed
commands executed
integration tests executed
security tests executed
known remaining gaps
```

No claim of “complete” is allowed when a release-blocking item remains unchecked.

---

# 55. Final release gate

The Rust Core release gate is:

```text
                    RUST CORE
                         |
                         v
              ┌─────────────────────┐
              │ Security Gate       │
              │ auth / tenant / RLS │
              └──────────┬──────────┘
                         |
                         v
              ┌─────────────────────┐
              │ Data Gate            │
              │ tx / idempotency     │
              │ repositories / DB    │
              └──────────┬──────────┘
                         |
                         v
              ┌─────────────────────┐
              │ Runtime Gate         │
              │ execution/task/worker│
              └──────────┬──────────┘
                         |
                         v
              ┌─────────────────────┐
              │ Distributed Gate     │
              │ leases/quota/outbox  │
              └──────────┬──────────┘
                         |
                         v
              ┌─────────────────────┐
              │ Python Contract Gate │
              │ real gRPC bridge     │
              └──────────┬──────────┘
                         |
                         v
              ┌─────────────────────┐
              │ Live E2E Gate        │
              │ real PostgreSQL      │
              │ real broker          │
              │ real worker          │
              │ real Python service  │
              └──────────┬──────────┘
                         |
                         v
                =====================
                RUST CORE APPROVED
                =====================
                         |
                         v
                START PYTHON ORCHESTRATOR
```

**Do not mark the Rust Core complete before the full gate passes.**

---

# 56. Bottom line

The current Rust workspace has a meaningful foundation and should be **repaired and completed rather than discarded**.

The highest-value work is not adding more isolated modules. It is connecting the existing modules into one secure, transactional, durable, distributed execution pipeline.

The required final outcome is:

```text
Trusted request
    -> tenant-secure DB context
    -> policy/quota
    -> canonical execution engine
    -> durable task
    -> transactional outbox
    -> broker
    -> fenced worker
    -> real execution runtime
    -> authenticated Python AgentBridge
    -> tool/policy enforcement
    -> checkpoint/recovery
    -> durable usage/audit
    -> terminal execution state
```

Once that path is real, tested and production-safe, the **Python Orchestrator** can be built against a stable, explicit contract instead of compensating for Rust Core gaps.
