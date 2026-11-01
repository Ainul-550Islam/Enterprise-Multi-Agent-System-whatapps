# NEXT PHASE — CANONICAL DURABLE EXECUTION PIPELINE

## ROLE

Act as a Principal Rust Engineer + Distributed Systems Engineer + Database Reliability Engineer + Production SRE.

The Rust ↔ Python AgentBridge integration is now reported complete.

Current reported verification includes:

- Rust gRPC AgentBridge
- Python AgentBridge server
- production GrpcAgentBridge
- identity propagation
- timeout/cancellation/error propagation
- provider failover integration
- 475 Rust tests without NATS
- 484 Rust tests with NATS
- 9 E2E tests
- 1890 Python tests
- 36/36 deliverable files byte-for-byte verified

Use the actual repository as source of truth. Do not assume the status report is correct without checking the source.

---

# PRIMARY OBJECTIVE

Now implement the **single canonical durable execution lifecycle**.

The required production path is:

User Request
→ authenticated principal
→ authoritative tenant context
→ Authorization
→ ExecutionService
→ atomic Execution + Idempotency + Task + Audit + Outbox
→ durable broker
→ Worker
→ Task Attempt
→ Execution Runtime
→ GrpcAgentBridge
→ Python Agent Runtime
→ Model/Tool execution
→ Result
→ durable Execution/Step/Task state
→ Events
→ Usage/Audit
→ final result

There MUST NOT be competing execution lifecycles.

---

# ABSOLUTE RULE — NO SHORTENING

For every modified/new file:

- output the COMPLETE file
- beginning to end
- no `...`
- no omitted sections
- no placeholder
- no "existing code"
- no "rest unchanged"
- no synthetic replacement
- no shortened code

Never remove existing unrelated logic.

The existing implementation must be preserved except where the current phase explicitly requires a change.

---

# FILE COMMENT RULE

Every new file MUST contain a clear comment explaining:

- why the file exists
- what layer owns it
- what production responsibility it has
- what it must NOT do

Example:

```rust
// PURPOSE:
// Canonical durable task lifecycle owned by the application/persistence boundary.
// This module must not create a second execution state machine.
```

For Python:

```python
# PURPOSE:
# Production execution entrypoint for requests received through AgentBridgeApi.
# This module delegates to the existing canonical Agent Runtime and does not
# maintain Rust-owned execution state.
```

---

# PHASE 1 — INSPECT CURRENT EXECUTION PATH

Before writing code, trace:

- `ExecutionService`
- `ExecutionExecutor`
- `OrchestrationEngine`
- `WorkflowRuntime`
- task repository
- execution repository
- task_attempt repository
- outbox repository
- event publisher
- worker main
- worker task handler
- broker producer
- broker consumer
- AgentBridge
- persistence transaction helper
- idempotency implementation

Identify ALL independent execution entrypoints.

Produce an internal dependency map.

Do not invent another execution service.

There must be exactly one canonical lifecycle.

---

# PHASE 2 — CANONICAL STATE MACHINE

Define the authoritative execution lifecycle.

Execution states must have explicit transitions.

At minimum inspect and correctly support:

- pending
- queued
- running
- succeeded
- failed
- cancelled

Do not introduce states unless the existing domain model requires them.

Every transition must have:

- valid source state
- target state
- actor/owner where relevant
- timestamp
- version
- failure information when applicable

Invalid transitions must fail safely.

---

# PHASE 3 — ATOMIC EXECUTION SUBMISSION

Modify the canonical execution submission path so that the initial mutation is atomic.

One transaction must cover the appropriate durable operations:

1. validate request
2. verify idempotency
3. create execution
4. create task
5. create initial task-attempt state if the domain requires it
6. create audit event
7. create outbox event
8. commit

No operation may be reported successful before the transaction commits.

Do NOT:

```text
commit execution
then create task
```

Do NOT:

```text
commit task
then publish event
```

Do NOT:

```text
publish message
then persist execution
```

The durable source of truth must be committed first.

---

# PHASE 4 — IDEMPOTENCY

Implement durable idempotency semantics.

The canonical idempotency model must prevent:

- duplicate execution creation
- duplicate task creation
- duplicate dispatch caused by repeated client requests

The database must enforce uniqueness where required.

Do NOT rely only on:

```text
SELECT then INSERT
```

for uniqueness.

Handle race conditions between concurrent identical requests.

The result must distinguish:

### New request

Creates execution and returns the canonical execution identity.

### Same idempotency key + same request fingerprint

Returns the existing execution safely.

### Same idempotency key + different fingerprint

Returns a deterministic idempotency conflict.

The fingerprint must use a stable canonical representation.

Do not include secrets in fingerprints.

---

# PHASE 5 — TASK CREATION

A successful execution submission must produce a durable task.

The task must contain the information required for a worker to recover the work after restart.

Do not place essential task state only in memory.

Task must be linked to:

- execution
- tenant
- project/workflow/agent where applicable
- lifecycle state
- attempt identity/state
- timestamps
- version/fencing field where already present in the domain

Do not duplicate execution-owned state into incompatible parallel state machines.

---

# PHASE 6 — OUTBOX

The task dispatch event must be written into the transactional outbox in the same database transaction as execution/task creation.

The outbox message must contain the canonical task identity.

At minimum carry:

- event ID
- event type
- aggregate/execution ID
- task ID
- tenant identity
- correlation/request IDs
- payload version
- creation timestamp

Do not place raw credentials or secrets in the outbox payload.

---

# PHASE 7 — PRODUCTION OUTBOX RELAY

Implement or complete the real production outbox relay.

The relay must:

- load pending events
- safely claim work
- publish to durable broker
- distinguish publish success/failure
- retry transient failures
- retain failed events
- prevent uncontrolled duplicate publication where possible
- expose backlog telemetry

Do not silently discard publish failures.

Do not mark published before broker acknowledgement according to the broker semantics.

Do not depend on process-local memory as the authoritative outbox state.

---

# PHASE 8 — BROKER DISPATCH

Use the existing NATS/JetStream or canonical broker infrastructure.

Do not introduce another broker.

The broker message must identify:

- task ID
- execution ID
- event ID
- tenant ID
- correlation ID
- attempt/version data where required

Consumers must validate the message before execution.

Malformed or unauthorized messages must not become successful executions.

---

# PHASE 9 — WORKER CONSUMPTION

Worker must consume the durable task event.

Production worker must NOT use:

- EchoHandler
- fake handler
- hardcoded result
- test handler
- InMemoryAgentBridge

The production handler must reach the real canonical execution runtime and then the real `GrpcAgentBridge`.

---

# PHASE 10 — TASK ATTEMPT DURABILITY

Implement true durable `task_attempts`.

For every actual worker attempt:

1. create/claim attempt
2. assign ownership
3. record started_at
4. execute
5. record success/failure/cancellation
6. record finished_at

Do not fabricate attempt counts.

Do not derive delivery count from broker metadata when the domain requires durable attempts.

An attempt must have deterministic linkage to:

- task
- execution
- worker identity
- attempt number
- start/finish state

---

# PHASE 11 — CONCURRENCY / STALE WORKER PROTECTION

The canonical task update path must prevent stale workers from overwriting newer state.

Use the repository/domain version or fencing semantics already present where possible.

A worker that has lost ownership MUST NOT be able to transition a task using stale state.

Test:

Worker A starts
→ ownership/version advances
→ Worker B becomes current owner
→ Worker A completes late
→ Worker A update is rejected

Never allow stale completion to silently overwrite the current task state.

---

# PHASE 12 — WORKER CAPACITY

Fix fetch/claim behavior so the worker never claims more runnable work than configured capacity permits.

`max_in_flight` must be enforced against actual active attempts.

Do not fetch a batch larger than remaining capacity unless the implementation can prove no excess claim can occur.

Test boundary cases:

- capacity 0
- capacity 1
- capacity exactly N
- N+1 available tasks
- task completes while another claim happens
- worker restart during claim

---

# PHASE 13 — EXECUTION RUNTIME HANDOFF

After durable task claim:

```text
Worker
 ↓
canonical ExecutionExecutor
 ↓
workflow/runtime
 ↓
GrpcAgentBridge
 ↓
Python Agent Runtime
```

There must be no second execution state machine inside the worker.

The worker is a delivery/execution ownership component.

The canonical application/runtime layer remains responsible for domain execution semantics.

---

# PHASE 14 — RESULT PERSISTENCE

On successful execution:

Persist durable state before acknowledging task completion.

At minimum verify the appropriate writes for:

- execution terminal state
- task terminal state
- execution step results
- attempt completion
- audit event
- usage record
- resulting event/outbox entry where required

Do not report success to the broker before durable state is committed.

---

# PHASE 15 — FAILURE PERSISTENCE

For failures:

Persist:

- terminal/transient classification
- stable error code
- safe error message
- attempt outcome
- execution/task state
- timestamps
- correlation/request identity
- retry information where appropriate

Never silently convert:

- timeout → success
- cancellation → success
- authorization denial → generic internal error
- quota exceeded → provider failure

Preserve structured error semantics coming from Python/AgentBridge.

---

# PHASE 16 — RETRY SEMANTICS

Use the existing retry classification.

At minimum distinguish:

### Retryable

- temporary broker failure
- provider rate limit
- provider transient 5xx
- dependency unavailable
- transient network error

### Terminal

- authentication failure
- authorization failure
- invalid request
- invalid configuration
- tenant mismatch
- policy denial
- invalid tool permission
- malformed execution input

Do not retry terminal errors indefinitely.

Honor existing deadline/cancellation behavior.

---

# PHASE 17 — CANCELLATION

A cancellation request must operate against the durable execution state.

Cancellation must safely handle:

- pending task
- queued task
- running task
- already succeeded task
- already failed task

A completed execution must not be reopened by cancellation.

Running cancellation must propagate toward the runtime/AgentBridge where supported.

Cancellation must be idempotent.

---

# PHASE 18 — AUDIT

The canonical submission/execution lifecycle must generate authoritative audit events.

Audit events must be associated with:

- tenant
- organization where applicable
- principal/service identity
- execution
- task
- action
- timestamp
- correlation ID

Do not record secret values.

Do not create duplicate audit histories from multiple execution paths.

---

# PHASE 19 — USAGE

When execution produces billable/resource usage, persist authoritative usage information.

At minimum distinguish:

- model/provider
- input/output token counts where available
- execution/task identity
- tenant
- timestamp
- cost metadata where available

Do not use synthetic counters.

Do not mark usage successful if model execution failed.

Do not duplicate usage on idempotent replay.

---

# PHASE 20 — EVENTS

Terminal transitions should produce durable events where the existing architecture requires them.

Events must be derived from authoritative state.

Do not create contradictory events from:

- worker
- scheduler
- application service

for the same logical lifecycle transition.

Choose one canonical event ownership path.

---

# PHASE 21 — REMOVE COMPETING EXECUTION PATHS

Search for:

- duplicate ExecutionService logic
- worker-specific execution lifecycle
- orchestration-specific terminal state writes
- direct task state mutations outside canonical repository methods
- scheduler-created executions bypassing canonical service
- test production paths
- old execution handlers
- deprecated handlers still reachable

Classify each:

- CANONICAL
- TEST-ONLY
- DEV-ONLY
- DEAD
- DUPLICATE

Delete only genuinely obsolete production paths.

Do not delete code merely because it looks duplicated without tracing callers.

---

# PHASE 22 — TRANSACTION BOUNDARIES

Audit every mutation involved in execution submission and terminal completion.

Verify:

- transaction begins before authoritative mutations
- correct connection/transaction object is passed through repositories
- no hidden connection checkout breaks atomicity
- commit happens only after required writes
- rollback behavior is correct
- post-commit side effects cannot make the DB state falsely appear successful

No repository may silently open an independent transaction when atomicity is required.

---

# PHASE 23 — DATABASE CONCURRENCY

Add/verify:

- unique constraints
- foreign keys
- execution/task indexes
- task claim indexes
- idempotency constraints
- version/concurrency columns
- outbox claim indexes

Inspect migrations for production-safe ordering.

Do not make destructive schema changes without preserving existing data.

---

# PHASE 24 — FAILURE / RECOVERY TEST MATRIX

Add tests for:

1. DB failure before commit
2. DB failure after execution insert but before task insert
3. DB failure during outbox insert
4. broker publish failure
5. worker crash before attempt creation
6. worker crash after attempt creation
7. worker crash after runtime result
8. worker crash before terminal state commit
9. duplicate broker delivery
10. duplicate client idempotency request
11. stale worker completion
12. retryable provider error
13. terminal provider error
14. cancellation during execution
15. process restart
16. partial workflow failure
17. duplicate outbox relay delivery

The final state must never become silently successful or permanently impossible to recover.

---

# PHASE 25 — MULTI-TENANT TESTING

Explicitly test:

Tenant A creates execution
→ task belongs to Tenant A

Tenant B must NOT:

- read it
- claim it
- execute it
- cancel it
- receive its result
- receive its events
- receive its usage

Test both API-level and DB-level enforcement where supported by the current security architecture.

Do not weaken the existing Python tenant-isolation tests.

---

# PHASE 26 — OBSERVABILITY

Add metrics for:

- execution submissions
- duplicate idempotency requests
- idempotency conflicts
- task creation
- task claim latency
- active attempts
- task retries
- stale worker rejections
- execution duration
- successful execution
- failed execution
- cancelled execution
- outbox backlog
- outbox publish failure
- broker redelivery
- dead-letter count

Logs must include safe:

- request ID
- correlation ID
- execution ID
- task ID
- tenant-safe identity

Never log secrets.

---

# PHASE 27 — TEST OUTPUT REQUIREMENT

Run the entire existing test suite after changes.

Rust:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --locked
cargo test --workspace --locked --features nats
cargo doc --workspace --all-features --no-deps
```

Run relevant E2E and integration suites.

Do not claim green unless actually executed.

---

# PHASE 28 — NO REGRESSION

Preserve the completed AgentBridge behavior:

- protobuf contract
- gRPC adapter
- Python server
- identity propagation
- deadline
- cancellation
- structured errors
- provider failover
- tenant-aware tool execution

The current Python suite and Rust bridge suite must remain green.

---

# PHASE 29 — FULL FILE OUTPUT

For every changed file:

```text
FILE: exact/path
PURPOSE: exact responsibility
```

Then output the COMPLETE file.

No partial snippets.

No omitted functions.

No abbreviations.

No placeholders.

No pseudo-code.

---

# PHASE 30 — COMPLETION GATE

This phase is COMPLETE only when:

- one canonical execution lifecycle exists
- ExecutionService creates the durable Task
- execution + task + idempotency + required audit + outbox are atomic
- duplicate idempotency requests are safe
- conflicting fingerprints are rejected
- durable task attempts exist
- worker claims real durable tasks
- stale workers cannot overwrite current state
- max_in_flight cannot overclaim
- production worker reaches canonical Execution Runtime
- production execution reaches GrpcAgentBridge
- outbox relay is production-wired
- broker delivery is durable
- terminal state is persisted before successful acknowledgement
- failures are persisted correctly
- usage is authoritative
- audit is authoritative
- cancellation is durable and idempotent
- duplicate delivery is safe
- process restart is recoverable
- tenant isolation is preserved
- existing AgentBridge tests remain green

---

# FINAL STOP CONDITION

STOP after completing the canonical durable execution pipeline.

Do NOT start:

- frontend
- billing UI
- SSO
- RAG enhancements
- unrelated integrations
- cosmetic refactoring

Do NOT create a second execution service.

Do NOT introduce a new broker.

Do NOT use an in-memory production fallback.

Do NOT remove working AgentBridge functionality.

Do NOT shorten files.

Do NOT provide partial files.

Wait for explicit approval before the next phase.