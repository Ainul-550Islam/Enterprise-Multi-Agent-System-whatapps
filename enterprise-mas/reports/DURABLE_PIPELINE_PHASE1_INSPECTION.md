# placeholder

### F2 - CRITICAL: the worker calls `GrpcAgentBridge` directly, bypassing the runtime
**Phase 13 violation.**

`AgentRunHandler::execute` (`crates/worker-service/src/agent_execution.rs:269`):

```rust
let verdict = self.bridge.run(request).await?;
```

The canonical consumer of `AgentBridge` is `AgentNodeExecutor`
(`crates/execution/src/node_adapters.rs:198`, calling `self.bridge.run(...)` at
line 268) - reached only from inside `WorkflowRuntime`, which is reached only
from `Executor`, which has no caller (F1). So the production lane and the
canonical lane are two disjoint paths.

### F3 - CRITICAL: `ExecutionService::submit` creates no Task, no outbox row, and is not atomic
**Phase 3, 5, 6 violations.**

`crates/application/src/execution_service.rs:61-146` performs, in order:

1. `self.executions.save(&execution)` - line 125
2. `self.executions.idempotency_put(...)` - line 127
3. `audit::record_mutation(...)` - line 131

Three independent writes. There is **no `UnitOfWork` / `atomic()` anywhere in
the file**. No `Task` is created. No outbox event is enqueued. This is exactly
the shape the spec forbids:

> Do NOT: commit execution then create task

### F4 - HIGH: idempotency has no request fingerprint
**Phase 4 violation.**

On an idempotency hit the service returns the prior execution unconditionally
(lines 97-110). The spec requires three distinguishable outcomes - new,
same-key+same-fingerprint (safe replay), and same-key+**different**-fingerprint
(**deterministic conflict**). The third is missing entirely: two different
requests sharing a key silently return the first request's execution.

### F5 - HIGH: a second, in-memory idempotency mechanism exists
**Phase 4 violation.**

`crates/orchestration/src/idempotency.rs` defines `IdempotencyRecord`,
`Reservation::{Acquired, ReplayCompleted, ConflictInFlight}` and
`InMemoryIdempotencyStore`. `OrchestrationEngine::submit_task` uses it
(`engine.rs:485-494`). This competes with the durable
`executions.idempotency_get/put` used by `ExecutionService`, and the in-memory
store **cannot** survive a restart or coordinate across workers.

### F6 - HIGH: the outbox relay is implemented but never wired
**Phase 7 violation.**

The machinery exists and looks correct:

- `PostgresOutboxStore` implements `OutboxStorePort`: `enqueue` (:113),
  `fetch_due` (:118), `mark_published` (:142), `mark_failed` (:160),
  `mark_dead_lettered` (:187)
- `OutboxDispatcher::dispatch_due(limit)` - `crates/events/src/outbox.rs:221`

But `dispatch_due` is called **only from that module's own tests**
(`outbox.rs:296, 299, 345, 349, 353`). No service binary runs it. The outbox
therefore fills and nothing is ever published.

### F7 - HIGH: no durable `task_attempts` persistence
**Phase 10 violation.**

The schema **exists** (`migrations/0006_execution/up.sql:91-108`) with
`UNIQUE (task_id, attempt)`, `worker_id`, `lease_expires`, `status`,
`started_at`, `finished_at`, and a lease index. The **domain model exists**
(`crates/domain/src/task_attempt.rs`, `AttemptStatus` with
running/succeeded/failed/cancelled/timed_out).

What is missing is everything in between: **there is no repository**
(`crates/persistence/src/repositories/` has no `task_attempts.rs`), and no code
reads or writes the table.

Instead the worker derives attempts from **broker delivery count**
(`crates/worker/src/consumer.rs:553`): `if !self.config.retry
.allows_redelivery(deliveries) { ... "attempts exhausted after {deliveries}
deliveries" }` - precisely what the spec forbids: *"Do not derive delivery
count from broker metadata when the domain requires durable attempts."*

### F8 - HIGH: `max_in_flight` is not enforced - the worker overclaims
**Phase 12 violation.**

`crates/worker/src/consumer.rs:176-184`:

```rust
if in_flight.len() >= self.config.max_in_flight {
    // Leave overflow un-fetched in the next cycle:
    // already-fetched ones are mediated regardless.
    tracing::warn!("in-flight saturation: mediation deferred");
}
let mediator = self.mediator();
let sequence = message.sequence;
in_flight.spawn(async move { (sequence, mediator.mediate(message).await) });
```

There is **no `continue`**. The message says "deferred"; the code spawns it
anyway - the comment itself admits it ("mediated regardless"). Additionally the
fetch size is fixed: `self.broker.fetch(..., self.config.batch_size)` (line 163)
does not clamp `batch_size` to remaining capacity. So with `max_in_flight = 4`
and `batch_size = 32`, up to 32 tasks can be in flight.

### F9 - MEDIUM: `tasks` has no version/fencing column
**Phase 11 / 23 gap.**

`migrations/0006_execution/up.sql:59-81` - the `tasks` table has no `version`
column and no optimistic-concurrency field. Phase 11 requires that a stale
worker "MUST NOT be able to transition a task using stale state". Whatever
fencing is introduced needs a column the schema does not currently have.

### F10 - MEDIUM: multiple independent `Task::new` sites
**Phase 21 - needs classification, not deletion.**

| Site | Line | Provisional class |
|---|---|---|
| `crates/orchestration/src/dispatcher.rs` | 304 | DUPLICATE/DEAD (verify callers) |
| `crates/orchestration/src/engine.rs` | 496 | CANONICAL-candidate |
| `crates/orchestration/src/scheduler.rs` | 279 | CANONICAL-candidate |
| `crates/orchestration/src/dead_letter.rs` | 359 | CANONICAL (DLQ re-task) |
| `crates/worker/src/consumer.rs` | 772 | TEST-ONLY (verify) |
| `crates/worker/src/lifecycle.rs` | 68 | TEST-ONLY (verify) |
| `crates/worker-service/src/main.rs` | 177 | DEV-ONLY (demo task) |

Per the spec: *"Do not delete code merely because it looks duplicated without
tracing callers."* Each will be caller-traced before any action.

### F11 - INFORMATIONAL: the infrastructure that is already good

- **`UnitOfWork` + `atomic()`** (`crates/persistence/src/transaction.rs`) - a
  well-designed unit-of-work with explicit commit/rollback and validated named
  savepoints. Its own doc comment says: *"This is the canonical shape services
  should use for multi-write flows (state change + outbox insert)."*
  `ExecutionService` simply does not use it (F3).
- **RLS tenant isolation** on `tasks` and `executions` - `FORCE ROW LEVEL
  SECURITY` with `app.current_tenant`. Phase 25 is enforceable at the DB layer.
- **`UNIQUE (tenant_id, idempotency_key)`** on `tasks` - DB-level uniqueness
  already exists, so Phase 4 need not rely solely on SELECT-then-INSERT.
- **`OutboxDispatcher`** - correct retry/dead-letter/backoff logic; only the
  wiring is missing (F6).

---

## 4. Entrypoint inventory

| # | Entrypoint | Reaches a Task? | Transactional? | Class |
|---|---|---|---|---|
| 1 | `api/src/handlers/executions.rs:44` (HTTP) | No | No | CANONICAL (must be fixed) |
| 2 | `api/src/grpc.rs:622` (gRPC) | No | No | CANONICAL (must be fixed) |
| 3 | `scheduler-service/src/durable.rs:135` | No | No | CANONICAL (must be fixed) |
| 4 | `OrchestrationEngine::submit_task` | Yes | Unverified | COMPETING |
| 5 | `OrchestrationEngine::start_execution` | Yes | Unverified | COMPETING |
| 6 | `Worker -> AgentRunHandler` | consumes | -- | CANONICAL lane, but bypasses runtime (F2) |
| 7 | `worker-service/src/main.rs:177` demo task | Yes | No | DEV-ONLY |

---

## 5. What Phase 1 concludes

The repository has **good primitives but a broken composition**:

- The transaction primitive exists and is unused by the submission path.
- The outbox relay exists and is unwired.
- The `task_attempts` schema exists and is unreachable from code.
- The canonical executor exists and is uncalled.
- Two idempotency mechanisms exist, one of them in-memory.
- The worker has a capacity limit it logs about and then ignores.

The dominant defect is **F1 + F2**: the canonical
`Executor -> WorkflowRuntime -> AgentNodeExecutor -> AgentBridge` chain is fully
implemented and completely bypassed by the production worker, which calls
`GrpcAgentBridge` directly.

This is consistent with the spec's premise that "there MUST NOT be competing
execution lifecycles" - today there are two disjoint ones, and the one that
runs is the shallower one.

---

## 6. Next

Phase 2 (canonical state machine) and Phase 3 (atomic submission) follow. Every
classification marked "verify" or "candidate" above will be caller-traced before
any code is changed, per Phase 21's rule.
