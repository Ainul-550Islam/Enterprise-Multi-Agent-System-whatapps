# 🛠️ Code Review & Health Report

## Executive Summary

### Health status: 🔴 **Not production-ready**

**Review target:** `Enterprise-Multi-Agent-System-whatapps/enterprise-mas` at commit `293484ebc51e7927d03564a16ef5755a4a91d0a4`.

This was a deep static and test-assisted review of the Rust control plane/worker, Python orchestrator, gRPC/protobuf bridge, persistence/broker implementation, Data Hub ingestion, outbound HTTP, and security boundaries.

The repository has a number of well-designed foundations—typed error mapping, RLS-scoped agent snapshots, bounded network responses, body-bound internal signatures, and a substantial cross-language test suite. However, several P0/P1 defects break core production invariants:

- the **default PostgreSQL worker lane cannot hand a fetched task to the durable attempt owner**;
- successful agent output is **discarded before durable execution settlement**;
- lease loss and shutdown/cancellation do not reliably stop work, allowing duplicate paid/model/tool side effects;
- an idempotent execution retry can consume quota more than once;
- fresh internal HMACs are replayable for 60 seconds;
- the outbound HTTP guard validates one DNS resolution but connects using another, and honours environment proxy settings by default;
- the production Python composition has no authoritative execution-state reader or Rust state reporter;
- untrusted Data Hub content can block the asyncio event loop and exceed aggregate memory budgets before limits are enforced.

### Evidence and validation performed

| Area | Result |
|---|---|
| Python formatting/lint, strict typing, bytecode compilation | Passed (`ruff format --check`, `ruff check`, strict `mypy`, `compileall`) |
| Python tests | `1,976 passed, 1 failed`; the remaining failure is a stale protobuf-contract expectation, so the Python quality gate is not green |
| Rust formatting | Passed |
| Rust workspace tests | Passed: `cargo test --workspace --all-features --locked` |
| Rust strict Clippy | Failed because `crates/execution/src/agent_bridge.rs:94-105` manually implements a derivable `Default` under `-D warnings` |
| Cross-language bridge E2E | All 12 ignored AgentBridge E2E tests passed |
| HMAC replay | Reproduced: the exact valid Python internal-service signature was accepted twice in its freshness window |
| Data Hub cursor | Reproduced: an encoder-produced unpadded cursor could not be decoded on the next sync |

### Priority order

1. **Stop shipping/running the default Pg worker path** until claim, attempt, receipt and settlement ownership are unified.
2. Make successful output and quota charging part of a single, durable, idempotent lifecycle.
3. Make lease loss, shutdown, client cancellation and durable cancellation actively stop in-flight work.
4. Close replay and outbound-egress bypasses before exposing internal gRPC or HTTP tools to untrusted workloads.
5. Rework Data Hub synchronization/parsing as a streaming, resource-isolated pipeline.

No source files were modified during this review.

---

## Critical Issues & Bugs

### C-01 — **P0 / Critical: The default PostgreSQL broker and durable-attempt protocol are incompatible**

**Locations**

- `rust-core/crates/worker-service/src/main.rs:378-384` — `broker.kind` defaults to `pg`.
- `rust-core/crates/persistence/src/repositories/broker.rs:63-81, 163-174` — broker fetch/reap mutates `tasks.status` itself.
- `rust-core/crates/worker/src/consumer.rs:474-530` — every fetched message is subsequently claimed through `AttemptCoordinator`.
- `rust-core/crates/worker/src/attempts.rs:274-324` — named durable claim logic.
- `rust-core/crates/persistence/src/durable.rs:1055-1077` — durable claim accepts only `pending` or `queued` rows.

**Type:** Worker lifecycle correctness / availability / duplicate-work risk.

**Impact:** The configured default path repeatedly changes a task from `queued` to `running` in `PgTaskBroker::fetch_durable`, then asks the durable attempt coordinator to claim the same task. The coordinator only claims `pending`/`queued` rows, so it cannot create the ownership fence or attempt record. The consumer defers/NACKs it and the Pg broker returns it to `queued`, producing a loop rather than executing the task.

Current state transition:

```text
queued
  -> PgTaskBroker.fetch_durable(): running
  -> AttemptCoordinator.claim_one(): no candidate (only pending/queued allowed)
  -> consumer NAK
  -> PgTaskBroker: queued
  -> repeat
```

**Before**

```rust
// persistence/src/repositories/broker.rs:63-77
const CLAIM_SQL: &str = "WITH claimable AS (
    SELECT id FROM tasks
    WHERE status = 'queued' AND not_before <= now()
    ...
)
UPDATE tasks t
SET status = 'running', not_before = now() + make_interval(secs => $2), ...";
```

```rust
// persistence/src/durable.rs:1055-1077
WHERE status IN ('pending', 'queued')
  AND not_before <= $1
...
UPDATE tasks t SET
   status = 'running',
   locked_by = $5,
   locked_until = $6,
   attempt_count = t.attempt_count + 1,
   version = t.version + 1
```

**Required after-state**

There must be **one** durable owner of `tasks.status`, `locked_by`, `locked_until`, attempt creation, and the delivery receipt. Do not repair this by merely allowing a second claim of `running` rows; that removes the fence and creates concurrent execution.

For the Pg lane, make durable claim the fetch operation. A focused target shape is:

```rust
// New Pg-only consumer path: a durable claim is the delivery.
let claims = self.attempts.claim(batch, &Timestamp::now()).await?;
for claim in claims {
    in_flight.spawn(async move {
        mediator.mediate_claim(claim).await  // no second claim after broker fetch
    });
}
```

Or, if a broker abstraction remains mandatory, its Pg implementation must delegate atomically to the durable claim repository and return a durable delivery receipt produced by that same transaction:

```rust
// Pseudocode for the required ownership boundary.
let delivery = durable.claim_for_delivery(ClaimQuery { ... }).await?;
// Atomically: task fence + task_attempt + delivery receipt.
// No independent UPDATE tasks SET status = 'running' in PgTaskBroker.
```

**Regression tests required**

- Live PostgreSQL test using the default `broker.kind=pg`: submit one task, run the worker, assert exactly one attempt, handler invocation, durable completion, output and final ACK.
- Two workers racing for the same task: exactly one durable attempt and one handler invocation.
- Crash after durable claim but before settlement: one recoverable redelivery, never a permanently `running` orphan.

---

### C-02 — **P0 / Critical: PostgreSQL delivery receipts are volatile, one-shot, and collision-prone**

**Locations**

- `rust-core/crates/persistence/src/repositories/broker.rs:92-119` — sequence is derived from part of a UUID and mapped only in process memory.
- `rust-core/crates/persistence/src/repositories/broker.rs:259-260` — mapping is recorded locally.
- `rust-core/crates/persistence/src/repositories/broker.rs:398-455` — `ack()` calls destructive `take()` even for `InProgress` heartbeats.

**Type:** Broker acknowledgement integrity / restart safety.

**Impact:** A first `AckInstruction::InProgress` removes the local mapping, so later heartbeats and final ACK/NACK no longer identify the durable task. After restart, all in-flight mappings are absent and acknowledgements are routed to the volatile event lane. In addition, `sequence_of()` keeps only the first 64 UUIDv7 bits—48 timestamp bits, the version nibble, and 12 random bits—so high-throughput tasks created in the same millisecond can collide and overwrite each other in `BTreeMap<u64, Uuid>`.

**Before**

```rust
// broker.rs:92-119
fn sequence_of(id: Uuid) -> u64 { ...first 8 UUID bytes... }

struct FlightLedger {
    by_sequence: Mutex<BTreeMap<u64, Uuid>>,
}

fn take(&self, sequence: u64) -> Option<Uuid> {
    self.by_sequence.lock().unwrap().remove(&sequence)
}
```

```rust
// broker.rs:398-402
let Some(row_id) = self.flight.take(sequence) else {
    return self.events.ack(consumer, sequence, instruction).await;
};
```

**After**

Persist a cryptographically/randomly unique delivery ID and retain it through all heartbeats and settlement. ACK/NACK/TERM must be conditional database operations, not an in-memory lookup:

```rust
// Conceptual durable receipt model.
struct DeliveryReceipt {
    id: DeliveryId,       // UUIDv4/v7 full value, never timestamp prefix
    task_id: TaskId,
    task_version: u64,
    worker: String,
    lease_until: Timestamp,
}

async fn heartbeat(receipt: &DeliveryReceipt) -> Result<bool> {
    // UPDATE ... WHERE receipt_id=$1 AND task_version=$2 AND state='in_flight'
    // Keep receipt alive; do not consume it here.
}

async fn settle(receipt: DeliveryReceipt, outcome: ...) -> Result<()> {
    // Atomically close receipt and settle the matching fenced attempt.
}
```

**Regression tests required**

- `InProgress`, another `InProgress`, then final ACK: all target the same durable receipt.
- Restart between fetch and ACK: durable receipt permits correct redelivery/settlement rather than event-lane routing.
- More than 4,096 UUIDv7 tasks produced in one millisecond: no receipt collision or overwritten mapping.

---

### C-03 — **P0 / Critical: Successful agent output is lost before durable settlement**

**Locations**

- `rust-core/crates/worker-service/src/agent_execution.rs:531-563` — handler creates JSON containing `output` and `usage`.
- `rust-core/crates/worker/src/consumer.rs:678-682, 823-852` — consumer extracts only usage.
- `rust-core/crates/worker/src/attempts.rs:345-360, 544-552` — success outcome has only usage; completed execution has no output.
- `rust-core/crates/application/src/durable.rs:323-377` — durable outcome types cannot carry output.
- `rust-core/crates/persistence/src/durable.rs:1312-1315` — completion persists `None` as the execution output.

**Type:** Data-loss / execution-result integrity.

**Impact:** Python can return a valid `AgentSucceeded.output_json`, Rust can receive it, and the worker can log success, yet the authoritative execution record is completed with no output. A later read or idempotent replay cannot retrieve the produced result.

**Before**

```rust
// worker-service/src/agent_execution.rs:531-538
serde_json::json!({
    "outcome": "succeeded",
    "output": output,
    "usage": usage_json(outcome.usage.as_ref()),
})
```

```rust
// worker/src/consumer.rs:678-682
Ok(value) => {
    self.attempts
        .succeed(claim, usage_from(value), finalize_execution, &now)
        .await
}
```

```rust
// persistence/src/durable.rs:1312-1315
ExecutionOutcome::Completed => {
    if loaded.status != ExecutionStatus::Completed {
        loaded.complete(&settlement.actor, None)?;
    }
}
```

**After**

Replace untyped handler JSON with a typed success result and carry bounded output through the same transaction as task/attempt/status/usage:

```rust
pub struct HandlerSuccess {
    pub output: serde_json::Value,
    pub usage: ExecutionUsage,
}

pub enum TaskOutcome {
    Succeeded { output: serde_json::Value, usage: ExecutionUsage },
    Failed { failure: TransitionFailure },
    Cancelled,
}

pub enum ExecutionOutcome {
    Completed { output: serde_json::Value },
    Failed { failure: TransitionFailure },
    Cancelled,
}
```

```rust
// In the durable transaction, after an explicit output-size/schema check.
ExecutionOutcome::Completed { output } => {
    loaded.complete(&settlement.actor, Some(output.clone()))?;
}
```

**Regression tests required**

- Execute through the real bridge, reload the execution from PostgreSQL, and assert the exact JSON result is durable.
- Repeated idempotency-key submission returns the same stored output, not an empty completed result.
- Oversized or invalid result output fails deterministically before partial task/execution settlement.

---

### C-04 — **P0 / Critical: Lease loss is logged but execution continues; shutdown and durable cancellation are not wired to active work**

**Locations**

- `rust-core/crates/worker/src/consumer.rs:617-658` — heartbeat logs `Ok(false)`/errors, then handler continues.
- `rust-core/crates/worker-service/src/main.rs:278-284` — a shutdown token is moved into `AgentRunHandler`; no retained clone is cancelled on SIGTERM/SIGINT.
- `rust-core/crates/worker/src/grace.rs:68-73` — signal watcher only flips consumer drain state.
- `rust-core/crates/worker-service/src/agent_execution.rs:454-456` — handler derives child cancellation tokens from the never-cancelled root.
- `rust-core/crates/application/src/execution_service.rs:422-...` — durable cancellation is represented, but active workers do not observe it while running.

**Type:** Distributed ownership / duplicate side effects / cost control.

**Impact:** When `renew()` returns `false`, another worker may legally reclaim the task while the old worker continues the model/tool call. The settlement fence protects the final write, but it cannot undo external side effects, paid inference, or webhook/tool operations already performed. SIGTERM starts a drain but does not cancel in-flight AI work. A user/API cancellation is durable state but not an active stop signal for an already-running worker.

**Before**

```rust
// worker/src/consumer.rs:623-629
match attempts.renew(task_id, &Timestamp::now()).await {
    Ok(true) => {},
    Ok(false) => tracing::warn!(%task_id, "durable lease lost during execution"),
    Err(err) => tracing::warn!(%task_id, %err, "durable lease renew failed"),
}
...
let outcome = self.handler.execute(task_msg).await;
```

```rust
// worker-service/src/main.rs:278-284
let cancellation = CancellationToken::new();
dispatcher = dispatcher.with_handler(
    "execution.run",
    Arc::new(AgentRunHandler::new(resolved_bridge, cancellation)),
);
```

**After**

Give each attempt an execution-control token. Lease loss, durable cancellation and process shutdown must all cancel it; handler execution must race against it and propagate gRPC cancellation to Python.

```rust
let root_cancel = CancellationToken::new();
let signal_cancel = root_cancel.clone();
let (shutdown_handle, shutdown_watch) = mas_worker::grace::channel();
tokio::spawn(async move {
    mas_worker::grace::watch_signals(shutdown_handle).await;
    signal_cancel.cancel();
});

// Handler receives root_cancel.clone(), not the only owned instance.
```

```rust
// Per claimed attempt.
let attempt_cancel = root_cancel.child();
let renewal_cancel = attempt_cancel.clone();

match attempts.renew(task_id, &Timestamp::now()).await {
    Ok(true) => {}
    Ok(false) => {
        renewal_cancel.cancel();
        return; // stop heartbeating/ack extension after ownership loss
    }
    Err(err) => { /* bounded retry policy; eventually cancel/fence */ }
}

let outcome = tokio::select! {
    result = handler.execute(task_msg, attempt_cancel.child()) => result,
    () = attempt_cancel.cancelled() => Err(AppError::cancelled("attempt ownership/cancellation lost")),
};
```

Also add a durable cancellation watcher/check that sets `attempt_cancel` for its execution. Do not merely mark the final settlement cancelled after an uncontrolled handler has continued.

**Regression tests required**

- Force `renew()` to return `false` while a blocking model/tool invocation is in progress; assert its cancellation channel fires and a second worker cannot overlap side effects.
- SIGTERM with a long AgentBridge RPC: assert Rust cancels the RPC, Python sees cancellation, and the task is redelivered or terminally cancelled according to policy.
- Request durable cancellation mid-run; assert prompt/model/tool work stops before final settlement.

---

### C-05 — **P0 / Critical: Idempotent submission can consume execution quota multiple times**

**Locations**

- `rust-core/crates/application/src/execution_service.rs:265-282` — quota is consumed before submission/idempotency resolution.
- `rust-core/crates/application/src/execution_service.rs:285-318` — durable `submit()` performs idempotent creation/replay afterwards.
- `rust-core/crates/application/src/control_plane/mod.rs:781-833` — `None` means immediate quota consumption; reservation-key path exists but is not used here.

**Type:** Billing/quota integrity / concurrency race.

**Impact:** Two concurrent requests with the same idempotency key, or a retry after an ambiguous client timeout, can each consume one execution quota unit before `durable.submit()` collapses them into one execution. A submission failure after quota consumption can also leave a charge with no execution.

**Before**

```rust
let decision = self.control_plane.authorize_execution(
    tenant_id,
    ...,
    QuotaDimension::Executions,
    1,
    &serde_json::json!({"project_id": project.to_string()}),
    None, // immediate consumption
    &ctx.requested_at,
).await?;

let receipt = self.durable.submit(intent).await?;
```

**After**

Resolve idempotency and consume quota in one database transaction. A replay must return before a second quota decision; a new submission must consume exactly once under a durable uniqueness key.

```rust
let receipt = self.durable.submit_and_charge_once(
    intent,
    ExecutionCharge {
        tenant_id,
        dimension: QuotaDimension::Executions,
        amount: 1,
        // Stable for retries; generated at ingress if the caller omitted a key.
        charge_key: submission_charge_key(tenant_id, idempotency_key),
    },
).await?;
```

The transaction must implement this ordering:

```text
lock/read idempotency row
  -> existing matching request: return replay, do not charge
  -> new request: check quota + INSERT unique charge + create execution/task/outbox
  -> commit
```

Do not “refund on error” as a separate best-effort operation; it introduces another race. The idempotency row and charge ledger need durable uniqueness constraints and the same transaction boundary.

**Regression tests required**

- 100 concurrent submissions with the same idempotency key: one execution/task/outbox record and one quota unit.
- Inject a transaction failure after validation: no quota debit and no execution.
- Retry after a committed request but lost HTTP response: replay returned even if the tenant quota is now exhausted.

---

### C-06 — **P0 / Critical: Internal HMAC signatures are replayable during the entire freshness window**

**Locations**

- `rust-core/crates/security/src/internal_service.rs:116-177`.
- `python-orchestrator/src/mas_orchestrator/security/internal_service.py:134-203`.
- `python-orchestrator/src/mas_orchestrator/grpc/agent_bridge_servicer.py:522-584`.
- Data Hub inherits the same verification through `python-orchestrator/src/mas_orchestrator/grpc/data_hub_servicer.py:252-299`.

**Type:** Internal authentication replay vulnerability.

**Impact:** The MAC correctly binds service, timestamp and body, but neither implementation records that a fresh valid signature has already been used. A captured request can be resent unchanged any number of times for up to 60 seconds. This was reproduced directly against the Python verifier. Timestamp freshness limits replay duration; it does **not** provide single-use replay protection.

**Before**

```python
# security/internal_service.py:195-203
if abs(now_ms - signed_at_ms) > MAX_SKEW_SECONDS * 1000:
    raise refusal

expected = _mac(raw, claimed_service, signed_at_ms, body)
if not hmac.compare_digest(presented_mac, expected):
    raise refusal

return InternalServiceIdentity(service=claimed_service, signed_at_ms=signed_at_ms)
```

**After**

Version the scheme and add a per-request nonce plus a shared, atomic replay ledger. Verification must fail closed if the replay store is unavailable for protected production endpoints.

```python
class ReplayGuard(Protocol):
    async def consume_once(self, *, service: str, fingerprint: bytes, ttl_ms: int) -> bool: ...

# v2 MAC input: v2:service:timestamp_ms:nonce:sha256(body)
fingerprint = hashlib.sha256(signature.encode("ascii")).digest()
ttl_ms = max(1, MAX_SKEW_SECONDS * 1000 - abs(now_ms - signed_at_ms))
if not await replay_guard.consume_once(
    service=claimed_service,
    fingerprint=fingerprint,
    ttl_ms=ttl_ms,
):
    raise AuthenticationError("invalid internal service signature", details={"reason": "unauthenticated"})
```

Use an atomic distributed primitive such as Redis `SET key value NX PX ttl`, or a PostgreSQL uniqueness insert with expiry cleanup. Rust and Python must share the v2 canonical format and replay-key derivation. Retries should be signed again with a new nonce; semantic idempotency remains the responsibility of the request/execution ID.

**Regression tests required**

- Two concurrent verifications of the same valid v2 request: exactly one succeeds.
- Same body/timestamp but a fresh nonce: accepted once each.
- Replay after restart and across two Python instances: rejected.
- Replay-store outage: production bridge fails closed and does not invoke the service.

---

### C-07 — **P0 / Critical: The outbound HTTP “SSRF guard” has a DNS TOCTOU and proxy bypass**

**Locations**

- `python-orchestrator/src/mas_orchestrator/providers/base_http.py:348-351` — `httpx.AsyncClient` defaults to `trust_env=True`.
- `python-orchestrator/src/mas_orchestrator/providers/base_http.py:398-411` — validation resolves first, `httpx` connects later.
- `python-orchestrator/src/mas_orchestrator/providers/base_http.py:451-525` — validated DNS answer is not pinned to the transport connection.

**Type:** SSRF / egress-policy bypass.

**Impact:** `_validate_dns()` checks one resolver result, then `httpx` performs its own hostname resolution at connection time. A rebinding hostname can resolve publicly during validation and privately (for example, metadata/loopback) during connection. Separately, `httpx` honours `HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY` by default; the proxy resolves/connects on a different network path, bypassing local address validation unless that proxy independently enforces the same egress policy.

**Before**

```python
# providers/base_http.py:398-411
self._policy.validate_url(current_url)
await self._validate_dns(current_url)

request = self._client.build_request(method, current_url, ...)
response = await self._client.send(request, stream=True)
```

```python
# providers/base_http.py:348-351
self._client = httpx.AsyncClient(
    timeout=self._timeout,
    follow_redirects=False,
)
```

**After**

Disable ambient proxies and use a transport/egress component that validates the exact IP it connects to while preserving TLS SNI and hostname certificate validation. Merely replacing the URL host with an IP is not sufficient because it breaks SNI/certificate verification.

```python
self._client = httpx.AsyncClient(
    timeout=self._timeout,
    follow_redirects=False,
    trust_env=False,
    transport=ValidatedEgressTransport(resolver=self._resolver, policy=self._policy),
)
```

```python
# Required transport invariant (implementation sketch).
approved_ips = await resolver.resolve(request.url.host)
policy.validate_all(approved_ips)
return await connector.connect_to_approved_ip(
    approved_ips,
    server_name=request.url.host,       # TLS SNI/certificate hostname
    host_header=request.url.host,
)
```

If an outbound proxy is required, configure it explicitly and enforce the allowlist/private-range policy at the proxy too; do not inherit it from process environment.

**Regression tests required**

- Resolver returns a public IP on validation and loopback/`169.254.169.254` on connection: request is refused before bytes are sent.
- `HTTPS_PROXY` is present in environment: transport does not use it unless explicitly configured.
- Explicit proxy deployment: proxy egress policy rejects private/rebound destinations.

---

### C-08 — **P1 / High: Python production composition has no authoritative execution state or terminal-state reporter**

**Locations**

- `python-orchestrator/src/mas_orchestrator/services/composition.py:345-356` — production bundle calls `build_execution_service(runtime)` without state dependencies.
- `python-orchestrator/src/mas_orchestrator/services/execution_service.py:178-191` — defaults to `UnknownStateReader` and `LoggingStateReporter`.
- `python-orchestrator/src/mas_orchestrator/services/execution_service.py:229-259` — a duplicate/in-flight decision is returned as `COMPLETED` with empty content.
- `python-orchestrator/src/mas_orchestrator/grpc/agent_bridge_servicer.py:1012-1020` — all `result.succeeded` values become `AgentSucceeded`.

**Type:** Idempotency/state-authority violation / incorrect gRPC semantics.

**Impact:** Production Python proceeds on every delivery because state is always `UNKNOWN`; it only logs terminal results instead of reporting them to Rust. If a real reader is later injected, `WAIT`/`SKIP` are still serialized as a successful empty response. That can cause a worker to settle an in-flight or duplicate delivery as success instead of waiting for/replaying the authoritative result.

**Before**

```python
# services/execution_service.py:187-191
self._emitter = emitter or EventEmitter()
self._state_reader = state_reader or UnknownStateReader()
self._reporter = reporter or LoggingStateReporter()
```

```python
# services/composition.py:355
execution_service = build_execution_service(runtime)
```

```python
# services/execution_service.py:232-240
if decision.action is not DuplicateAction.PROCEED:
    result = ExecutionResult(
        execution_id=context.execution_id,
        agent_id=context.agent_id,
        termination=Termination.COMPLETED,
        content="",
        duration_ms=duration_ms,
    )
```

**After**

Require production wiring to provide a durable Rust-backed reader and reporter. Treat their absence as a startup configuration error, not a development fallback. Preserve duplicate/in-progress semantics on the wire.

```python
# composition.py — fail closed in production.
execution_service = build_execution_service(
    runtime,
    state_reader=RustExecutionStateReader(required_bridge),
    reporter=RustStateTransitionReporter(required_bridge),
)
```

```python
# execution_service.py — no fabricated success.
if decision.action is DuplicateAction.WAIT:
    raise ExecutionInProgressError("execution is already running", retry_after_seconds=...)
if decision.action is DuplicateAction.SKIP:
    return await self._state_reader.authoritative_result(context.execution_id)
```

If protobuf needs an explicit deferred/replayed branch, add it in a backwards-compatible version rather than overloading `AgentSucceeded` with an empty output.

**Regression tests required**

- Production settings without reader/reporter fail at boot.
- Duplicate completed call returns the stored authoritative result.
- In-flight duplicate maps to a retryable/deferred result, never `AgentSucceeded(output_json="\"\"")`.
- Reporter failure is visible and prevents falsely claiming a canonical terminal transition.

---

### C-09 — **P1 / High: Python task cancellation is converted into a normal result before the servicer can propagate cooperative cancellation**

**Locations**

- `python-orchestrator/src/mas_orchestrator/agents/runtime.py:182-190`.
- `python-orchestrator/src/mas_orchestrator/services/execution_service.py:268-294`.
- `python-orchestrator/src/mas_orchestrator/grpc/agent_bridge_servicer.py:888-894, 909-965`.

**Type:** asyncio cancellation propagation / resource and billing control.

**Impact:** gRPC cancels the servicer task. `AgentRuntime`/`ExecutionService` catch `asyncio.CancelledError` and return a normal `Termination.CANCELLED` result. The outer task is then no longer in a cancelled state, its done callback does not set `cancel_event`, and `disarm()` removes the callback. Work relying on the cooperative event can outlive the caller’s cancellation.

**Before**

```python
# agents/runtime.py:182-190
try:
    outcome = await self._run_bounded(agent, initial, context, cancel_event)
except asyncio.CancelledError as error:
    outcome = AgentOutcome(
        agent_id=spec.agent_id,
        termination=Termination.CANCELLED,
        error=error,
        ...,
    )
```

**After**

External task cancellation must set the cooperative event and propagate. Normal event-driven cancellation should be represented by the domain `CancellationRequestedError`/`Termination.CANCELLED` path rather than swallowing `asyncio.CancelledError`.

```python
except asyncio.CancelledError:
    cancel_event.set()
    raise
```

Apply this at `AgentRuntime.execute` and `ExecutionService.execute`. The bridge already has the correct boundary behavior in `_dispatch` when it receives a real `CancelledError`:

```python
except asyncio.CancelledError:
    cancel_event.set()
    raise
```

**Regression tests required**

- Use the actual `ExecutionService`, cancel the outer gRPC/servicer task, and assert: outer task remains cancelled; `cancel_event.is_set()`; no normal success reply; child model/tool tasks are cancelled.
- Cancellation while a tool is awaiting I/O, during streaming, and while Python is emitting/reporting a result.

---

### C-10 — **P1 / High: A malformed persisted task can panic a worker and bypass normal settlement/slot release**

**Locations**

- `rust-core/crates/worker-service/src/agent_execution.rs:427-467`.
- `rust-core/crates/worker/src/consumer.rs:864-879`.

**Type:** Panic safety / poison-message handling / resource leak.

**Impact:** A nil/corrupt tenant, organization or project ID causes `RuntimeContext::new()` to panic. The JoinSet reaper logs the join failure; it does not run normal failure classification/settlement. The async stack unwinds around the slot-release path, so a run slot may remain leased until expiry and the task can cycle as an unclassified poison message.

**Before**

```rust
// worker-service/src/agent_execution.rs:439-444
.unwrap_or_else(|error| {
    panic!("execution.run task carries an invalid scope: {error}")
});
```

**After**

Make construction fallible and classify persisted corruption as a terminal, auditable failure. Add a defensive supervision boundary for unexpected task panics as well.

```rust
fn runtime_context(
    &self,
    message: &TaskQueueMessage,
    execution_id: ExecutionId,
) -> Result<RuntimeContext> {
    RuntimeContext::new(
        message.tenant_id,
        message.organization_id,
        message.project_id,
        "mas-worker",
        Environment::Production,
    )
    .map_err(|e| AppError::validation(format!("corrupt task scope: {e}")))
}

let context = self.runtime_context(message, execution_id)?;
```

The consumer should settle this as a non-retryable corruption/dead-letter outcome and release any acquired run slot on every exit path.

**Regression tests required**

- Corrupt scope ID: no process/task panic, one terminal failure/dead-letter, slot released, no bridge invocation.
- Deliberately panicking test handler: supervisor converts it to a bounded internal failure and preserves worker liveness.

---

### C-11 — **P1 / High: Data Hub source state is not reliably resumable and aggregate sync memory is enforced too late**

**Locations**

- `python-orchestrator/src/mas_orchestrator/data_hub/sources.py:575-606` — cursor encoder strips Base64 padding but decoder does not restore it.
- `python-orchestrator/src/mas_orchestrator/data_hub/sources.py:327-405` — S3 starts with `continuation = None` every sync and counts unchanged leading keys.
- `python-orchestrator/src/mas_orchestrator/data_hub/sources.py:159-206, 328-405` — adapters retain every changed document before returning.
- `python-orchestrator/src/mas_orchestrator/data_hub/service.py:266-292` — aggregate byte budget is checked only after adapter collection.
- `python-orchestrator/src/mas_orchestrator/config/settings.py:351-354` — defaults permit 32 MiB documents and 500 pages per sync against a 64 MiB aggregate cap.

**Type:** Availability / synchronization correctness / memory exhaustion.

**Impact:** An encoder-produced cursor can fail on the next sync. For S3 prefixes larger than the page cap, every sync begins lexically at the same unchanged objects, counts them toward the cap, and can starve later keys forever. Separately, an adapter can download and retain up to roughly `500 × 32 MiB` before `DataHubService` observes that the 64 MiB sync limit was exceeded.

**Before**

```python
# sources.py:582-603
payload = json.loads(base64.urlsafe_b64decode(cursor.encode("ascii")))
...
encoded = base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")
```

```python
# sources.py:327-333
previous = _decode_cursor(source.cursor)
changed: list[DiscoveredDocument] = []
continuation: str | None = None
...
while discovered < self._max_pages:
```

```python
# service.py:283-290
changed_bytes = sum(len(item.content) for item in batch.documents)
if changed_bytes > self._data_settings.max_sync_bytes:
    raise InvalidRequestError(...)
```

**After**

Fix the codec immediately, then redesign source sync as a bounded stream with a durable checkpoint/object state rather than an in-memory ETag map and accumulated byte list.

```python
# Strict URL-safe Base64 no-padding decoder.
if len(cursor) % 4 == 1:
    raise InvalidRequestError("Data Hub source cursor is malformed")
padded = cursor + "=" * (-len(cursor) % 4)
raw = base64.b64decode(padded.encode("ascii"), altchars=b"-_", validate=True)
payload = json.loads(raw)
```

```python
# Target streaming contract.
async for item, checkpoint in adapter.iter_changed(source, checkpoint=stored_checkpoint):
    if len(item.content) > budget.remaining_bytes:
        break
    await ingest_document(item)
    await persist_source_checkpoint_after_success(checkpoint)
    budget.consume(len(item.content))
```

Persist a listing checkpoint (and durable per-object ETag/content identity where needed) only after successful ingestion. This avoids lexical starvation, permits bounded incremental progress and makes the aggregate budget an admission control before download/retention.

**Regression tests required**

- Cursor round trip for payload lengths yielding Base64 modulo 0, 2 and 3; reject modulo 1/malformed data.
- S3 bucket with more unchanged leading objects than `max_pages`: later changed key is eventually reached after resumptions.
- Default limits with three 32 MiB changed objects: no more than 64 MiB is downloaded/retained and progress checkpoint remains correct.
- Adapter/document failure: source cursor remains at last successfully ingested item.

---

### C-12 — **P1 / High: Untrusted document parsing can block the shared asyncio loop and exceed parser bounds before enforcement**

**Locations**

- `python-orchestrator/src/mas_orchestrator/rag/ingestion.py:102-118` — synchronous parser runs in the gRPC event-loop coroutine.
- `python-orchestrator/src/mas_orchestrator/rag/parsing.py:204-230` — PDF text is extracted before the output limit is meaningfully enforced.
- `python-orchestrator/src/mas_orchestrator/rag/parsing.py:221-226` — repeatedly joining accumulated pages is quadratic and can append one giant page past the cap.
- `python-orchestrator/src/mas_orchestrator/grpc/data_hub_servicer.py:301-323` — `asyncio.timeout` cannot preempt CPU-bound synchronous parsing.

**Type:** Availability / untrusted-file processing.

**Impact:** A crafted PDF/DOCX/CSV/HTML payload can monopolize the event loop, delay unrelated gRPC calls and escape timely cancellation. The PDF path calls `pypdf` extraction before enforcing `MAX_PARSED_CHARS`; a highly expanding page can consume memory/CPU before rejection.

**Before**

```python
# rag/ingestion.py:102-106
try:
    text = self._parser.parse(document.content, detected)
except Exception as error:
    raise RetrievalError(...)
```

```python
# rag/parsing.py:221-226
pages: list[str] = []
for page_number, page in enumerate(reader.pages, start=1):
    if len("\n".join(pages)) > MAX_PARSED_CHARS:
        break
    pages.append(f"Page {page_number}\n{page.extract_text() or ''}")
return _normalise("\n\n".join(pages))
```

**After**

Run rich-format parsing in a bounded process pool/sandbox with queue concurrency, wall-clock deadline, memory/CPU limits and a maximum extracted-character budget enforced during extraction. `asyncio.to_thread()` is insufficient because it cannot safely kill a stuck parser.

```python
text = await self._parser_pool.parse(
    content=document.content,
    format=detected,
    timeout_seconds=self._parse_timeout_seconds,
    max_output_chars=MAX_PARSED_CHARS,
    memory_limit_bytes=self._parse_memory_limit_bytes,
)
```

At minimum, make accumulation linear and reject rather than append past the limit:

```python
used = 0
for page_number, page in enumerate(reader.pages, start=1):
    extracted = page.extract_text() or ""
    fragment = f"Page {page_number}\n{extracted}"
    if used + len(fragment) > MAX_PARSED_CHARS:
        raise InvalidRequestError("PDF exceeds the maximum parsed size")
    pages.append(fragment)
    used += len(fragment)
```

**Regression tests required**

- Malicious/high-expansion PDF and DOCX: parser process is killed/rejected inside configured memory/time bounds.
- Concurrent health RPC remains responsive while a pathological document is parsed.
- One giant PDF page exceeds the parsed-character cap without storing a giant extracted string in the service process.

---

## Warnings & Improvements

### W-01 — Redirect processing resolves relative URLs against the original request, not the current hop

**Location:** `python-orchestrator/src/mas_orchestrator/providers/base_http.py:423-434`.

A chained redirect such as `/v1/a -> next -> ../final?x=1` is resolved incorrectly because the code parses `url` (the initial URL) rather than `current_url`, and string-concatenates the location.

```python
# Before
base = urlparse(url)
current_url = f"{base.scheme}://{base.netloc}{location}"

# After
from urllib.parse import urljoin
current_url = urljoin(current_url, location)
```

Validate every resulting hop as today. Add tests for path-relative, query-relative, `../`, absolute and scheme-relative locations.

### W-02 — Redirects can forward provider credentials to another allowlisted host and repeat unsafe methods

**Locations:** `providers/base_http.py:362-368, 423-434`; `providers/openai.py:181-192`; `providers/anthropic.py:164-176`.

The manually followed redirect rebuilds headers on every hop. A provider transport allows both default provider hosts, so a cross-host redirect can receive `Authorization` or `x-api-key`. The code also preserves `POST`/body across every redirect status, including statuses whose conventional semantics differ.

- Restrict each provider transport to its specific origin whenever possible.
- Refuse cross-origin redirect when credentials are present, or explicitly strip all credential-bearing headers.
- Follow redirects automatically only for safe/idempotent methods unless full RFC-correct 301/302/303/307/308 behavior is implemented and tested.

### W-03 — HTTP tool logs raw model-supplied URLs, including query tokens or userinfo

**Location:** `python-orchestrator/src/mas_orchestrator/tools/builtins/http.py:218-225`.

```python
_LOGGER.info(..., extra={..., "url": url})
```

Presigned URLs, OAuth query tokens and embedded credentials can enter central logs. Log only a sanitized origin/path (no query, fragment or userinfo), and make `HttpPolicy.validate_url()` reject userinfo/nonstandard ports for generic HTTP tools as the Data Hub URL validators already do.

### W-04 — Webhook signing has freshness but no replay protection when activated

**Locations:** `python-orchestrator/src/mas_orchestrator/tools/builtins/webhook.py:254-257, 308-322`.

A captured signed delivery can be replayed for `MAX_CLOCK_SKEW_SECONDS`. No production construction path was found in this review, lowering immediate exposure, but the verifier should require a signed delivery ID and receivers must atomically deduplicate it for the timestamp retention window.

### W-05 — Web crawler frontier is unbounded relative to `max_pages`

**Locations:** `python-orchestrator/src/mas_orchestrator/data_hub/sources.py:151-205`.

`visited` is capped, but queued/scheduled URLs are not deduplicated before append. Each visited HTML page can add up to `max_pages * 8` links. Keep a `scheduled` set, cap the frontier independently, use a fixed per-page fanout cap, and cap URL length before `urljoin`/queueing.

```python
if absolute and absolute not in visited and absolute not in scheduled:
    if len(queue) >= max_frontier:
        break  # or return a resumable partial checkpoint
    scheduled.add(absolute)
    queue.append((absolute, depth + 1, root_host))
```

### W-06 — Exact-size HTTP responses are flagged as truncated

**Location:** `python-orchestrator/src/mas_orchestrator/providers/base_http.py:436-442`.

`_read_bounded()` returns a valid body up to the cap and raises on a body greater than the cap. Therefore `len(body) >= max_response_bytes` incorrectly calls an exactly-at-limit valid body truncated. Return `is_truncated=False` after a successful bounded read, or distinguish an observed extra byte explicitly.

### W-07 — Strict Rust CI is blocked by a derivable manual `Default`

**Location:** `rust-core/crates/execution/src/agent_bridge.rs:94-105`.

Strict Clippy fails under `-D warnings` because this implementation is derivable.

```rust
// Before
impl Default for AgentRuntimeConfiguration {
    fn default() -> Self { ... }
}

// After
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct AgentRuntimeConfiguration {
    ...
}
```

Confirm serde defaults remain equivalent, then make `cargo clippy --workspace --all-targets --all-features -- -D warnings` a blocking CI check.

### W-08 — Python test gate is not green because a protobuf-contract assertion is stale

The Python baseline completed with `1,976 passed, 1 failed`. Treat contract-generated code, `.proto` source and descriptor-shape tests as one reviewed change. Add a reproducible generation/check command that fails CI on drift rather than leaving a stale expectation in the regular test suite.

---

## Verified & Clean Modules

The following were positively verified, with the caveat that “clean” applies to the stated property—not necessarily every neighboring integration concern above.

1. **RLS-scoped immutable agent snapshots:** `worker-service` uses `ResolvedAgentBridge` and `PostgresAgentRuntimeSnapshotResolver`; Python strictly parses the Rust-supplied snapshot and rejects root supervisor execution without delegated snapshots. This is a meaningful improvement over process-local agent authority.
2. **Cross-language bridge protocol compatibility:** all 12 ignored AgentBridge Rust↔Python E2E tests passed, including protocol/version/error-path behavior exercised by that suite.
3. **gRPC input/authentication hardening:** request bounds, body-bound HMAC verification, service allowlists, constant-time MAC comparison and signed body/metadata reconciliation are present. The critical caveat is the missing single-use replay ledger.
4. **Data Hub rollback behavior:** source ingestion preserves the old source cursor on document failure and removes partial index state instead of silently advancing a failed batch (`data_hub/service.py:217-231, 330-345`).
5. **Outbound response bounding/redaction foundations:** explicit connect/read/write/pool timeouts, incremental response-byte limits, host allowlists, redirect-hop validation and sensitive response-header redaction are present. The DNS/proxy and redirect defects above must still be corrected.
6. **Rust test/format baseline:** workspace formatting and full locked/all-feature test run passed. No source-level unsafe-memory finding was identified in the reviewed Rust worker/core paths.
7. **Python static discipline:** Ruff, strict mypy and compilation passed, indicating a generally strong baseline for local correctness checks.

---

## Suggested Test Cases

### P0 release blockers

| Test | Expected invariant |
|---|---|
| **Pg default end-to-end worker test** | With only PostgreSQL configured, submitted task reaches exactly one durable claim, one handler invocation, one result persistence and one terminal settlement. |
| **Durable receipt heartbeat/restart test** | Multiple `InProgress` calls and a final ACK use the same persistent receipt; restart does not reroute ACKs to an event lane. |
| **Lost-lease race test** | Force lease renewal to return false while worker A is in a blocking tool/model call; worker A is cancelled before worker B can cause overlapping external work. |
| **Result durability test** | Python `output_json` survives Rust worker settlement, DB reload and idempotent replay byte-for-byte/canonically. |
| **Concurrent idempotency + quota test** | N identical submissions race concurrently; exactly one execution and one quota debit are committed. |
| **HMAC replay cluster test** | Two processes concurrently receive the exact fresh signed request; one is accepted and one is `UNAUTHENTICATED`; behavior survives restart. |
| **DNS rebind and proxy test** | Validation-public/connect-private host and environment proxy configurations cannot contact loopback, RFC1918, link-local or metadata IPs. |

### P1 correctness, cancellation and data-plane tests

| Test | Expected invariant |
|---|---|
| **Real gRPC cancellation propagation** | Cancelling the Rust RPC leaves the Python task cancelled, sets the cooperative event, cancels provider/tool work and does not serialize a normal reply. |
| **SIGTERM/drain test** | Signal triggers worker root cancellation, bridge RPC cancellation and bounded task outcome; no in-flight paid work is silently detached. |
| **Durable API cancellation test** | Cancellation requested after claim reaches the active worker without waiting for task completion. |
| **Corrupt persisted task test** | Invalid scope IDs become terminal poison/dead-letter records; no panic, slot leak or process-wide failure. |
| **Cursor codec property test** | Every valid map with Base64 no-pad length classes round-trips; invalid modulo-1 and malformed/oversized inputs fail closed. |
| **S3 large-prefix resume test** | More leading unchanged objects than page cap cannot starve later changed keys; checkpoint advances only after successful ingestion. |
| **Aggregate sync-budget test** | Adapter cannot download/retain beyond aggregate budget; memory remains bounded under 32 MiB object defaults. |
| **Parser isolation test** | Pathological PDF/DOCX/CSV cannot block health RPCs, exceed process memory or evade deadline cancellation. |
| **Crawler frontier test** | Link farms with duplicate/unique long URLs never exceed frontier, scheduled-set or URL-length limits. |

### Continuous quality gates

1. Make strict Clippy blocking after deriving `Default`.
2. Restore the Python suite to fully green and enforce generated-protobuf/source/test synchronization.
3. Run live PostgreSQL broker/claim/settlement tests in CI; in-memory doubles cannot validate the SQL ownership invariants.
4. Run fault-injection tests for broker restart, DB timeout, DNS rebinding, cancellation and replay-store outage.
5. Add load tests that measure high-cardinality UUIDv7 delivery behavior, crawler frontiers, parser process exhaustion and concurrent idempotency contention.
