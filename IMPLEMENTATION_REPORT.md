# Enterprise Multi-Agent System — Implementation Report

**Status:** Complete

**Date:** 2026-10-08

**Scope completed:** Rust core/worker safety, durable PostgreSQL ownership and replay protection, scheduler completion authentication, Python orchestrator async/resource handling, gRPC integration, HTTP egress security, Data Hub synchronization bounds, tests, operational documentation, validation tooling, and live PostgreSQL migration/RLS verification.

## 1. Completion Summary

The identified safety and correctness gaps were implemented rather than only documented. The resulting design has these central properties:

- Execution admission performs entitlement and policy preflight before durable work, and charges an execution quota exactly once only for a newly created durable submission.
- Broker delivery is a read-only transport hint. Durable task-attempt claiming remains the sole lifecycle authority.
- Broker message identities and delivery ownership records are PostgreSQL-generated and survive process restarts.
- Worker delivery receipts are reserved before durable claims, tied to the matching attempt, renewed with the lease, and settled before the matching broker signal.
- Internal service signatures are versioned, body-bound HMAC v2 signatures with timestamps and nonces. Production nonce consumption is durable and shared across replicas.
- The scheduler completion endpoint starts only with its required secret, listener configuration, database pool, and durable replay store.
- Guarded Python HTTP egress validates all DNS answers and pins the actual TCP dial to an approved literal address, closing the check-then-connect DNS rebinding window.
- Untrusted document parsing is isolated in bounded child processes. Data Hub web and object synchronization use aggregate transfer limits, bounded frontier state, and item-level checkpoints.
- HTTP and gRPC now share a deliberately capability-gated, untenanted platform-admin lane for global organization catalogue operations; tenant-owned operations remain resolver-scoped and canonically authorized.
- Worker pause semantics are preserved: a deliberate `max_in_flight = 0` fetches and runs no work rather than failing startup.
- Synchronous Python agent lookup preserves fire-and-forget telemetry while retrieving and safely logging asynchronous emission failures.
- The full checksum-pinned migration registry now has a real PostgreSQL 17.11 validation run: all 38 migrations, pgvector, runtime-role RLS, pooled-connection isolation, and plane-role grants were exercised on an isolated disposable database.
- The Python pgvector adapter now verifies the authenticated PostgreSQL identity, rejects direct or reachable superuser/`BYPASSRLS` authority, uses transaction-local `mas_rag` narrowing, and leaves every Psycopg pool configure hook transaction-idle.

## 2. Critical Findings and Remediations

### 2.1 Quota could be charged before durable idempotency resolution

**Risk:** A retry carrying the same idempotency key could consume quota more than once. In the in-memory/reference composition, a quota refusal after durable creation could also leave runnable work or an idempotency key behind.

**Affected locations:**

- `rust-core/crates/application/src/execution_service.rs:242-365`
- `rust-core/crates/application/src/durable.rs:159-236, 542-565`
- `rust-core/crates/application/src/control_plane/mod.rs:813-893`
- `rust-core/crates/application/src/durable_memory.rs:686-790`
- `rust-core/crates/persistence/src/durable.rs:915-1135`

**Before:** the durable submission was not distinguished from an idempotent replay before the quota effect in the application flow.

```rust
let receipt = self.durable.submit(intent).await?;
```

**After:** the service uses local admission only for the non-transactional in-memory adapter, resolves durable creation versus replay, then consumes quota only for the sole `Created` winner.

```rust
let receipt = self.durable.submit(intent).await?;
if receipt.kind == SubmissionKind::Created {
    self.control_plane
        .consume_created_execution(tenant_id, 1, &ctx.requested_at)
        .await?;
}
receipt
```

```rust
match self.durable.submission_admission(&intent).await? {
    SubmissionAdmission::New => {
        self.control_plane
            .preflight_created_execution(tenant_id, 1, &ctx.requested_at)
            .await?;
    },
    SubmissionAdmission::Replayed | SubmissionAdmission::Conflicting => {},
    SubmissionAdmission::Unknown => {
        return Err(AppError::internal(
            "local submission adapter did not provide an idempotency admission decision",
        ));
    },
}
```

**Result:** policy and entitlement denials produce no work. Replays are quota read-only. Production PostgreSQL paths use the authoritative request transaction; the reference adapter uses a local admission gate so a quota denial occurs before a runnable task exists.

### 2.2 The broker previously performed task lifecycle claiming and terminalization

**Risk:** A broker fetch changed task state to `running`, broker retry metadata became an implicit retry budget, and a direct terminal broker update could race the durable attempt coordinator.

**Affected locations:**

- `rust-core/crates/persistence/src/repositories/broker.rs:56-114, 296-434, 585-684`
- `rust-core/crates/worker/src/consumer.rs:396-459, 627-669, 745-812, 850-930`
- `rust-core/crates/worker/src/attempts.rs:1-290`
- `rust-core/crates/worker/src/failure_matrix.rs:1-240`

**Before:** fetching a queue item claimed it directly, and a generic broker termination query could settle task state.

```rust
const TERM_SQL: &str = "UPDATE tasks SET status = 'dead_lettered', updated_at = now()
    WHERE id = $1 AND status = 'running'";
```

```rust
sqlx::query(TERM_SQL)
    .bind(row_id)
    .execute(&self.pool)
    .await
    .map_err(map_sqlx)?;
```

**After:** fetch is read-only, normal lifecycle decisions live in durable attempts, and the sole transport-only terminal case is an explicitly guarded undecodable-frame quarantine.

```rust
const POISON_QUARANTINE_SQL: &str =
    "UPDATE tasks AS t SET status = 'dead_lettered', updated_at = now()
    WHERE t.id = $1 AND t.status = 'queued' AND t.locked_by IS NULL
      AND NOT EXISTS (
          SELECT 1 FROM broker_delivery_receipts r
          WHERE r.task_id = t.id
            AND r.status IN ('reserved', 'in_flight')
            AND r.lease_expires_at > now()
      )";
```

```rust
let receipt = match self
    .receipts
    .reserve(DeliveryReservationRequest {
        consumer_name: self.config.consumer_name.clone(),
        task_id: task_id.into_uuid(),
        tenant_id: tenant_id.into_uuid(),
        worker_id: self.config.worker_name.clone(),
        delivery_count: deliveries.max(1),
        lease_expires_at: receipt_expiry_at,
    })
    .await
{
    Ok(Some(receipt)) => receipt,
    Ok(None) => {
        return MessageOutcome {
            settlement: Settlement::DeferredToReceiptOwner,
            handler_failed: false,
            stale_ownership: false,
        };
    },
    Err(error) => {
        tracing::error!(%task_id, sequence, %error, "delivery receipt reservation failed; deferring");
        let settlement = self.nak_transient(sequence, deliveries).await;
        return MessageOutcome {
            settlement,
            handler_failed: false,
            stale_ownership: false,
        };
    },
};
```

**Result:** broker delivery counts are telemetry, not the durable retry budget. A duplicate delivery with a live receipt is left unsettled so that the owner or normal lease expiry determines recovery. The poison-frame exception cannot overwrite a claimed or receipt-reserved task.

### 2.3 UUID-prefix message IDs and volatile delivery ownership were unsafe across replicas

**Risk:** Deriving a sequence from the first eight bytes of a UUIDv7 can collide for values generated in the same millisecond. A process-local flight map cannot represent durable ownership after a restart or across workers.

**Affected locations:**

- `rust-core/migrations/0033_delivery_and_replay_receipts/up.sql:30-80`
- `rust-core/migrations/0034_delivery_receipt_integrity/up.sql:14-81`
- `rust-core/crates/persistence/src/repositories/delivery_receipts.rs:18-153`
- `rust-core/crates/worker/src/delivery_receipts.rs:19-264`
- `rust-core/crates/persistence/src/repositories/broker.rs:219-434`
- `rust-core/crates/persistence/src/migrations.rs:210-219`

**Before:** the transport sequence was derived from a UUID prefix.

```rust
fn sequence_of(id: Uuid) -> u64 {
    let bytes = id.as_bytes();
    u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ])
}
```

**After:** PostgreSQL allocates permanent broker-message and per-delivery receipt identities, with a unique active receipt per task and cross-table integrity constraints.

```sql
CREATE TABLE IF NOT EXISTS broker_delivery_receipts (
    receipt_sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    consumer_name text NOT NULL REFERENCES broker_consumers (consumer_name) ON DELETE CASCADE,
    task_id uuid NOT NULL REFERENCES tasks (id) ON DELETE CASCADE,
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    attempt_id uuid NULL REFERENCES task_attempts (id) ON DELETE SET NULL,
    worker_id text NULL,
    delivery_count integer NOT NULL CHECK (delivery_count BETWEEN 1 AND 64),
    status text NOT NULL CHECK (status IN ('reserved', 'in_flight', 'acked', 'nacked', 'terminated', 'expired')),
    lease_expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    settled_at timestamptz NULL
);
```

```rust
let row = sqlx::query(
    "INSERT INTO broker_delivery_receipts
        (consumer_name, task_id, tenant_id, worker_id, delivery_count, status, lease_expires_at)
     VALUES ($1, $2, $3, $4, $5, 'reserved', $6)
     ON CONFLICT DO NOTHING
     RETURNING receipt_sequence",
)
.bind(&request.consumer_name)
.bind(request.task_id)
.bind(request.tenant_id)
.bind(&request.worker_id)
.bind(delivery_count)
.bind(request.lease_expires_at.into_datetime())
.fetch_optional(&self.pool)
.await
.map_err(map_sqlx)?;
```

**Result:** delivery identities are database-generated, restart-safe, and auditable. Migration 0034 adds composite foreign keys so a receipt’s task, tenant, and attached attempt must describe one coherent execution.

### 2.4 Signed internal calls had freshness but no durable replay consumption

**Risk:** A captured valid request could be replayed during its timestamp window after a process restart or against a different replica.

**Affected locations:**

- `python-orchestrator/src/mas_orchestrator/security/internal_service.py:1-215`
- `python-orchestrator/src/mas_orchestrator/security/internal_replay.py:1-95`
- `python-orchestrator/src/mas_orchestrator/grpc/agent_bridge_servicer.py:316-605`
- `python-orchestrator/src/mas_orchestrator/main.py:195-263`
- `python-orchestrator/src/mas_orchestrator/config/validation.py:526-555`
- `rust-core/crates/security/src/internal_service.rs:1-284`
- `rust-core/crates/scheduler-service/src/server.rs:74-185`
- `rust-core/crates/scheduler-service/src/main.rs:198-220, 397-430`

**Before:** verification returned after checking the HMAC and timestamp, without atomically consuming a nonce.

```python
verified = verify_internal_signature(
    self._config.internal_service_secret,
    metadata.internal_signature,
    metadata.internal_service,
    now_millis(),
    _canonical_request_bytes(request),
    self._config.allowed_internal_callers,
)
```

**After:** a nonce is consumed in a PostgreSQL receipt table before application work proceeds, with fail-closed behavior when the receipt authority is unavailable.

```python
if self._replay_guard is None or not await self._replay_guard.consume(verified):
    raise AuthenticationError(
        "invalid internal service signature",
        details={"reason": "unauthenticated"},
    )
```

```python
await cursor.execute(
    """
    INSERT INTO internal_request_receipts
        (service_name, nonce, signed_at_ms, expires_at)
    VALUES (%s, %s, %s, to_timestamp(%s / 1000.0))
    ON CONFLICT (service_name, nonce) DO NOTHING
    RETURNING nonce
    """,
    (
        identity.service,
        identity.nonce,
        identity.signed_at_ms,
        identity.expires_at_ms,
    ),
)
return await cursor.fetchone() is not None
```

**Result:** v2 signatures bind service, timestamp, nonce, and body digest. Python production composition uses `PostgresReplayGuard`; the scheduler uses the same durable receipt protocol; in-memory replay protection is clearly limited to tests and explicitly non-production composition.

### 2.5 Guarded HTTP validated a hostname but did not guarantee the validated address was dialed

**Risk:** DNS rebinding could cause a host approved during URL policy evaluation to resolve differently at connect time and reach an internal address. Raw resolver errors could also escape the typed transport boundary.

**Affected locations:**

- `python-orchestrator/src/mas_orchestrator/providers/base_http.py:228-358, 430-581`
- `python-orchestrator/tests/security/test_http_policy.py:1-330`

**Before:** URL-level validation could not guarantee the network backend used the approved DNS answer.

```python
await self._validate_dns(current_url)
response = await self._client.send(request, stream=True)
```

**After:** the HTTP Core backend resolves, validates every answer, rejects a mixed public/private answer set, and passes the validated literal IP to the actual TCP delegate while preserving the hostname for TLS SNI and certificate verification.

```python
try:
    addresses = await _resolve_host(host)
except OSError as error:
    raise httpcore.ConnectError("host could not be resolved") from error
if not addresses:
    raise httpcore.ConnectError("host resolved to no addresses")
for address in addresses:
    try:
        self._policy.validate_resolved_address(address)
    except ToolPermissionDeniedError as error:
        raise httpcore.ConnectError("resolved address is blocked") from error
address = next(iter(dict.fromkeys(addresses)))
return await self._delegate.connect_tcp(
    address,
    port,
    timeout=timeout,
    local_address=local_address,
    socket_options=socket_options,
)
```

**Result:** approved DNS answers are the actual TCP destination. DNS failures map to `httpcore.ConnectError`, then to the existing public HTTPX error handling. Regression tests prove literal-address dialing, mixed-answer rejection, and normalized resolver failure.

### 2.6 Parser hangs and source synchronization could exhaust shared resources or lose progress

**Risk:** parsing untrusted rich documents on the server event loop can block unrelated traffic. Crawler/object-source work could retain or transfer beyond a synchronization budget, grow its frontier unnecessarily, or advance past a failed document.

**Affected locations:**

- `python-orchestrator/src/mas_orchestrator/rag/parser_pool.py:1-173`
- `python-orchestrator/src/mas_orchestrator/rag/ingestion.py:63-132`
- `python-orchestrator/src/mas_orchestrator/data_hub/sources.py:97-236, 354-579`
- `python-orchestrator/src/mas_orchestrator/data_hub/models.py:104-143`
- `python-orchestrator/src/mas_orchestrator/data_hub/service.py:241-365`
- `python-orchestrator/tests/unit/test_parser_pool.py:1-171`
- `python-orchestrator/tests/unit/test_data_hub_sources.py:1-211`

**Before:** parsing occurred directly through the normal parser path, and per-document limits did not by themselves cap aggregate synchronization transfer or provide item checkpoints.

```python
text = self._parser.parse(document.content, detected)
```

**After:** parsing is isolated in a spawned child, bounded by semaphore, timeout, address-space limit where supported, and forced termination during cancellation or timeout.

```python
process.start()
deadline = time.monotonic() + self._config.timeout_seconds
try:
    while received is None:
        if cancel_requested.is_set():
            raise InvalidRequestError("document parsing was cancelled")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise InvalidRequestError(
                "document parsing exceeded its time limit",
                details={"timeout_seconds": self._config.timeout_seconds},
            )
        received = results.get(timeout=min(remaining, 0.1))
finally:
    if process.is_alive():
        process.terminate()
    process.join(timeout=1.0)
    if process.is_alive():
        process.kill()
        process.join(timeout=1.0)
    results.close()
    results.join_thread()
```

```python
remaining = self._max_sync_bytes - fetched_bytes
response = await self._transport.request(
    "GET",
    url,
    max_response_bytes=min(self._max_document_bytes, remaining),
)
fetched_bytes += len(response.body)
```

**Result:** no parser job can permanently occupy an event-loop worker; the Data Hub bounds aggregate web and object-storage transfer, rejects partial content, limits crawler frontier growth, deduplicates scheduled URLs, and returns the checkpoint immediately before a failed or not-yet-affordable item.

### 2.7 Global platform catalogue access was inconsistent across transports

**Risk:** Platform administrators could be forced through tenant scope resolution for global organization operations, while a broad unscoped helper risked making tenant-owned gRPC operations bypass canonical authorization. The result was either an unavailable control-plane catalogue or an accidental cross-tenant authority path.

**Affected locations:**

- `rust-core/crates/api/src/context.rs:120-157`
- `rust-core/crates/api/src/middleware.rs:149-170`
- `rust-core/crates/api/src/grpc.rs:153-205, 320-405`
- `rust-core/crates/api/src/handlers/control_plane.rs:1035-1048`

**Before:** the gRPC path used helpers that derived a normal service context without making the global capability explicit.

```rust
fn unscoped(ctx: &RequestContext) -> Result<ServiceContext, Status> {
    ctx.service_context().map_err(to_status)
}
```

```rust
let service_ctx = unscoped(&ctx)?;
```

**After:** a verified, usable no-claim `platform:admin` principal may enter an explicit global lane, while all tenant-owned gRPC calls use the canonical authorization helper.

```rust
pub(crate) fn platform_service_context(&self) -> Result<ServiceContext> {
    let principal = self.authenticated_principal()?;
    if !principal.scopes.permits("platform:admin") {
        return Err(AppError::forbidden(
            "insufficient scope for a platform operation",
        ));
    }
    self.build_service_context(principal, None, None, None)
}
```

```rust
let service_ctx = authorized(
    &ctx,
    mas_security::ResourceKind::Tenant,
    mas_security::Action::Administer,
    None,
)?;
```

**Result:** platform organization catalogue operations work identically over HTTP and gRPC without fabricating a tenant. A principal with ordinary denied scopes cannot mint an untenanted context, and tenant registration/listing retains resource/action authorization and scoped service context construction.

### 2.8 Consumer validation accidentally rejected intentional zero-capacity pause mode

**Risk:** A worker deliberately configured with `max_in_flight = 0` should stay alive but claim no work. Rejecting it at startup turns a safe operational pause into a restart loop and defeats existing capacity-zero semantics.

**Affected locations:**

- `rust-core/crates/worker/src/consumer.rs:97-130`
- `rust-core/crates/worker/src/consumer.rs:1237-1265`
- `rust-core/crates/worker/src/consumer.rs:1959-2012`

**Before:** validation treated zero capacity as malformed configuration.

```rust
if self.max_in_flight == 0 {
    return Err(AppError::validation("max_in_flight must be at least 1"));
}
```

**After:** the configuration still rejects zero fetch batches and unsafe timer windows, but allows zero execution capacity. The existing saturated-capacity branch performs no broker fetch when the local room is zero.

```rust
if self.batch_size == 0 {
    return Err(AppError::validation("batch_size must be at least 1"));
}
```

```rust
let local_room = self.config.max_in_flight.saturating_sub(in_flight.len());
let batch = self.config.batch_size.min(local_room).min(durable_room);
if batch == 0 {
    tokio::select! {
        biased;
        () = shutdown.changed() => break,
        () = tokio::time::sleep(self.config.poll_interval) => {},
    }
    continue;
}
```

**Result:** `capacity_zero_claims_nothing_and_runs_nothing` passes and verifies the task remains queued with no handler invocation.

### 2.9 Background lookup telemetry could fail as an unobserved asyncio task

**Risk:** `AgentService.spec()` is synchronous and intentionally schedules telemetry. If an emitter rejected an event (for example, its screening boundary raised `InvalidRequestError`), the task exception was never retrieved and surfaced only as an event-loop warning, losing the operational signal.

**Affected locations:**

- `python-orchestrator/src/mas_orchestrator/services/agent_service.py:69-92, 144-163`
- `python-orchestrator/tests/unit/test_services.py:144-169`

**Before:** the task was scheduled and discarded.

```python
try:
    asyncio.get_running_loop().create_task(coro)
except RuntimeError:
    coro.close()
```

**After:** the scheduled task has a completion callback that retrieves its exception, treats loop-shutdown cancellation as expected, and logs only a safe event name and exception class.

```python
try:
    task = asyncio.get_running_loop().create_task(coro)
except RuntimeError:
    coro.close()
else:
    task.add_done_callback(_observe_lookup_telemetry)
```

```python
error = task.exception()
if error is not None:
    _LOGGER.warning(
        "agent lookup telemetry emission failed",
        extra={
            "event": "agent.lookup_emit_failed",
            "error_type": type(error).__name__,
        },
    )
```

**Result:** lookup remains non-blocking and cannot fail the registry operation, while a telemetry failure is observable without logging event payloads or secrets. Regression coverage proves an `InvalidRequestError` is retrieved and reported.

### 2.10 Validation exposed two stale test assumptions, which were corrected rather than bypassed

**Affected locations:**

- `rust-core/tests/integration/full_stack.rs:56-131, 168-314`
- `rust-core/crates/persistence/src/repositories/search.rs:287-304`
- `python-orchestrator/tests/security/test_internal_service.py:120-124`

**Before:** the full-stack fixture used a random principal with no tenant membership; after resolver hardening it could no longer use scope headers as authority. The search test also expected two escape characters before SQL `LIKE` wildcards even though the query defines a single `ESCAPE '\'` character. Finally, the HMAC freshness-edge test read the system clock twice, allowing a millisecond race outside the exact boundary.

```rust
let state = inmemory_state();
let scope = seed_scope(&state).await;
```

```rust
assert_eq!(escape_like(r"a%_\\b"), r"a\\%\\_\\\\b");
```

```python
edge = _now_ms() - (MAX_SKEW_SECONDS * 1000)
identity = verify_internal_signature(SECRET, _sign(at_ms=edge), "mas-api", _now_ms(), BODY)
```

**After:** the integration fixture installs an accepted owner membership and matching verified token; the test literal matches the one-character escape semantics; and the freshness test pins one clock value.

```rust
MembershipStorePort::save(store.as_ref(), &membership)
    .await
    .expect("activate owner membership");
state.verifier = Arc::new(
    mas_api::auth::StaticTokenVerifier::new().with_token("test-token", principal),
);
```

```rust
assert_eq!(escape_like(r"a%_\\b"), r"a\%\_\\\\b");
```

```python
now = _now_ms()
edge = now - (MAX_SKEW_SECONDS * 1000)
identity = verify_internal_signature(SECRET, _sign(at_ms=edge), "mas-api", now, BODY)
```

**Result:** the end-to-end client test now proves resolver-backed authority rather than header authority, literal search wildcard behavior is accurately pinned, and the HMAC boundary test is deterministic.

### 2.11 Live PostgreSQL validation exposed two incorrect test assumptions and an undocumented server prerequisite

**Risk:** The API-key integration test expected a separate pre-authentication connection to read an uncommitted request transaction, which PostgreSQL correctly prevents. The broker test tried to complete a `queued` task without the durable claim that creates its ownership fence and attempt ledger. In addition, the runbook advertised a plain PostgreSQL image even though migration 0028 requires the server-side `vector` extension. These failures obscured valid persistence behavior and made the documented disposable setup unable to migrate.

**Affected locations:**

- `rust-core/crates/persistence/tests/live_pg.rs:637-655`
- `rust-core/crates/persistence/tests/live_pg.rs:799-838`
- `rust-core/docs/runbooks/postgres-live-tests.md:3-30`

**Before:** the test re-read a request-scoped write using a distinct transaction before the request committed, and attempted an illegal `queued → completed` transition.

```rust
scoped(pool.clone(), tenant, organization, |pool| async move {
    let store = ApiKeyStore::new(pool);
    store.touch_last_used(key_id, tenant).await.expect("touch");
    let after = store.find_by_prefix(&prefix).await.expect("re-read");
    let touched = after
        .iter()
        .find(|candidate| candidate.id == key_id)
        .expect("row");
    assert!(
        touched.last_used_at.is_some(),
        "touch_last_used writes the marker"
    );
})
.await;
```

```rust
let lifecycle = TaskStore::new(pool.clone());
let mut settled = lifecycle
    .load(decoded.task_id)
    .await
    .expect("lifecycle load")
    .expect("task row");
settled
    .complete(&TransitionActor::worker("itest-worker").expect("actor"), None)
    .expect("complete");
lifecycle.save(&settled).await.expect("lifecycle save");
```

**After:** the marker is asserted only after the scoped transaction commits; the broker test performs the production durable claim and settlement protocol before ACK; and the runbook requires pgvector.

```rust
scoped(pool.clone(), tenant, organization, |pool| async move {
    let store = ApiKeyStore::new(pool);
    store.touch_last_used(key_id, tenant).await.expect("touch");
})
.await;

let after = auth_store.find_by_prefix(&prefix).await.expect("re-read");
let touched = after
    .iter()
    .find(|candidate| candidate.id == key_id)
    .expect("row");
assert!(
    touched.last_used_at.is_some(),
    "touch_last_used writes the marker"
);
```

```rust
let durable = PgDurableExecution::new(pool.clone());
let claim_now = Timestamp::now();
let claim = durable
    .claim(ClaimQuery {
        worker: "itest-worker".to_owned(),
        capacity: 1,
        lease_until: claim_now
            .checked_add(Duration::from_secs(30))
            .expect("claim lease"),
        now: claim_now,
        tenant_id: Some(scope.tenant_id()),
        task_id: Some(decoded.task_id),
    })
    .await
    .expect("durable claim")
    .into_iter()
    .next()
    .expect("our task claim");
durable
    .settle(TaskSettlement {
        tenant_id: scope.tenant_id(),
        task_id: decoded.task_id,
        attempt_id: claim.attempt.id,
        worker: "itest-worker".to_owned(),
        expected_version: claim.task.version(),
        outcome: TaskOutcome::Succeeded {
            result: serde_json::Value::Null,
            usage: ExecutionUsage::default(),
        },
        execution: None,
        actor: TransitionActor::worker("itest-worker").expect("actor"),
        now: Timestamp::now(),
    })
    .await
    .expect("durable settle");
```

```markdown
Use PostgreSQL 16+ **with the `pgvector` server extension installed**.
Migration `0028_rag_pgvector_plane` executes `CREATE EXTENSION vector`.
```

**Result:** `cargo test -p mas-persistence --test live_pg -- --nocapture` passed all 17 tests using separate migrator and non-BYPASSRLS `mas_app` credentials. `scripts/migrate.sh status` reported all 38 migrations applied with zero pending migrations. The runbook now uses `pgvector/pgvector:pg16` for its disposable Docker example and documents the package-managed extension check.

### 2.12 Pgvector authentication enforcement left valid pools unusable and missed reachable elevated roles

**Risk:** The initial pool configure hook performed the identity `SELECT` with autocommit disabled and returned while the connection was still inside a transaction. Psycopg discarded every such connection and eventually surfaced a `PoolTimeout`, so a valid least-privilege RAG login could never start. In addition, checking only the flags of `session_user` and `current_user` allowed a nominally unprivileged login that could `SET ROLE` to a `BYPASSRLS` or superuser role. That is elevated authority even if its direct role flags are false.

**Affected locations:**

- `python-orchestrator/src/mas_orchestrator/rag/pgvector_index.py:45-68, 163-210, 236-266, 486-530`
- `python-orchestrator/tests/unit/test_pgvector_index.py:112-209`
- `python-orchestrator/tests/integration/test_pgvector_role_boundary.py:1-59`
- `python-orchestrator/README.md:15`

**Before:** the configure hook did not restore the connection to the pool-required idle state, and the probe only inspected direct role names.

```python
async def configure(connection: Any) -> None:
    await register_vector_async(connection)
    await _assert_rag_session_authority(connection)
```

```sql
SELECT bool_or(rolsuper OR rolbypassrls)
FROM pg_roles
WHERE rolname IN (session_user, current_user)
```

**After:** a direct autocommit preflight preserves a typed `ConfigurationError`, every pool-created connection validates and rolls back the probe transaction, and the SQL checks every superuser/`BYPASSRLS` role reachable through `SET ROLE` membership. The RAG transaction itself still narrows with `SET LOCAL ROLE mas_rag` before setting the tenant GUC.

```python
async def configure(connection: Any) -> None:
    try:
        await register_vector_async(connection)
        await _assert_rag_session_authority(connection)
    finally:
        await connection.rollback()

async def authority_probe() -> None:
    connection = await AsyncConnection.connect(
        database_url,
        autocommit=True,
        application_name="mas-rag-plane-preflight",
    )
    try:
        await _assert_rag_session_authority(connection)
    finally:
        await connection.close()
```

```sql
EXISTS (
    SELECT 1
    FROM pg_roles AS candidate
    WHERE (candidate.rolsuper OR candidate.rolbypassrls)
      AND (
          pg_has_role(session_user, candidate.oid, 'SET')
          OR pg_has_role(current_user, candidate.oid, 'SET')
      )
)
```

**Result:** live PostgreSQL 17.11 tests proved that a dedicated non-superuser member login opens a clean pool and passes the RAG-table health probe, while both a superuser DSN and a non-superuser login with `SET ROLE mas_worker` (`BYPASSRLS`) are rejected. Unit tests cover direct-superuser, missing-role, pre-selected-role, typed-preflight, and transaction-local role-ordering behavior; opt-in integration tests make the live regression repeatable in CI.

## 3. Implementation Detail by Component

### Rust application, policy, quota, and persistence

- Added `SubmissionAdmission` and local-admission capability methods to the durable port, then implemented them in the in-memory and PostgreSQL durable adapters.
- Split side-effect-free authorization preflight from the created-execution quota mutation.
- Preserved execution completion output through `ExecutionAction::Complete { output }`, `durable_memory`, and PostgreSQL persistence rather than assigning output only to a response object.
- Added lease and ownership fencing for task attempts and receipt association.
- Registered migrations 0033 through 0038 in the checksum-pinned migration registry, including receipt integrity, approval/Data Hub/connector parity, and bounded trigram search indexes.
- Added bounded tenant-scoped search SQL with literal `ILIKE` escaping and matching `pg_trgm` index expressions.
- Made full-stack test identities resolver-valid by seeding accepted memberships instead of trusting scope headers.

### Rust broker and worker

- Reworked `PgTaskBroker` so reads do not move task lifecycle state.
- Added persisted consumer policy and database-generated `broker_task_messages` sequence allocation.
- Added the worker delivery-receipt port, an in-memory test implementation, and a PostgreSQL production implementation.
- Reserves receipts before claims, attaches the matching attempt, renews receipt liveness during heartbeat, and persists receipt settlement before broker ACK, NAK, or TERM behavior.
- Retained only the documented poison-frame quarantine path for undecodable payloads.
- Preserved intentional zero-capacity worker pause behavior while retaining validation for zero batches and unsafe heartbeat windows.

### Rust API scope resolution, authorization, and search

- Added an explicit, capability-gated `platform_service_context` for verified no-claim platform administrators; it is separate from tenant scope resolution and cannot be reached by ordinary principals.
- Made HTTP and gRPC organization catalogue operations use that global lane, while tenant-owned gRPC registration/listing calls use canonical resource/action authorization and scoped contexts.
- Added regression coverage for HTTP/gRPC global catalogue access, bounded search malformed pagination, and a resolver-backed real HTTP/CLI integration fixture.

### Rust scheduler and internal service authentication

- Replaced v1 body-digest-only signatures with v2 HMAC signatures containing a cryptographically random nonce.
- Added durable nonce consumption to scheduler completion routes and changed production startup to require listener configuration, signing secret, and scheduler database pool before dispatch starts.
- Added replay regression coverage in the scheduler router tests.

### Python bridge, configuration, and gRPC

- Added `PostgresReplayGuard` and production validation for `security.internal_replay_database_url_secret_ref`.
- Wired the durable guard through `main.py` and `AgentBridgeServicerConfig`.
- Preserved in-memory replay protection only for isolated non-production/test composition.
- Tightened tenant metadata reconciliation and bridge authentication integration.
- Regenerated and checked the Rust/Python bridge deliverable manifest at `RUST_PYTHON_BRIDGE_PHASE16_FILES.md`.
- Added safe completion observation for fire-and-forget `AgentService` lookup telemetry tasks.

### Python HTTP, RAG, and Data Hub

- Replaced policy-only DNS validation with an HTTP Core network backend that pins actual TCP connections to validated addresses.
- Refused Unix-socket egress through the guarded transport and retained bounded streaming, redirect validation, proxy refusal, and header redaction.
- Added `ParserPool` process isolation and connected ingestion to the bounded parser path.
- Added aggregate web/object synchronization transfer accounting, cursor v2 continuation/start-after state, item-before/item-after checkpoints, response truncation rejection, and bounded crawl frontier behavior.
- Hardened the pgvector RAG adapter with server-side DSN-principal validation, rejection of pre-selected and `SET ROLE`-reachable elevated roles, an idle-safe pool configure hook, transaction-local `mas_rag` activation, and repeatable live-role boundary tests.

## 4. Validation Evidence

| Area | Command or suite | Result |
|---|---|---|
| Rust formatting and diff hygiene | `source .cargo-env && cargo fmt --all -- --check`; `git diff --check` | Passed. |
| Rust full workspace | `source .cargo-env && CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 cargo test --workspace --all-targets` | Passed. The 12 Rust-to-Python E2E cases are explicitly ignored in the default suite because they spawn a separate Python-service harness. |
| Rust/Python bridge E2E | `MAS_E2E_PYTHON=python3 cargo test -p mas-ai-bridge --test e2e_python -- --ignored --nocapture` | Passed: all 12 real TCP/gRPC bridge cases, including signed/unsigned/wrong-secret paths, cancellation, deadline, scope, and error propagation. |
| Rust strict static analysis | `source .cargo-env && CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 cargo clippy --workspace --all-targets -- -D warnings` | Passed. |
| Focused worker regression | `cargo test -p mas-worker --lib` | Passed: 71 tests, including zero-capacity pause semantics. |
| API and authorization regression | `cargo test -p mas-api --lib`; `cargo test -p mas-api --test security_matrix --features testutils`; `cargo test -p mas-api --test root_integration --features testutils` | Passed: 52 API library tests, 29 security-matrix tests, and 2 real HTTP/CLI integration tests. |
| Persistence migration/search regression | `cargo test -p mas-persistence migrations::tests::registry_is_contiguous_and_ordered`; focused `like_escaper_keeps_user_wildcards_literal` | Passed. |
| Live PostgreSQL migrations, repositories, and RLS | `MAS_TEST_DATABASE_URL=… MAS_TEST_DATABASE_APP_URL=… cargo test -p mas-persistence --test live_pg -- --nocapture`; `DATABASE_URL=… scripts/migrate.sh status` | Passed on isolated PostgreSQL 17.11 with pgvector 0.8.0: all 17 tests passed under separate migrator and `mas_app` credentials; all 38 checksum-pinned migrations are applied with zero pending. |
| Python format and lint | `ruff format --check .`; `ruff check .` | Passed: 245 files formatted and lint-clean. |
| Python strict typing | `mypy src/mas_orchestrator` | Passed: 173 source files with no issues. |
| Python test suite | `pytest -q` | Passed: 2,003 tests; two live-pgvector tests correctly skip without explicit non-production DSNs. |
| Pgvector role-boundary integration | `MAS_TEST_RAG_DATABASE_URL=… MAS_TEST_RAG_ELEVATED_DATABASE_URL=… pytest -q tests/unit/test_pgvector_index.py tests/integration/test_pgvector_role_boundary.py` | Passed: 8 tests against PostgreSQL 17.11/pgvector 0.8.0, including a `SET ROLE`-reachable `BYPASSRLS` rejection. |

## 5. Live PostgreSQL Integration Validation

A real, isolated PostgreSQL environment was provisioned specifically for validation; no production DSN or credentials were used.

- **Server:** PostgreSQL 17.11 on port 5432, with `pgcrypto`, `pg_trgm`, and pgvector 0.8.0 available.
- **Database lifecycle:** a disposable `mas_live_validation` database was recreated under a dedicated local migrator role before the full suite.
- **Migration result:** all 38 embedded, checksum-pinned migrations (`0001_extensions` through `0038_bounded_search_indexes`) applied successfully. `scripts/migrate.sh status` verified 38 applied and 0 pending.
- **Runtime-role validation:** `mas_app` was provisioned as a LOGIN role with `rolbypassrls = false`. The suite exercised unscoped denial, scoped visibility, cross-tenant write denial, one-connection `SET LOCAL` cleanup, and confined machine-plane grants.
- **Repository/lifecycle result:** `cargo test -p mas-persistence --test live_pg -- --nocapture` passed all 17 cases, including API-key prefix/touch behavior, durable broker publish/claim/settle/ACK behavior, expiry recovery, idempotency, migrations, and RLS.
- **RAG role-boundary result:** a separate disposable `mas_rag_role_validation` database was migrated through 0038. A direct member login granted `SET ROLE mas_rag` opened the real Psycopg pool and passed `document_chunks` health; a superuser and a non-superuser that could `SET ROLE mas_worker` (`BYPASSRLS`) were both rejected before pool opening. The opt-in Python integration tests passed all 8 targeted cases.

The obsolete environment limitation has therefore been removed. Production deployment still needs its own approved database, least-privilege credentials and extension package installation; the validation databases are disposable local test infrastructure only.

## 6. Deployment Requirements

1. Install the PostgreSQL server package for **pgvector** before migration `0028_rag_pgvector_plane`; verify that `pg_available_extensions` contains `vector`. A plain PostgreSQL image is not sufficient.
2. Apply every un-applied migration through the project ledger and verify it with `scripts/migrate.sh status`. Do not edit an already-applied migration because checksums are pinned.
3. Provision distinct LOGIN credentials for `mas_app` and the confined plane roles through `scripts/provision-plane-roles.sh`; request-serving traffic must not use a superuser or a BYPASSRLS role.
4. Provision the Python RAG DSN as a dedicated LOGIN role with `SET ROLE mas_rag` membership. It must connect directly as that login, have no superuser/BYPASSRLS role reachable through `SET ROLE`, and must not use a pre-selected connection role.
5. Provide `MAS_INTERNAL_SERVICE_SECRET` to the API/scheduler side and the matching `MAS_AI_BRIDGE_INTERNAL_SIGNING_SECRET` to the Rust worker/Python bridge integration.
6. Configure Python `security.internal_replay_database_url_secret_ref` with a PostgreSQL DSN that may insert into `internal_request_receipts`.
7. Configure scheduler `MAS_SCHEDULER_COMPLETION_ADDR` and `MAS_DATABASE_SCHEDULER_URL`; production startup intentionally fails closed without them.
8. Configure worker `MAS_DATABASE_WORKER_URL` for `broker_delivery_receipts` and receipt settlement.
9. Run bounded hourly retention for expired internal replay receipts as documented in `rust-core/docs/internal-service-authentication-v2.md`.
10. Keep the completion listener on the private service network even though it requires HMAC authentication.

## 7. Deliverables and Changed-File Inventory

Key new files include:

- `rust-core/migrations/0033_delivery_and_replay_receipts/up.sql`
- `rust-core/migrations/0034_delivery_receipt_integrity/up.sql`
- `rust-core/crates/worker/src/delivery_receipts.rs`
- `rust-core/crates/persistence/src/repositories/delivery_receipts.rs`
- `rust-core/docs/internal-service-authentication-v2.md`
- `python-orchestrator/src/mas_orchestrator/security/internal_replay.py`
- `python-orchestrator/src/mas_orchestrator/rag/parser_pool.py`
- `python-orchestrator/tests/security/test_internal_replay.py`
- `python-orchestrator/tests/unit/test_parser_pool.py`
- `python-orchestrator/tests/unit/test_data_hub_sources.py`
- `python-orchestrator/src/mas_orchestrator/rag/pgvector_index.py` (DSN-principal validation and transaction-local RAG role fence)
- `python-orchestrator/tests/unit/test_pgvector_index.py`
- `python-orchestrator/tests/integration/test_pgvector_role_boundary.py`
- `rust-core/crates/persistence/src/repositories/search.rs`
- `rust-core/migrations/0038_bounded_search_indexes/up.sql`
- `rust-core/crates/persistence/tests/live_pg.rs` (commit-aware API-key assertion and durable claim/settle broker validation)
- `rust-core/docs/runbooks/postgres-live-tests.md` (pgvector prerequisite, compatible disposable image, and current 17-test count)

The implementation spans Rust and Python source, migrations, tests, documentation, and operational wiring, plus the regenerated bridge manifest at the workspace root.

## 8. Final Assessment

The implementation is complete for the reviewed scope. The major failure modes are now constrained by durable authority boundaries rather than process-local state: task execution has receipt-backed ownership, signature replay is database-backed, quota/idempotency ordering is explicit, platform-global access is explicit and capability-gated, guarded egress pins the socket destination, untrusted parsing/synchronization have hard resource limits, and the pgvector plane proves and narrows its actual database authority rather than trusting a configuration label. Rust workspace tests, strict Clippy, Python formatting/lint/type checks, all 2,003 standard Python tests, the 12 explicit Rust/Python bridge E2E cases, the full 17-case live PostgreSQL suite, and the 8-case live pgvector role-boundary regression suite are green. The live database validation applied and verified all 38 migrations on PostgreSQL 17.11 with pgvector, exercised a non-BYPASSRLS `mas_app` role, and rejected a superuser or `SET ROLE`-reachable BYPASSRLS RAG credential. Remaining production work is standard deployment hygiene: install pgvector, provision separate least-privilege role credentials and secrets, apply the ledger through the approved release process, and retain/monitor operational records.
