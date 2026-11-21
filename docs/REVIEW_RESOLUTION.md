# Review Resolution — C-01…C-12, W-01…W-08

**Purpose:** resolve every finding of the previous `enterprise-mas-code-review-report.md`
against the code as it stands now, with `file:line` evidence taken from the working
tree. An item that does not hold is marked `open`; nothing here is marked resolved
without a line that can be read.

**Reviewed tree:** `Enterprise-Multi-Agent-System-whatapps` @ `3da5869` (local `HEAD`,
equal to `origin/main`), plus the uncommitted repair set described at the end.

**Method:** each finding was re-checked in the current source, not in the report. Line
numbers below are from the current tree.

---

## Summary

| ID | Severity | Status | Evidence |
|----|----------|--------|----------|
| C-01 | P0 | **resolved** | `rust-core/crates/persistence/src/repositories/broker.rs:64-84`, `:296-311` |
| C-02 | P0 | **resolved** (design changed) | `broker.rs:119-137`, `:235`, `:407-423`, `:599-681`; `rust-core/migrations/0033_delivery_and_replay_receipts/up.sql`; `rust-core/migrations/0034_delivery_receipt_integrity/up.sql` |
| C-03 | P0 | **resolved** | `rust-core/crates/worker/src/attempts.rs:346-359`; `crates/persistence/src/durable.rs:1492,1547,1845` |
| C-04 | P0 | **resolved** | `rust-core/crates/worker/src/consumer.rs:850,863,946-949`; tests `a_lost_lease_stops_the_run_instead_of_running_to_completion`, `a_durable_cancellation_stops_the_run_at_the_next_safe_point` |
| C-05 | P0 | **resolved** | `rust-core/crates/application/src/execution_service.rs:243-258`, `:313-341` |
| C-06 | P0 | **resolved** | `python-orchestrator/src/mas_orchestrator/security/internal_service.py:11,49-51`; `security/internal_replay.py:23,31,56`; `grpc/agent_bridge_servicer.py:593`; `rust-core/crates/api/src/handlers/internal.rs:304-320` |
| C-07 | P0 | **resolved** | `python-orchestrator/src/mas_orchestrator/providers/base_http.py:240-246`, `:301-320`, `:440-447`, `:625-640` |
| C-08 | P1 | **resolved** | `python-orchestrator/src/mas_orchestrator/services/composition.py:360-387`; `services/execution_service.py:254-266`; `services/execution_state.py:87,165`; tests `TestAuthoritativeState` (8), `TestDurableStateReporter` (8) |
| C-09 | P1 | **resolved** | `agents/runtime.py:186-190`; `services/execution_service.py:313-319` |
| C-10 | P1 | **resolved** | `rust-core/crates/worker-service/src/agent_execution.rs:438-458`; test `build_dispatch_refuses_a_corrupt_scope_instead_of_panicking` |
| C-11 | P1 | **resolved** | `data_hub/sources.py:391-412`, `:478`, `:736-761`; `data_hub/service.py:284-291` still post-collection backstop |
| C-12 | P1 | **resolved** | `rag/parser_pool.py:35,51,83`; `rag/ingestion.py:96-102,136-140`; `rag/parsing.py:230-239` |
| W-01 | warning | **resolved** | `providers/base_http.py:601` |
| W-02 | warning | **resolved** | `providers/base_http.py:559-564`, `:595-598` |
| W-03 | warning | **resolved** | `tools/builtins/http.py:67-70`, `:231` |
| W-04 | warning | **resolved** | `tools/builtins/webhook.py:332-347`, `:352`, `:391`, `:414`; tests `TestWebhookReplay` (8) |
| W-05 | warning | **resolved** | `data_hub/sources.py:162-170`, `:173`, `:229-236`, `:256-276`, `:287-290` |
| W-06 | warning | **resolved** | `providers/base_http.py:611` |
| W-07 | warning | **resolved** | `rust-core/crates/execution/src/agent_bridge.rs:77` (`AgentRuntimeConfiguration` derives `Default`) |
| W-08 | warning | **resolved** | `python-orchestrator/tests/contract/test_proto_contract.py` (28 `test_` functions; included in `scripts/check.sh` 2173 passed) |

---

## P0

### C-01 — default PostgreSQL broker vs durable-attempt protocol — **resolved**

`FETCH_SQL` is now a read-only statement and says so explicitly: the durable
`claim_in` is the sole writer of `queued → running`, ownership and the attempt row.

- `rust-core/crates/persistence/src/repositories/broker.rs:64-70` — comment: *"This
  statement is deliberately read-only: `PgDurableExecution::claim_in` is the sole
  writer of `queued → running`…"*
- `rust-core/crates/persistence/src/repositories/broker.rs:72-84` — `FETCH_SQL`
  only `SELECT`s; an expired `running` row is emitted as a recovery hint.
- `rust-core/crates/persistence/src/repositories/broker.rs:296-311` — `fetch_durable`
  performs no `UPDATE`; the comment at `:306-311` states the coordinator installs
  status, fence and attempt atomically.

The loop described in the report (`queued → running → claim finds nothing → NAK`)
cannot occur because the broker never writes `running`.

### C-02 — volatile, one-shot, collision-prone delivery receipts — **resolved**

- Collision: `sequence_of()` no longer exists. The message sequence is now persisted
  per task in `broker_task_messages` with a unique key:
  `broker.rs:235` (`durable_message_sequence`), `:423`.
- One-shot: `AckInstruction::InProgress` no longer removes the mapping — it extends
  it: `broker.rs:667-681`.
- Ownership: `FlightLedger` is documented as *"only duplicate suppression, never
  ownership"* (`broker.rs:119-121`); the ack path is conditional SQL on task status
  (`broker.rs:599-681`), and `NAK_SQL`/`POISON_QUARANTINE_SQL` refuse to touch a row
  that has a live `broker_delivery_receipts` row (`broker.rs:91-119`). Delivery count
  is persisted by the receipt port, not inferred from process memory (`broker.rs:407-419`).
- Receipt tables: `rust-core/migrations/0033_delivery_and_replay_receipts/up.sql`,
  `rust-core/migrations/0034_delivery_receipt_integrity/up.sql`.

Residual, by design: the local hint map is process-local, so an `ack` arriving after
a restart falls back to the event lane (`broker.rs:601-604`). That is no longer a
correctness path — durable settlement is owned by `task_attempts`/the claim fence.

### C-03 — successful agent output lost before durable settlement — **resolved**

- `rust-core/crates/worker/src/attempts.rs:346-359` — `succeed()` takes
  `result: serde_json::Value` and builds `TaskOutcome::Succeeded { result, usage }`.
- `rust-core/crates/application/src/durable.rs:346-360` — `TaskOutcome::Succeeded`
  carries `result` (JSON `null` is a distinct, valid result).
- `rust-core/crates/persistence/src/durable.rs:1492` —
  `task.complete(&settlement.actor, Some(result.clone()))`.
- `rust-core/crates/persistence/src/durable.rs:1547` — `loaded.complete(..., output)`.
- `rust-core/crates/persistence/src/durable.rs:1845` —
  `execution.complete(&request.actor, output.clone())`.

Output now lands in the same transaction as task/attempt/status/usage.

### C-04 — lease loss logged but execution continues; shutdown/cancellation not wired — **resolved**

The defect is fixed (2026-11-15):

- A per-attempt `StopReason` oneshot now connects the heartbeat to the run:
  `Ok(false)` from `attempts.renew(...)` stops the run (`StopReason::LeaseLost`)
  instead of only warning.
- The heartbeat also probes the durable task each cycle; a durably cancelled
  task stops the run (`StopReason::Cancelled`) at the next safe point.
- `handler.execute` now runs as a spawned task raced against the stop signal
  (`tokio::select!`): the in-flight handler future is aborted (dropped at its
  next await point) and the attempt settles as `AppError::cancelled(...)`,
  which `classify` maps to `FailureClass::Cancelled` — a deliberate stop,
  never a failure, never requeued.

Graceful SIGTERM keeps its drain semantics on purpose (pinned by
`graceful_shutdown_drains_in_flight_work`): the fetch loop stops, in-flight
work is drained within `drain_timeout`, and the drain deadline drops whatever
is left.

**Verification:** `consumer::tests::a_lost_lease_stops_the_run_instead_of_running_to_completion`
and `consumer::tests::a_durable_cancellation_stops_the_run_at_the_next_safe_point`
run a handler that never returns; without the fix neither test can finish
except through the drain timeout with `abandoned > 0`.

### C-05 — idempotent submission can consume quota multiple times — **resolved**

- `rust-core/crates/application/src/execution_service.rs:243-258` — quota is
  explicitly *not* charged in preflight; only entitlement/policy (side-effect free)
  run first.
- `rust-core/crates/application/src/execution_service.rs:313-341` — the durable
  `submit()` result is inspected, and `consume_created_execution` is called **only**
  for `SubmissionKind::Created`. A replay is read-only.
- `:313-331` — the in-memory adapter's local admission gate is taken before the
  durable submit, so a routine quota refusal happens before a runnable task exists.

### C-06 — internal HMAC replayable during the freshness window — **resolved**

The v2 scheme binds a nonce and both sides keep a one-time receipt.

- Python canonical form and parser:
  `python-orchestrator/src/mas_orchestrator/security/internal_service.py:11`
  (``v2:<service>:<unix_millis>:<nonce>:<hmac-sha256-hex>``), `:49-51`.
- Receipt guard: `python-orchestrator/src/mas_orchestrator/security/internal_replay.py`
  (`InternalReplayGuard` protocol `:23`, `InMemoryReplayGuard` explicitly non-production `:31`,
  `PostgresReplayGuard` production consumer `:56`).
- Enforcement point: `python-orchestrator/src/mas_orchestrator/grpc/agent_bridge_servicer.py:593`
  — the nonce is consumed before the request enters application work.
- Rust side: `rust-core/crates/api/src/handlers/internal.rs:304-320`
  (`consume_internal_nonce`, `INSERT … ON CONFLICT DO NOTHING` into
  `internal_request_receipts`; a DB failure is treated as a bad signature).

### C-07 — outbound HTTP DNS TOCTOU and proxy bypass — **resolved**

- `python-orchestrator/src/mas_orchestrator/providers/base_http.py:440-447` —
  `trust_env=False`, with the comment that it refuses `HTTPS_PROXY` and similar.
- `:240-246` — `validate_resolved_address()` refuses a resolved address in a blocked
  range.
- `:301-320` — resolution is pinned and every resolved address is validated.
- `:625-640` — provider hosts are resolved immediately before the TCP connection and
  each address is re-validated.

### C-08 — production composition has no authoritative state reader/reporter — **resolved**

Fixed (2026-11-15) — the named types now exist and the duplicate path is no
longer serialised as a fresh success:

- `services/execution_state.py` (new): `RustExecutionStateReader` — a read-only,
  tenant-scoped lookup of Rust's `executions.status` (RLS armed through
  `set_config('app.current_tenant', ...)`), mapping `pending/running/paused/
  completed/failed/cancelled` onto `ExecutionState`, answering `UNKNOWN`
  (never raising) when the state cannot be determined; and
  `ExecutionInProgressError` (retryable, `DUPLICATE_REQUEST` code) raised for a
  duplicate of an in-flight run.
- `services/idempotency.py`: `ExecutionStateReader.state_of` now takes the
  authoritative `tenant_id` (a state read must be tenant-confined).
- `services/execution_service.py`: `DuplicateAction.WAIT` raises
  `ExecutionInProgressError` instead of returning an empty `COMPLETED` result;
  `SKIP` reports the **authoritative** termination (`FAILED` stays `FAILED`,
  `CANCELLED` → `CANCELLED`, `TIMED_OUT` → `DEADLINE`) — a redelivery of a
  failed run can no longer read as success.
- `services/composition.py`: wires `RustExecutionStateReader` from the platform
  database secret reference; with no database configured the stand-in is kept
  and the gap is logged at startup (`startup.state_reader_unwired`).

**Known limitation (closed):** the durable transition *reporter* is
implemented — `services/execution_state.py` also carries
`RustStateTransitionReporter`, which writes the terminal fact back to
Rust's `executions` ledger through the same tenant-scoped channel as the
reader (`set_config('app.current_tenant', ...)` per report, so the RLS
policy confines the write). It mirrors the domain's settle shape (`status`,
`error`, `finished_at`, `version` bump), only asserts over a non-terminal
row (whoever settles first wins), is idempotent on redelivery, and raises
`StateReportError` on an unscoped report, an unknown execution, a
conflicting terminal fact, or a database failure — a reporter that cannot
deliver must raise. `services/composition.py` wires it beside the reader
from the same database secret reference; with no database configured the
logging stand-in is kept and the gap is logged
(`startup.state_reporter_unwired`). Live-DB behaviour remains NOT_RUN (no
live infra in this environment).

**Verification:** `tests/unit/test_services_events.py::TestAuthoritativeState`
(8 tests: WAIT refusal, SKIP termination mapping ×3, reader status mapping,
missing-row → UNKNOWN, database failure → UNKNOWN, tenant scoping) and
`::TestDurableStateReporter` (8 tests: terminal write, tenant scoping,
termination mapping, unscoped refusal, database-failure raise, idempotent
redelivery, conflicting fact, unknown execution).

### C-09 — task cancellation converted into a normal result — **resolved**

- `python-orchestrator/src/mas_orchestrator/agents/runtime.py:186-190` —
  `except asyncio.CancelledError: cancel_event.set(); raise`.
- `python-orchestrator/src/mas_orchestrator/services/execution_service.py:313-319` —
  same pattern, with the comment that re-raising lets the bridge cancel in-flight
  provider/tool calls.
- Cooperative cancellation (`CancellationRequestedError`) keeps its explicit durable
  outcome immediately after that `except`, which is the intended domain path.

---

## P1

### C-10 — malformed persisted task panics a worker — **resolved**

Fixed (2026-11-15):

- `agent_execution.rs::runtime_context` is now fallible: a nil scope identifier
  becomes `AppError::validation("execution.run task carries an invalid scope: …")`,
  which the consumer classifies as terminal work — the corrupt row is
  dead-lettered once instead of killing a worker process per bad row.
- `consumer.rs::execute` runs the handler as a spawned task with a
  panic-supervision boundary: a panicking handler surfaces as
  `Err(AppError::internal("task handler panicked or was aborted; …"))` and is
  settled as a classified attempt failure; the worker survives.

**Verification:** `build_dispatch_refuses_a_corrupt_scope_instead_of_panicking`
(worker-service) — a nil-tenant task is refused with `AppError::Validation`,
no panic.

### C-11 — Data Hub cursor/resume/memory — **resolved**

Resolved parts:

- Cursor padding: `python-orchestrator/src/mas_orchestrator/data_hub/sources.py:736-761`
  — `_decode_cursor` restores Base64 padding and rejects malformed/oversized input.
- S3 resume: `sources.py:391-412` — `continuation` and `start_after` are persisted in
  the cursor and reused, so a large unchanged prefix no longer restarts lexically.

Admission control (2026-11-15):

- The in-tree adapters enforce the aggregate budget **before download and
  retention**: every fetch is bounded by the remaining budget
  (`max_response_bytes = min(max_document_bytes, remaining)`), the S3 adapter
  additionally admits each object by its declared size **before** the GET
  (`transfer_bytes + size > max_sync_bytes` → return the checkpoint before the
  key, no request issued), and every retained byte is accounted as it is kept.
- `service.py:284-291`'s post-collection sum stays as the backstop for
  third-party adapters registered behind the `SourceSyncAdapter` protocol —
  it is a guard, not the primary control.

**Verification (the report's regression set):**
`test_object_sync_resumes_after_the_checkpointed_prefix` (unchanged prefix
larger than `max_pages` reaches the later changed key after resumptions, no
lexical restart), `test_object_sync_admits_the_aggregate_budget_before_downloading`
(three changed objects; the third is never requested once its size would cross
the cap — the report's "three 32 MiB objects / 64 MiB" scenario at KiB scale,
byte accounting is scale-invariant — and the cursor ends at the last retained
object). Cursor round-trip/reject tests pre-exist. Live-DB behaviour NOT_RUN.

### C-12 — untrusted parsing blocks the loop / exceeds bounds — **resolved**

- `python-orchestrator/src/mas_orchestrator/rag/parser_pool.py` — process-isolated
  parser with a wall-clock timeout and an `RLIMIT_AS` memory cap
  (`ParserPoolConfig`, `_apply_memory_limit`).
- `python-orchestrator/src/mas_orchestrator/rag/ingestion.py:96-102` — the pool is the
  default for all untrusted parsers; `:136-140` awaits `self._parser_pool.parse(...)`.
- `python-orchestrator/src/mas_orchestrator/rag/parsing.py:230-239` — PDF page-count
  cap then linear accumulation via `_append_limited`, which rejects instead of
  appending past the cap.

---

## Warnings

- **W-01 resolved** — `providers/base_http.py:601` uses
  `current_url = urljoin(current_url, location)`.
- **W-02 resolved** — cross-origin redirect with credentials is refused
  (`base_http.py:559-564`), and redirects for non-idempotent methods are refused
  (`:595-598`).
- **W-03 resolved** — `tools/builtins/http.py:67-70` `_safe_url_for_log` strips query
  and fragment; used at `:231`.
- **W-04 resolved** — `tools/builtins/webhook.py` now signs a per-delivery id
  together with the timestamp (`{timestamp}.{delivery_id}.` + canonical body at
  `:347`; `x-mas-delivery-id` header at `:57`; `_sign` at `:332`).
  `WebhookTool.verify` (`:352`) takes the delivery id and an optional
  `DeliveryDedup` (`:391`) — an atomic (locked, bounded, LRU) receiver-side
  seen-or-record store (`:414`); a verified-but-already-processed delivery is rejected,
  which makes capture-and-replay inside the skew window useless. Deliberate
  re-sends carry fresh ids and still process. Failed verifications never
  consume an id. Verified by `tests/unit/test_builtins.py::TestWebhookReplay`
  (8 tests incl. `test_dedup_is_atomic_under_concurrency`). A multi-worker receiver must
  back `DeliveryDedup` with a durable shared store (documented).
- **W-05 resolved** — `data_hub/sources.py:162-170` keeps a `scheduled` set;
  `:173` `max_frontier = min(max_pages * 16, 20_000)`; `:229-236` dedupes before
  enqueueing; `_collect_links` enforces a fixed per-page fanout cap (`max_pages * 8`
  at `:229`); and `_validate_link` (`:256-276`) / `_validate_https_url` (`:287-290`)
  cap URL length at 2048 characters (`_MAX_URL_LENGTH` at `:57`) before `urljoin`
  and before queueing, dropping hostile/oversized links individually instead of
  failing the sync. Tests:
  `test_web_sync_tolerates_a_hostile_link_farm_within_its_bounds`,
  `test_web_sync_keeps_link_farm_work_bounded_relative_to_max_pages`,
  `test_web_links_drop_oversized_and_hostile_targets_individually`.
- **W-06 resolved** — `providers/base_http.py:611` sets `is_truncated=False` after a
  successful bounded read.
- **W-07 resolved** — `rust-core/crates/execution/src/agent_bridge.rs:77` derives
  `Default` (`#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]`);
  the manual `impl Default` is gone.
- **W-08 resolved** — `python-orchestrator/tests/contract/test_proto_contract.py`
  defines 28 `test_` functions and is included in `scripts/check.sh` (2173 passed,
  6 skipped). Isolated collect with system pytest this turn is NOT_RUN
  (`PytestConfigWarning: Unknown config option: asyncio_default_fixture_loop_scope`).
  The historical “72 passed” claim is not reproduced as a standalone number here.

---

## Repair-set changes (not part of C/W, but part of this tree)

Two real defects found and fixed while restoring the tree:

1. **Future-timestamp skew in the Python verifier** —
   `python-orchestrator/src/mas_orchestrator/security/internal_service.py` accepted a
   signature whose `signed_at_ms` was in the future because it used
   `abs(now_ms - signed_at_ms)`. Rust refuses every future timestamp, so the same
   request was accepted by Python and rejected by Rust, making
   `tests/security/test_internal_service.py::test_a_signature_from_the_future_is_refused`
   fail intermittently (observed 1 failure in 15 runs). The condition is now
   `signed_at_ms > now_ms or now_ms - signed_at_ms > MAX_SKEW_SECONDS * 1000`;
   25 consecutive runs, 0 failures.
2. **`ruff` E501** — `python-orchestrator/src/mas_orchestrator/tools/builtins/webhook.py:188`
   was 101 chars (> 100); the string was split. `ruff check` is clean.

---

## Verification (commands actually run in this tree)

Rust, from `enterprise-mas/rust-core` (toolchain 1.98.1):

```
cargo fmt --all -- --check                                   # exit 0 (via scripts/check.sh)
cargo metadata --offline --no-deps                          # exit 0, 24 members
cargo clippy --workspace --all-targets --locked -- -D warnings   # exit 0 (via scripts/check.sh)
cargo test --workspace                                       # exit 0
#   1250 passed, 0 failed, 13 ignored
```

Python, from `enterprise-mas/python-orchestrator`:

```
bash scripts/check.sh            # exit 0
#   ruff format (check) PASS
#   ruff lint PASS
#   mypy (strict) PASS
#   pytest PASS — 2173 passed, 6 skipped, 0 failed
#   compileall PASS
```

The 6 skips are live-PostgreSQL tests that require `MAS_TEST_MEMORY_DATABASE_URL`,
`MAS_TEST_MEMORY_ELEVATED_DATABASE_URL`, `MAS_TEST_RAG_DATABASE_URL` and
`MAS_TEST_RAG_ELEVATED_DATABASE_URL`; they skip loudly rather than passing vacuously.

Deliverable completeness (Phase 16), run directly:

```
python3 build_phase16_deliverable.py       # 36 files, no omissions
python3 verify_deliverable_completeness.py # 36/36 byte-for-byte
```

Not run in this environment (no docker daemon, no live PostgreSQL/NATS): the
live-PG/NATS suites and the compose smoke test. Those stages report `NOT_RUN`, not
`PASS`. Migration `0054_memory_plane_role` is registered and its registry test
passes, but the SQL itself has not been applied to a live PostgreSQL instance
here.

---

## New work added by this repair

- **Memory plane** — `migrations/0054_memory_plane_role/up.sql` creates the
  `mas_memory` role (`NOLOGIN NOBYPASSRLS`) and the `memory_records` table with
  `tenant_id`, `ENABLE`+`FORCE` RLS and a tenant-isolation policy; `down.sql`
  reverses it behind an explicit `mas.allow_data_loss` opt-in. The role is
  registered in `crates/persistence/src/migrations.rs` and granted `LOGIN` by
  `scripts/provision-plane-roles.sh` (which previously referenced a role no
  migration created).
- **Durable Python memory store** —
  `python-orchestrator/src/mas_orchestrator/memory/postgres_store.py` implements
  the existing `MemoryStore` protocol against that table through `mas_memory`,
  with the same server-side authority probe and `SET LOCAL ROLE` +
  `app.current_tenant` scope the RAG plane uses. Exported from
  `memory/__init__.py`; unit tests in
  `tests/unit/test_memory_postgres_store.py` (7 passed) and live-PG tests in
  `tests/integration/test_memory_plane_store.py` (skip without a DSN).


---

## Verification — 2026-11-15 (P2 turn: open C/W items + M-1…M-6)

Gates after this turn's changes (both green; counts before → after):

| Gate | Before | After |
|---|---|---|
| Python `bash scripts/check.sh` (ruff format+lint, mypy strict, pytest, compileall) | 2109 passed, 6 skipped | **2127 passed, 6 skipped** |
| Rust `cargo test` (mas-worker + mas-worker-service) | 1195 workspace total | +3 new tests green; workspace = **1198 passed / 0 failed / 13 ignored**, clippy `-D warnings` clean |

- `cargo fmt --all -- --check` clean; clippy `-D warnings` runs with the workspace gate.
- NOT_RUN (unchanged, honest): any live-DB / live-infra behaviour (no live infra in
  this environment); E2E/manual flows the user runs themselves.

### M-1…M-6 (from `enterprise-mas/reports/CODE_REVIEW.md`)

- **M-1 resolved** — tracked 21 MB `.cargo/bin/rustup` + `.cargo/env` removed from
  the tree (preserved in `/home/user/_removed_files/dot-cargo/`), untracked from
  git, and `.gitignore` documents `.cargo/bin/` + `.cargo/env`.
- **M-2 resolved (verified, nothing to change)** — every path README §6/§13 names
  exists (`crates/api/proto/.../mas.proto`, `python-orchestrator/scripts/
  generate_proto.sh`, `scripts/{bootstrap-toolchain,check,dev-up,dev-down,migrate,bench}.sh`,
  `config/development.toml`); the review's false names are not present anywhere.
- **M-3 resolved** — README test-count claims refreshed to the verified numbers
  in this document's table and the README status block (see the P2 worktrack
  entry for the final tally).
- **M-4 resolved** — `rust-core/Cargo.toml` stale `[todo]` member header replaced:
  all listed crates are implemented; `[workspace]` members is the authority.
- **M-5 resolved (P1-rust)** — billing schema is no longer orphaned:
  `crates/persistence/src/repositories/{billing,billing_customers,plan_prices,billing_checkout}.rs`
  plus the application/integrations/api layers read and write it (verified green
  in P1-rust).
- **M-6 resolved** — `api/src/main.rs::dev_verifier()` no longer registers a
  built-in `"dev-token"`: every dev token is declared through `MAS_DEV_TOKENS` /
  `server.dev_tokens` (`scripts/dev-up.sh` declares `dev-token:dev-user:user` for
  local bring-up; `docs/QUICKSTART.md` documents it). An empty table authenticates
  nobody. Production's `enforce_production_guardrails` is unchanged.
