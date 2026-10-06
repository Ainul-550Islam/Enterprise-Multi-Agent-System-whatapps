# Enterprise Policy + Distributed Quota + Usage + Entitlement/Billing Control Plane

**Status: COMPLETE — all gates green, including the gaps this report previously flagged as open.**

*Addendum (same item, continuation): the two limitations reported below — "the admin write surface
is not built" and the control plane's untested request path — are now closed. §1, §2, §4.8, §5 and
§7 have been updated in place; the four defects found while closing them are recorded in §5 as
entries 11–14.*

*Second addendum (same item, continuation): the two limitations the first addendum still recorded as
open are now closed — the worker holds a concurrency slot for the duration of a run (§4.9), and the
control-plane refusal path is audited (§4.10). §1, §2, §5, §6 and §7 have been updated in place.
Wiring the slot surfaced three further production defects in the reservation path, recorded in §5 as
entries 15–17; a missing privilege grant found by the same method is entry 18, and a latent test
defect that only manifests once the shared test database has aged is entry 19.*
Item 6 of the programme. Preceded by: full codebase review (1), Python AI Orchestrator (2), Rust↔Python AgentBridge (3), Canonical Durable Execution Pipeline (4), Enterprise Security/Authorization/Tenancy/RLS Hardening (5).

---

## 1. Verification — every command below was executed, and these are its real outputs

| Command | Result |
|---|---|
| `cargo build --workspace --locked` | **clean** |
| `cargo test --workspace --locked` | **956 passed, 0 failed** |
| `cargo test --workspace --locked --features nats` | **956 passed, 0 failed** |
| `cargo test -p mas-persistence --test control_plane_pg` (live Postgres) | **38 passed, 0 failed** |
| `cargo test -p mas-api --test control_plane_admin` (live Postgres, admin writes) | **18 passed, 0 failed** |
| `cargo test -p mas-worker-service --test run_slots_pg` (live Postgres, the concurrency gate) | **15 passed, 0 failed** |
| `cargo test -p mas-persistence --test plane_grants` (live Postgres, plane privileges) | **6 passed, 0 failed** |
| `cargo test -p mas-persistence --test live_pg` (live Postgres) | **16 passed, 0 failed** |
| `cargo test -p mas-worker --lib` | **66 passed, 0 failed** |
| `cargo test -p mas-worker-service` (lib + bin) | **23 passed, 0 failed** |
| `cargo test -p mas-application --lib` | **101 passed, 0 failed** |
| `cargo test -p mas-api --lib` | **44 passed, 0 failed** |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | **0 errors** |
| `cargo fmt --all -- --check` | **clean** |
| `cargo doc --no-deps --workspace` | **0 warnings, 0 errors** |
| `python3 -m pytest tests -q` (Python orchestrator, item 2, unmodified) | **1,936 passed, 0 failed** |

Environment for the live suites: `bash pg_up.sh`, then
`MAS_ENV=test MAS_TEST_DATABASE_URL=postgres://mas:mas@127.0.0.1:55432/mas_live`
`MAS_TEST_DATABASE_APP_URL=postgres://mas_app:mas_app_pw@127.0.0.1:55432/mas_live`.

The Python suite needs `pip install pytest-asyncio grpcio grpcio-tools` first, and pip packages do
not survive a workspace restore. Without `pytest-asyncio` ≥ 0.24, collection aborts with
`Unknown config option: asyncio_default_fixture_loop_scope` and reports "20 errors" — that is a
missing dependency, not a failing test.

Baseline before this phase was **823 passed / 0 failed** (item 5). The increase to 956 is new coverage, and no previously-passing test was weakened: where the gate made an old test fail, the failure was the gate being correct and the *fixture* or the *code* was fixed (see §5).

**The live control-plane suite was run 20 consecutive times with 0 failures.** One test was found to be flaky during that exercise and was corrected — see §4.3. After the addendum, the 37-test control-plane suite and the 15-test admin suite were each run 3 further times with 0 failures. After the second addendum the whole workspace was run twice more (with and without `--features nats`) at 956/0, and the run-slot suite (15) and the admin suite (18) were each run 3–4 further times at 0 failures.

One caveat worth stating: the workspace total of 956 includes the live-PostgreSQL suites. Without `MAS_TEST_DATABASE_URL` those suites skip loudly rather than pass, and the total drops accordingly.

---

## 2. Files

### Created (17 files, 336,912 bytes) — plus 4 in the first addendum (44,197 bytes) and 8 in the second (76,672 bytes)

| Bytes | File |
|---:|---|
| 31,654 | `migrations/0020_control_plane/up.sql` |
| 4,818 | `migrations/0020_control_plane/down.sql` |
| 62,326 | `crates/persistence/tests/control_plane_pg.rs` |
| 41,895 | `crates/application/src/control_plane/mod.rs` |
| 21,635 | `crates/persistence/src/repositories/control_plane/subscriptions.rs` |
| 24,456 | `crates/persistence/src/repositories/control_plane/entitlements.rs` |
| 18,275 | `crates/persistence/src/repositories/control_plane/reservations.rs` |
| 18,155 | `crates/persistence/src/repositories/control_plane/policies.rs` |
| 17,971 | `crates/api/src/handlers/control_plane.rs` |
| 17,349 | `crates/persistence/src/repositories/control_plane/usage.rs` |
| 16,237 | `crates/persistence/src/repositories/control_plane/plans.rs` |
| 15,448 | `crates/persistence/src/repositories/control_plane/counters.rs` |
| 12,077 | `crates/application/src/control_plane/memory.rs` |
| 11,407 | `crates/persistence/src/repositories/control_plane/billing.rs` |
| 10,754 | `crates/persistence/src/repositories/control_plane/adapters.rs` |
| 7,858 | `crates/application/src/control_plane/ports.rs` |
| 4,597 | `crates/persistence/src/repositories/control_plane/mod.rs` |

**Addendum:**

| Bytes | File |
|---:|---|
| 36,937 | `crates/api/tests/control_plane_admin.rs` (15 live tests over the admin write surface) |
| 4,434 | `crates/persistence/src/repositories/control_plane/executor.rs` |
| 1,778 | `migrations/0021_control_plane_grants/up.sql` |
| 1,048 | `migrations/0021_control_plane_grants/down.sql` |

**Second addendum — the concurrency slot the worker holds for the duration of a run:**

| Bytes | File |
|---:|---|
| 32,634 | `crates/worker-service/tests/run_slots_pg.rs` (15 live tests, the real adapter over the real port) |
| 12,948 | `crates/worker/src/run_slot.rs` (`RunSlotPort`, `RunSlot`, `NoRunSlots`) |
| 10,308 | `crates/worker-service/src/run_slots.rs` (`ControlPlaneRunSlots`, the production impl) |
| 9,681 | `crates/persistence/tests/plane_grants.rs` (6 live tests, the plane privilege contract) |
| 4,825 | `crates/worker-service/tests/support/mod.rs` (live harness for the worker service) |
| 1,895 | `migrations/0022_worker_metering_grant/up.sql` |
| 747 | `migrations/0022_worker_metering_grant/down.sql` |
| 1,634 | `crates/worker-service/src/lib.rs` (the library half of the worker host crate) |

### Modified

`crates/api/src/router.rs`, `crates/api/src/state.rs`, `crates/api/src/main.rs`,
`crates/application/src/execution_service.rs`, `crates/application/src/tenancy_service.rs`,
`crates/application/src/audit.rs`,
`crates/domain/src/reservation.rs`, `crates/domain/src/overrides.rs`, `crates/domain/src/entitlement.rs`,
`crates/policy/src/engine.rs`, `crates/quota/src/engine.rs`, `crates/quota/src/counter.rs`,
`crates/tenancy/src/entitlements.rs`, `crates/scheduler-service/src/durable.rs`,
`crates/api/src/handlers/control_plane.rs`, `crates/persistence/src/error.rs`,
`crates/persistence/src/migrations.rs`, `crates/persistence/tests/control_plane_pg.rs`,
and all eight control-plane repositories (`billing`, `counters`, `entitlements`, `plans`,
`policies`, `reservations`, `subscriptions`, `usage`) plus `control_plane/mod.rs`.

**Second addendum — modified:**

`crates/worker/src/consumer.rs` (slot taken before the durable claim, renewed by the heartbeat,
released on every exit path), `crates/worker/src/attempts.rs` (the coordinator carries the gate),
`crates/worker/src/lib.rs`, `crates/application/src/control_plane/mod.rs` (the Step 3b block, the
idempotent `reserve`, the lease guard), `crates/application/src/control_plane/memory.rs`,
`crates/persistence/src/repositories/control_plane/reservations.rs` (refresh-in-place,
`extend_lease`), `crates/persistence/src/repositories/control_plane/adapters.rs`,
`crates/domain/src/reservation.rs`, `crates/quota/src/engine.rs`,
`crates/api/src/handlers/control_plane.rs` (refusals are audited),
`crates/api/tests/control_plane_admin.rs`, `crates/persistence/tests/live_pg.rs`,
`crates/persistence/tests/support/mod.rs`, `crates/worker-service/src/main.rs`,
`crates/worker-service/Cargo.toml`.

Every new and modified file carries a `PURPOSE:` header stating responsibility, owning layer, production role, **data ownership**, and what it must **not** do. Verified programmatically: **0 files missing**.

---

## 3. Architecture

```
                    ┌──────────────────────────────────────────┐
 HTTP/gRPC/scheduler│  ControlPlane (mas-application)          │
        ──────────►│  THE canonical gate                      │
                   │                                          │
                   │   1. Entitlement  (fails closed)         │
                   │   2. Policy       (before any charge)     │
                   │   3. Quota        (the only side effect)  │
                   └───────────────┬──────────────────────────┘
                                   │ ports (mas-application defines them;
                                   │ mas-persistence implements them —
                                   │ persistence already depends on
                                   │ application, so the gate cannot name a
                                   │ Postgres type without a cycle)
                   ┌───────────────▼──────────────────────────┐
                   │  Postgres, migration 0020, RLS-enforced   │
                   │  plans · subscriptions · overrides        │
                   │  policies · quota_definitions · counters  │
                   │  reservations · usage_records · receipts  │
                   └──────────────────────────────────────────┘
```

One gate, one policy engine, one quota engine, one usage ledger. `ControlPlane` is a **required** constructor argument of `ExecutionService` — not `Option<...>` — because an optional gate is a second path, and a second path is one in which entitlement, policy and quota are simply not enforced.

---

## 4. What was built, against the spec

### 4.1 Distributed quota (Phases 6–9)

`PostgresCounterStore::try_reserve` is a single atomic statement:

```sql
INSERT INTO quota_counters (...) VALUES (...)
ON CONFLICT (tenant_id, dimension, bucket) DO UPDATE
   SET value = quota_counters.value + EXCLUDED.value
 WHERE quota_counters.value + EXCLUDED.value <= $5
RETURNING value
```

There is no read-modify-write window, so no interleaving can lose an update. `decrement` saturates at zero (`GREATEST(value - $n, 0)`), so a double release cannot mint headroom. `gc_buckets` never reclaims bucket 0, because gauge dimensions live there.

The spec's case — replica A reserves 70, replica B reserves 50, ceiling 100 — is a live test against real Postgres, on independent connections:

```
test two_replicas_cannot_together_exceed_one_ceiling ... ok
test a_concurrent_storm_never_breaches_the_ceiling ... ok
```

`QuotaExceeded` is a distinct error variant (HTTP **429**, `is_retryable() == false`), separated from `RateLimited` precisely so a worker does not burn its attempt budget re-submitting work that cannot succeed until the billing period rolls over.

### 4.2 Reserve → execute → settle → release (Phase 8)

`PostgresReservationStore`:
- `reserve` — idempotent on `(tenant, reservation_key)`; a retry returns the original hold, it does not charge twice.
- `settle` — guarded by `AND state = 'held'`, capped by `LEAST($3, reserved_amount)`. A duplicate returns `applied: false` **and** `released: 0`: a redelivered completion must neither meter the work twice *nor refund it twice*.
- `release` — cannot refund twice.
- `reclaim_expired` — bounded `FOR UPDATE SKIP LOCKED`; a settled reservation is never reclaimed (that would refund work already metered).

Crash recovery is a live test: a reservation whose lease lapses is reclaimed exactly once, and the headroom returns to the counter.

### 4.3 A flaky test found and fixed

Running the suite 20× surfaced one intermittent failure in the ceiling test. The cause was **the assertion, not the implementation**: with amounts of 70 and 50 and a ceiling of 100, *two 50s may both be granted* — 50 + 50 fills the ceiling exactly and there is nothing illegal about it. The test asserted "exactly one caller wins", which is stronger than the guarantee.

This matters, so it is worth stating plainly: a test that asserts more than the invariant will fail on a correct implementation, and it will fail *intermittently*, which is the worst kind of signal. The test was split in two — one proving the named 70/50 case, one proving the general invariant (the granted sum never breaches the ceiling) — with the reasoning recorded in a comment so the next reader does not "fix" it back. **20/20 runs green since.**

### 4.4 Exactly-once metering (Phases 10–12, 17, 25)

The usage ledger is the single write path. It refuses a zero quantity (so "no usage" stays distinguishable from "usage of zero") and refuses any write with neither a `task_id` nor a `metering_key` (so a duplicate delivery is always recognisable). Cost is integer micro-units; `UsageTotal::cost()` returns `None` when more than one currency contributed, because adding them would be meaningless.

The worker already metered actual consumption through the canonical durable pipeline built in item 4 — `write_usage` runs in the same transaction as the settlement, and outbox events are written after it, so an event never precedes its durable record. What this phase adds is the Postgres control-plane ledger that the settled usage lands in, with the de-duplication constraints.

### 4.5 Entitlements, plans, subscriptions (Phases 13–16, 20)

Resolution is canonical and server-side: tenant → subscription → plan → plan entitlements → overrides → effective. A caller cannot submit an entitlement claim; there is no code path that reads one.

Tenant provisioning creates a subscription, because a tenant with no subscription genuinely cannot run anything and the gate refuses its work. `DEFAULT_PLAN_CODE` points at a real row in `plans` (seeded by the migration), not a constant the code special-cases — an operator can change what the base plan grants without a release. Tenant provisioning and the gate share **one** store instance; two would let a tenant be activated and still be refused every execution.

Overrides are half-open `[effective_from, effective_to)`, carry tenant/feature/value/reason/actor/period, and never edit the plan. Two live overrides for the same target cannot coexist, so the effective limit never depends on row order.

### 4.6 Policies (Phases 2–5)

One policy service. `publish` runs `mas_policy::compiler::compile` **first** — it is the single activation gate, so an incoherent definition cannot become active on any caller path. Deletion refuses an active policy.

The cache key already incorporated each policy's id, version, priority and checksum, so a publish invalidates it without an explicit purge. An empty policy set is **not** a denial: policy is an additional restriction layered on the authorization a tenant already carries, so treating "no policy configured" as "denied" would turn every unconfigured tenant into one that cannot execute — which presents as an outage, not as a security control. Where a policy *does* participate, the secure default is unchanged. All 23 pre-existing policy tests still pass.

### 4.7 Billing (Phases 18, 19, 24, 29)

`billing_webhook_receipts` has a unique index on `(provider, event_id)` — a duplicate delivery returns the original receipt, never a second one. `signature_valid = false` produces a `rejected` receipt with **no tenant resolved**, because a tenant named by an unverified callback is not believed. Only a SHA-256 of the payload is stored; never the payload, never a signature, never a secret.

The table carries an append-only trigger from the earlier security work. That trigger contradicted the receipt's own processing state machine — it forbade the one transition the design requires. Rather than weaken the guarantee, the trigger was replaced with one that admits exactly the legal forward step (`received → applied|ignored|rejected`), refuses any change to a terminal state, and makes the verdict columns (`provider`, `event_id`, `payload_hash`, `signature_valid`) **immutable**. Deletion remains forbidden outright: destroying the evidence that a provider event arrived is how a replay becomes undetectable.

Cross-tenant isolation is tested as the `mas_app` role (`NOBYPASSRLS`): tenant B sees **zero** rows across `subscriptions`, `subscription_events`, `usage_records`, `quota_overrides`, `entitlement_overrides`, `quota_counters`, `quota_reservations`, `quota_definitions` and `policies`, and cannot insert usage billed to tenant A. Without `MAS_TEST_DATABASE_APP_URL` those assertions **skip with a printed banner** rather than passing vacuously under a superuser.

### 4.8 API surface (Phase 32)

**Reads** (six endpoints, authenticated layer): subscription, entitlements, usage list, usage totals, receipts list, receipt by id. The tenant comes from the authenticated scope — never the path, never the body — so no caller can page through another tenant's consumption. Page size is clamped to 200 rather than rejected. In the in-memory development lane, which has no database, the billing endpoints return a deterministic error rather than an empty list that would read as "nothing consumed".

**Writes** (seven endpoints, `platform:admin` only) — added in the addendum:

| Method | Route | Guards |
|---|---|---|
| `POST` | `/v1/admin/plans` | `platform:admin`, plan code is `snake_case` |
| `PUT` | `/v1/admin/plans/{code}/features` | plan must exist; unknown feature refused |
| `POST` | `/v1/admin/tenants/{tenant_id}/subscription` | duplicate live subscription is a conflict |
| `POST` | `/v1/admin/tenants/{tenant_id}/subscription/transitions` | **reason required**, **state must be legal**, **`expected_version` guard** |
| `POST` | `/v1/admin/tenants/{tenant_id}/overrides/features` | reason + actor + effective period |
| `POST` | `/v1/admin/tenants/{tenant_id}/overrides/quotas` | reason, known dimension/period/enforcement, non-negative ceiling |
| `POST` | `/v1/admin/tenants/{tenant_id}/overrides/features/{feature}/end` | ends now; **never deletes** |

Every one of them is authorise → validate → mutate → **audit**, in that order, and the audit result is propagated: a mutation that cannot be recorded is reported rather than applied silently. Two decisions deserve their own note.

The `expected_version` guard is what makes two operators safe. Both read version 1; the first transition moves it to 2; the second is refused rather than overwriting a state it never saw.

Ending an override is never deletion. `end_feature_override` ends it now and the row stays in the history, because "this tenant once had this grant — who gave it and why" is precisely what an auditor asks for.

`platform:admin` alone does not reach another tenant's rows. An admin bound to tenant A that posts tenant B's subscription is refused by row-level security, and that refusal is reported as a deterministic `FORBIDDEN` — see §5.14.

### 4.9 The concurrency slot a run holds for its duration (Phases 8, 22, 23, 30)

Submission *consumes* one `Executions` unit — a count knowable at submission time. A run, by
contrast, holds a **slot** for an unknown duration, and that is what a reservation describes. So the
worker now reserves one `ConcurrentRuns` slot before it starts a run and returns it when the run
stops, whichever way it stops.

**Where the seam is.** `mas-worker` deliberately has no dependency on `mas-persistence` or
`mas-quota`. It declares `RunSlotPort` (`acquire` / `renew` / `release`) and says *when*; the
control plane says *whether*. `mas-worker-service` — the only crate that can see both the runtime
pool and the plane — supplies `ControlPlaneRunSlots`, which runs every call inside
`with_authoritative_scope`, so the reservation is written under the tenant's own row-level-security
policy and never by a role that can see across tenants. Without this gate, `max_in_flight` is the
only concurrency limit in the system, and it is a per-process constant: N workers would run N × 8
runs for a tenant whose plan allows four.

**The ordering, and why it is what it is.** The slot is taken **before** the durable claim. Taken
after, a refusal would leave a durable attempt behind for work that never started, and attempts are
finite: under sustained pressure a tenant's tasks would be dead-lettered for the crime of arriving
while the tenant was busy. Taken first, a refusal costs nothing.

**Ownership.** `acquire` returns one of three answers, and the third is the interesting one:

| Answer | Meaning | Caller's obligation |
|---|---|---|
| `Acquired` | this call took the slot | run the work; **release it on every exit path** |
| `HeldElsewhere` | another live attempt already holds it | defer; **do not release** — it is not yours |
| `NotMetered` | the tenant has no `ConcurrentRuns` ceiling | run the work; nothing to release |

A redelivery that arrives while the first attempt still holds its lease is ordinary broker
behaviour. Releasing the hold in that case would hand the tenant's headroom to the next caller
while the original run is still executing — the ceiling would stop being a ceiling. The consumer is
where that rule is enforced (`RunSlot::must_be_released()`), because the plane has no way to tell
"the owner" from "someone who guessed the key".

**The release is unconditional on the paths that own it.** `mediate` calls the inner
`mediate_with_slot` and then releases, so settled, failed, deferred and at-capacity exits all
return the slot. A slot released only on success leaks on every failure, and the tenant's ceiling
would ratchet down until it could run nothing at all. The lease is the backstop for a release that
itself fails; the heartbeat renews both the durable task lease and the slot lease on the same tick,
so a run that is still going never has its slot swept and a run that died loses it.

**Live-tested, not unit-tested alone.** `crates/worker-service/tests/run_slots_pg.rs` drives the
real adapter through the real port against PostgreSQL as `mas_app`, inside an authoritative scope —
the configuration the worker actually runs in. Fifteen tests: the ceiling holds, eight racing
workers with a ceiling of three get exactly three (and five refusals), an unmetered tenant is not
metered, a renewed slot survives the sweep that would otherwise take it, a crashed worker's slot
lapses with its lease, a backwards lease is refused, two workers asking for the same run share one
slot, a double release refunds once, a retry after a *finished* run takes a fresh slot and leaks
none, a retry after a crash reclaims the dead attempt's charge, tenant B is unaffected by tenant A's
held slot, another tenant's slot is invisible inside this tenant's scope, and a slot is never
granted outside the tenant's own scope.

### 4.10 A refused control-plane mutation is now audited

Every control-plane write recorded an audit event **on success only**. Two entire classes of
attempt — the ones that were refused, and the ones that were authorised but did not apply — left no
trace at all. The attempt an auditor most needs to see was the one thing never written down.

All seven admin handlers now route through two wrappers, and between them all three outcomes are
recorded:

| Outcome | Where it is recorded | Recorded as |
|---|---|---|
| Refused — the caller never held `platform:admin` | `authorize_admin` | `Denied` |
| Failed — authorised, but validation or the store refused it | `audited_mutation` | `Failure` |
| Applied | `audited_mutation` | `Success` |

The third row is the one that closed last, and it is not a small case. "An operator tried to set
this tenant's quota to −5 and was refused" and "nothing happened" read identically in a log that
only ever contains successes — and reconstructing who did what to a tenant's commercial state is
the entire purpose of the log. A transition refused for a stale `expected_version` belongs here
too: two operators, one stale read, and the loser's attempt is what lets an auditor see the
sequence rather than just the result.

`resource_id` names what was **attempted**, not what resulted, which is what makes the failure
record identify the thing that was not applied.

The three paths do **not** treat an audit failure the same way, and the asymmetry is deliberate:

- **Applied** — the audit failure propagates. A change that cannot be attributed is reported,
  because the change is already in the database and the caller has a right to know it is
  unattributed.
- **Refused or failed** — the audit failure is logged and swallowed. Nothing was applied, and
  replacing the caller's real error — the unknown enum value, the stale version, the missing
  reason — with "could not write an audit row" would hide the one piece of information the
  response exists to give.

Refusal attribution is best-effort: to the principal when there is one, and as `Anonymous` when
there is not. Two refusal classes are pinned down separately, in
`an_unauthenticated_attempt_never_reaches_a_control_plane_decision`: an authenticated but
under-scoped caller is a `403` **with** a `Denied` event, and a caller with no credential is a `401`
**without** one, because authentication fails in the middleware before any control-plane decision
exists to audit. Four live tests cover the matrix.

---

## 5. Real defects found and fixed along the way

These were not in a status report — they were found by running the code.

1. **`policies` already existed** (migration 0007) with a different shape. The control-plane store was writing columns that did not exist. Rather than create a second policy table, 0020 extends 0007's and maps 0007's `kind`/`spec` onto the domain's `policy_type`/`definition`. `active` is **derived** from `status`, not stored beside it — two columns for one concept is how a policy ends up active and disabled at once.
2. **`plans` had a boolean `active`** that could not express the lifecycle (active/retired/archived) and could not distinguish "withdrawn from sale" from "deleted". Rather than `DROP COLUMN` — which this codebase's own guard forbids, correctly — the legacy column is now maintained by a trigger as a derived view of `status`, so the two can never disagree and existing readers keep working.
3. **`DROP COLUMN` was attempted and rejected by the migration guard.** "A production migration must be additive." The guard was right; the design was changed to satisfy it.
4. **The append-only receipt trigger forbade the transition the design requires** (§4.7).
5. **A whitespace-only override reason passed validation** — a real gap between the documented intent ("a blank reason is refused") and the code. Both constructors now reject it.
6. **Stores invented their own clock.** `reserve` read `now()` internally, so a caller operating on a fixed timestamp got a different answer than production. `now` is now a port parameter throughout.
7. **Subscription history ordering was not total.** Two transitions could share a timestamp, leaving "which came first" to physical row order. `subscription_events` gained a monotonic `seq` column.
8. **A time-boxed override could never be revoked early** — `end_feature` required `effective_to IS NULL`. It now permits ending early and refuses ending *late*, which would silently extend a grant.
9. **`Settlement` lived in persistence** while the gate needed it in `mas-application` (which cannot depend on persistence). Moved to the domain, where both can see it.
10. **Disk exhaustion** twice stopped the build (19 GB in `target/`). Cleared; not a code defect, but it is why the toolchain had to be reinstalled once.

Found while closing the gaps this report had recorded as open — all four are production defects, not test fixtures:

11. **Every control-plane repository ran outside the request's transaction.** All tenant-owned control-plane tables are `ENABLE` *and* `FORCE ROW LEVEL SECURITY`, and every policy reads `app.current_tenant`, which `scoped_request` sets `SET LOCAL` inside the one transaction it opens per request. The control-plane stores took a connection straight from the pool instead, so under `mas_app` they ran with no GUC at all: **inserts were refused with SQLSTATE 42501**, and reads came back *empty* — and an empty usage list is indistinguishable from a tenant that consumed nothing. Fixed by `control_plane/executor.rs`: each statement now runs on the request's transaction when a scope is bound, and falls back to a pooled connection only when there is no request (where `FORCE RLS` still fails closed). `subscriptions::transition` was the one method with a transaction of its own; it now joins the request's transaction instead of opening a second one, and commits only what it opened itself.
12. **`mas_app` had no `USAGE` on any sequence.** Migration 0001 deliberately excluded sequences from `ALTER DEFAULT PRIVILEGES` ("ids come from pgcrypto") — correct for every table whose key is a uuid. Migration 0020 then added `subscription_events.seq bigserial`, so **every subscription transition failed with 42501** in production, including the one a billing webhook performs. Migration 0021 grants `USAGE` on all sequences in the schema, not just that one, so the next `bigserial` does not reintroduce it.
13. **One table, two write paths.** `create_subscription` wrote through the control plane's entitlement port while `transition_subscription` wrote through the subscription store. Both reach `subscriptions`, and in one test lane that port was the in-memory control plane — so the row landed where nothing else could see it. All admin handlers now use one store, and the cross-tenant write that this had been silently permitting is now refused (§5.11/§5.14).
14. **A row-level-security refusal surfaced as a 500.** `42501` mapped to `DATABASE_ERROR`, which tells the caller to retry — and a cross-tenant write fails identically on every retry. Worse, it reports a security boundary *working* as though the platform had broken. `map_database` now distinguishes the two failures that share this SQLSTATE: a message naming row-level security is a **403 `FORBIDDEN`**, deterministic and non-retryable; an ordinary missing grant stays a 500, because someone has to fix the grant.

Found while wiring the worker's concurrency slot, and while auditing the refusal path. All five are production defects, not test fixtures, and none of them was visible to a suite that only ever read the code or only ever connected as the owner:

15. **`ControlPlane::reserve` double-charged the counter on a retry.** The method was documented as idempotent on `reservation_key`, and for the *row* it was: `INSERT … ON CONFLICT DO NOTHING` returned the existing hold. But the charge happened **before** that lookup, so a retry incremented the counter and was then handed the hold it already had — a second charge with only one reservation to refund it. On a periodic dimension that silently eats the tenant's allowance; on `ConcurrentRuns`, whose counter is a gauge that only comes back through a release, the leak is permanent and invisible until the tenant cannot run anything. The idempotency check now runs **before** the charge, and the decision it returns is the gauge's standing read from the counter rather than a number invented to fill the struct.

16. **A finished reservation was resurrected by the next run with the same key.** Reservation rows are never deleted — they are settled or released and kept as the record of the charge. `ON CONFLICT DO NOTHING` meant a retry of the same task hit the old terminal row, was handed it, and charged the counter for it; the settlement that followed released nothing, because the row was already terminal. Every retry of a task leaked one unit. The clause is now `ON CONFLICT … DO UPDATE … WHERE quota_reservations.state <> 'held'`: a live hold is still returned unchanged (that is the idempotency the clause exists for), and a terminal row is refreshed in place. The in-memory store applies the same rule, so the dev path cannot drift from production.

17. **A backwards lease extension was silently swallowed.** `extend_lease` refuses to move a lease backwards by construction — its `WHERE` clause is `state = 'held' AND lease_expires_at < until` — so a renewal that shortened the lease matched no row and returned `None`. The caller could not distinguish "refused" from "nothing there", and a heartbeat that computed an earlier deadline would have quietly failed to extend. `extend_run_slot` now reads the hold first and reports a validation error, because a lease is what a concurrent sweep reads to declare a run dead: shortening it would let the sweeper reclaim a slot from under a run that is still executing.

18. **`mas_worker` had no `INSERT` on `usage_records`.** Every successful run settles through `write_usage`, which inserts usage rows as the worker role; migration 0017 never granted it. So **every successful run rolled back and retried**, and the failure was invisible to every existing suite for a stark reason: a failed run carries no usage, so the failure path settled fine. Migration 0022 grants the privilege, and `crates/persistence/tests/plane_grants.rs` now asserts the plane-role contract from both sides — each role must have what it needs *and must not have* what it does not, because a grant widened for convenience is a grant a compromised plane can use.

19. **A live test that only failed once the shared test database had aged.** `schedules_due_scan_and_run_journal` asserted that a schedule forced into the past appears in `list_due(&now, 25)`. That method pages by `next_run_at ASC`, and the shared test database keeps the due rows every earlier run of the suite left behind — so after 25 runs the fixed page was full of *other* runs' schedules and the row the test had just made fell off the end. Not a defect in `list_due`, and not caused by this phase's changes; it is what an assertion that depends on how much debris is lying around looks like when the debris finally accumulates. The page is now sized from a count of what is actually due, and the reason is recorded in the test so nobody "simplifies" it back.

---

## 6. Completion gate

| Requirement | Status |
|---|---|
| One canonical policy service | ✅ `mas-policy::PolicyEngine`, one store back-end |
| Policy durable | ✅ migration 0007 + 0020, versioned, published, audited |
| Decisions auditable | ✅ decision carries policy id/version/reason/scope/correlation/timestamp |
| Production quota distributed | ✅ Postgres counters, atomic `try_reserve` |
| No in-memory quota authoritative | ✅ in-memory stores are test/dev only, documented as such, unreachable from production config |
| Reservation concurrency-safe | ✅ 38 live tests incl. 20× repeat; plus 8-way race on the run slot |
| Release/recovery works | ✅ lease expiry reclaim, live-tested; **and wired** — the worker now takes, renews and returns a slot, released on every exit path |
| `quota_exceeded` deterministic | ✅ distinct variant, 429, non-retryable, tenant-scoped |
| One canonical usage ledger | ✅ `usage_records` via one write path |
| Duplicate work does not double-meter | ✅ unique indexes; live-tested at 16× concurrency; a retried reservation no longer double-charges (§5.15/§5.16) |
| Failed/cancelled semantics explicit | ✅ zero quantity refused; failed-before-provider meters nothing; a provider failure meters what was actually reported |
| Plans + subscriptions persisted | ✅ deterministic transitions, versioned, every step recorded |
| Entitlement resolution canonical | ✅ server-side chain, no caller claims |
| Overrides safe | ✅ reason + actor + period required; plan never edited |
| Feature gates backend-enforced | ✅ 8 spec-named features added to `FeatureKey` |
| Billing ownership boundary clear | ✅ platform ledger authoritative; provider never source of truth |
| Webhook security enforced | ✅ signature verify, replay prevention, idempotent, receipted, no payload/secret stored |
| Usage flows through durable eventing | ✅ outbox written after the durable record |
| All changes auditable | ✅ every mutation records `Applied`, `Failed` **or** `Denied` — all three outcomes, not just the successes; receipt verdicts immutable; control-plane writes run inside the request transaction |
| Cross-tenant isolation passes | ✅ 9 tables checked as `mas_app`; 6 "Tenant A must not" cases; an admin write refused by RLS as `FORBIDDEN`; another tenant's run slot invisible under this tenant's scope |
| Concurrency tests pass | ✅ 38 live, 20× repeat, 0 failures; plus 15 live run-slot tests incl. an 8-way race |
| Admin write surface exists | ✅ 7 endpoints, `platform:admin` only, audited on all three outcomes, 18 live tests |
| All existing tests green | ✅ 956/956, no test weakened |
| The worker enforces the ceiling it is given | ✅ slot taken before the claim, renewed by the heartbeat, released on every exit; unmetered tenants are never refused |

---

## 7. Known limitations, stated plainly

- **The Python orchestrator was not modified this phase** — no Python file was touched. Re-verified at the end of the second addendum: **1,936 passed, 0 failed** (`python3 -m pytest tests -q`, after `pip install pytest-asyncio grpcio grpcio-tools`). The two failures previously recorded here (a spawned subprocess that could not import `mas_orchestrator` after a snapshot restore) do not reproduce once those packages are installed in the environment.
- *(Closed in the addendum.)* **The admin write surface is now built** — seven endpoints, `platform:admin` only, each authorise → validate → mutate → audit, with 15 live tests covering refusal without the scope, refusal without a reason, refusal on a stale version, refusal across tenants, and the audit trail itself (§4.8).
- *(Closed in the second addendum.)* **A control-plane mutation does not share a transaction with the audit event that records it — and that is deliberate, not a gap.** This report previously listed it as open; inspecting the audit plane's wiring changed the verdict. Audit events are written on their own pool as `mas_audit`, a role with `INSERT` on `audit_events` and nothing else, for two load-bearing reasons: a denial must be recordable for a tenant the caller was never scoped to (which is exactly what row-level security forbids on the runtime role), and **the record of an attempt must survive the failure of the attempt**. Merging the two transactions would destroy the second property precisely when it matters, because a rolled-back transaction takes its audit row with it. The separation is now asserted from both sides in `an_audit_event_survives_the_rollback_of_the_mutation_it_describes`: the mutation really does roll back, and the audit event describing it really is still there.
- *(Closed in the second addendum.)* **The worker now reserves and releases a concurrency slot** for the duration of a run (§4.9). What remained open underneath it was not a missing caller but three real defects in the reservation path, found by wiring it (§5.15–§5.17).
- *(Closed at the end of the second addendum.)* **A mutation that fails after the caller was authorised is now audited** as `Failure` (§4.10). Every one of the seven handlers wraps its validate-and-mutate body in `audited_mutation`, so an unknown enum value, a negative ceiling, a malformed date, a missing reason, a stale `expected_version` and a write refused by row-level security all leave a line in the audit log naming what was attempted. Nothing is left in this phase that is known and not done.
- **Submission consumes rather than reserves.** This is deliberate. A reservation is for a dimension whose final amount is unknown when work starts (tokens, or a concurrency slot held for the duration). An execution is a count knowable at submission: exactly one. Reserving instead would hold the unit until the lease lapsed, so a tenant on a daily ceiling of 100 could only submit a handful per day — the counter would measure the lease rather than the work.
- **No payment secrets exist anywhere in the schema or logs.** Receipts store a SHA-256 and a boolean.

---

## 8. Stop condition

Per the instruction: **stop after Policy + Distributed Quota + Usage + Entitlement/Billing.** No React, dashboard, marketing or cosmetic work was performed.

Nothing forbidden was created: no second policy engine, no second quota engine, no second usage ledger, no second subscription authority, no direct Python writes to Rust-owned state, no in-memory production quota, no synthetic usage, no unauthenticated webhook processing.

All seven gaps this report has recorded as open are now closed: the admin write surface exists, the control plane runs inside the request's scope, the runtime role has the grant it was missing, a row-level-security refusal is answered with `FORBIDDEN` rather than `DATABASE_ERROR`, the worker holds and returns a concurrency slot, and every control-plane mutation is audited on all three of its possible outcomes.

Two of those seven deserve a note, because in both cases the work changed the report rather than only the code. "The mutation and its audit event do not share one transaction" was not a defect — reading the audit plane's wiring showed the separation is what makes an audit record survive the rollback of the change it describes; the real gap underneath it, that refusals were not recorded at all, was what got closed. And "the worker does not reserve a concurrency slot" turned out to be sitting on three genuine defects in the reservation path, found only by wiring it (§5.15–§5.17).

**Nothing in this phase is now known and not done.** §7 lists no open limitation.

**Awaiting explicit approval before the next phase.**
