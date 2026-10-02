# Line-by-line adjudication of `audit.md` (2026-09-30)

**Checked against:** workspace `rust-core` as of 2026-10-01 (after the
pg-adapters and nats-wiring phases; gates green, `cargo test --workspace
--locked` = 429 passed / 0 failed).
**Method:** every audit section and every §0 blocker verified by source
inspection (`grep`/`sed` evidence, `file:line` citations). Nothing below is
unverified opinion. Verdict vocabulary:
**CONFIRMED** (audit claim is true) · **PARTIAL** (true in part / nuance) ·
**NOT-CONFIRMED** (audit claim is wrong or stale) · **REQ: OPEN / PARTIAL /
DONE** for the audit's requirement items (state of the required fix today).

---

## §0 — 14 named blockers (all verified individually)

| # | Audit blocker | Verdict | Evidence |
|---|---------------|---------|----------|
| 1 | API principal loses tenant/org/scope | **CONFIRMED** | `crates/api/src/auth.rs:22-27` — `Principal { subject, kind }` only. `crates/api/src/prod_auth.rs:1-12` doc self-describes producing `Principal{ ApiKey, subject = key id }`. `crates/api/src/context.rs:15-19,66` — scope comes from `x-tenant-id`/`x-organization-id` headers, gated only on *presence*, never compared to the credential. Nothing loads tenant/org/scopes from the key row. |
| 2 | Submission persists only, never enters the pipeline | **CONFIRMED** | `crates/application/src/execution_service.rs:122-141` — `executions.save` → `idempotency_put` → `audit::record_mutation`; zero references to Task/outbox/engine in the whole file. |
| 3 | Production binds `EchoHandler` | **CONFIRMED** | `crates/worker-service/src/main.rs` — `EchoHandler` (still present); bound in `run_production` when `MAS_WORKER_HANDLERS` contains `execution.run`. E2B: also used in `--dev-inmemory`, acceptable there. |
| 4 | Two execution lifecycles | **CONFIRMED** | `crates/orchestration/src/engine.rs` (ports at :234-304, engine tests use Noop in-memory) vs `crates/application/src/execution_service.rs` (independent state machine calls). API composition uses only the latter. |
| 5 | Only in-memory AgentBridge | **CONFIRMED** | `crates/execution/src/agent_bridge.rs:156,237` — sole implementation is `InMemoryAgentBridge`. No `grpc_agent_bridge.rs`. |
| 6 | Engine ports lack production adapters | **CONFIRMED** | Search for `TaskStorePort|ExecutionStorePort|WorkflowCatalogPort|PolicyPort|QuotaPort|EventPublisher|IdempotencyStore|DeadLetterStore` across `crates/persistence/src/` — only hit is the *application-level* `ExecutionStorePort` impl in `repositories/services.rs:553` (imported from `mas_application::stores`). Engine-level ports have no PG/other durable impls. No `repositories/orchestration.rs`, `idempotency.rs`, `dead_letter.rs`, `workflow_catalog.rs`. |
| 7 | Mutations not atomic | **CONFIRMED** | `execution_service.rs:122-141` — three independent awaited writes (`save` / `idempotency_put` / audit). `UnitOfWork`/`atomic` exist (`persistence/src/transaction.rs:20,95`) and outbox has `enqueue_in`, but the submission path uses none of it. |
| 8 | RLS not in normal request path | **CONFIRMED** | `set_config` machinery exists solely in `persistence/src/rls.rs:44-122`. Grep over `crates/api/src`, `crates/application/src`, `crates/worker-service/src`, `crates/scheduler-service/src` for `RlsContext|set_config|transaction_scoped` → **zero hits in all four**. No `scoped_db.rs`. With 33 `FORCE ROW LEVEL SECURITY` statements in the migrations, prod daemons today get rows only if their DB role bypasses RLS (unsafe) — or see zero rows (fail-closed). Either posture is not the audited intent. Repositories *do* carry `&RlsContext` scoped reads (`repositories/mod.rs:6` doc), but no caller applies the GUCs. |
| 9 | Persistence coverage only part of schema | **CONFIRMED** | 33 tables across `migrations/0001–0012`; `rows.rs` covers 13 aggregates (`grep pub struct .*Row` = Agent, AgentVersion, Audit, Execution, Membership, Organization, Outbox, Project, Schedule, Task, Tenant, User, Workflow). `repositories/` holds: agents, api_keys, audit, broker, executions, outbox, schedules, scoped, services, tasks, tenancy, workflows. Missing for 19 schema entities: session, tool, tool_permission, workflow_nodes, workflow_edges, task_attempt, policy, policy_decision, subscription, entitlement, quota, usage_record, schedule_run, connector, credential, webhook, webhook_delivery, notification, execution_steps. (Audit also listed `ApiKey` — **over-count**: `repositories/api_keys.rs` exists and backs `PostgresApiKeyVerifier`.) |
| 10 | In-memory leases in prod paths | **CONFIRMED** | `crates/scheduler-service/src/main.rs:21,45,197` — `InMemoryLeaseStore::new()` in dev AND production composition. `crates/worker-service/src/main.rs` — `Arc::new(InMemoryLeaseStore::new())` bound into production `TaskConsumer`. |
| 11 | Quota/concurrency not distributed | **CONFIRMED** | `crates/quota` has an in-memory permit ledger (`README.md:71` describes it); no PG/Redis backend; engine `QuotaPort` (`engine.rs:284`) has no production impl; `grep redis` across **all** Cargo.tomls → no dependency. |
| 12 | Scheduler completion identity + auth | **CONFIRMED** (and worse) | `crates/scheduler-service/src/durable.rs:165-178` — `mark_finished(schedule)` UPDATEs **all** `outcome='dispatched'` unfinished runs of the schedule_id (exactly the forbidden semantics). `server.rs:43-58` — `post_completion` receives `execution_id` but calls `ledger.finish(schedule_id)`; no auth middleware anywhere in `server.rs`/`main.rs` (grep = no `auth`, no `Authorization`). Plus: production `main.rs:196-218` never binds the completion listener at all — dispatched runs can never be finished in prod. |
| 13 | Synthetic delivery count | **CONFIRMED** | `crates/persistence/src/repositories/broker.rs:181,394` — `deliveries: 1` hard-coded. `task_attempts` table exists (0006:91-114) with `UNIQUE(task_id, attempt)` + lease indexes, but `INSERT INTO task_attempts` appears **nowhere** in the tree (grep = 0). |
| 14 | No wired outbox relay | **CONFIRMED** | `events::outbox::OutboxDispatcher` exists as the audited foundation; `PostgresOutboxStore` fetch uses SKIP LOCKED. Grep for dispatcher wiring across all three binaries + `api/src/state.rs` → zero. No `crates/outbox-relay/`. |

**§0 verdict: 14/14 blockers CONFIRMED against source. Zero of them are stale
or wrong.** The nats-wiring phase (this week) added a real JetStream lane but
did not touch any blocker.

---

## §1 — Audit method & limitations

- **NOT-CONFIRMED (corrective note):** the audit self-declares no toolchain
  runs. In *this* workspace the toolchain exists: latest gate run
  = fmt ✓ / clippy `-D warnings` ✓ / 429 tests ✓ / rustdoc `-D warnings` ✓
  (both default and `--features nats` spaces). So the gates half of the audit
  caveat is already discharged *for the code as it stands*; the remediation
  code must re-pass them.
- **CONFIRMED:** 21 workspace crates (`ls crates` = 21); migrations 0001–0012.
- **CONFIRMED + worse:** README contains stale hard-coded claims: "372 tests
  passing" (line ~71), "378 lib/unit/integration tests" (line ~156), "All 20
  workspace crates" (actual 21, actual tests 429).

## §2 Severity model — accepted as the release policy (P0/P1/P2). No code issue.

## §3 Canonical architecture — **REQ: OPEN.** The canonical path shown breaks at
three real joints today: (a) API→engine (no engine call), (b) execution→task
(no task row), (c) runtime→Python (no gRPC bridge). Everything else
(auth/tenancy/policy/quota/broker/worker crates) exists as *parts*, not as the
chain.

## §4 Root correction — **CONFIRMED** (two paths exist) · **REQ: OPEN.**
`execution_service.rs` not yet a facade over `OrchestrationEngine`; required
file list (application/{context,stores,ports}, orchestration/{engine,ports},
execution/executor) unchanged in this respect. Acceptance criteria (atomic
execution+task, replay-safe, no permanently-pending execution) all **unmet**.

## §5 P0 auth/principal — **CONFIRMED in full** (see blocker 1).
Extra precision the audit missed:
- `security::principal` (extended principal with tenant/org/roles/scopes)
  *does exist* as a richer type — audit's suggested starting point is correct.
- The app layer's only scope defense today is store-level "foreign id →
  NotFound/404" filtering (per README + execution_service filters), which
  keys off caller headers — i.e., header authority, the exact anti-pattern
  named in §51's prohibition list. **REQ: OPEN.**

## §6 P0 RLS — **CONFIRMED** (see blocker 8). All sub-requirements
(`scoped_db.rs`, tx wrapper, explicit privileged paths, role requirements,
fail-closed tests) **OPEN**. Positive note for planning: `RlsContext` is
already fail-closed, parameter-bound, no string-concat SQL, and
`live_pg` proves the GUC mechanics against real PG.

## §7 P0 engine ports — **CONFIRMED · REQ: OPEN** (see blocker 6). None of the
named ports have durable impls; `NoopPorts` appear in engine tests (correct
scope for Noop: tests only).

## §8 P0 real worker handler — **CONFIRMED · REQ: OPEN** (see blocker 3).
`worker-service/src/execution_handler.rs` missing. Precondition chain noted
for ordering: handler needs durable execution state (§7) and a bridge (§9).

## §9 P0 Python bridge — **CONFIRMED · REQ: OPEN** (see blocker 5). No gRPC
adapter, no s2s auth, no protobuf coupling to Python.

## §10 P0 canonical protobuf — **CONFIRMED · REQ: OPEN.** `find -name '*.proto'`
= exactly one file: `crates/api/proto/mas/api/v1/mas.proto` (public API
proto). No `contracts/proto/` tree; none of the 8 required runtime protos
exist.

## §11 P0 atomic submission — **CONFIRMED · REQ: OPEN** (see blocker 2/7).
Repository `*_in` transaction variants exist only for outbox + a few tenant
stores; execution/task/audit submission variants missing.

## §12 P0 dispatch task creation — **CONFIRMED · REQ: OPEN** (see blocker 2).
The required payload-field list is a good target spec; none of it exists —
`TaskQueueMessage` in contracts currently has: ids, op, input, priorities,
attempt ceilings, deadline, correlation — cross-check needed when designing
the engine command (`operation`/`execution_id`/`project`/`tenant`/`org`/
`correlation` present; `not_before`, `idempotency key`, `resource limits`
per audit extend it).

## §13 P0 durable attempts — **CONFIRMED in detail** (see blocker 13).
Additional finding: `TaskStore.save` (`repositories/tasks.rs:25-…`) is an
unconditional UPSERT `ON CONFLICT (id) DO UPDATE` with no version/status
guard — stale lifecycle writes clobber current state even within one worker.
Broker-side claims+nak/term are status-guarded (`AND status='running'`), so
downgrade protection is half-present, half-missing. **REQ: OPEN.**

## §14 P0 `max_in_flight` — **CONFIRMED** —
`crates/worker/src/consumer.rs:161-176`: fetch is issued with
`self.config.batch_size` (not the remaining capacity
`max_in_flight - in_flight.len()`); the cap is enforced only *after* the
broker already claimed rows (inside the per-message loop at :176). Leases
are therefore acquired beyond capacity, and a shutdown with an oversized
batch abandons claimed-but-not-run work. **REQ: OPEN** (simple fix:
`.fetch(consumer, (max_in_flight - in_flight.len()).min(batch_size))`).

## §15 P0 distributed concurrency — **CONFIRMED · REQ: OPEN** (see blocker 11).

## §16 P0 distributed quota — **CONFIRMED · REQ: OPEN** — no redis dep, no
durable counter backend; `EngineQuotaBridge` exists (README) but has no
production binding.

## §17 P0 outbox relay — **CONFIRMED · REQ: OPEN** (see blocker 14).

## §18 P1 persistence coverage — **CONFIRMED with one over-count** (see
blocker 9): 19 missing entities exactly as the audit lists them in the
"Required" block; `ApiKey` is already covered (api_keys.rs). **REQ: OPEN.**

## §19 P1 application ports — **CONFIRMED** — `stores.rs` defines 10 ports
(Organization/Tenant/Project/User/Membership/Agent/AgentVersion/Workflow/
Execution/Schedule). None of the audit's 19 additional ports exist.
**REQ: OPEN.**

## §20 P1 cross-scope integrity — **CONFIRMED** — e.g. `tasks`
(0006:60-…): `tenant_id REFERENCES tenants (id)`, `organization_id REFERENCES
organizations (id)` — two independent FKs, no composite invariant that
`tenant.organization_id == row.organization_id`. Same pattern schema-wide.
**REQ: OPEN** (migration 0013 is the first free number — audit's suggestion
matches the ledger).

## §21 P1 CAS transitions — **PARTIAL-CONFIRMED** — broker SETTLEs are
conditional on `status='running'` (NAK/TERM/extend-lease SQL in broker.rs);
but `tasks` has **no `version` column** (no `version` in 0006), no fencing
token, and lifecycle `save` is guard-free (see §13). **REQ: OPEN.**

## §22 P1 scheduler correctness — **ALL FOUR SUB-ITEMS CONFIRMED** (see
blocker 12): in-memory lease in prod (22.1), schedule-scoped bulk finish
(22.2), unauthenticated endpoint (22.3 — plus prod doesn't bind it at all),
and `schedule_runs` unique key exists but the dispatcher's duplicate-fire
guard rides it correctly (idempotency key `sched:<id>:<ms>` + unique journal)
which is the one **DONE** sub-aspect (audit 22.4's "surface the conflict as
safe idempotent replay" is already implemented in `durable.rs`).

## §23 P1 production tool runtime — **CONFIRMED · REQ: OPEN** — `ToolRunner`
exists in execution with catalog/permissions concepts; integrations has
manifests + connector registry; no migrations-backed tool store/service and
no runtime composition.

## §24 P1 SSRF — **PARTIAL-CONFIRMED** — `integrations/http_guard.rs`
(EgressPolicy allowlists, DNS pinning, redirect re-validation, caps) and a
`GuardedHttpClient` over `HttpClientPort` exist; **no production caller binds
it** (and a *test* wiring in `node_adapters.rs:747` uses
`EgressPolicy::Unrestricted`). Audit's core sentence — "its existence is
insufficient" — holds. **REQ: OPEN.**

## §25 P1 policy wiring — **CONFIRMED · REQ: OPEN** — policy crate complete;
no `PolicyRepository`/routes/pg store; engine `PolicyPort` only Noop impls.
No `AllowAllPolicy` appears in any production composition today (only
in-memory/test), so that part of the prohibition list is not yet violated —
but only because the engine is not composed at all.

## §26 P1 audit hash chain — **PARTIAL** — append-only triggers exist
(0011:39-41 `audit_events_no_update/no_delete`) — audit acknowledges this. No
hash chain (`grep previous_hash` over migrations+persistence = 0). No
"verifier tool". README does not claim "tamper-proof" (claims typed crypto
errors for encryption only). **REQ: OPEN,** chain unimplemented, as pinpointed.

## §27 P1 webhooks/notifications — **PARTIAL-CONFIRMED** — integrations crate
already has the full webhook *machinery* (fail-closed inbound HMAC,
two-sided replay window, store-and-forward outbound dispatcher with backoff,
Retry-After honoring, circuit breaker, DEAD-letter exhaustion) over
`InMemoryWebhookStore`/`InMemorySecretResolver`. Missing: PG persistence,
WebhookService/NotificationService, routes. Audit's behavioral list is ~80%
implemented as library primitives, 0% wired. **REQ: PARTIAL.**

## §28 P1 API surface — **CONFIRMED** — current router (api/src/router.rs:20-51)
has tenancy/agents/workflows/executions(submit+transitions)/schedules/health.
Of the audit's 12 required route groups only
`/v1/executions/{id}/cancel` exists. All others absent. **REQ: OPEN.**

## §29 P1 human identity foundation — **PARTIAL** — `User`, `Membership`,
`Session` domain types exist (domain/src/session.rs); `sessions` table exists
(0003:81); no OIDC/JWKS/SCIM anywhere (none required yet); no sessions row/
repo/port. The audit only asks that no *insecure custom* auth be invented —
vacuously satisfied (nothing invented). **REQ: PARTIAL-OPEN.**

## §30 P1 scheduler canonical submission — **PARTIAL-CONFIRMED (audit is
half-stale here).** Production `DurableDispatcher` (`durable.rs:126-141`)
*does* call the canonical application `ExecutionService::submit` — so the
"separate simplified path" complaint is inaccurate for the prod path. The
real situation is worse in a different way: the canonical service it calls is
itself inert (blocker 2). **Verdict: requirement resolves automatically once §4
lands; no separate scheduler refactor needed. Flagged as an audit reading
error worth correcting in the next audit revision.**

## §31 P1 authoritative usage — **CONFIRMED · REQ: OPEN** — table exists,
nothing writes it.

## §32 P1 entitlement runtime — **PARTIAL** — tenancy crate has the plan
matrix + contract overrides + entitlement checks (README evidence); but no
entitlements rows/repo (0008 table exists; gaps per blocker 9) and no
runtime chain wiring feature-check→quota→execution. **REQ: PARTIAL-OPEN.**

## §33 P1 health/readiness — **PARTIAL** — api has `/v1/health/live` +
`/v1/health/ready` with dependency aggregation (README); worker-service has a
boot-time broker `ping` (nats lane) + PG connect; scheduler-service has
`healthz` only; outbox/python/redis deps can't appear in readiness since
they don't exist yet. **REQ: PARTIAL.**

## §34 P1 observability — **PARTIAL-CONFIRMED** — registry/logging/tracing/
redaction foundation complete (README evidence), but the audit's named metric
matrix does not exist: `grep task_claim_latency|outbox_pending|
api_auth_failures` = 0 hits. **REQ: OPEN.**

## §35 P1 failure model — **PARTIAL** — `AppError` taxonomy already contains
validation/unauthorized/forbidden/not_found/**conflict**/**rate_limited**/
**timeout**/serialization/external_service/internal etc. (`error.rs:155,159,
163,179`); retryability lives in the worker's settlement classifier (Retryable
vs Exhausted mapping exists). Missing: dedicated `quota_exceeded`,
`policy_denied` codes, and a *declared* retryable/terminal classification per
variant as an API. **REQ: PARTIAL.**

## §36 P1 durable checkpoint — **CONFIRMED with a path error** — checkpoints
live in `crates/orchestration/src/checkpoint.rs` (the audit's
`crates/execution/src/checkpoint` does not exist). The module is pure data +
restore semantics; durable storage depends on an engine
`ExecutionStorePort` impl, which does not exist. **REQ: OPEN.**

## §37 P1 approval lifecycle — **PARTIAL-CONFIRMED** — domain node type
`Approval` (`workflow_node.rs:30,82,205`) and step state `WaitingApproval`
(`execution_step.rs:189`) exist. No approval table/repo/endpoint; the
`exec/resume` route does not consult approvals. **REQ: OPEN.**

## §38 P1 idempotency fingerprint — **CONFIRMED** — keys are stored inline in
`executions.state._mas_spec.idempotency_key` with lookup by
`(tenant, key)` (`repositories/executions.rs` header comment); no request
fingerprint, no expiry, no conflict-on-payload-change semantics.
**REQ: OPEN.**

## §39 P1 internal endpoint security — **CONFIRMED** — the only internal
endpoint (scheduler completions) is unauthenticated (§22.3). Worker has none.
**REQ: OPEN.**

## §40 P1 config/secrets — **PARTIAL** — layered config + startup validation
of mandatory deps exist (production binaries refuse empty DATABASE_URL,
guard against dev tokens in api, refuse broker.kind=nats without URL);
`config/production.toml` holds no secrets (env-refs only). Audit's
requirements are largely the working conventions already; the missing bit is
secret-manager references (integricons SecretReference concept exists in
security). **REQ: PARTIAL-DONE.**

## §41 P1 repo concurrency semantics — **PARTIAL** — primitives exist:
`UnitOfWork` + `atomic()` + SAVEPOINTs (transaction.rs), SKIP LOCKED claims
(outbox + task broker), status-guarded settles. Not declared per-repository,
and missing on TaskStore/execution paths. **REQ: PARTIAL.**

## §42 P2 toolchain alignment — **CONFIRMED** — `[workspace.package]
rust-version = "1.85.0"` vs `rust-toolchain.toml channel = "1.98.1"`. The
policy (MSRV vs pin) is undocumented. **REQ: OPEN** (decide + document +
CI-validate).

## §43 P2 doc sync — **CONFIRMED** — see §1 verdict: README claim counts stale
(372/378/20-crates vs actual 429/21). `docs/production-wiring.md` and
`docs/nats-wiring.md` are current as of today; README is not. **REQ: OPEN.**

## §44 P2 artifact hygiene — **CONFIRMED** — `.cargo-env` contains absolute
`/home/user/.toolchains/...` paths and has **no** `.gitignore` entry (grep
empty), i.e., it is inside the repo artifact with host paths. Workspace `.git`
is absent in this sandbox, so "committed" can't be shown, but the file is on
disk inside the project tree. Also present: `.devcontainer/`
(referenced in runbook) — fine. **REQ: OPEN** (make paths repo-relative or
gitignore the file).

## §45 Required production composition — **REQ: OPEN** — today's prod
composition is PG+worker+scheduler only; of the listed 16 components,
Postgres, Task broker (pg/nats), Worker, Scheduler, Authorization (partial),
Health registry (partial) exist; the rest uncomposed.

## §46 Required new files — **0/16 exist.** Verified individually (existence
check against each path): `application/src/authorization.rs`,
`persistence/src/scoped_db.rs`, `repositories/{idempotency,policies,quota,
tools,connectors,webhooks,subscriptions,sessions}.rs`,
`execution/src/grpc_agent_bridge.rs`,
`worker-service/src/execution_handler.rs`, `crates/outbox-relay/*`,
`scheduling/src/postgres_lease.rs`, `worker/src/task_fencing.rs`,
`contracts/proto/*_runtime.proto` — all MISSING (§46's list is accurate).

## §47 Required migrations — **REQ: OPEN** — next free number is exactly 0013
as the audit guessed; ledger ends at 0012_outbox. None of 0013–0020 exist
(the table list above shows none of their targets: no approval_records, no
audit hash columns, task_attempts exists but no fencing columns, etc.).

## §48 Required e2e tests — **REQ: OPEN with one caveat** — none of the 12
multi-replica/security/chaos suites exist. What exists today:
live PG suite (10 tests: pool/migrations/repos/audit/outbox/RLS-GUC
mechanics, env-gated skip-clean) and live NATS suite (5 tests, skip-clean —
never run against a server in this sandbox). §48.10's "encryption tamper"
test IS covered as unit tests in security (typed error on tamper) — the only
sub-item plausibly already satisfied at unit scope.

## §49 Validation commands — **STATUS** — gates green today at 429 tests
(including clippy all-targets via `scripts/check.sh`; equal-but-broader than
the audit's list). Live suite note for the audit record: PG/NATS/Redis/Python
envs — PG suite executed in-session historically; **NATS suite never ran
against a server in this sandbox** (skip-clean only, by design); Redis/Python
don't exist.

## §50 CI gates — **CONFIRMED as gap** — no CI configuration exists in the
repo (no `.github/`, no CI files). All listed gates currently run only via
`scripts/check.sh` locally. **REQ: OPEN.**

## §51 Production prohibition list — current status per item:

| Prohibited in production | Still present? |
|---|---|
| `EchoHandler` | **YES** (worker prod path, env-gated binding) |
| `AllowAllPolicy` | No (only tests; engine uncomposed) |
| `InMemoryLeaseStore` cross-replica | **YES** (scheduler + worker prod paths) |
| `InMemoryAgentBridge` | **YES** (only impl exists) |
| In-memory quota counters | YES-in-parts (engine port unbound; crate impl in-memory only) |
| Noop event publisher | No released (only tests) — engine not wired |
| Noop policy impl | No (only tests) |
| caller-trusted tenant headers | **YES** (context.rs scope = headers, principal has no tenant) |
| unscoped DB tx for tenant data | **YES** (no GUC application in prod binaries) |
| plaintext secret persistence | No evidence found (no secret columns written; config clean) |
| unauthenticated scheduler completion | **YES** (dev route; prod route not even bound) |
| stale worker overwrite | **YES** (guard-free TaskStore upsert + synthetic deliveries) |
| synthetic delivery count | **YES** (broker.rs:181/394) |

**8 of 13 prohibitions are on production paths today.**

## §52 Definition of Done — honest current status: **Security** 2/7 (encryption
tamper, no-secret-logging posture only). **Execution** 0/8. **Distributed
runtime** 0/7 (lease fencing for *schedule* object exists in-d:c process,
but not durable). **Data** 1/4 (append-only audit triggers). **Eventing**
1/4 (idempotent consumers exist in worker). **Scheduler** 2/4 (unique-journal
duplicate-fire guard + canonical-application submit — but (3)/(4) open).
**Contracts** 1/4 (correlation metadata propagation helpers). **API** 1/4
(stable error contract). **Quality** 6/7 (all local gates but live suite).

## §53 Implementation order — **ADOPTED as the project roadmap** (see
roadmap section below). R1→R10 dependency order is sound; two ordering
suggestions from verification data:
- pull **R0.5: doc/gate hygiene (§42-§44)** forward — hours of work,
  cleans the baseline before R1;
- inside R2, move `task_attempts` + fencing (§13/§21) next to execution
  atomicity (they share the migration + repository work), i.e., merge into
  R3's start rather than waiting for R4.

## §54 Coding-agent rules — **Standing constraints already honored by this
project:** (Rule 1 no elision = user's standing instruction; Rule 3 no prod
stubs = enforced by this phase policy; Rule 8 tests-with-fixes = practiced;
Rule 10 final-report format = adopted verbatim for each phase report below.)

## §55 Release gate / §56 bottom line — accepted as the target definition;
current gate position: **Security Gate open** (first gate), everything
downstream blocked per its chain.

---

# Roadmap decision

Adopted: work the audit in its own R1→R10 order, starting **Phase R1 —
security identity/context**: trusted principal (`security::principal`
extension → API verification), tenant/org credential binding with header
cross-checks, service-level authorization context, and `scoped_db` request
integration (§5 + §6, including the forced-RLS/daemon-role answer). Ledger +
per-item acceptance checks go into `docs/audit-remediation.md` alongside this
adjudication.

# Evidence appendix (command→result used above)

- Crate count/list, migrations list, README claims: `ls crates`, `ls migrations`, `grep` README.
- Principal/context shape: `api/src/auth.rs:22-27`, `api/src/prod_auth.rs`,
  `api/src/context.rs:15-66`.
- Submission body: `application/src/execution_service.rs:61-141`.
- Engine ports + Noop impls: `orchestration/src/engine.rs:234-304,1330-1364`.
- Persistence coverage: `grep pub struct .*Row rows.rs`,
  `ls repositories`, `grep CREATE TABLE migrations`.
- Synthetic deliveries/task_attempts/TaskStore upsert:
  `broker.rs:181,394`, global `INSERT INTO task_attempts` (0 hits),
  `tasks.rs:25-30`.
- Leases in prod: `scheduler-service/main.rs:197`, `worker-service/main.rs`.
- Scheduler completion: `durable.rs:165-178`, `server.rs:43-58`, prod main
  :196-218 (no listener).
- max_in_flight fetch: `worker/src/consumer.rs:161-176`.
- RLS plumbing/callers: `rls.rs:44-122`; `RlsContext|set_config` grep across
  api/application/worker-service/scheduler-service = 0 hits.
- Proto files: single `crates/api/proto/mas/api/v1/mas.proto`.
- Toolchain: `rust-version = "1.85.0"`, `channel = "1.98.1"`.
- `.cargo-env` absolute paths, no gitignore entry.
- No CI config (`.github`/CI files absent).
- No redis dep anywhere in Cargo manifests.
- gates: `scripts/check.sh` → "✔ all gates green";
  `cargo test --workspace --locked` → 429 passed / 0 failed.
