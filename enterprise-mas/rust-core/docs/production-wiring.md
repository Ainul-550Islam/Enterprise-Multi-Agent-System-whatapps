# Production wiring plan (Phase 19+) — repository-complete production mode

Decision record + execution ledger for "binaries run without
`--dev-inmemory`". User-selected option: repository-complete production
mode (`pg-adapters`).

## Key decisions

1. **Direction of dependency**: `mas-persistence` gains
   `mas-application`, `mas-scheduling`, `mas-worker` deps and implements
   their store ports (`XStorePort`) over PostgreSQL. Acyclic: none of
   these crates depend on `mas-persistence`.
2. **No 13th migration**: the migration inventory (0001–0012) is the
   committed spec. Leases therefore map onto:
   - worker task claims → existing `task_attempts` rows
     (`uniq(task_id, attempt)`, `lease_expires`, status transitions) —
     the schema was designed for exactly this;
   - scheduler write-lock → `pg_try_advisory_lock` on a hashed resource
     name held by a session-pinned pooled connection (documented
     tradeoff: replicas must share the PostgreSQL endpoint).
3. **Broker in Pg-only production mode**: a `PgTaskBroker` (new,
   `mas-worker`-facing impl of `mas_messaging::broker::BrokerPort`)
   where publish = `INSERT INTO tasks`, receive = `FOR UPDATE SKIP
   LOCKED` polling, nack/requeue = status transitions on the same row.
   Real JetStream lands in the later `nats-wiring` phase; both runtimes
   keep the same port contract.
4. **Production auth**: `PostgresApiKeyVerifier` (`mas-api`
   `TokenVerifierPort`) resolves bearer tokens against `api_keys`
   (hashed lookup per security doctrine — secrets never stored
   plaintext); the dev static-token verifier stays dev-only. The
   production guardrail (refuse `server.dev_tokens` under
   `MAS_ENV=production`) already exists in `mas-api`.
5. **Verification reality**: no PostgreSQL/Docker available in the
   workspace sandbox → gates stay unit-level: pure mapper tests (row
   payload composition/decomposition, status-string mapping, upsert
   column-contract snapshots) + compile + clippy + in-process wiring
   integration. A `#[ignore]` `DATABASE_URL`-gated integration test per
   repository provides the honest live path; the runbook documents it.
6. **Config**: `[persistence] url = ""` key family joins config/* (prod
   overlay documents it must come from `MAS_PERSISTENCE_URL`;
   development/test stay empty with `--dev-inmemory` the default).

## Execution ledger

| Step | Status |
|---|---|
| Plan recorded | done |
| `mas-persistence` deps (+application/scheduling/worker) | done |
| `rows.rs` row structs: Agent, AgentVersion, Workflow, Execution, Schedule | done |
| `repositories/agents.rs` (AgentStore+AgentVersionStore over agents/agent_versions) | done |
| `repositories/workflows.rs` (WorkflowStore; graph JSONB on workflows row) | done |
| `repositories/executions.rs` (ExecutionStore + idempotency key slot) | done |
| `repositories/schedules.rs` (app ScheduleStorePort + scheduling list_due/record_run) | done |
| `repositories/tasks.rs` (worker TaskLifecyclePort over tasks/task_attempts) | done |
| `repositories/lease.rs` (advisory-lock LeaseStorePort) | DEFERRED — revised: schedule_runs UNIQUE(schedule_id, planned_at) is the replica-safe fire-guard (each planned tick has exactly one journal row); advisory locks postponed until multi-replica leader needs emerge (recorded decision §2 revisited) |
| `repositories/broker.rs` (PgTaskBroker BrokerPort) | done |
| `services.rs` (PgServices: 10 port impls incl. existing tenancy stores) | done |
| api/worker/scheduler production composition roots + config keys | done |
| `PostgresApiKeyVerifier` + prod guardrail tests | done (verifier unit-tested; guardrail factored into `enforce_production_guardrails` + 2 bin tests) |
| DATABASE_URL-gated integration tests + runbook | done (`crates/persistence/tests/live_pg.rs`, 10 tests over `MAS_TEST_DATABASE_URL`; `docs/runbooks/postgres-live-tests.md`) |
| Gates (fmt/clippy/test/doc) | pending |
