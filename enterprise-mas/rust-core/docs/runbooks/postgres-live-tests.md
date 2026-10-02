# Runbook: live PostgreSQL integration suite

**Location:** `crates/persistence/tests/live_pg.rs` (10 tests)
**Cost:** one PostgreSQL 16+ instance you are allowed to mutate.
**Never point at production.**

## 1. Bring up PostgreSQL

Any PostgreSQL 16+ works. Fastest disposable instance:

```bash
docker run --name mas-itest-pg \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=mas_itest \
  -p 55432:5432 -d postgres:16-alpine
```

(Matches the compose service in `.devcontainer/` when you're inside the dev
container: the service `postgres` is already up; use
`postgres://postgres:postgres@postgres:5432/postgres`.)

## 2. Run the suite

The suite migrates the database itself (idempotent ledger), seeds fixtures
through the real store code paths, and asserts round-trips for every
production repository:

```bash
cd rust-core
source .cargo-env
export MAS_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:55432/mas_itest

# Full suite, with the skip banner suppressed by the env var being present:
cargo test -p mas-persistence --test live_pg

# Same thing via the migration helper first (DBA-style apply, then verify):
DATABASE_URL=$MAS_TEST_DATABASE_URL scripts/migrate.sh up
DATABASE_URL=$MAS_TEST_DATABASE_URL scripts/migrate.sh status
cargo test -p mas-persistence --test live_pg
```

The binary also honors a bare `DATABASE_URL` when `MAS_TEST_DATABASE_URL` is
unset; without either variable it prints the skip banner and exits green —
that is intentional (CI/dev loops without a database stay cheap), so **mark
the job that runs the live suite separately** and never treat the green skip
as signal.

Each test is self-contained (fresh UUID fixtures), safe to run concurrently
(`--test-threads` default), and safe to re-run against the same database.

## 3. What the suite covers

| Test | Repository(s) under test |
|---|---|
| `migrations_are_idempotent` | `MigrationRunner` (ledger apply/`verify`/) |
| `tenancy_round_trip` | `OrganizationStore`, `TenantStore`, `ProjectStore` (RLS-scoped reads) |
| `agents_round_trip` | `AgentStore`, `AgentVersionStore` incl. `UNIQUE (tenant_id, slug)` |
| `workflows_round_trip` | `WorkflowStore` |
| `executions_round_trip_and_idempotency` | `ExecutionStore` + idempotency slot |
| `schedules_due_scan_and_run_journal` | `ScheduleStore.list_due`, `record_run` (duplicate-fire guard) |
| `tasks_round_trip` | `TaskStore` (worker lifecycle port) |
| `api_keys_prefix_lookup_and_touch` | `ApiKeyStore` (`find_by_prefix`, `touch_last_used`) |
| `audit_append_list_and_counts` | `PostgresAuditLog` (append, correlation fetch, outcome counts) |
| `broker_publish_fetch_ack_round_trip` | `PgTaskBroker` (`BrokerPort` publish/fetch/ack + idempotent republish) |

## 4. Running the binaries against the same database

```bash
export MAS_DATABASE_URL=$MAS_TEST_DATABASE_URL \
       MAS_ENV=staging \
       MAS_DATABASE_MIGRATE_ON_BOOT=false  # ledger must be verified green

cargo build -p mas-api -p mas-worker-service -p mas-scheduler-service
target/debug/mas-api         # durable stores + PostgresApiKeyVerifier
target/debug/mas-worker      # durable broker lane + pending-execution lifecycle
target/debug/mas-scheduler   # durable due-scan + scheduler journal
```

Tier-0 E2E signal after the trio is up:

```bash
DATABASE_URL=$MAS_DATABASE_URL scripts/smoke.sh
```

## 5. Clean up

```bash
docker rm -f mas-itest-pg            # if you used the disposable container
```

## 6. Failure playbook

| Symptom | Cause | Fix |
|---|---|---|
| `connection refused` | PG not up or wrong port | `docker ps`; use the mapped port (`55432` in the example above) |
| `relation ... does not exist` on first boot | Migrations not applied | The suite applies them itself; for binary bring-up run `scripts/migrate.sh up` first |
| `duplicate key` in fixtures | Re-ran into a dirty DB you meant to reset | `dropdb/createdb` the test DB; fixtures use fresh ids and tolerate existing data otherwise |
| Skip banner prints on CI | Env var named differently | Export exactly `MAS_TEST_DATABASE_URL` (or `DATABASE_URL`) |
| `verify` fails with checksum drift | SQL file edited after apply | Always create `migrations/NNNN_name/`, never edit an applied file's `up.sql` |
