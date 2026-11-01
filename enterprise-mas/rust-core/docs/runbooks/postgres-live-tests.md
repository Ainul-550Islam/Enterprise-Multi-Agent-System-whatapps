# Runbook: live PostgreSQL integration suite

**Location:** `crates/persistence/tests/live_pg.rs` (17 tests)
**Cost:** one PostgreSQL 16+ instance you are allowed to mutate.
**Never point at production.**

## 1. Bring up PostgreSQL

Use PostgreSQL 16+ **with the `pgvector` server extension installed**.
Migration `0028_rag_pgvector_plane` executes `CREATE EXTENSION vector`, so a
plain official `postgres` image is insufficient and will fail migration with
`extension "vector" is not available`. Fastest disposable instance:

```bash
docker run --name mas-itest-pg \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=mas_itest \
  -p 55432:5432 -d pgvector/pgvector:pg16
```

For package-managed PostgreSQL, install the matching server package first (for
example, `postgresql-17-pgvector` on Debian for PostgreSQL 17), then verify:

```bash
psql "$MAS_TEST_DATABASE_URL" -Atqc \
  "SELECT default_version FROM pg_available_extensions WHERE name = 'vector';"
```

(Matches the compose service in `.devcontainer/` when you're inside the dev
container, provided that its PostgreSQL image also includes pgvector; use
`postgres://postgres:postgres@postgres:5432/postgres`.)

## 1b. Two URLs, and why you need both

The suite needs **two** credentials, and the difference is the whole point.

| Variable | Role | BYPASSRLS | Used for |
|---|---|---|---|
| `MAS_TEST_DATABASE_URL` | migrator (`mas` locally) | yes | every lifecycle, schema and repository round-trip |
| `MAS_TEST_DATABASE_APP_URL` | runtime (`mas_app`) | **no** | the row-level-security assertions |

A superuser bypasses row-level security. That makes the migrator URL correct
for a test about migrations — and worthless for a test about isolation, because
a policy that is switched off cannot fail. Running `tenant_b_cannot_read_...`
on the migrator URL proves only that our SQL says `WHERE tenant_id = $1`; it
does not prove that the database would stop us if the SQL forgot.

So the three tests that end in `..._row_level_security`,
`..._previous_request_tenant` and `..._their_own_tables` run on the app URL.
**If `MAS_TEST_DATABASE_APP_URL` is unset they skip, loudly**, rather than
reporting green. A vacuous isolation test is worse than no isolation test:
it buys confidence it has not earned.

```bash
# Grant LOGIN to the plane roles the migrations deliberately create
# NOLOGIN. Passwords come from the environment, never argv.
export MAS_DATABASE_ADMIN_URL=$MAS_TEST_DATABASE_URL
export MAS_APP_PASSWORD=...       # and _DIRECTORY_ _WORKER_ _OUTBOX_ _AUDIT_
export MAS_WORKER_PASSWORD=...    #   _SCHEDULER_  — see the script for the
export MAS_SCHEDULER_PASSWORD=... #   complete list
./scripts/provision-plane-roles.sh --dry-run   # print, mask passwords
./scripts/provision-plane-roles.sh             # apply

export MAS_TEST_DATABASE_APP_URL=postgres://mas_app:...app...@127.0.0.1:55432/mas_itest
```

The script grants `LOGIN` and nothing else — no `BYPASSRLS`, no extra table
grants, no role attributes. `--dev` generates random passwords and refuses to
run against anything but loopback. `--dry-run` masks every password.

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
| `concurrent_migrate_is_serialized` | `MigrationRunner` advisory lock under concurrency |
| `tenant_b_cannot_read_claim_cancel_or_settle_tenant_a_execution` | `ExecutionStore` under an armed cross-tenant scope |
| `the_same_idempotency_key_in_two_tenants_is_two_executions` | `ExecutionStore` idempotency slot tenancy |
| **`the_runtime_role_cannot_bypass_row_level_security`** | row-level security, **as `mas_app`** — unscoped ⇒ 0 rows; scoped ⇒ exactly one; cross-tenant `UPDATE`/`DELETE` ⇒ 0 rows affected |
| **`a_pooled_connection_does_not_inherit_the_previous_request_tenant`** | pooled-connection leakage — one socket, two callers; and the contrasting session-level-GUC case that *does* leak |
| **`the_plane_roles_are_confined_to_their_own_tables`** | privilege narrowness for `mas_worker`/`mas_audit`/`mas_outbox`/`mas_directory`/`mas_scheduler` |

### Why the pooled-connection test is a real test

The pool it exercises holds exactly **one** connection (`min = max = 1`), so
the second `acquire()` is guaranteed to be the same socket as the first. Two
things are then asserted in opposite directions, because a test that can only
fail one way can also pass for the wrong reason:

1. `SET LOCAL` (transaction scope) ⇒ the next request on that socket sees
   nothing. Verified directly against the cluster: `inside txn = 1`,
   `after commit = 0`.
2. `set_config(..., false)` (session scope) ⇒ the next request on that socket
   still sees the previous tenant's rows: `after commit = 1`. **This is the
   leak.** It is asserted, not merely described, so the reason `scoped_request`
   uses `SET LOCAL` and nothing else stays executable.

### Why the RLS test is not vacuous

Both directions were confirmed by hand before the assertion was trusted:

| Probe | Expected | Observed |
|---|---|---|
| `relrowsecurity` / `relforcerowsecurity` on `tenants` | `t` / `t` | `t` / `t` |
| `mas_app` privileges on `tenants` | must include `SELECT`, `UPDATE`, `DELETE` | `DELETE,INSERT,SELECT,UPDATE` |
| unscoped `SELECT count(*) FROM tenants` as `mas_app` | `0` | `0` |
| same query as the superuser (proves rows exist) | `> 0` | `245` |

Without the second and fourth rows the first and third would be meaningless:
a role with no privileges and a table with no rows both produce `0`.

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
| `role "mas_app" does not exist` / `cannot login` | Plane roles are created `NOLOGIN` and have no password | Run `scripts/provision-plane-roles.sh` (see §1b) |
| `min_connections must be <= max_connections` | Hand-built pool config for the app pool | Not reachable from the suite; it sets both to `1` |
| `must connect as a role that is subject to row-level security` | `MAS_TEST_DATABASE_APP_URL` points at the migrator | Point it at `mas_app`; the suite refuses to continue rather than testing nothing |
| `verify` fails with checksum drift | SQL file edited after apply | Always create `migrations/NNNN_name/`, never edit an applied file's `up.sql` |
