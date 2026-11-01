-- PURPOSE: Migration 0022 — the grant the worker plane needs to meter the work
-- it settles.
--
-- Owning layer: database schema.
--
-- Production responsibility: `PgDurableExecution::settle` writes usage
-- observations inside the same transaction that makes the attempt terminal
-- (`write_usage` → `INSERT INTO usage_records`). Settlement runs on the worker
-- plane, as `mas_worker` — the only role that can claim a task across tenants,
-- which is why migration 0017 created it `BYPASSRLS`.
--
-- Migration 0017 granted that role exactly five tables: `tasks`,
-- `task_attempts`, `executions`, `execution_steps`, `outbox_events`.
-- `usage_records` was added by 0008 and extended by 0020, and was never
-- granted to the worker. In production that means every attempt whose outcome
-- carried usage — that is, **every successful run** — failed its settlement
-- with SQLSTATE 42501, and the metering for completed work was never written.
-- The durable unit rolled back, so the attempt was retried and failed the same
-- way, which is the worst shape this bug can take: it looks like a flaky
-- worker rather than a missing grant.
--
-- This migration grants the one privilege the settlement path needs.
--
-- It must NOT:
--   • grant `SELECT`, `UPDATE` or `DELETE` on `usage_records` to the worker.
--     Metering is append-only by design and the role that appends a meter has
--     no business reading or rewriting one;
--   • widen any other table for `mas_worker` (the 0017 comment is explicit:
--     a compromised worker must not reach credentials, api_keys or sessions);
--   • be edited after it has been applied anywhere.

-- 0022_worker_metering_grant: the worker plane may append usage it observed.

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mas_worker') THEN
        GRANT INSERT ON usage_records TO mas_worker;
    END IF;
END
$$;
