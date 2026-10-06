-- PURPOSE: Migration 0014 — the schema foundation of the canonical durable
-- execution pipeline.
--
-- Owning layer: database schema.
--
-- Production responsibility: move the pipeline's guarantees out of application
-- logic and into the database, where a race cannot get around them: a
-- tenant-scoped unique index on the idempotency key, a unique index making two
-- open attempts on one task unrepresentable, claim and lease-recovery indexes,
-- outbox claim columns, a unique usage index that refuses to double-meter one
-- task, and version columns for the optimistic fence.
--
-- It must NOT:
--   • be destructive — every statement is `ADD COLUMN` / `CREATE INDEX
--     IF NOT EXISTS`;
--   • introduce a new execution lifecycle or table that competes with the
--     canonical one;
--   • be edited after it has been applied anywhere.

-- 0014_durable_pipeline: schema foundation for the canonical durable
-- execution pipeline.
--
-- WHY THIS MIGRATION EXISTS
-- -------------------------
-- The pipeline needs guarantees that cannot be provided by application code
-- alone, because application code cannot make two concurrent transactions
-- agree:
--
--   1. **Idempotency enforced by the database, not by SELECT-then-INSERT.**
--      Two identical submissions arriving together must produce one execution.
--      A read-then-write races; a UNIQUE constraint does not.
--   2. **A claim that cannot be handed to two workers.** `SELECT ... FOR UPDATE
--      SKIP LOCKED` plus an ownership fence on the row.
--   3. **One open attempt per task.** A unique partial index makes "two
--      machines running the same task" unrepresentable rather than unlikely.
--   4. **Optimistic concurrency that fails closed.** A version column on the
--      task row is what lets a stale worker's write be *rejected* instead of
--      silently overwriting newer state.
--   5. **Usage that cannot be double-metered on replay.**
--
-- SAFETY
-- ------
-- Everything here is additive or a widening:
--   * no table, column or index is dropped;
--   * every new column is nullable or has a constant DEFAULT, so the backfill
--     is a metadata-only operation on PostgreSQL 11+;
--   * CHECK constraints are only ever widened (a value that was legal before
--     stays legal);
--   * the one UPDATE below reclassifies rows that violate a *new* invariant
--     (multiple concurrent attempts per task) into a status that already
--     existed and is already terminal — no row is deleted;
--   * index creation uses IF NOT EXISTS so a re-run is a no-op.
--
-- ORDERING
-- --------
-- Columns and constraint widening first, then data reconciliation, then the
-- partial unique index that depends on that reconciliation, then the plain
-- indexes. Creating `task_attempts_one_open_per_task` before the
-- reconciliation would fail on any database that already has duplicates.

-- ===========================================================================
-- 1. executions: queued state, idempotency, fingerprint, version, error code
-- ===========================================================================

-- 'queued' is a real lifecycle state: the execution exists and is committed,
-- but no worker has taken it yet. Without it an execution sits as 'pending'
-- (never dispatched) or lies as 'running' (dispatched but not started).
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'executions_status_check' AND conrelid = 'executions'::regclass
    ) THEN
        ALTER TABLE executions DROP CONSTRAINT executions_status_check;
    END IF;
END $$;

ALTER TABLE executions ADD CONSTRAINT executions_status_check
    CHECK (status IN ('pending', 'queued', 'running', 'paused',
                      'completed', 'failed', 'cancelled'));

ALTER TABLE executions
    ADD COLUMN IF NOT EXISTS idempotency_key     text NULL,
    ADD COLUMN IF NOT EXISTS request_fingerprint text NULL,
    ADD COLUMN IF NOT EXISTS error_code          text NULL,
    ADD COLUMN IF NOT EXISTS queued_at           timestamptz NULL,
    ADD COLUMN IF NOT EXISTS version             bigint NOT NULL DEFAULT 1;

ALTER TABLE executions DROP CONSTRAINT IF EXISTS executions_version_check;
ALTER TABLE executions ADD CONSTRAINT executions_version_check
    CHECK (version > 0);

-- The idempotency guard. NULL keys never conflict (PostgreSQL treats NULLs as
-- distinct in unique indexes), so submissions that did not ask for idempotency
-- are unaffected and remain independently insertable.
-- Two concurrent identical submissions therefore race on this constraint, not
-- on application logic: exactly one INSERT succeeds, the other gets a
-- constraint violation and is resolved as a replay or a conflict.
CREATE UNIQUE INDEX IF NOT EXISTS executions_idempotency_idx
    ON executions (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Replay lookup also needs the fingerprint it stored, by execution id.
CREATE INDEX IF NOT EXISTS executions_fingerprint_idx
    ON executions (tenant_id, id)
    WHERE request_fingerprint IS NOT NULL;

CREATE INDEX IF NOT EXISTS executions_project_time_idx
    ON executions (tenant_id, project_id, created_at DESC);

-- ===========================================================================
-- 2. tasks: attempt accounting, ownership fence, optimistic version, failure
-- ===========================================================================

ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS version             bigint NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS attempt_count       integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS max_attempts        integer NOT NULL DEFAULT 3,
    ADD COLUMN IF NOT EXISTS locked_by           text NULL,
    ADD COLUMN IF NOT EXISTS locked_until        timestamptz NULL,
    ADD COLUMN IF NOT EXISTS queued_at           timestamptz NULL,
    ADD COLUMN IF NOT EXISTS started_at          timestamptz NULL,
    ADD COLUMN IF NOT EXISTS finished_at         timestamptz NULL,
    ADD COLUMN IF NOT EXISTS last_attempt_id     uuid NULL,
    ADD COLUMN IF NOT EXISTS error               jsonb NULL,
    ADD COLUMN IF NOT EXISTS request_fingerprint text NULL;

ALTER TABLE tasks DROP CONSTRAINT IF EXISTS tasks_version_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_version_check CHECK (version > 0);

ALTER TABLE tasks DROP CONSTRAINT IF EXISTS tasks_attempt_count_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_attempt_count_check
    CHECK (attempt_count >= 0);

ALTER TABLE tasks DROP CONSTRAINT IF EXISTS tasks_max_attempts_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_max_attempts_check
    CHECK (max_attempts > 0);

-- Optimistic-concurrency / ownership fence lookups: "is this still mine, at
-- the version I read?" is the question every settlement asks.
CREATE INDEX IF NOT EXISTS tasks_owner_idx
    ON tasks (locked_by, status)
    WHERE locked_by IS NOT NULL;

-- Lease recovery: find running tasks whose lease has lapsed.
CREATE INDEX IF NOT EXISTS tasks_lease_expiry_idx
    ON tasks (locked_until)
    WHERE status = 'running' AND locked_until IS NOT NULL;

-- Claim hot path: eligible work, highest priority and longest-waiting first.
-- Covers both 'pending' and 'queued' because the canonical machine uses
-- 'queued' for committed-but-unclaimed work and 'pending' for rows written by
-- paths that predate it.
CREATE INDEX IF NOT EXISTS tasks_claim_idx
    ON tasks (priority DESC, not_before ASC, id ASC)
    WHERE status IN ('pending', 'queued');

-- Reconciling a task back to its execution (settlement and recovery).
CREATE INDEX IF NOT EXISTS tasks_execution_idx
    ON tasks (execution_id)
    WHERE execution_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS tasks_tenant_status_idx
    ON tasks (tenant_id, status, created_at DESC);

-- ===========================================================================
-- 3. task_attempts: the durable attempt ledger
-- ===========================================================================

-- 'timed_out' is a distinct outcome from 'failed': it is retryable, and
-- operators need to separate "the dependency broke" from "the work ran too
-- long".
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'task_attempts_status_check' AND conrelid = 'task_attempts'::regclass
    ) THEN
        ALTER TABLE task_attempts DROP CONSTRAINT task_attempts_status_check;
    END IF;
END $$;

ALTER TABLE task_attempts ADD CONSTRAINT task_attempts_status_check
    CHECK (status IN ('running', 'succeeded', 'failed', 'expired',
                      'cancelled', 'timed_out'));

ALTER TABLE task_attempts
    ADD COLUMN IF NOT EXISTS execution_id    uuid NULL,
    ADD COLUMN IF NOT EXISTS correlation_id  text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS error           jsonb NULL,
    ADD COLUMN IF NOT EXISTS error_code      text NULL,
    ADD COLUMN IF NOT EXISTS error_retryable boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS result_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS version         bigint NOT NULL DEFAULT 1;

-- Backfill the execution link from the task row: an attempt must be
-- reconcilable with its execution without going through the task, so that a
-- recovery sweep can run after a task row has moved on.
UPDATE task_attempts a
SET execution_id = t.execution_id
FROM tasks t
WHERE a.task_id = t.id
  AND a.execution_id IS NULL
  AND t.execution_id IS NOT NULL;

-- Backfill correlation from the task row so attempts are traceable.
UPDATE task_attempts a
SET correlation_id = t.correlation_id
FROM tasks t
WHERE a.task_id = t.id
  AND (a.correlation_id IS NULL OR a.correlation_id = '')
  AND t.correlation_id IS NOT NULL
  AND t.correlation_id <> '';

-- ── Reconciliation before the unique partial index ────────────────────────
-- The new invariant is "at most one running attempt per task". Databases that
-- predate this migration may hold several (a crashed worker, a deployment that
-- wrote attempts without fencing). Rather than delete anything, every attempt
-- on a task except the newest running one is reclassified 'expired' — a status
-- that already existed and is already terminal, so no data is lost and no
-- reader has to learn a new value. Their `finished_at` is stamped so the row is
-- self-consistent.
UPDATE task_attempts a
SET status       = 'expired',
    finished_at  = COALESCE(a.finished_at, now()),
    lease_expires = COALESCE(a.lease_expires, now())
WHERE a.status = 'running'
  AND a.id <> (
        SELECT b.id
        FROM task_attempts b
        WHERE b.task_id = a.task_id
          AND b.status = 'running'
        ORDER BY b.attempt DESC, b.created_at DESC
        LIMIT 1
  );

-- Attempts whose lease lapsed long ago and were never closed: close them, so
-- capacity accounting (which counts open attempts) cannot be pinned at the
-- ceiling forever by rows no worker is driving.
UPDATE task_attempts
SET status       = 'expired',
    finished_at  = COALESCE(finished_at, now())
WHERE status = 'running'
  AND lease_expires IS NOT NULL
  AND lease_expires < now() - interval '1 day';

-- The guarantee itself: **duplicate parallel execution of one task is now
-- unrepresentable.** Two workers racing to start an attempt on the same task
-- collide here; the loser gets a constraint violation and must back off, which
-- is exactly the behaviour the pipeline needs.
CREATE UNIQUE INDEX IF NOT EXISTS task_attempts_one_open_per_task
    ON task_attempts (task_id)
    WHERE status = 'running';

CREATE INDEX IF NOT EXISTS task_attempts_execution_idx
    ON task_attempts (execution_id)
    WHERE execution_id IS NOT NULL;

-- Capacity accounting: "how many attempts is this worker still driving?"
CREATE INDEX IF NOT EXISTS task_attempts_worker_active_idx
    ON task_attempts (worker_id, status)
    WHERE status = 'running';

CREATE INDEX IF NOT EXISTS task_attempts_correlation_idx
    ON task_attempts (correlation_id)
    WHERE correlation_id <> '';

-- ===========================================================================
-- 4. outbox_events: claimable relay rows
-- ===========================================================================

-- The relay must be able to take ownership of a batch of rows inside a
-- transaction (claim), publish, and then mark them. Without claim columns a
-- second relay replica selects the same rows — duplicate publication — or the
-- relay marks rows published before the broker acknowledged them.
ALTER TABLE outbox_events
    ADD COLUMN IF NOT EXISTS claimed_by    text NULL,
    ADD COLUMN IF NOT EXISTS claimed_at    timestamptz NULL,
    ADD COLUMN IF NOT EXISTS task_id       uuid NULL,
    ADD COLUMN IF NOT EXISTS execution_id  uuid NULL;

-- Claim hot path: unclaimed, due rows, oldest first.
CREATE INDEX IF NOT EXISTS outbox_events_claim_idx
    ON outbox_events (next_attempt_at ASC, enqueued_at ASC)
    WHERE status IN ('pending', 'failed') AND claimed_by IS NULL;

-- Reclaim path: rows whose claimant died mid-publish.
CREATE INDEX IF NOT EXISTS outbox_events_stale_claim_idx
    ON outbox_events (claimed_at)
    WHERE status = 'pending' AND claimed_by IS NOT NULL;

CREATE INDEX IF NOT EXISTS outbox_events_task_idx
    ON outbox_events (task_id)
    WHERE task_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS outbox_events_execution_idx
    ON outbox_events (execution_id)
    WHERE execution_id IS NOT NULL;

-- ===========================================================================
-- 5. usage_records: durable, replay-safe metering
-- ===========================================================================

ALTER TABLE usage_records
    ADD COLUMN IF NOT EXISTS execution_id uuid NULL,
    ADD COLUMN IF NOT EXISTS task_id      uuid NULL;

-- Metering must survive redelivery without duplicating: the settlement that
-- writes usage is fenced by task version, and this index is the second line of
-- defence — a duplicate write for the same task and dimension is rejected by
-- the database rather than double-billing the tenant.
-- Rows written before this migration (task_id IS NULL) are unaffected.
CREATE UNIQUE INDEX IF NOT EXISTS usage_records_task_dimension_idx
    ON usage_records (task_id, resource)
    WHERE task_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS usage_records_execution_idx
    ON usage_records (execution_id)
    WHERE execution_id IS NOT NULL;
