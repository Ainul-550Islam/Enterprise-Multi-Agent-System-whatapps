-- PURPOSE: Migration 0013 — materialize `created_by` on `tasks`.
--
-- Owning layer: database schema.
--
-- Production responsibility: `domain::Task::created_by` is a required
-- aggregate field, but the broker's durable publish path hand-writes task rows
-- and has no human creator to name. This migration gives the column a
-- backfilled default so hand-written rows hydrate instead of failing.
--
-- It must NOT:
--   • drop the column or relax it to unusable;
--   • leave existing rows NULL, which would fail hydration;
--   • be edited after it has been applied anywhere.

-- 0013_task_created_by: materialize `created_by` on `tasks`.
--
-- `domain::Task::created_by` is a REQUIRED aggregate field, but the broker's
-- durable publish path (`PgTaskBroker::publish`) hand-writes task rows and has
-- no way to know a human creator: a queued task is created by the platform on
-- behalf of an execution, not by an interactive user.
--
-- Before this migration the value lived only inside `payload._mas_spec` for
-- rows written through `TaskStore`, and was absent entirely for rows written
-- by the broker — so `TaskStore::load` could not hydrate broker-written rows
-- ("row hydration failed for table tasks"), which made every durable task
-- unloadable by the worker lifecycle.
--
-- The column is therefore real, NOT NULL, and defaulted to the platform
-- principal. Existing rows are backfilled from the legacy spec location so no
-- creator attribution is lost.

ALTER TABLE tasks
    ADD COLUMN created_by text NOT NULL DEFAULT 'system';

-- Backfill: rows previously persisted through `TaskStore` keep their creator
-- inside the payload spec envelope.
UPDATE tasks
SET created_by = payload -> '_mas_spec' ->> 'created_by'
WHERE payload ? '_mas_spec'
  AND payload -> '_mas_spec' ->> 'created_by' IS NOT NULL
  AND payload -> '_mas_spec' ->> 'created_by' <> '';

-- Once materialized, the spec copy is redundant and would shadow the column.
UPDATE tasks
SET payload = payload - '_mas_spec' || jsonb_build_object(
        '_mas_spec',
        (payload -> '_mas_spec') - 'created_by'
    )
WHERE payload ? '_mas_spec'
  AND payload -> '_mas_spec' ? 'created_by';

CREATE INDEX tasks_created_by_idx ON tasks (created_by);
