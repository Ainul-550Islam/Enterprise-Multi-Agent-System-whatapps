-- PURPOSE: The scheduler-plane database role (security Phase 14 / 25).
--
-- Owning layer: the database, applied by the migration runner.
--
-- Production responsibility: create `mas_scheduler` — the one role permitted
-- to scan `schedules` across every tenant — with BYPASSRLS confined to
-- SELECT on `schedules` and `schedule_runs`, and make it NOLOGIN until
-- provisioning grants it a login. `mas_app` deliberately receives no
-- BYPASSRLS here either.
--
-- Security boundary: the due-scan is a cross-tenant read by construction.
-- A scheduler replica does not serve a request; it asks "which schedules are
-- due anywhere?" and only learns the tenant after the row is in hand. The
-- exemption is therefore confined three ways — by role (never `mas_app`), by
-- table (`schedules` and `schedule_runs` only), and by operation (the due-scan
-- is SELECT; the subsequent write-back runs on `mas_app` inside a scoped
-- transaction for the tenant the row named).
--
-- It must NOT:
--   • grant BYPASSRLS to `mas_app`;
--   • grant `mas_scheduler` any privilege on tenant data (tasks, executions,
--     agents, credentials, api_keys, audit, outbox);
--   • make the role LOGIN (provisioning does that, deliberately).

-- Migration 0019 — the scheduler plane role.
--
-- WHY THIS EXISTS (security Phase 25 — worker/scheduler explicit identities)
-- =========================================================================
-- Migrations 0015 and 0017 created `mas_directory`, `mas_worker`,
-- `mas_outbox` and `mas_audit` for the same reason this one creates
-- `mas_scheduler`: a plane that cannot know its tenant before its query runs
-- cannot be expressed as an RLS policy, and the alternative — granting
-- `mas_app` BYPASSRLS — switches row-level security off for the whole
-- platform.
--
-- The scheduler is the one plane 0017 missed. Its tick is:
--
--   1. `list_due(now, limit)` — "which schedules are due anywhere?" The
--      answer spans every tenant, by definition.
--   2. for each due row: journal the planned tick and dispatch the run, now
--      that the row has told us which tenant owns it.
--
-- Step 1 is the only cross-tenant statement in the loop. Step 2 knows its
-- tenant and therefore runs under `mas_app` inside a scoped transaction —
-- the same `SET LOCAL app.current_tenant` path a request takes. Splitting the
-- tick this way keeps the bypass to a single SELECT on a single table instead
-- of letting the whole loop run exempt.
--
-- INVARIANTS
--   * `mas_scheduler` holds BYPASSRLS, and SELECT on exactly two tables.
--   * It is never the role behind a request-serving repository.
--   * Adding a tenant-data table must NOT widen these grants.
--
-- MIGRATION PATH (operational)
--   1. Apply with the migrator role.
--   2. Provision a credential for `mas_scheduler` in the secret manager and
--      grant it LOGIN. It is created NOLOGIN so an unconfigured deployment
--      cannot authenticate as the plane role by accident.
--   3. Configure `MAS_DATABASE_SCHEDULER_URL`. A deployment that leaves it
--      unset gets the runtime pool, whose due-scan returns zero rows under
--      RLS — the scheduler then idles visibly instead of silently skipping
--      tenants.
--   4. Rotation: rotate the credential, update the secret, reload.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mas_scheduler') THEN
        CREATE ROLE mas_scheduler NOLOGIN BYPASSRLS;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO mas_scheduler;

-- SELECT only. The scheduler never rewrites a schedule through the plane
-- role: the post-fire write-back (`last_run_at`, `next_run_at`, status) runs
-- on `mas_app` under the tenant scope the due-scan row named.
GRANT SELECT ON schedules TO mas_scheduler;
GRANT SELECT ON schedule_runs TO mas_scheduler;

-- Deliberately NOT granted: tasks, executions, execution_steps,
-- task_attempts, agents, credentials, api_keys, sessions, audit_events,
-- outbox_events. A compromised scheduler discloses when work is planned and
-- nothing about the work itself.

COMMENT ON ROLE mas_scheduler IS
    'Schedule tick plane (security Phase 25). BYPASSRLS is required because '
    'the due-scan asks which schedules are due in ANY tenant; the tenant is '
    'only known once the row is in hand. Grants are SELECT on schedules and '
    'schedule_runs and nothing else — the post-fire write-back and the run '
    'journal run on mas_app inside a scoped transaction for that tenant. '
    'No privileges on tenant data tables.';
