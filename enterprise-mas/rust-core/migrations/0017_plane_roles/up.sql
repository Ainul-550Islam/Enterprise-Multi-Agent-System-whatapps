-- PURPOSE: The machine-plane database roles (security Phase 14).
--
-- Owning layer: the database, applied by the migration runner.
--
-- Production responsibility: create `mas_worker`, `mas_outbox` and `mas_audit`
-- -- the three planes that cannot know a tenant before their query runs -- each
-- with BYPASSRLS confined to specific tables and operations, and each NOLOGIN
-- until provisioned. Record on `mas_app` that it must never be granted
-- BYPASSRLS.
--
-- Security boundary: BYPASSRLS is the sharpest privilege in the schema. It is
-- granted here only to roles that are (a) not reachable at runtime as a login
-- role, (b) confined by table and operation, and (c) documented in
-- `pg_description` so an auditor can read WHY without reading the migration.
-- `mas_app` -- the role request traffic uses -- never receives it.
--
-- It must NOT:
--   • grant BYPASSRLS to `mas_app`;
--   • make any of these roles LOGIN (provisioning does that, deliberately);
--   • grant more than the operation a plane actually performs.

-- Migration 0017 — the machine-plane database roles.
--
-- WHY THIS EXISTS (security Phase 14 / RLS policy audit Phase 13)
-- ==============================================================
-- Every tenant-data table is `ENABLE` + `FORCE ROW LEVEL SECURITY`, and every
-- policy reads `app.current_tenant`. That is correct for request traffic,
-- which always knows its tenant.
--
-- Three planes cannot know a tenant in advance:
--
--   * the **worker plane** — `SELECT … FROM tasks FOR UPDATE SKIP LOCKED`
--     deliberately scans every tenant's queue. Its tenant authority is the row
--     it wins the lease on, which by definition is not known before the query
--     runs.
--   * the **outbox plane** — relays rows committed by other transactions, so
--     the transaction that set the GUC has already ended.
--   * the **audit plane** — must record a denial *for a tenant the caller was
--     never scoped to*. Writing that row under the caller's scope is exactly
--     what RLS forbids, and rightly so; but the event is the evidence that the
--     denial happened.
--
-- Each of these therefore needs a role that is exempt from RLS. The
-- alternative — granting `BYPASSRLS` to `mas_app` — would switch row-level
-- security off for the entire platform, which is not a trade worth making.
--
-- The exemption is confined three ways, and all three are enforced here:
--
--   1. **By role.** Three separate roles, none of which is `mas_app`.
--   2. **By table.** Each role is granted only the tables its plane touches.
--      `mas_worker` cannot read `credentials` or `api_keys`; `mas_outbox`
--      cannot read `tasks`; `mas_audit` can only INSERT.
--   3. **By operation.** `mas_audit` gets INSERT and nothing else, so an audit
--      writer cannot rewrite history. `mas_outbox` gets SELECT and UPDATE on
--      one table.
--
-- INVARIANTS
--   * No superuser is used at runtime. These three roles plus `mas_app` and
--     `mas_directory` are the complete set.
--   * No plane role may be used to serve a request. They have no grants on the
--     tables a request reads (`agents`, `workflows`, `executions` for the
--     worker's own use aside), and an attempt fails at the database.
--   * Adding a tenant-data table must NOT widen these grants. The lists below
--     are explicit for that reason.
--
-- MIGRATION PATH (Phase 14, operational)
--   1. Apply with the migrator role.
--   2. Provision credentials for each role in the secret manager and grant
--      LOGIN with them. All three are created `NOLOGIN`, so an unconfigured
--      deployment cannot authenticate as a plane role by accident.
--   3. Configure `MAS_DATABASE_WORKER_URL`, `MAS_DATABASE_OUTBOX_URL` and
--      `MAS_DATABASE_AUDIT_URL`. A deployment that leaves one unset gets the
--      runtime pool, which means the plane sees no rows under RLS; that fails
--      visibly and immediately rather than silently.
--   4. Rotation: rotate the credential, update the secret, reload. The roles
--      themselves need no change.

-- ---------------------------------------------------------------------------
-- Worker plane
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mas_worker') THEN
        CREATE ROLE mas_worker NOLOGIN BYPASSRLS;
    END IF;
END
$$;

-- The durable execution pipeline only. Deliberately NOT granted: credentials,
-- api_keys, sessions, connectors, webhooks — a compromised worker must not be
-- able to read tenant secrets.
GRANT USAGE ON SCHEMA public TO mas_worker;
GRANT SELECT, INSERT, UPDATE ON tasks TO mas_worker;
GRANT SELECT, INSERT, UPDATE ON task_attempts TO mas_worker;
GRANT SELECT, INSERT, UPDATE ON executions TO mas_worker;
GRANT SELECT, UPDATE ON execution_steps TO mas_worker;
GRANT SELECT, UPDATE ON outbox_events TO mas_worker;

COMMENT ON ROLE mas_worker IS
    'Durable execution worker plane (security Phase 14). BYPASSRLS is required '
    'because claiming a task scans every tenant''s queue; the tenant authority '
    'is the row the lease is won on. Grants are confined to tasks, '
    'task_attempts, executions, execution_steps and outbox_events — it has no '
    'access to credentials, api_keys or sessions.';

-- ---------------------------------------------------------------------------
-- Outbox relay plane
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mas_outbox') THEN
        CREATE ROLE mas_outbox NOLOGIN BYPASSRLS;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO mas_outbox;
GRANT SELECT, UPDATE ON outbox_events TO mas_outbox;

COMMENT ON ROLE mas_outbox IS
    'Transactional outbox relay (security Phase 14). BYPASSRLS is required '
    'because it relays rows committed by transactions that have already ended. '
    'Grants are SELECT and UPDATE on outbox_events and nothing else.';

-- ---------------------------------------------------------------------------
-- Audit plane
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mas_audit') THEN
        CREATE ROLE mas_audit NOLOGIN BYPASSRLS;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO mas_audit;
-- INSERT only: an audit writer records events, it does not edit them. The
-- append-only triggers on audit_events (migration 0011) already forbid UPDATE
-- and DELETE; this makes the restriction visible in the grant as well.
GRANT INSERT ON audit_events TO mas_audit;

COMMENT ON ROLE mas_audit IS
    'Audit writer (security Phase 14). BYPASSRLS is required to record a '
    'denial for a tenant the caller was never scoped to — the event is the '
    'evidence the denial happened. INSERT only on audit_events.';

-- ---------------------------------------------------------------------------
-- A loud record of what the runtime role is NOT
-- ---------------------------------------------------------------------------
COMMENT ON ROLE mas_app IS
    'Application runtime role (security Phase 14). No BYPASSRLS: request '
    'traffic always sets app.current_tenant / app.current_org with SET LOCAL '
    'and is confined by row-level security. Cross-tenant or pre-scope reads '
    'use mas_directory; machine planes use mas_worker, mas_outbox or '
    'mas_audit. Never grant this role BYPASSRLS.';
