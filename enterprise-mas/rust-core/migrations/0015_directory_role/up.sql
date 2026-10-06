-- PURPOSE: The tenancy-directory database role (security Phase 3 / 14).
--
-- Owning layer: the database, applied by the migration runner.
--
-- Production responsibility: create `mas_directory` -- the one role permitted
-- to resolve a tenant from identity BEFORE a scope exists -- with BYPASSRLS
-- confined to SELECT on four catalog tables, and make it NOLOGIN until
-- provisioning grants it a login. `mas_app` deliberately receives no
-- BYPASSRLS.
--
-- Security boundary: resolving tenancy is a chicken-and-egg read: the
-- directory must be queried before any GUC can be set, and under
-- `FORCE ROW LEVEL SECURITY` an unscoped read returns nothing. Rather than
-- weaken the policies or grant `mas_app` a bypass, this migration creates a
-- separate, table-confined role and gives the process a separate pool for it.
--
-- It must NOT:
--   • grant BYPASSRLS to `mas_app`;
--   • grant `mas_directory` anything beyond SELECT on the four catalog
--     tables;
--   • let the runtime pool be used for directory reads.

-- Migration 0015 — the tenancy directory role.
--
-- WHY THIS EXISTS (security Phase 3 / 14)
-- =======================================
-- Every tenant-owned table is `ENABLE` + `FORCE ROW LEVEL SECURITY`. The
-- directory tables that describe the hierarchy (`organizations`, `tenants`,
-- `projects`, `memberships`) are no exception: a `tenants` row is visible
-- only when `app.current_tenant` or `app.current_org` already matches it.
--
-- That creates a bootstrap that cannot be resolved inside a scoped
-- transaction. To resolve *which* tenant a request is entitled to, the
-- platform must first read the hierarchy — but reading the hierarchy already
-- requires knowing the tenant. Any attempt to solve this inside `mas_app`
-- would require either (a) granting `mas_app` BYPASSRLS, which would disable
-- row-level security for every tenant table in the system, or (b) a
-- `SECURITY DEFINER` function that accepts a user id from the caller, which
-- would let any caller impersonate any user because SQL has no notion of the
-- application-level principal. Both are worse than the problem.
--
-- The resolution is to confine the exception instead of widening the rule.
-- This migration creates a dedicated role that:
--
--   * exists for exactly one purpose — reading the hierarchy catalog;
--   * is granted SELECT on the four directory tables and nothing else;
--   * is granted NO privilege on any tenant data table (executions, tasks,
--     agents, credentials, usage, audit, outbox, …), so a compromise of this
--     role discloses the shape of the tenancy tree and no tenant data;
--   * holds BYPASSRLS, which is unavoidable for a pre-scope read and is the
--     reason the grant surface above is deliberately minimal.
--
-- The application connects to PostgreSQL with TWO pools:
--
--   * `mas_app`      — runtime role, NO BYPASSRLS, used for all tenant data.
--                      Every transaction sets `app.current_tenant` /
--                      `app.current_org` with `SET LOCAL` before touching a
--                      row.
--   * `mas_directory` — this role, used only to resolve a principal's tenant,
--                      organization and project before a scope exists.
--
-- INVARIANTS
--   * `mas_directory` must never be the role behind a request-serving
--     repository. It has no grants on tenant tables, so an attempt fails
--     closed at the database, but the code must not rely on that alone.
--   * `mas_directory` must never receive INSERT/UPDATE/DELETE on anything.
--   * Adding a tenant data table must NOT widen this role's grants; the grant
--     list below is intentionally explicit rather than `ON ALL TABLES`.
--
-- MIGRATION PATH (Phase 14, operational)
--   1. Apply this migration with the *migrator* role (the schema owner).
--   2. Create OS-level credentials for `mas_directory` in the secret manager
--      and grant `mas_directory` login with that password. The role is created
--      NOLOGIN so an unconfigured deployment cannot accidentally authenticate
--      as it.
--   3. Configure `MAS_DATABASE_DIRECTORY_URL` alongside `MAS_DATABASE_URL`.
--      If it is unset, the API refuses to serve tenant-scoped routes rather
--      than silently falling back to the runtime role — a fallback to
--      `mas_app` would mean unscoped directory reads that return zero rows,
--      which looks like "tenant not found" rather than a misconfiguration.
--   4. Rotation: create the new credential, update the secret, reload. The
--      role itself needs no change.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mas_directory') THEN
        -- BYPASSRLS is confined to this role and to the four catalog tables
        -- below. It is NOT granted to mas_app, and no superuser is used at
        -- runtime.
        CREATE ROLE mas_directory NOLOGIN BYPASSRLS;
    END IF;
END
$$;

-- The complete grant surface. Explicit, enumerated, and deliberately not
-- `ON ALL TABLES IN SCHEMA public`: a future tenant data table must not
-- inherit visibility to this role.
GRANT SELECT ON organizations TO mas_directory;
GRANT SELECT ON tenants TO mas_directory;
GRANT SELECT ON projects TO mas_directory;
GRANT SELECT ON memberships TO mas_directory;

-- USAGE on the schema is required to reach the tables at all; nothing more.
GRANT USAGE ON SCHEMA public TO mas_directory;

-- A defensive record of what this role is for, so `psql \du+` and any audit
-- of `pg_roles` explains the BYPASSRLS rather than leaving it to be read as
-- an oversight.
COMMENT ON ROLE mas_directory IS
    'Tenancy directory resolver (security Phase 3/14). BYPASSRLS is confined '
    'to SELECT on organizations/tenants/projects/memberships only; it exists '
    'because resolving a principal''s tenant must happen before a scope is '
    'known. No privileges on tenant data tables. Never used by a '
    'request-serving repository.';
