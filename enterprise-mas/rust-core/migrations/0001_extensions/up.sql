-- 0001_extensions: base extensions, shared trigger functions, app roles.
--
-- Applied transactionally by persistence::migrations::MigrationRunner.
-- RLS policies for every tenant-owned table live in their own migration
-- (platform rule: RLS is SQL-side, never app code).

-- gen_random_uuid() for all primary keys.
CREATE EXTENSION IF NOT EXISTS pgcrypto;
-- Case-insensitive text for slugs/emails.
CREATE EXTENSION IF NOT EXISTS citext;

-- Maintains `updated_at` on UPDATE for every mutable aggregate table.
CREATE OR REPLACE FUNCTION mas_set_updated_at() RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

-- Enforces append-only tables (audit): raises on UPDATE/DELETE.
CREATE OR REPLACE FUNCTION mas_forbid_mutation() RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'table % is append-only; UPDATE/DELETE is forbidden', TG_TABLE_NAME;
END;
$$;

-- Application runtime role: no login by default, no RLS bypass.
-- Every tenant-owned table is FORCE RLS, which applies to table owners, so
-- the migrator/owner role must be used only for migrations while ordinary
-- traffic runs as mas_app with an `app.current_tenant` GUC set per request.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mas_app') THEN
        CREATE ROLE mas_app NOLOGIN;
    END IF;
END
$$;

-- All tables created after this migration by the migrator role are usable by
-- mas_app. (Sequences are intentionally excluded; ids come from pgcrypto.)
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO mas_app;
