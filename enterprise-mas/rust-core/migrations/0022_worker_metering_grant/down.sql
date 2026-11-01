-- PURPOSE: Migration 0022 (down) — removes the grant added by `up.sql`.
--
-- Owning layer: database schema.
--
-- Production responsibility: leave the database exactly as it was before 0022
-- applied, so a failed rollout can be reverted without manual surgery. Only
-- `INSERT` on `usage_records` is revoked, from `mas_worker` only — precisely
-- the privilege `up.sql` granted and nothing else.
--
-- It must NOT:
--   • revoke a privilege from any other role;
--   • drop the `usage_records` table (this migration granted a privilege, it
--     did not create an object).

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mas_worker') THEN
        REVOKE INSERT ON usage_records FROM mas_worker;
    END IF;
END
$$;
