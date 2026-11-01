-- PURPOSE: Migration 0021 (down) — removes the sequence grant added by
-- `up.sql`.
--
-- Owning layer: database schema.
--
-- Production responsibility: leave the database exactly as it was before 0021
-- applied, so a failed rollout can be reverted without manual surgery. The
-- grant is revoked from every sequence in the schema, which is precisely the
-- set `up.sql` granted it to.
--
-- It must NOT:
--   • drop a sequence — this migration granted a privilege, it did not create
--     an object, and a down-migration must reverse only what it did;
--   • revoke a privilege from any role other than `mas_app`.

DO $$
DECLARE
    sequence_name text;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mas_app') THEN
        FOR sequence_name IN
            SELECT sequencename FROM pg_sequences WHERE schemaname = 'public'
        LOOP
            EXECUTE format(
                'REVOKE USAGE, SELECT ON SEQUENCE public.%I FROM mas_app',
                sequence_name
            );
        END LOOP;
    END IF;
END
$$;
