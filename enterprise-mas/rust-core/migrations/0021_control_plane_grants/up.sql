-- PURPOSE: Migration 0021 — the sequence grant the runtime role needs.
--
-- Owning layer: database schema.
--
-- Production responsibility: migration 0001 deliberately excluded sequences
-- from `ALTER DEFAULT PRIVILEGES` ("ids come from pgcrypto"), which is the
-- right default for every table whose primary key is a uuid. Migration 0020
-- then introduced `subscription_events.seq bigserial` — a monotonic counter for
-- the append-only subscription history, because two transitions can share an
-- `occurred_at` and the history's order must still be the order they happened.
-- Under `mas_app` that INSERT failed with SQLSTATE 42501: the role could
-- INSERT into the table but had no USAGE on the sequence behind it. In
-- production that means every subscription transition — including the one a
-- billing webhook performs — returned a 500.
--
-- This migration closes that gap, and does it for every sequence in the
-- schema rather than just the one column, so the next `bigserial` does not
-- reintroduce the same failure.
--
-- It must NOT:
--   • grant anything to a role other than `mas_app` (the plane roles have
--     their own, narrower grants in 0015/0017/0019);
--   • be edited after it has been applied anywhere;
--   • drop or rewrite an existing table.

-- 0021_control_plane_grants: sequence usage for the runtime role.

DO $$
DECLARE
    sequence_name text;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mas_app') THEN
        FOR sequence_name IN
            SELECT sequencename FROM pg_sequences WHERE schemaname = 'public'
        LOOP
            EXECUTE format(
                'GRANT USAGE, SELECT ON SEQUENCE public.%I TO mas_app',
                sequence_name
            );
        END LOOP;
    END IF;
END
$$;
