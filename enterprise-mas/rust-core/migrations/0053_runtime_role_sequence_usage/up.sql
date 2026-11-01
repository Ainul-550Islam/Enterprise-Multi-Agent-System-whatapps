-- PURPOSE: Migration 0053 — restore the runtime role's sequence usage across public.
--
-- Owning layer: database schema.
--
-- Production responsibility: 0021 granted `mas_app` USAGE on every public
-- sequence that existed at that point; sequences created afterwards
-- (0033's broker receipt and message sequences) were granted to
-- `mas_worker` only, leaving the runtime role unable to advance them — and
-- any future bigserial/IDENTITY column in `public` reintroduces the same
-- blind spot. This migration re-runs the 0021 sweep so the invariant
-- "every public sequence is usable by the runtime role" holds again.
--
-- Data ownership: no data is touched; privileges only.
--
-- MUST NOT: widen any worker-plane privilege or grant beyond USAGE/SELECT.

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
