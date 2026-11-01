-- PURPOSE: Reversal of migration 0019.
--
-- Owning layer: the database, applied by the migration runner.
--
-- Production responsibility: remove the `mas_scheduler` role and every grant
-- it was given, so a rollback leaves no orphaned BYPASSRLS role behind. An
-- orphaned role with BYPASSRLS is worse than no role at all: it survives the
-- code that justified it and outlives the review that approved it.
--
-- Security boundary: a down migration is destructive and is therefore only
-- ever applied deliberately, by an operator, never by the application at
-- boot.
--
-- It must NOT:
--   • be run automatically;
--   • drop any object other than the ones 0019 created;
--   • leave a BYPASSRLS role in the cluster after it completes.

REVOKE ALL ON schedule_runs FROM mas_scheduler;
REVOKE ALL ON schedules FROM mas_scheduler;
REVOKE USAGE ON SCHEMA public FROM mas_scheduler;

DROP OWNED BY mas_scheduler;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mas_scheduler') THEN
        DROP ROLE mas_scheduler;
    END IF;
END
$$;
