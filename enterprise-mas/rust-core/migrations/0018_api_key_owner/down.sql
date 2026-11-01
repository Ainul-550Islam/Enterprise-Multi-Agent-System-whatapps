-- PURPOSE: Reversal of migration 0018.
--
-- Owning layer: the database, applied by the migration runner.
--
-- Production responsibility: drop the index and the column added by 0018 so a
-- rollback returns `api_keys` to its pre-0018 shape.
--
-- Security boundary: a down migration is a destructive operation and is
-- therefore only ever applied deliberately, by an operator, never by the
-- application at boot.
--
-- It must NOT:
--   • be run automatically;
--   • drop any column other than the one 0018 added.

DROP INDEX IF EXISTS api_keys_user_id_idx;

ALTER TABLE api_keys
    DROP COLUMN IF EXISTS user_id;
