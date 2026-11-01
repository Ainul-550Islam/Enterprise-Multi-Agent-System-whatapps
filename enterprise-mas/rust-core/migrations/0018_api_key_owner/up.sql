-- PURPOSE: Add the owning-user column that `api_keys` projections already read.
--
-- Owning layer: the database, applied by the migration runner.
--
-- Production responsibility: create `api_keys.user_id`. The repository
-- (`crates/persistence/src/repositories/api_keys.rs`) projects `user_id` in
-- its `COLUMNS` list and reads it in `row_from`, and the API-key use-case
-- records which human a key belongs to — but no migration ever created the
-- column. On a real database every `find_by_prefix` therefore failed with
-- `42703 undefined column`, which is the query that authenticates every API
-- key. This migration closes that gap.
--
-- Security boundary: the column is nullable, because a service-account key is
-- owned by a service rather than by a person, and making it NOT NULL would
-- force every such key to borrow a human's identity. A NULL owner means "no
-- human behind this credential", which audit must be able to represent. It
-- carries no authorization weight: the key's authority is its
-- `tenant_id` + `organization_id` + `role` binding, resolved server-side.
--
-- It must NOT:
--   • be made NOT NULL — a service key legitimately has no human owner;
--   • be granted a foreign key that would make deleting a user delete keys;
--   • be used to widen a key's scope; it is attribution only.

ALTER TABLE api_keys
    ADD COLUMN IF NOT EXISTS user_id UUID;

COMMENT ON COLUMN api_keys.user_id IS
    'Attribution only: the human the key was issued to, when there is one. '
    'NULL for service-account keys. Carries no authorization weight — a key''s '
    'authority is its tenant_id + organization_id + role binding, resolved '
    'server-side at verification time.';

-- An index for "which keys does this person hold?", the query the credential
-- management surface needs. Partial, because NULL owners are the common case
-- for service keys and indexing them would be waste.
CREATE INDEX IF NOT EXISTS api_keys_user_id_idx
    ON api_keys (user_id)
    WHERE user_id IS NOT NULL;
