-- +goose Down
DROP INDEX IF EXISTS idx_memory_grants_expires;
DROP INDEX IF EXISTS idx_memory_grants_tenant;
DROP POLICY IF EXISTS tenant_grant_access ON memory_grants;
ALTER TABLE memory_grants DISABLE ROW LEVEL SECURITY;
DROP TABLE IF EXISTS memory_grants;
