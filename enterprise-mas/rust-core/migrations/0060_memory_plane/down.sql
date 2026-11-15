-- +goose Down
DROP INDEX IF EXISTS idx_agent_memory_embedding;
DROP INDEX IF EXISTS idx_agent_memory_tenant_scope;
DROP INDEX IF EXISTS idx_agent_memory_expires;
DROP POLICY IF EXISTS tenant_memory_access ON agent_memory_records;
ALTER TABLE agent_memory_records DISABLE ROW LEVEL SECURITY;
DROP TABLE IF EXISTS agent_memory_records;
-- Drop the confined role
DROP ROLE IF EXISTS mas_memory;
