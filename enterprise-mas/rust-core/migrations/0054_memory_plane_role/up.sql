-- PURPOSE: Migration 0054 — isolate the durable memory plane from Rust-owned data.
--
-- Durable, episodic and semantic memory outlives a single execution and is
-- written by the Python orchestrator, which owns AI execution. Rust owns the
-- schema and the tenant boundary; Python writes only through the dedicated
-- `mas_memory` role. This mirrors the RAG plane (0028): the role is NOLOGIN,
-- receives no BYPASSRLS, and can touch exactly one table.
--
-- This migration is additive. The production Python DSN must authenticate as
-- a deployment login that assumes only `mas_memory`; `scripts/provision-plane-roles.sh`
-- grants that role LOGIN. Task-local memory is per-execution and never stored
-- here — it lives and dies with the execution that produced it.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mas_memory') THEN
        CREATE ROLE mas_memory NOLOGIN NOBYPASSRLS;
    END IF;
END
$$;

CREATE TABLE memory_records (
    id TEXT PRIMARY KEY CHECK (char_length(id) BETWEEN 1 AND 200),
    tenant_id UUID NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    scope TEXT NOT NULL CHECK (scope IN ('episodic', 'durable', 'semantic')),
    content TEXT NOT NULL CHECK (char_length(content) BETWEEN 1 AND 65536),
    sensitivity TEXT NOT NULL DEFAULT 'internal'
        CHECK (sensitivity IN ('public', 'internal', 'sensitive', 'restricted')),
    retain_until_epoch_ms BIGINT NULL CHECK (retain_until_epoch_ms IS NULL OR retain_until_epoch_ms > 0),
    delete_on_execution_end BOOLEAN NOT NULL DEFAULT FALSE,
    agent_id TEXT NULL CHECK (agent_id IS NULL OR char_length(agent_id) <= 200),
    execution_id TEXT NULL CHECK (execution_id IS NULL OR char_length(execution_id) <= 200),
    task_id TEXT NULL CHECK (task_id IS NULL OR char_length(task_id) <= 200),
    created_epoch_ms BIGINT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    redacted BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id)
);

CREATE INDEX memory_records_tenant_scope_idx
    ON memory_records (tenant_id, scope, created_epoch_ms DESC);
CREATE INDEX memory_records_tenant_agent_idx
    ON memory_records (tenant_id, agent_id);
CREATE INDEX memory_records_tenant_execution_idx
    ON memory_records (tenant_id, execution_id);
CREATE INDEX memory_records_tenant_expiry_idx
    ON memory_records (tenant_id, retain_until_epoch_ms)
    WHERE retain_until_epoch_ms IS NOT NULL;

CREATE TRIGGER memory_records_updated_at BEFORE UPDATE ON memory_records
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE memory_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_records FORCE ROW LEVEL SECURITY;
CREATE POLICY memory_records_tenant_isolation ON memory_records
    USING (
        tenant_id = nullif(current_setting('app.current_tenant', true), '')::uuid
    )
    WITH CHECK (
        tenant_id = nullif(current_setting('app.current_tenant', true), '')::uuid
    );

-- 0001 grants default privileges to mas_app; remove that grant explicitly so
-- only the dedicated memory role can access this table.
REVOKE ALL ON memory_records FROM PUBLIC;
REVOKE ALL ON memory_records FROM mas_app;
GRANT USAGE ON SCHEMA public TO mas_memory;
GRANT SELECT, INSERT, UPDATE, DELETE ON memory_records TO mas_memory;
REVOKE CREATE ON SCHEMA public FROM mas_memory;
