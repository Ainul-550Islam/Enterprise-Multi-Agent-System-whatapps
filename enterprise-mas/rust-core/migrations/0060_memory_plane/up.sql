-- +goose Up
-- +statement_begin
CREATE TABLE agent_memory_records (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL,
    agent_id UUID NOT NULL,
    scope TEXT NOT NULL,
    content JSONB NOT NULL,
    embedding vector(1536),
    expires_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
-- +statement_end
-- Enable RLS
ALTER TABLE agent_memory_records ENABLE ROW LEVEL SECURITY;
-- Create confined role for memory plane
CREATE ROLE IF NOT EXISTS mas_memory NOLOGIN;
GRANT mas_memory ON TABLE agent_memory_records;
-- Policy: tenants can only access their own records
CREATE POLICY tenant_memory_access ON agent_memory_records
    USING (tenant_id = current_setting('app.current_tenant_id'::uuid));
-- Index for vector similarity
CREATE INDEX idx_agent_memory_embedding ON agent_memory_records USING ivfflat (embedding vector_cosine_ops)
    WITH (metrics = 10, build_count = 100);
-- Index on tenant and scope for scoping queries
CREATE INDEX idx_agent_memory_tenant_scope ON agent_memory_records (tenant_id, scope);
-- Index on expires_at for retention cleanup
CREATE INDEX idx_agent_memory_expires ON agent_memory_records (expires_at);
