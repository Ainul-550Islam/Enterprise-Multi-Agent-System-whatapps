-- +goose Up
-- +statement_begin
CREATE TABLE memory_grants (
    id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL,
    grantee_role TEXT NOT NULL,
    scope TEXT NOT NULL,
    granted_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    expires_at TIMESTAMP WITH TIME ZONE,
    granted_by UUID NOT NULL,
    revoked_at TIMESTAMP WITH TIME ZONE
);
-- Enable RLS
ALTER TABLE memory_grants ENABLE ROW LEVEL SECURITY;
-- Role: mas_memory can read grants for their tenant
CREATE POLICY tenant_grant_access ON memory_grants
    USING (tenant_id = current_setting('app.current_tenant_id'::uuid));
-- Index for tenant scoping
CREATE INDEX idx_memory_grants_tenant ON memory_grants (tenant_id);
-- Index on expires_at for auto-revocation
CREATE INDEX idx_memory_grants_expires ON memory_grants (expires_at);
-- Comment: Grants govern which roles may access which tenant's memory records
-- +statement_end
