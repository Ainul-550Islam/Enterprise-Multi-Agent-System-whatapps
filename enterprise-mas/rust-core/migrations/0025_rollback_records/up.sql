-- PURPOSE: Append-only, tenant-scoped governance rollback history.
--
-- A rollback is a forward mutation: it records the source version and the new
-- version created from it. The idempotency key prevents a retry from creating
-- a second rollback record for the same tenant operation.

CREATE TABLE rollback_records (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    resource_kind TEXT NOT NULL CHECK (resource_kind IN ('agent', 'workflow', 'policy')),
    resource_id UUID NOT NULL,
    from_version BIGINT NOT NULL CHECK (from_version > 0),
    to_version BIGINT NOT NULL CHECK (to_version > 0),
    actor JSONB NOT NULL CHECK (jsonb_typeof(actor) = 'object'),
    reason TEXT NOT NULL CHECK (char_length(reason) BETWEEN 1 AND 1024),
    idempotency_key TEXT NOT NULL CHECK (char_length(idempotency_key) BETWEEN 1 AND 256),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX rollback_records_resource_idx
    ON rollback_records (tenant_id, resource_kind, resource_id, created_at DESC);

CREATE TRIGGER rollback_records_no_update
    BEFORE UPDATE ON rollback_records
    FOR EACH ROW EXECUTE FUNCTION mas_forbid_mutation();
CREATE TRIGGER rollback_records_no_delete
    BEFORE DELETE ON rollback_records
    FOR EACH ROW EXECUTE FUNCTION mas_forbid_mutation();

ALTER TABLE rollback_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE rollback_records FORCE ROW LEVEL SECURITY;
CREATE POLICY rollback_records_tenant_isolation ON rollback_records
    USING (tenant_id::text = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id::text = current_setting('app.current_tenant', true));
