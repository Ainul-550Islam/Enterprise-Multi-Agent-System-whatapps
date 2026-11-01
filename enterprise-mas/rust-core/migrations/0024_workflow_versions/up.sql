-- PURPOSE: Immutable, tenant-scoped snapshots of published workflow graphs.
--
-- A publish or rollback appends a new row. Historical graph bytes, checksum,
-- topological order and rollback provenance are never rewritten.

CREATE TABLE workflow_versions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    workflow_id UUID NOT NULL REFERENCES workflows (id) ON DELETE CASCADE,
    version INTEGER NOT NULL CHECK (version > 0),
    graph JSONB NOT NULL CHECK (jsonb_typeof(graph) = 'object'),
    topo_order TEXT[] NOT NULL DEFAULT '{}',
    checksum TEXT NOT NULL CHECK (checksum ~ '^[a-f0-9]{64}$'),
    rollback_of UUID NULL REFERENCES workflow_versions (id) ON DELETE RESTRICT,
    created_by UUID NULL REFERENCES users (id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, workflow_id, version),
    UNIQUE (tenant_id, id)
);

CREATE INDEX workflow_versions_lookup_idx
    ON workflow_versions (tenant_id, workflow_id, version DESC);

CREATE TRIGGER workflow_versions_no_update
    BEFORE UPDATE ON workflow_versions
    FOR EACH ROW EXECUTE FUNCTION mas_forbid_mutation();
CREATE TRIGGER workflow_versions_no_delete
    BEFORE DELETE ON workflow_versions
    FOR EACH ROW EXECUTE FUNCTION mas_forbid_mutation();

ALTER TABLE workflow_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE workflow_versions FORCE ROW LEVEL SECURITY;
CREATE POLICY workflow_versions_tenant_isolation ON workflow_versions
    USING (tenant_id::text = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id::text = current_setting('app.current_tenant', true));

-- Preserve the current published snapshot as v1 before new publishes begin.
-- The graph lives inside workflows.spec; older rows without a graph are not
-- guessed into a version record.
INSERT INTO workflow_versions (
    tenant_id,
    workflow_id,
    version,
    graph,
    topo_order,
    checksum
)
SELECT
    tenant_id,
    id,
    GREATEST(COALESCE(NULLIF(spec ->> 'published_version', '')::integer, 1), 1),
    COALESCE(spec -> 'graph', '{}'::jsonb),
    COALESCE(ARRAY(SELECT jsonb_array_elements_text(spec -> 'topological_order')), '{}'),
    encode(
        digest(convert_to(COALESCE((spec -> 'graph')::text, '{}'::text), 'UTF8'), 'sha256'),
        'hex'
    )
FROM workflows
WHERE status = 'published'
  AND jsonb_typeof(spec -> 'graph') = 'object'
ON CONFLICT (tenant_id, workflow_id, version) DO NOTHING;
