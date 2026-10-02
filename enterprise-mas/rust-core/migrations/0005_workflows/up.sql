-- 0005_workflows: workflows, nodes, edges.

CREATE TABLE workflows (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    organization_id uuid NOT NULL REFERENCES organizations (id) ON DELETE RESTRICT,
    project_id      uuid NULL REFERENCES projects (id) ON DELETE SET NULL,
    name            text   NOT NULL,
    slug            citext NOT NULL,
    status          text   NOT NULL DEFAULT 'draft'
                    CHECK (status IN ('draft', 'validating', 'published', 'disabled', 'archived')),
    spec            jsonb  NOT NULL, -- triggers, limits, metadata
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, slug)
);
CREATE INDEX workflows_tenant_idx ON workflows (tenant_id);
CREATE TRIGGER workflows_updated_at BEFORE UPDATE ON workflows
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE workflows ENABLE ROW LEVEL SECURITY;
ALTER TABLE workflows FORCE ROW LEVEL SECURITY;
CREATE POLICY workflows_isolation ON workflows
    USING (tenant_id::text = current_setting('app.current_tenant', true));

CREATE TABLE workflow_nodes (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    workflow_id uuid NOT NULL REFERENCES workflows (id) ON DELETE CASCADE,
    node_key    text NOT NULL, -- stable key used by edges/config
    node_type   text NOT NULL,
    spec        jsonb NOT NULL, -- action/config/retry/compensation payload
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (workflow_id, node_key)
);
CREATE INDEX workflow_nodes_workflow_idx ON workflow_nodes (workflow_id);
CREATE TRIGGER workflow_nodes_updated_at BEFORE UPDATE ON workflow_nodes
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE workflow_nodes ENABLE ROW LEVEL SECURITY;
ALTER TABLE workflow_nodes FORCE ROW LEVEL SECURITY;
CREATE POLICY workflow_nodes_isolation ON workflow_nodes
    USING (tenant_id::text = current_setting('app.current_tenant', true));

CREATE TABLE workflow_edges (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    workflow_id   uuid NOT NULL REFERENCES workflows (id) ON DELETE CASCADE,
    from_node_key text NOT NULL,
    to_node_key   text NOT NULL,
    condition     jsonb NULL, -- guard expression payload
    created_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (workflow_id, from_node_key, to_node_key),
    FOREIGN KEY (workflow_id, from_node_key)
        REFERENCES workflow_nodes (workflow_id, node_key) ON DELETE CASCADE,
    FOREIGN KEY (workflow_id, to_node_key)
        REFERENCES workflow_nodes (workflow_id, node_key) ON DELETE CASCADE,
    CHECK (from_node_key <> to_node_key)
);
CREATE INDEX workflow_edges_workflow_idx ON workflow_edges (workflow_id);

ALTER TABLE workflow_edges ENABLE ROW LEVEL SECURITY;
ALTER TABLE workflow_edges FORCE ROW LEVEL SECURITY;
CREATE POLICY workflow_edges_isolation ON workflow_edges
    USING (tenant_id::text = current_setting('app.current_tenant', true));
