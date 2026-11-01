-- 0004_agents: agents, agent versions, tools, tool permissions.
--
-- Complex manifests/policies ride in the `spec` JSONB payload (full serde
-- shape of the aggregate); indexable identity columns are materialized.

CREATE TABLE agents (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    organization_id uuid NOT NULL REFERENCES organizations (id) ON DELETE RESTRICT,
    name            text   NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    slug            citext NOT NULL,
    status          text   NOT NULL DEFAULT 'draft'
                    CHECK (status IN ('draft', 'active', 'disabled', 'archived')),
    spec            jsonb  NOT NULL, -- full Agent aggregate minus materialized columns
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, slug)
);
CREATE INDEX agents_tenant_idx ON agents (tenant_id);
CREATE TRIGGER agents_updated_at BEFORE UPDATE ON agents
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE agents ENABLE ROW LEVEL SECURITY;
ALTER TABLE agents FORCE ROW LEVEL SECURITY;
CREATE POLICY agents_isolation ON agents
    USING (tenant_id::text = current_setting('app.current_tenant', true));

CREATE TABLE agent_versions (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id     uuid NOT NULL REFERENCES agents (id) ON DELETE CASCADE,
    tenant_id    uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    version      integer NOT NULL CHECK (version > 0),
    deployment   text    NOT NULL DEFAULT 'not_deployed'
                 CHECK (deployment IN ('not_deployed', 'deploying', 'deployed', 'failed', 'rolled_back')),
    spec         jsonb   NOT NULL, -- immutable AgentVersion snapshot
    created_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (agent_id, version)
);
CREATE INDEX agent_versions_agent_idx ON agent_versions (agent_id);

-- Published versions are immutable snapshots.
ALTER TABLE agent_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE agent_versions FORCE ROW LEVEL SECURITY;
CREATE POLICY agent_versions_isolation ON agent_versions
    USING (tenant_id::text = current_setting('app.current_tenant', true));

CREATE TABLE tools (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    name        text NOT NULL,
    kind        text NOT NULL DEFAULT 'http',
    spec        jsonb NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);
CREATE TRIGGER tools_updated_at BEFORE UPDATE ON tools
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE tools ENABLE ROW LEVEL SECURITY;
ALTER TABLE tools FORCE ROW LEVEL SECURITY;
CREATE POLICY tools_isolation ON tools
    USING (tenant_id::text = current_setting('app.current_tenant', true));

CREATE TABLE tool_permissions (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    tool_id     uuid NOT NULL REFERENCES tools (id) ON DELETE CASCADE,
    subject     text NOT NULL, -- agent id / role / project id the grant applies to
    grant_spec  jsonb NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX tool_permissions_key ON tool_permissions (tool_id, subject);
CREATE TRIGGER tool_permissions_updated_at BEFORE UPDATE ON tool_permissions
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE tool_permissions ENABLE ROW LEVEL SECURITY;
ALTER TABLE tool_permissions FORCE ROW LEVEL SECURITY;
CREATE POLICY tool_permissions_isolation ON tool_permissions
    USING (tenant_id::text = current_setting('app.current_tenant', true));
