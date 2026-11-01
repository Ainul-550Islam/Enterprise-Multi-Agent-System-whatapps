-- 0002_tenancy: organizations, tenants, projects (Organization → Tenant → Project).

CREATE TABLE organizations (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    legal_name   text    NOT NULL CHECK (length(legal_name) BETWEEN 1 AND 200),
    display_name text    NOT NULL CHECK (length(display_name) BETWEEN 1 AND 200),
    slug         citext  NOT NULL UNIQUE CHECK (slug ~ '^[a-z0-9][a-z0-9-]{1,62}$'),
    status       text    NOT NULL DEFAULT 'active'
                 CHECK (status IN ('active', 'suspended', 'archived')),
    settings     jsonb   NOT NULL DEFAULT '{}'::jsonb,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE TRIGGER organizations_updated_at BEFORE UPDATE ON organizations
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

-- Organizations are org-scoped: visible when the caller's org context matches.
ALTER TABLE organizations ENABLE ROW LEVEL SECURITY;
ALTER TABLE organizations FORCE ROW LEVEL SECURITY;
CREATE POLICY organizations_isolation ON organizations
    USING (id::text = current_setting('app.current_org', true));

CREATE TABLE tenants (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id uuid NOT NULL REFERENCES organizations (id) ON DELETE RESTRICT,
    name            text   NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    slug            citext NOT NULL CHECK (slug ~ '^[a-z0-9][a-z0-9-]{1,62}$'),
    status          text   NOT NULL DEFAULT 'provisioning'
                    CHECK (status IN ('provisioning', 'active', 'suspended', 'archived')),
    environment     text   NOT NULL
                    CHECK (environment IN ('development', 'staging', 'production')),
    isolation_mode  text   NOT NULL DEFAULT 'shared_rls'
                    CHECK (isolation_mode IN ('shared_rls', 'schema_per_tenant', 'dedicated_database')),
    settings        jsonb  NOT NULL DEFAULT '{}'::jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (organization_id, slug)
);
CREATE INDEX tenants_organization_idx ON tenants (organization_id);
CREATE TRIGGER tenants_updated_at BEFORE UPDATE ON tenants
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenants FORCE ROW LEVEL SECURITY;
-- A tenant row is visible to its own context; organization-level catalog ops
-- additionally match the org context.
CREATE POLICY tenants_isolation ON tenants
    USING (id::text = current_setting('app.current_tenant', true)
        OR organization_id::text = current_setting('app.current_org', true));

-- Needed by the projects dual-link FK below.
ALTER TABLE tenants ADD CONSTRAINT tenants_id_org_key UNIQUE (id, organization_id);

CREATE TABLE projects (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    organization_id uuid NOT NULL REFERENCES organizations (id) ON DELETE RESTRICT,
    name            text   NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    slug            citext NOT NULL CHECK (slug ~ '^[a-z0-9][a-z0-9-]{1,62}$'),
    description     text   NULL,
    status          text   NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'archived')),
    config          jsonb  NOT NULL DEFAULT '{}'::jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, slug),
    -- Dual-link coherence: the project's org must equal its tenant's org.
    FOREIGN KEY (tenant_id, organization_id)
        REFERENCES tenants (id, organization_id) ON DELETE CASCADE DEFERRABLE
);
CREATE INDEX projects_tenant_idx ON projects (tenant_id);
CREATE TRIGGER projects_updated_at BEFORE UPDATE ON projects
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE projects FORCE ROW LEVEL SECURITY;
CREATE POLICY projects_isolation ON projects
    USING (tenant_id::text = current_setting('app.current_tenant', true));
