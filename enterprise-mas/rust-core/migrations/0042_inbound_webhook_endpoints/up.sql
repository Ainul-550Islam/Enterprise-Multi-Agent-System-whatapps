-- PURPOSE: Canonical tenant-scoped public inbound-webhook endpoint registry.
--
-- Owning layer: database schema, request-plane configuration, and public
-- ingress routing boundary.
--
-- Production role: map an opaque UUID route to server-owned tenant,
-- organization, source, signing-secret reference, key-version policy, and
-- enabled state without allowing public requests to name a tenant or a secret
-- manager path.
--
-- Security boundary: normal `mas_app` traffic is FORCE-RLS scoped; the
-- `mas_inbound` role receives column-level SELECT only for enabled-route
-- lookup and remains unable to read secret-reference paths or credentials.
--
-- Data ownership: this table owns ingress endpoint configuration; canonical
-- secret_references owns non-secret manager metadata; the secret manager owns
-- signing-key material; receipt rows own verified deliveries.
--
-- Prohibited responsibilities: storing secret bytes, granting public callers
-- a tenant selector, allowing an endpoint to reference another tenant's key,
-- or serving as an outbound webhook configuration table.

CREATE OR REPLACE FUNCTION mas_inbound_key_versions_valid(values_to_check text[])
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT values_to_check IS NOT NULL
       AND cardinality(values_to_check) <= 3
       AND (
            SELECT count(*) = count(DISTINCT candidate.value)
            FROM unnest(values_to_check) AS candidate(value)
       )
       AND NOT EXISTS (
            SELECT 1
            FROM unnest(values_to_check) AS candidate(value)
            WHERE candidate.value !~ '^[A-Za-z0-9._-]{1,128}$'
       );
$$;

CREATE TABLE IF NOT EXISTS inbound_webhook_endpoints (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    organization_id uuid NOT NULL,
    source text NOT NULL CHECK (source ~ '^[A-Za-z0-9._:-]{1,128}$'),
    signing_secret_reference_id uuid NOT NULL,
    required_key_version text NULL
        CHECK (required_key_version IS NULL OR required_key_version ~ '^[A-Za-z0-9._-]{1,128}$'),
    -- During rotation, exactly these provider `kid` values may be resolved.
    -- An empty array permits only unsigned-with-kid-absent mode unless an exact
    -- required_key_version is configured; it is never an arbitrary Vault
    -- version selector.
    accepted_key_versions text[] NOT NULL DEFAULT '{}'::text[]
        CHECK (mas_inbound_key_versions_valid(accepted_key_versions)),
    -- If an allowlist is configured, it must explicitly contain the required
    -- version too; this prevents a direct SQL write from creating an endpoint
    -- whose public `kid` policy disagrees with the exact-version policy.
    CHECK (
        cardinality(accepted_key_versions) = 0
        OR required_key_version IS NULL
        OR required_key_version = ANY (accepted_key_versions)
    ),
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT inbound_webhook_endpoints_tenant_org_scope_fk
        FOREIGN KEY (tenant_id, organization_id)
        REFERENCES tenants (id, organization_id)
        ON DELETE CASCADE,
    CONSTRAINT inbound_webhook_endpoints_secret_scope_fk
        FOREIGN KEY (tenant_id, signing_secret_reference_id)
        REFERENCES secret_references (tenant_id, id)
        ON DELETE RESTRICT,
    CONSTRAINT inbound_webhook_endpoints_tenant_source_unique
        UNIQUE (tenant_id, source)
);

CREATE INDEX IF NOT EXISTS inbound_webhook_endpoints_enabled_route_idx
    ON inbound_webhook_endpoints (id)
    WHERE enabled;

CREATE TRIGGER inbound_webhook_endpoints_updated_at
    BEFORE UPDATE ON inbound_webhook_endpoints
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE inbound_webhook_endpoints ENABLE ROW LEVEL SECURITY;
ALTER TABLE inbound_webhook_endpoints FORCE ROW LEVEL SECURITY;
CREATE POLICY inbound_webhook_endpoints_isolation ON inbound_webhook_endpoints
    USING (tenant_id::text = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id::text = current_setting('app.current_tenant', true));

GRANT SELECT, INSERT, UPDATE, DELETE ON inbound_webhook_endpoints TO mas_app;
-- Public ingress needs only these non-secret fields after a route UUID has
-- been parsed. In particular, the role cannot read `secret_references.path`.
GRANT SELECT (
    id,
    tenant_id,
    organization_id,
    source,
    signing_secret_reference_id,
    required_key_version,
    accepted_key_versions,
    enabled
) ON inbound_webhook_endpoints TO mas_inbound;
