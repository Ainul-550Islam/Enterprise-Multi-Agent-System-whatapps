-- PURPOSE: Align the original integrations connector table with the Rust
-- Connector aggregate without discarding existing configuration metadata.
--
-- The old table stored a generic `kind` and legacy lifecycle terms. Rust owns
-- a provider key, organization binding, auth mode and safe probe telemetry.
-- Backfill maps only deterministic values: old `configuring` becomes pending
-- verification, old `degraded` becomes error, and legacy rows default to the
-- least-privileged `none` auth mode until an operator explicitly binds a
-- credential.

ALTER TABLE connectors
    ADD COLUMN organization_id UUID NULL,
    ADD COLUMN provider TEXT NULL,
    ADD COLUMN display_name TEXT NULL,
    ADD COLUMN auth_mode TEXT NULL,
    ADD COLUMN credential_id UUID NULL,
    ADD COLUMN last_tested_at TIMESTAMPTZ NULL,
    ADD COLUMN last_error TEXT NULL;

UPDATE connectors AS connector
SET organization_id = tenant.organization_id,
    provider = connector.kind,
    auth_mode = 'none',
    status = CASE connector.status
        WHEN 'configuring' THEN 'pending_verification'
        WHEN 'degraded' THEN 'error'
        ELSE connector.status
    END
FROM tenants AS tenant
WHERE connector.tenant_id = tenant.id;

ALTER TABLE connectors
    ALTER COLUMN organization_id SET NOT NULL,
    ALTER COLUMN provider SET NOT NULL,
    ALTER COLUMN auth_mode SET NOT NULL;

ALTER TABLE connectors
    DROP CONSTRAINT IF EXISTS connectors_status_check,
    ADD CONSTRAINT connectors_status_check
        CHECK (status IN ('pending_verification', 'active', 'error', 'disabled')),
    ADD CONSTRAINT connectors_provider_check
        CHECK (provider ~ '^[A-Za-z0-9._-]{1,64}$'),
    ADD CONSTRAINT connectors_auth_mode_check
        CHECK (auth_mode IN ('none', 'oauth2', 'api_key', 'basic')),
    ADD CONSTRAINT connectors_display_name_check
        CHECK (display_name IS NULL OR char_length(display_name) BETWEEN 1 AND 200),
    ADD CONSTRAINT connectors_last_error_check
        CHECK (last_error IS NULL OR char_length(last_error) BETWEEN 1 AND 1024),
    ADD CONSTRAINT connectors_tenant_id_unique UNIQUE (tenant_id, id),
    ADD CONSTRAINT connectors_tenant_organization_fk
        FOREIGN KEY (tenant_id, organization_id)
        REFERENCES tenants (id, organization_id)
        ON DELETE CASCADE,
    ADD CONSTRAINT connectors_tenant_credential_fk
        FOREIGN KEY (tenant_id, credential_id)
        REFERENCES credentials (tenant_id, id)
        ON DELETE RESTRICT;

CREATE INDEX connectors_tenant_provider_idx
    ON connectors (tenant_id, provider, name);
