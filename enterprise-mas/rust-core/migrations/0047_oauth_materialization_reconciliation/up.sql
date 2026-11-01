-- PURPOSE: Durable reconciliation for OAuth callback materialization uncertainty.
--
-- Owning layer: PostgreSQL OAuth callback/background-lifecycle security boundary.
--
-- Production role: record a deterministic token-materialization intent before
-- Vault write, let the callback atomically mark it bound with its credential,
-- and let a fenced worker safely delete an unbound location after a grace
-- period when a process/connection failed between external write and commit.
--
-- Security boundary: rows contain UUID metadata and redacted lifecycle codes
-- only—no token bytes, Vault routes, provider responses, or OAuth code/PKCE
-- material. The callback role cannot claim or dead-letter work; the background
-- role cannot read generic connector configuration or credential JSON.
--
-- Data ownership: this table owns only materialization intent reconciliation;
-- credentials own committed binding truth, material adapters own deterministic
-- external locations, and secret managers own token bytes.
--
-- Prohibited responsibilities: synchronous best-effort deletion as the sole
-- recovery mechanism, external HTTP in SQL, arbitrary secret paths, unbounded
-- retries, or treating unknown durable settlement outcomes as provider errors.

CREATE TABLE IF NOT EXISTS oauth_token_materialization_jobs (
    secret_reference_id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    connector_id uuid NOT NULL,
    credential_id uuid NULL,
    state text NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'leased', 'failed', 'bound', 'deleted', 'dead')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0 AND attempts <= 8),
    next_attempt_at timestamptz NULL,
    lease_owner text NULL CHECK (lease_owner IS NULL OR char_length(lease_owner) BETWEEN 1 AND 128),
    lease_expires_at timestamptz NULL,
    fence bigint NOT NULL DEFAULT 0 CHECK (fence >= 0),
    last_error text NULL CHECK (last_error IS NULL OR char_length(last_error) BETWEEN 1 AND 128),
    completed_at timestamptz NULL,
    dead_at timestamptz NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT oauth_token_materialization_jobs_connector_scope_fk
        FOREIGN KEY (tenant_id, connector_id)
        REFERENCES connectors (tenant_id, id)
        ON DELETE CASCADE,
    CONSTRAINT oauth_token_materialization_jobs_lease_shape_check
        CHECK (
            (state = 'leased' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
            OR (state <> 'leased' AND lease_owner IS NULL AND lease_expires_at IS NULL)
        ),
    CONSTRAINT oauth_token_materialization_jobs_terminal_shape_check
        CHECK (
            (state IN ('bound', 'deleted') AND completed_at IS NOT NULL AND dead_at IS NULL AND next_attempt_at IS NULL)
            OR (state = 'dead' AND dead_at IS NOT NULL AND completed_at IS NULL AND next_attempt_at IS NULL)
            OR (state IN ('pending', 'leased', 'failed') AND completed_at IS NULL AND dead_at IS NULL)
        ),
    CONSTRAINT oauth_token_materialization_jobs_bound_shape_check
        CHECK ((state = 'bound' AND credential_id IS NOT NULL) OR state <> 'bound'),
    CONSTRAINT oauth_token_materialization_jobs_due_shape_check
        CHECK (
            (state IN ('pending', 'failed') AND next_attempt_at IS NOT NULL)
            OR state NOT IN ('pending', 'failed')
        )
);
CREATE INDEX IF NOT EXISTS oauth_token_materialization_jobs_due_idx
    ON oauth_token_materialization_jobs (next_attempt_at ASC, secret_reference_id ASC)
    WHERE state IN ('pending', 'failed');
CREATE INDEX IF NOT EXISTS oauth_token_materialization_jobs_lease_idx
    ON oauth_token_materialization_jobs (lease_expires_at ASC, secret_reference_id ASC)
    WHERE state = 'leased';
CREATE INDEX IF NOT EXISTS oauth_token_materialization_jobs_tenant_connector_idx
    ON oauth_token_materialization_jobs (tenant_id, connector_id, secret_reference_id);
CREATE TRIGGER oauth_token_materialization_jobs_updated_at
    BEFORE UPDATE ON oauth_token_materialization_jobs
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE oauth_token_materialization_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE oauth_token_materialization_jobs FORCE ROW LEVEL SECURITY;
CREATE POLICY oauth_token_materialization_jobs_isolation ON oauth_token_materialization_jobs
    USING (tenant_id::text = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id::text = current_setting('app.current_tenant', true));

CREATE TABLE IF NOT EXISTS oauth_token_materialization_dead_letters (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    secret_reference_id uuid NOT NULL,
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    connector_id uuid NOT NULL,
    attempts integer NOT NULL CHECK (attempts >= 0 AND attempts <= 8),
    fence bigint NOT NULL CHECK (fence > 0),
    reason text NOT NULL CHECK (char_length(reason) BETWEEN 1 AND 128),
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT oauth_token_materialization_dead_letters_connector_scope_fk
        FOREIGN KEY (tenant_id, connector_id)
        REFERENCES connectors (tenant_id, id)
        ON DELETE CASCADE,
    CONSTRAINT oauth_token_materialization_dead_letters_unique_fence
        UNIQUE (secret_reference_id, fence)
);
CREATE INDEX IF NOT EXISTS oauth_token_materialization_dead_letters_tenant_created_idx
    ON oauth_token_materialization_dead_letters (tenant_id, created_at DESC, id DESC);
ALTER TABLE oauth_token_materialization_dead_letters ENABLE ROW LEVEL SECURITY;
ALTER TABLE oauth_token_materialization_dead_letters FORCE ROW LEVEL SECURITY;
CREATE POLICY oauth_token_materialization_dead_letters_isolation ON oauth_token_materialization_dead_letters
    USING (tenant_id::text = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id::text = current_setting('app.current_tenant', true));

-- The callback writes an intent before external materialization and can mark
-- it bound only in the same credential-binding transaction. It has no access
-- to leases, retries, dead letters, or cross-tenant queue enumeration.
GRANT INSERT ON oauth_token_materialization_jobs TO mas_oauth;
GRANT SELECT (secret_reference_id, tenant_id, connector_id, state)
    ON oauth_token_materialization_jobs TO mas_oauth;
GRANT UPDATE (credential_id, state, next_attempt_at, completed_at, updated_at)
    ON oauth_token_materialization_jobs TO mas_oauth;

-- The dedicated background lifecycle role reconciles due intents and writes
-- redacted terminal evidence. Existing 0044 grants provide only needed OAuth
-- credential/reference metadata, never legacy credential JSON or config.
GRANT SELECT, UPDATE ON oauth_token_materialization_jobs TO mas_oauth_refresh;
GRANT INSERT ON oauth_token_materialization_dead_letters TO mas_oauth_refresh;

COMMENT ON TABLE oauth_token_materialization_jobs IS
    'Fenced reconciliation intents covering the external OAuth token write to '
    'PostgreSQL credential-bind transaction gap; contains UUID metadata only.';
