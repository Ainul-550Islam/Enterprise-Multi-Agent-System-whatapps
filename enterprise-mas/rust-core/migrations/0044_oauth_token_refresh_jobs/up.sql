-- PURPOSE: Durable fenced OAuth access-token refresh/revocation job plane.
--
-- Owning layer: PostgreSQL OAuth background-lifecycle security boundary.
--
-- Production role: retain due refresh schedule, bounded retry/backoff, leases,
-- monotonic fences, completed/dead state, and dead-letter evidence separately
-- from raw OAuth material. A dedicated mas_oauth_refresh role can claim and
-- settle these records across tenants without becoming a generic connector or
-- credential reader.
--
-- Security boundary: PostgreSQL stores no access/refresh token bytes. The
-- worker may read only provider/auth/link/reference lifecycle metadata needed
-- to refresh a leased credential, and may update only OAuth lifecycle columns.
-- Normal mas_app traffic retains FORCE RLS and gets no direct job grants.
--
-- Data ownership: secret managers own token material; credentials/references
-- own canonical metadata; this table owns refresh scheduling/lease/fence/dead
-- letter evidence.
--
-- Prohibited responsibilities: storing tokens, exposing a public refresh API,
-- granting generic connector config access, retrying without a bound, or
-- allowing stale workers to settle a newer lease.

CREATE TABLE IF NOT EXISTS oauth_token_refresh_jobs (
    credential_id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    connector_id uuid NOT NULL,
    state text NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'leased', 'failed', 'completed', 'dead')),
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
    CONSTRAINT oauth_token_refresh_jobs_credential_scope_fk
        FOREIGN KEY (tenant_id, credential_id)
        REFERENCES credentials (tenant_id, id)
        ON DELETE CASCADE,
    CONSTRAINT oauth_token_refresh_jobs_connector_scope_fk
        FOREIGN KEY (tenant_id, connector_id)
        REFERENCES connectors (tenant_id, id)
        ON DELETE CASCADE,
    CONSTRAINT oauth_token_refresh_jobs_lease_shape_check
        CHECK (
            (state = 'leased' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
            OR (state <> 'leased' AND lease_owner IS NULL AND lease_expires_at IS NULL)
        ),
    CONSTRAINT oauth_token_refresh_jobs_terminal_shape_check
        CHECK (
            (state = 'completed' AND completed_at IS NOT NULL AND dead_at IS NULL AND next_attempt_at IS NULL)
            OR (state = 'dead' AND dead_at IS NOT NULL AND completed_at IS NULL AND next_attempt_at IS NULL)
            OR (state IN ('pending', 'leased', 'failed') AND completed_at IS NULL AND dead_at IS NULL)
        ),
    CONSTRAINT oauth_token_refresh_jobs_due_shape_check
        CHECK (
            (state IN ('pending', 'failed') AND next_attempt_at IS NOT NULL)
            OR state NOT IN ('pending', 'failed')
        )
);
CREATE INDEX IF NOT EXISTS oauth_token_refresh_jobs_due_idx
    ON oauth_token_refresh_jobs (next_attempt_at ASC, credential_id ASC)
    WHERE state IN ('pending', 'failed');
CREATE INDEX IF NOT EXISTS oauth_token_refresh_jobs_lease_idx
    ON oauth_token_refresh_jobs (lease_expires_at ASC, credential_id ASC)
    WHERE state = 'leased';
CREATE TRIGGER oauth_token_refresh_jobs_updated_at
    BEFORE UPDATE ON oauth_token_refresh_jobs
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE oauth_token_refresh_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE oauth_token_refresh_jobs FORCE ROW LEVEL SECURITY;
CREATE POLICY oauth_token_refresh_jobs_isolation ON oauth_token_refresh_jobs
    USING (tenant_id::text = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id::text = current_setting('app.current_tenant', true));

CREATE TABLE IF NOT EXISTS oauth_token_refresh_dead_letters (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    credential_id uuid NOT NULL,
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    connector_id uuid NOT NULL,
    attempts integer NOT NULL CHECK (attempts >= 0 AND attempts <= 8),
    fence bigint NOT NULL CHECK (fence > 0),
    reason text NOT NULL CHECK (char_length(reason) BETWEEN 1 AND 128),
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT oauth_token_refresh_dead_letters_credential_scope_fk
        FOREIGN KEY (tenant_id, credential_id)
        REFERENCES credentials (tenant_id, id)
        ON DELETE CASCADE,
    CONSTRAINT oauth_token_refresh_dead_letters_connector_scope_fk
        FOREIGN KEY (tenant_id, connector_id)
        REFERENCES connectors (tenant_id, id)
        ON DELETE CASCADE,
    CONSTRAINT oauth_token_refresh_dead_letters_unique_fence
        UNIQUE (credential_id, fence)
);
CREATE INDEX IF NOT EXISTS oauth_token_refresh_dead_letters_tenant_created_idx
    ON oauth_token_refresh_dead_letters (tenant_id, created_at DESC, id DESC);
ALTER TABLE oauth_token_refresh_dead_letters ENABLE ROW LEVEL SECURITY;
ALTER TABLE oauth_token_refresh_dead_letters FORCE ROW LEVEL SECURITY;
CREATE POLICY oauth_token_refresh_dead_letters_isolation ON oauth_token_refresh_dead_letters
    USING (tenant_id::text = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id::text = current_setting('app.current_tenant', true));

-- Add jobs for credentials created by the pre-0044 canonical OAuth callback
-- lifecycle. Only known oauth2_token records with a future/present expiry are
-- backfilled; generic legacy secret references are deliberately never guessed
-- to be structured OAuth documents.
INSERT INTO oauth_token_refresh_jobs (
    credential_id, tenant_id, connector_id, state, attempts, next_attempt_at,
    created_at, updated_at
)
SELECT
    credential.id,
    credential.tenant_id,
    credential.connector_id,
    'pending',
    0,
    GREATEST(now(), credential.expires_at - interval '300 seconds'),
    now(),
    now()
FROM credentials AS credential
JOIN connectors AS connector
  ON connector.tenant_id = credential.tenant_id
 AND connector.id = credential.connector_id
WHERE credential.purpose = 'oauth2_token'
  AND credential.status IN ('active', 'rotating')
  AND credential.revoked_at IS NULL
  AND credential.expires_at IS NOT NULL
  AND connector.auth_mode = 'oauth2'
ON CONFLICT (credential_id) DO NOTHING;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mas_oauth_refresh') THEN
        CREATE ROLE mas_oauth_refresh NOLOGIN BYPASSRLS;
    END IF;
END
$$;
ALTER ROLE mas_oauth_refresh NOLOGIN BYPASSRLS;
GRANT USAGE ON SCHEMA public TO mas_oauth_refresh;

-- The refresh worker receives lifecycle metadata only: provider/auth mode and
-- canonical secret-reference pointers, never connector config or legacy JSON.
GRANT SELECT (id, tenant_id, provider, auth_mode, credential_id) ON connectors TO mas_oauth_refresh;
GRANT SELECT (
    id,
    tenant_id,
    connector_id,
    purpose,
    status,
    expires_at,
    revoked_at,
    secret_reference_id,
    secret_version
) ON credentials TO mas_oauth_refresh;
GRANT UPDATE (status, expires_at, secret_version, revoked_at, updated_at) ON credentials TO mas_oauth_refresh;
GRANT SELECT (
    id,
    tenant_id,
    provider,
    path,
    key_version,
    rotation_state,
    availability
) ON secret_references TO mas_oauth_refresh;
GRANT UPDATE (key_version, availability, updated_at) ON secret_references TO mas_oauth_refresh;
GRANT SELECT, UPDATE ON oauth_token_refresh_jobs TO mas_oauth_refresh;
GRANT INSERT ON oauth_token_refresh_dead_letters TO mas_oauth_refresh;

-- The one-shot callback role may create a refresh job only after it has
-- atomically created/bound a canonical oauth2_token credential. It cannot
-- claim, read, settle, or dead-letter jobs.
GRANT INSERT ON oauth_token_refresh_jobs TO mas_oauth;

COMMENT ON ROLE mas_oauth_refresh IS
    'OAuth token refresh worker. NOLOGIN/BYPASSRLS is limited to leased OAuth '
    'refresh jobs, provider/auth/reference lifecycle metadata, OAuth credential '
    'expiry/revocation settlement, reference version/availability settlement, '
    'and dead-letter inserts. It cannot read connector config, credential '
    'secret_ref JSON, API keys, tasks, users, raw tokens, or OAuth state.';
