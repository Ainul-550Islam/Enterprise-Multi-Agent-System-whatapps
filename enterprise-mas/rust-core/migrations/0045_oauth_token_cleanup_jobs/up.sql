-- PURPOSE: Durable fenced cleanup plane for revoked and superseded OAuth token material.
--
-- Owning layer: PostgreSQL OAuth background-lifecycle security boundary.
--
-- Production role: persist externally-visible token deletion work before a
-- worker performs it, lease that work with fences and bounded retries, and
-- record terminal cleanup evidence. The reference is marked deleted only by a
-- successful fenced cleanup settlement after secret-manager deletion.
--
-- Security boundary: no token bytes, provider response bodies, or secret
-- values are stored here. The dedicated refresh role receives only reference
-- metadata required by its narrow OAuth material adapter. The callback role
-- can enqueue cleanup but cannot claim or inspect work.
--
-- Data ownership: OAuth credentials own revocation; this table owns external
-- token cleanup lifecycle; secret managers own token bytes; secret references
-- own the post-deletion availability marker.
--
-- Prohibited responsibilities: best-effort cleanup as the only lifecycle,
-- generic secret deletion, unbounded retries, request-plane access, or marking
-- a reference deleted before external material deletion succeeds.

CREATE TABLE IF NOT EXISTS oauth_token_cleanup_jobs (
    credential_id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    connector_id uuid NOT NULL,
    secret_reference_id uuid NOT NULL,
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
    CONSTRAINT oauth_token_cleanup_jobs_credential_scope_fk
        FOREIGN KEY (tenant_id, credential_id)
        REFERENCES credentials (tenant_id, id)
        ON DELETE CASCADE,
    CONSTRAINT oauth_token_cleanup_jobs_connector_scope_fk
        FOREIGN KEY (tenant_id, connector_id)
        REFERENCES connectors (tenant_id, id)
        ON DELETE CASCADE,
    CONSTRAINT oauth_token_cleanup_jobs_reference_scope_fk
        FOREIGN KEY (tenant_id, secret_reference_id)
        REFERENCES secret_references (tenant_id, id)
        ON DELETE RESTRICT,
    CONSTRAINT oauth_token_cleanup_jobs_lease_shape_check
        CHECK (
            (state = 'leased' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
            OR (state <> 'leased' AND lease_owner IS NULL AND lease_expires_at IS NULL)
        ),
    CONSTRAINT oauth_token_cleanup_jobs_terminal_shape_check
        CHECK (
            (state = 'completed' AND completed_at IS NOT NULL AND dead_at IS NULL AND next_attempt_at IS NULL)
            OR (state = 'dead' AND dead_at IS NOT NULL AND completed_at IS NULL AND next_attempt_at IS NULL)
            OR (state IN ('pending', 'leased', 'failed') AND completed_at IS NULL AND dead_at IS NULL)
        ),
    CONSTRAINT oauth_token_cleanup_jobs_due_shape_check
        CHECK (
            (state IN ('pending', 'failed') AND next_attempt_at IS NOT NULL)
            OR state NOT IN ('pending', 'failed')
        )
);
CREATE INDEX IF NOT EXISTS oauth_token_cleanup_jobs_due_idx
    ON oauth_token_cleanup_jobs (next_attempt_at ASC, credential_id ASC)
    WHERE state IN ('pending', 'failed');
CREATE INDEX IF NOT EXISTS oauth_token_cleanup_jobs_lease_idx
    ON oauth_token_cleanup_jobs (lease_expires_at ASC, credential_id ASC)
    WHERE state = 'leased';
CREATE INDEX IF NOT EXISTS oauth_token_cleanup_jobs_reference_idx
    ON oauth_token_cleanup_jobs (tenant_id, secret_reference_id, credential_id);
CREATE TRIGGER oauth_token_cleanup_jobs_updated_at
    BEFORE UPDATE ON oauth_token_cleanup_jobs
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE oauth_token_cleanup_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE oauth_token_cleanup_jobs FORCE ROW LEVEL SECURITY;
CREATE POLICY oauth_token_cleanup_jobs_isolation ON oauth_token_cleanup_jobs
    USING (tenant_id::text = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id::text = current_setting('app.current_tenant', true));

CREATE TABLE IF NOT EXISTS oauth_token_cleanup_dead_letters (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    credential_id uuid NOT NULL,
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    connector_id uuid NOT NULL,
    secret_reference_id uuid NOT NULL,
    attempts integer NOT NULL CHECK (attempts >= 0 AND attempts <= 8),
    fence bigint NOT NULL CHECK (fence > 0),
    reason text NOT NULL CHECK (char_length(reason) BETWEEN 1 AND 128),
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT oauth_token_cleanup_dead_letters_credential_scope_fk
        FOREIGN KEY (tenant_id, credential_id)
        REFERENCES credentials (tenant_id, id)
        ON DELETE CASCADE,
    CONSTRAINT oauth_token_cleanup_dead_letters_connector_scope_fk
        FOREIGN KEY (tenant_id, connector_id)
        REFERENCES connectors (tenant_id, id)
        ON DELETE CASCADE,
    CONSTRAINT oauth_token_cleanup_dead_letters_reference_scope_fk
        FOREIGN KEY (tenant_id, secret_reference_id)
        REFERENCES secret_references (tenant_id, id)
        ON DELETE RESTRICT,
    CONSTRAINT oauth_token_cleanup_dead_letters_unique_fence
        UNIQUE (credential_id, fence)
);
CREATE INDEX IF NOT EXISTS oauth_token_cleanup_dead_letters_tenant_created_idx
    ON oauth_token_cleanup_dead_letters (tenant_id, created_at DESC, id DESC);
ALTER TABLE oauth_token_cleanup_dead_letters ENABLE ROW LEVEL SECURITY;
ALTER TABLE oauth_token_cleanup_dead_letters FORCE ROW LEVEL SECURITY;
CREATE POLICY oauth_token_cleanup_dead_letters_isolation ON oauth_token_cleanup_dead_letters
    USING (tenant_id::text = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id::text = current_setting('app.current_tenant', true));

-- Backfill revocations that predate this durable cleanup plane. Generic or
-- legacy references are retained as durable dead-letter candidates rather than
-- being guessed/deleted by a worker that does not own them.
INSERT INTO oauth_token_cleanup_jobs (
    credential_id, tenant_id, connector_id, secret_reference_id, state,
    attempts, next_attempt_at, created_at, updated_at
)
SELECT
    credential.id,
    credential.tenant_id,
    credential.connector_id,
    credential.secret_reference_id,
    'pending',
    0,
    now(),
    now(),
    now()
FROM credentials AS credential
JOIN secret_references AS reference
  ON reference.tenant_id = credential.tenant_id
 AND reference.id = credential.secret_reference_id
WHERE credential.purpose = 'oauth2_token'
  AND credential.status = 'revoked'
  AND credential.revoked_at IS NOT NULL
  AND reference.availability <> 'deleted'
ON CONFLICT (credential_id) DO NOTHING;

-- The callback can atomically enqueue a superseded credential's cleanup in its
-- credential-binding transaction but has no read/claim/settlement capability.
GRANT INSERT ON oauth_token_cleanup_jobs TO mas_oauth;

-- The dedicated background role receives only the queue/dead-letter tables.
-- Existing 0044 column grants on credential/reference lifecycle metadata are
-- sufficient for the claim and post-delete availability settlement.
GRANT SELECT, UPDATE ON oauth_token_cleanup_jobs TO mas_oauth_refresh;
GRANT INSERT ON oauth_token_cleanup_dead_letters TO mas_oauth_refresh;

COMMENT ON TABLE oauth_token_cleanup_jobs IS
    'Fenced durable deletion queue for OAuth material. External deletion must '
    'succeed before a worker changes canonical reference availability to deleted.';
COMMENT ON ROLE mas_oauth_refresh IS
    'OAuth lifecycle worker. It may lease bounded OAuth refresh and token-cleanup '
    'jobs, update OAuth credential/reference lifecycle metadata, and write '
    'dead-letter evidence. It cannot read connector config, credential secret_ref '
    'JSON, API keys, tasks, users, raw tokens, OAuth state, or request-plane data.';
