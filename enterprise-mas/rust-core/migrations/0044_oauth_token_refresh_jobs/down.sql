-- PURPOSE: Reverse migration 0044's OAuth refresh job tables and grants.
--
-- Owning layer: database OAuth background-lifecycle security boundary.
--
-- Production role: remove only refresh scheduling/dead-letter artifacts and
-- role privileges introduced by 0044 during a coordinated rollback.
--
-- Security boundary: credentials, canonical secret references, OAuth state,
-- and external token material are retained. Dropping them would silently
-- invalidate live connectors or delete secret-manager evidence.
--
-- Data ownership: this rollback removes job-plane data only; durable credential
-- and Vault material ownership remains unchanged.
--
-- Prohibited responsibilities: deleting raw Vault tokens, dropping shared
-- connector/credential tables, weakening RLS, or revoking unrelated mas_oauth
-- callback grants.

REVOKE INSERT ON oauth_token_refresh_jobs FROM mas_oauth;

REVOKE INSERT ON oauth_token_refresh_dead_letters FROM mas_oauth_refresh;
REVOKE SELECT, UPDATE ON oauth_token_refresh_jobs FROM mas_oauth_refresh;
REVOKE UPDATE (key_version, availability, updated_at) ON secret_references FROM mas_oauth_refresh;
REVOKE SELECT (
    id,
    tenant_id,
    provider,
    path,
    key_version,
    rotation_state,
    availability
) ON secret_references FROM mas_oauth_refresh;
REVOKE UPDATE (status, expires_at, secret_version, revoked_at, updated_at) ON credentials FROM mas_oauth_refresh;
REVOKE SELECT (
    id,
    tenant_id,
    connector_id,
    purpose,
    status,
    expires_at,
    revoked_at,
    secret_reference_id,
    secret_version
) ON credentials FROM mas_oauth_refresh;
REVOKE SELECT (id, tenant_id, provider, auth_mode, credential_id) ON connectors FROM mas_oauth_refresh;
REVOKE USAGE ON SCHEMA public FROM mas_oauth_refresh;

DROP POLICY IF EXISTS oauth_token_refresh_dead_letters_isolation ON oauth_token_refresh_dead_letters;
DROP TABLE IF EXISTS oauth_token_refresh_dead_letters;
DROP POLICY IF EXISTS oauth_token_refresh_jobs_isolation ON oauth_token_refresh_jobs;
DROP TRIGGER IF EXISTS oauth_token_refresh_jobs_updated_at ON oauth_token_refresh_jobs;
DROP TABLE IF EXISTS oauth_token_refresh_jobs;

COMMENT ON ROLE mas_oauth_refresh IS
    'OAuth refresh role privileges revoked by rollback of migration 0044. The '
    'role remains NOLOGIN/BYPASSRLS for explicit operational cleanup; remove '
    'the role only after confirming no deployment still references it.';
