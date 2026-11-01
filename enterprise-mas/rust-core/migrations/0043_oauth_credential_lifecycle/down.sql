-- PURPOSE: Reverse only migration 0043's callback-role grants.
--
-- Owning layer: database security/persistence boundary.
--
-- Production role: restore the pre-0043 OAuth callback role, which may consume
-- state and inspect reference metadata but cannot write connector/credential
-- lifecycle records.
--
-- Security boundary: this does not delete canonical credentials or external
-- token material because a rollback after callbacks may otherwise orphan a
-- connector binding or revoke live access without a coordinated migration.
--
-- Data ownership: existing OAuth reference and credential metadata remains
-- owned by the forward schema and must be reconciled operationally before
-- rolling back application code.
--
-- Prohibited responsibilities: deleting Vault token bytes, dropping shared
-- tables, weakening RLS, or revoking unrelated mas_oauth state grants.

REVOKE UPDATE (credential_id, updated_at) ON connectors FROM mas_oauth;
REVOKE SELECT (id, tenant_id, auth_mode, credential_id) ON connectors FROM mas_oauth;

REVOKE UPDATE (status, revoked_at, updated_at) ON credentials FROM mas_oauth;
REVOKE INSERT ON credentials FROM mas_oauth;
REVOKE SELECT (
    id,
    tenant_id,
    connector_id,
    secret_reference_id,
    status,
    revoked_at
) ON credentials FROM mas_oauth;

REVOKE UPDATE (key_version, availability, updated_at) ON secret_references FROM mas_oauth;
REVOKE INSERT ON secret_references FROM mas_oauth;

COMMENT ON ROLE mas_oauth IS
    'OAuth callback state consumer. NOLOGIN/BYPASSRLS exists only because a '
    'callback state is intentionally tenant-opaque before its one-shot hash is '
    'consumed. Grants are OAuth state SELECT/UPDATE and secret-reference '
    'metadata SELECT; it cannot read credentials, tokens, API keys or tasks.';
