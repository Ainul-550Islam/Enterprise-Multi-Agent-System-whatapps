-- PURPOSE: Add the narrowly privileged OAuth callback credential-reference
-- lifecycle without modifying released migration checksums.
--
-- Owning layer: database security/persistence boundary.
--
-- Production role: let the one-shot OAuth callback plane insert only canonical
-- external-secret references and non-secret credential metadata, revoke a
-- superseded connector credential, and bind the new credential atomically to
-- an OAuth2 connector after external token material has been written.
--
-- Security boundary: `mas_oauth` receives no SELECT on token bytes because no
-- SQL token column exists. Its grants are limited to reference metadata and
-- connector/credential lifecycle columns; it cannot read connector config,
-- users, API keys, tasks, outbox rows, or arbitrary credential JSON.
--
-- Data ownership: Vault/another secret manager owns access/refresh token
-- bytes. PostgreSQL owns only canonical reference and credential lifecycle
-- metadata plus connector binding.
--
-- Prohibited responsibilities: raw token storage, a generic credential admin
-- grant, relaxing RLS for mas_app, or pretending a callback succeeded when a
-- reference/credential transaction fails.

-- The callback has already consumed a state that carries tenant+connector
-- metadata. It may create only metadata, then bind it to the proven connector.
-- Column-level SELECT prevents accidental reads of connector config and legacy
-- credential `secret_ref` JSON by generic callback code.
GRANT SELECT (id, tenant_id, auth_mode, credential_id) ON connectors TO mas_oauth;
GRANT UPDATE (credential_id, updated_at) ON connectors TO mas_oauth;

GRANT SELECT (
    id,
    tenant_id,
    connector_id,
    secret_reference_id,
    status,
    revoked_at
) ON credentials TO mas_oauth;
GRANT INSERT ON credentials TO mas_oauth;
GRANT UPDATE (status, revoked_at, updated_at) ON credentials TO mas_oauth;

GRANT INSERT ON secret_references TO mas_oauth;
GRANT UPDATE (key_version, availability, updated_at) ON secret_references TO mas_oauth;

COMMENT ON ROLE mas_oauth IS
    'OAuth callback state consumer. NOLOGIN/BYPASSRLS exists only because a '
    'callback state is intentionally tenant-opaque before its one-shot hash is '
    'consumed. Grants are state consume, constrained secret-reference and '
    'OAuth credential lifecycle writes, connector credential binding, and no '
    'credential/config/token/API-key/task reads.';
