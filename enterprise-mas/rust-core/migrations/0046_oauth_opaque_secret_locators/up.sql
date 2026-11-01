-- PURPOSE: Replace persisted OAuth Vault routes with opaque canonical locators.
--
-- Owning layer: PostgreSQL OAuth credential/reference security boundary.
--
-- Production role: ensure OAuth PKCE and OAuth token records persist only an
-- opaque locator derived from the canonical UUID. Vault API routes are derived
-- at runtime by the narrow, operator-configured material adapters and are not
-- database metadata, audit fields, queues, or API output.
--
-- Security boundary: this migration never reads/writes token bytes. It removes
-- duplicated Vault paths from legacy OAuth credential JSON and prevents the
-- callback/refresh roles from retaining a broad reference SELECT grant.
--
-- Data ownership: external Vault route construction belongs exclusively to
-- OAuth material adapters; PostgreSQL owns opaque lifecycle identifiers.
--
-- Prohibited responsibilities: rotating/deleting external material, changing
-- generic non-OAuth secret references, restoring legacy raw paths, or relaxing
-- row-level security.

-- An OAuth token reference and a live PKCE verifier reference must not share a
-- UUID. The two opaque namespaces are intentionally distinct so fail closed if
-- historical corruption would make normalization ambiguous.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM credentials AS credential
        JOIN oauth_authorization_states AS state
          ON state.tenant_id = credential.tenant_id
         AND state.pkce_verifier_secret_id = credential.secret_reference_id
        WHERE credential.purpose = 'oauth2_token'
    ) THEN
        RAISE EXCEPTION
            'cannot normalize OAuth secret locators: a token credential and PKCE state share a reference id';
    END IF;
END
$$;

-- The token material adapter derives its external path from configured prefix
-- plus `id`; retain no Vault route in the canonical reference row.
UPDATE secret_references AS reference
SET path = 'oauth-token/' || reference.id::text,
    updated_at = now()
FROM credentials AS credential
WHERE credential.tenant_id = reference.tenant_id
  AND credential.secret_reference_id = reference.id
  AND credential.purpose = 'oauth2_token'
  AND reference.provider = 'vault'
  AND reference.path IS DISTINCT FROM ('oauth-token/' || reference.id::text);

-- PKCE verifier records use their own opaque namespace. State history is kept
-- intentionally, so this covers consumed/expired records as well as live ones.
UPDATE secret_references AS reference
SET path = 'oauth-pkce/' || reference.id::text,
    updated_at = now()
FROM oauth_authorization_states AS state
WHERE state.tenant_id = reference.tenant_id
  AND state.pkce_verifier_secret_id = reference.id
  AND reference.provider = 'vault'
  AND reference.path IS DISTINCT FROM ('oauth-pkce/' || reference.id::text);

-- `credentials.secret_ref` predates canonical references and remains NOT NULL
-- for compatibility. OAuth rows retain only their non-secret canonical id.
UPDATE credentials
SET secret_ref = jsonb_build_object('id', secret_reference_id::text),
    updated_at = now()
WHERE purpose = 'oauth2_token'
  AND secret_reference_id IS NOT NULL
  AND secret_ref IS DISTINCT FROM jsonb_build_object('id', secret_reference_id::text);

-- Revoke an older table-wide grant before re-granting precisely the opaque
-- metadata columns the callback needs. It cannot inspect a generic reference
-- or legacy credential JSON through this role.
REVOKE ALL PRIVILEGES ON secret_references FROM mas_oauth;
GRANT SELECT (
    id,
    tenant_id,
    provider,
    key_version,
    rotation_state,
    availability
) ON secret_references TO mas_oauth;
GRANT INSERT ON secret_references TO mas_oauth;
GRANT UPDATE (availability, updated_at) ON secret_references TO mas_oauth;
-- The callback must distinguish an old canonical OAuth credential from a
-- generic legacy credential before it enqueues OAuth-only external cleanup.
GRANT SELECT (purpose) ON credentials TO mas_oauth;

-- The background role derives its OAuth material locator from the reference id
-- and operator configuration, so it must not retain a read grant for the
-- generic `path` column at all.
REVOKE ALL PRIVILEGES ON secret_references FROM mas_oauth_refresh;
GRANT SELECT (
    id,
    tenant_id,
    provider,
    key_version,
    rotation_state,
    availability
) ON secret_references TO mas_oauth_refresh;
GRANT UPDATE (key_version, availability, updated_at) ON secret_references TO mas_oauth_refresh;

COMMENT ON COLUMN secret_references.path IS
    'Generic provider locator. OAuth rows store only opaque oauth-token/<uuid> '
    'or oauth-pkce/<uuid> locators; their material adapters derive Vault paths '
    'from operator configuration and the UUID at runtime.';
