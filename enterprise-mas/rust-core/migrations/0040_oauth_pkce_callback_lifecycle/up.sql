-- PURPOSE: Production-safe additive OAuth PKCE callback lifecycle hardening.
--
-- Owning layer: database schema/security boundary.
--
-- Production role: preserve the immutable 0039 migration checksum while
-- normalizing legacy state hashes and granting the narrowly privileged OAuth
-- callback role only the secret-reference lifecycle write needed after a
-- verifier is consumed from the external secret manager.
--
-- Security boundary: `mas_oauth` stays unable to read credentials, tokens,
-- API keys, tenants, connectors, or arbitrary secret material. The grant is
-- column-level and supports only availability/timestamp settlement.
--
-- Data ownership: OAuth state and secret-reference metadata remain in
-- PostgreSQL; verifier bytes remain outside PostgreSQL.
--
-- Prohibited responsibilities: writing verifier/token values, granting a
-- generic callback worker role, bypassing one-shot state consumption, or
-- changing historical migration 0039.

-- A SHA-256 hash is canonically lower-case hexadecimal. Before tightening the
-- check, fail explicitly if a legacy deployment has case-only duplicate rows:
-- collapsing them would otherwise make a potentially live callback ambiguous.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM oauth_authorization_states
        GROUP BY lower(state_hash)
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION
            'cannot canonicalize oauth_authorization_states.state_hash: case-only duplicate hashes exist; resolve before applying migration 0040';
    END IF;
END
$$;

UPDATE oauth_authorization_states
SET state_hash = lower(state_hash)
WHERE state_hash <> lower(state_hash);

ALTER TABLE oauth_authorization_states
    DROP CONSTRAINT IF EXISTS oauth_authorization_states_state_hash_check,
    ADD CONSTRAINT oauth_authorization_states_state_hash_check
        CHECK (state_hash ~ '^[a-f0-9]{64}$');

-- `mas_oauth` is defined in 0039 as a NOLOGIN/BYPASSRLS callback role. It must
-- never mutate reference provider/path/version or write secret bytes; callback
-- settlement only makes the one-time PKCE reference unreadable.
GRANT UPDATE (availability, updated_at) ON secret_references TO mas_oauth;

COMMENT ON ROLE mas_oauth IS
    'OAuth callback state consumer. NOLOGIN/BYPASSRLS exists only because a '
    'callback state is intentionally tenant-opaque before its one-shot hash is '
    'consumed. Grants are OAuth state SELECT/UPDATE plus secret-reference '
    'metadata SELECT and lifecycle-only availability UPDATE; it cannot read '
    'credentials, tokens, API keys or tasks.';
