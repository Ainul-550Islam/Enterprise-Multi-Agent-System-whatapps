-- PURPOSE: Reverse migration 0048's OAuth opaque-reference validation triggers.
--
-- Owning layer: database OAuth credential/state security boundary.
--
-- Production role: remove only the validation trigger functions during a
-- coordinated rollback; it intentionally leaves opaque locator data intact.
--
-- Security boundary: this rollback does not restore raw Vault paths or broaden
-- callback/background grants.
--
-- Data ownership: reference/credential/state rows remain owned by their
-- original tables; only their database invariant guards are removed.
--
-- Prohibited responsibilities: fabricating external paths, deleting OAuth
-- material, weakening RLS, or modifying generic secret references.

DROP TRIGGER IF EXISTS oauth_authorization_states_pkce_reference_invariant
    ON oauth_authorization_states;
DROP FUNCTION IF EXISTS mas_assert_oauth_pkce_reference();
DROP TRIGGER IF EXISTS credentials_oauth_reference_invariant ON credentials;
DROP FUNCTION IF EXISTS mas_assert_oauth_credential_reference();
