-- PURPOSE: Keep pre-authentication API-key lookup compatible with FORCE RLS
-- without granting the request role BYPASSRLS or trusting tenant headers.
--
-- A presented key supplies only its public, fixed-width prefix. The API binds
-- that prefix to `app.api_key_prefix_lookup` with SET LOCAL inside a dedicated
-- lookup transaction. This SELECT-only policy exposes at most rows matching
-- that exact prefix; the API still verifies the full digest in constant time
-- before constructing an authenticated principal. No tenant scope is inferred
-- from the prefix, and no write policy is widened.

CREATE POLICY api_keys_prefix_lookup
    ON api_keys
    FOR SELECT
    USING (
        prefix = current_setting('app.api_key_prefix_lookup', true)
        AND current_setting('app.api_key_prefix_lookup', true) ~ '^mas_[0-9a-f]{8}$'
    );
