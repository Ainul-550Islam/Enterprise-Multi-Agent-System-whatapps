-- PURPOSE: Enforce opaque OAuth reference invariants at the database boundary.
--
-- Owning layer: PostgreSQL OAuth credential/state security boundary.
--
-- Production role: reject a new OAuth credential or PKCE authorization state
-- unless its canonical reference uses the UUID-derived opaque locator and an
-- OAuth credential's legacy compatibility JSON contains only that UUID.
--
-- Security boundary: SECURITY DEFINER trigger functions expose no reference
-- path or secret values. They exist because callback roles intentionally have
-- no `secret_references.path` SELECT grant after migration 0046; application
-- bugs or compromised role SQL cannot reintroduce Vault routes into metadata.
--
-- Data ownership: PostgreSQL owns relationship/invariant validation; OAuth
-- material adapters own construction of real external Vault paths at I/O time.
--
-- Prohibited responsibilities: external I/O, token parsing/storage, generic
-- secret validation, changing existing non-OAuth references, or exposing a
-- raw provider/Vault locator in an error.

CREATE OR REPLACE FUNCTION mas_assert_oauth_credential_reference()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    stored_locator text;
BEGIN
    IF NEW.purpose <> 'oauth2_token' THEN
        RETURN NEW;
    END IF;
    IF NEW.secret_reference_id IS NULL THEN
        RAISE EXCEPTION 'OAuth credential requires a canonical secret reference';
    END IF;
    SELECT reference.path
      INTO stored_locator
      FROM public.secret_references AS reference
     WHERE reference.tenant_id = NEW.tenant_id
       AND reference.id = NEW.secret_reference_id;
    IF stored_locator IS DISTINCT FROM ('oauth-token/' || NEW.secret_reference_id::text) THEN
        RAISE EXCEPTION 'OAuth credential reference locator violates canonical opaque format';
    END IF;
    IF NEW.secret_ref IS DISTINCT FROM jsonb_build_object('id', NEW.secret_reference_id::text) THEN
        RAISE EXCEPTION 'OAuth credential legacy reference must contain only its canonical id';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS credentials_oauth_reference_invariant ON credentials;
CREATE TRIGGER credentials_oauth_reference_invariant
    BEFORE INSERT OR UPDATE OF purpose, secret_reference_id, secret_ref
    ON credentials
    FOR EACH ROW
    EXECUTE FUNCTION mas_assert_oauth_credential_reference();

CREATE OR REPLACE FUNCTION mas_assert_oauth_pkce_reference()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    stored_locator text;
BEGIN
    SELECT reference.path
      INTO stored_locator
      FROM public.secret_references AS reference
     WHERE reference.tenant_id = NEW.tenant_id
       AND reference.id = NEW.pkce_verifier_secret_id;
    IF stored_locator IS DISTINCT FROM ('oauth-pkce/' || NEW.pkce_verifier_secret_id::text) THEN
        RAISE EXCEPTION 'OAuth PKCE reference locator violates canonical opaque format';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS oauth_authorization_states_pkce_reference_invariant
    ON oauth_authorization_states;
CREATE TRIGGER oauth_authorization_states_pkce_reference_invariant
    BEFORE INSERT OR UPDATE OF pkce_verifier_secret_id
    ON oauth_authorization_states
    FOR EACH ROW
    EXECUTE FUNCTION mas_assert_oauth_pkce_reference();

COMMENT ON FUNCTION mas_assert_oauth_credential_reference() IS
    'Rejects OAuth credential raw Vault paths/legacy JSON; emits no secret locator.';
COMMENT ON FUNCTION mas_assert_oauth_pkce_reference() IS
    'Rejects OAuth PKCE raw Vault paths; emits no secret locator.';
