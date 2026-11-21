-- PURPOSE: Migration 0061 — cross-tenant hardening for the WhatsApp and
-- billing tables introduced by 0055–0060.
--
-- Two guarantees, enforced at the database so no application bug can lift
-- them:
--
-- 1. **Tenant immutability.** A row's `tenant_id` is fixed at insert. Without
--    this, a single UPDATE with a wrong scope could migrate a channel, a
--    message, a customer mapping or an application claim from one tenant to
--    another — the worst possible cross-tenant defect, and one RLS cannot
--    prevent (the updating tenant's own policy admits the row before and
--    after). `mas_forbid_tenant_reassignment()` refuses the transition.
--
-- 2. **RLS convergence.** ENABLE + FORCE ROW LEVEL SECURITY is re-asserted on
--    every table this hardening covers. The statements are idempotent: a
--    database that received 0055–0060 is unchanged, and one where a table
--    somehow lost FORCE converges back to the enforced state.
--
-- It must NOT:
--   • allow a tenant_id to change, for any role or reason — a legitimate move
--     between tenants is a delete + re-insert, which the append-only and
--     data-loss guards make loud;
--   • weaken any policy created by 0055–0060.

-- 1. Tenant immutability trigger function --------------------------------

CREATE OR REPLACE FUNCTION mas_forbid_tenant_reassignment() RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id THEN
        RAISE EXCEPTION
            'table %: tenant_id is immutable (row cannot migrate between tenants)',
            TG_TABLE_NAME;
    END IF;
    RETURN NEW;
END;
$$;

-- 2. Install the guard and re-assert RLS on every covered table -----------

DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'whatsapp_channels',
        'whatsapp_messages',
        'billing_customers',
        'plan_prices',
        'billing_checkout_sessions',
        'billing_event_applications'
    ] LOOP
        EXECUTE format(
            'ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
        EXECUTE format(
            'ALTER TABLE %I FORCE ROW LEVEL SECURITY', t);
        EXECUTE format(
            'DROP TRIGGER IF EXISTS %I_tenant_immutable ON %I', t, t);
        EXECUTE format(
            'CREATE TRIGGER %I_tenant_immutable BEFORE UPDATE ON %I '
            'FOR EACH ROW EXECUTE FUNCTION mas_forbid_tenant_reassignment()',
            t, t);
    END LOOP;
END
$$;

COMMENT ON FUNCTION mas_forbid_tenant_reassignment() IS
    'Refuses UPDATEs that would change tenant_id — rows cannot migrate '
    'between tenants.';
