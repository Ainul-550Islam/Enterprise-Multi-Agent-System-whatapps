-- Reverses 0061_cross_tenant_guard: removes the tenant-immutability triggers
-- and the helper function. No data is touched, so no data-loss opt-in is
-- required. The ENABLE/FORCE RLS assertions are intentionally not reversed —
-- 0055–0060 already establish them, and this migration does not own them.

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
        IF to_regclass('public.' || t) IS NOT NULL THEN
            EXECUTE format('DROP TRIGGER IF EXISTS %I_tenant_immutable ON %I', t, t);
        END IF;
    END LOOP;
END
$$;

DROP FUNCTION IF EXISTS mas_forbid_tenant_reassignment();
