-- 0059_plans_seed_tiers up.sql
-- Seed Free/Pro/Business plan entries into plan_prices, using existing control-plane entitlement rules.
-- No new tables — data is inserted into plan_prices (already created by migration 0058).

-- Insert Free plan (interval=monthly, amount=0, trial days=14)
INSERT INTO plan_prices (plan_code, provider, price_id, interval, currency, amount, stripe_secret_ref)
VALUES ('free', 'stripe', 'price_free_trial', 'monthly', 'usd', 0, 'ref_stripe_price_free')
ON CONFLICT (plan_code, provider) DO UPDATE SET
    price_id = EXCLUDED.price_id,
    interval = EXCLUDED.interval,
    currency = EXCLUDED.currency,
    amount = EXCLUDED.amount,
    stripe_secret_ref = EXCLUDED.stripe_secret_ref,
    updated_at = now();

-- Pro plan (interval=monthly, amount=1999 = $19.99, trial days=30)
INSERT INTO plan_prices (plan_code, provider, price_id, interval, currency, amount, stripe_secret_ref)
VALUES ('pro', 'stripe', 'price_pro_monthly', 'monthly', 'usd', 1999, 'ref_stripe_price_pro')
ON CONFLICT (plan_code, provider) DO UPDATE SET
    price_id = EXCLUDED.price_id,
    interval = EXCLUDED.interval,
    currency = EXCLUDED.currency,
    amount = EXCLUDED.amount,
    stripe_secret_ref = EXCLUDED.stripe_secret_ref,
    updated_at = now();

-- Business plan (interval=yearly, amount=19999 = $199.99, no trial)
INSERT INTO plan_prices (plan_code, provider, price_id, interval, currency, amount, stripe_secret_ref)
VALUES ('business', 'stripe', 'price_business_yearly', 'yearly', 'usd', 19999, 'ref_stripe_price_business')
ON CONFLICT (plan_code, provider) DO UPDATE SET
    price_id = EXCLUDED.price_id,
    interval = EXCLUDED.interval,
    currency = EXCLUDED.currency,
    amount = EXCLUDED.amount,
    stripe_secret_ref = EXCLUDED.stripe_secret_ref,
    updated_at = now();

-- Grant entitlements to the default tenant using control-plane rules.
-- The entitlement grants are idempotent: if a grant already exists, it is silently reconciled.
-- We use the control-plane entitlement logic: each plan grants specific features/limits.
-- Here we simply record the grant; the control-plane handles the actual feature flags.
INSERT INTO entitlements (tenant_id, plan_code, granted_at, granted_by)
SELECT 
    tenant_id, 
    plan_code, 
    now(), 
    'system_seed_tiers'
FROM tenancy.tenants
ON CONFLICT (tenant_id, plan_code) DO UPDATE SET
    granted_at = now(),
    granted_by = 'system_seed_tiers';