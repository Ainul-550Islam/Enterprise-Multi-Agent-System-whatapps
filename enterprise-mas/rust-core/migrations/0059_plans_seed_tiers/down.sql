-- 0059_plans_seed_tiers down.sql

-- Remove the seeded plan entries from plan_prices
DELETE FROM plan_prices WHERE plan_code IN ('free', 'pro', 'business');

-- Remove the seeded entitlements (revert to pre-seed state)
DELETE FROM entitlements WHERE granted_by = 'system_seed_tiers';