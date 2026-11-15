-- 0058_plan_prices down.sql

DROP TRIGGER IF EXISTS plan_prices_updated_at_trigger ON plan_prices;
DROP FUNCTION IF EXISTS plan_prices_audit() ON plan_prices;

DROP INDEX IF EXISTS idx_plan_prices_interval;
DROP INDEX IF EXISTS idx_plan_prices_currency;
DROP INDEX IF EXISTS idx_plan_prices_code;

DROP TABLE IF EXISTS plan_prices CASCADE;