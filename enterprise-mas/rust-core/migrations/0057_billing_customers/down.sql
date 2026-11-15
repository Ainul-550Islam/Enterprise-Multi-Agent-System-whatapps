-- 0057_billing_customers down.sql

DROP TRIGGER IF EXISTS billing_customers_updated_at_trigger ON billing_customers;
DROP FUNCTION IF EXISTS billing_customers_audit() ON billing_customers;

DROP INDEX IF EXISTS idx_billing_customers_provider;
DROP INDEX IF EXISTS idx_billing_customers_customer;
DROP INDEX IF EXISTS idx_billing_customers_tenant;

DROP TABLE IF EXISTS billing_customers CASCADE;