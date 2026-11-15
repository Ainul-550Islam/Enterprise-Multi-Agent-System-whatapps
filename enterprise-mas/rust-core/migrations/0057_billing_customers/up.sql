-- 0057_billing_customers up.sql
-- billing_customers table RLS-armed per-tenant

-- Table: billing_customers
-- Stores Stripe customer binding per tenant, idempotent by (tenant_id, provider, customer_id)
CREATE TABLE IF NOT EXISTS billing_customers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    provider TEXT NOT NULL DEFAULT 'stripe',
    customer_id TEXT NOT NULL,
    stripe_secret_ref TEXT NOT NULL,   -- reference to secret store key
    status TEXT NOT NULL DEFAULT 'active',  -- active | disabled | error
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now,

    -- One binding per (tenant, provider, customer_id)
    UNIQUE(tenant_id, provider, customer_id),

-- Row-level security: tenants can only access their own rows
    CONSTRAINT tenant_billing_customers_check CHECK (tenant_id = current_setting('app.tenant_id')::uuid),

    -- Indexes for common queries
    INDEX idx_billing_customers_tenant ON billing_customers (tenant_id),
    INDEX idx_billing_customers_customer ON billing_customers (customer_id),
    INDEX idx_billing_customers_provider ON billing_customers (provider)
);

-- Audit trigger: record every UPDATE
CREATE OR REPLACE FUNCTION billing_customers_audit()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER billing_customers_updated_at_trigger
    BEFORE UPDATE ON billing_customers
    FOR EACH ROW EXECUTE FUNCTION billing_customers_audit();