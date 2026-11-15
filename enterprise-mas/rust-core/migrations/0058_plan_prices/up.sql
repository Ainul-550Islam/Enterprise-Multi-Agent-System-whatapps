-- 0058_plan_prices up.sql
-- plan_prices table: maps internal plan_code → Stripe price_id + metadata

-- Table: plan_prices
-- Holds Stripe price metadata for checkout/portal session creation.
-- One row per (plan_code, provider), ensured unique by plan_code.
CREATE TABLE IF NOT EXISTS plan_prices (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    plan_code TEXT NOT NULL,           -- internal plan identifier (e.g. "free", "pro", "business")
    provider TEXT NOT NULL DEFAULT 'stripe',
    price_id TEXT NOT NULL UNIQUE,     -- Stripe price ID (e.g. "price_123456789")
    interval TEXT NOT NULL,            -- "monthly" or "yearly"
    currency TEXT NOT NULL,            -- ISO 4217 currency code (e.g. "usd", "eur")
    amount INTEGER NOT NULL,           -- amount in smallest currency unit (cents for USD)
    stripe_secret_ref TEXT NOT NULL,   -- reference to secret store key
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now,

    -- One price per (plan_code, provider)
    CONSTRAINT uq_plan_code_provider UNIQUE (plan_code, provider),

    -- Row-level security: tenants can only read their own prices
    CONSTRAINT tenant_plan_prices_check CHECK (tenant_id IS NULL OR tenant_id = current_setting('app.tenant_id')::uuid),

    -- Indexes
    INDEX idx_plan_prices_code ON plan_prices (plan_code),
    INDEX idx_plan_prices_currency ON plan_prices (currency),
    INDEX idx_plan_prices_interval ON plan_prices (interval)
);

-- Audit trigger
CREATE OR REPLACE FUNCTION plan_prices_audit()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER plan_prices_updated_at_trigger
    BEFORE UPDATE ON plan_prices
    FOR EACH ROW EXECUTE FUNCTION plan_prices_audit();