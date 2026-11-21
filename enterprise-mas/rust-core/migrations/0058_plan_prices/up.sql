-- PURPOSE: Migration 0058 — the plan ↔ billing-provider price mapping.
--
-- A checkout needs to know which provider price (`price_…`) to charge for a
-- platform plan on a given provider and billing interval. Before this table
-- that mapping was a constant compiled into the process; it is data now.
--
-- Uniqueness, two guarantees:
--   • `UNIQUE (tenant_id, provider, plan_id, billing_interval)` — one price
--     per plan/interval/provider within a tenant's catalogue;
--   • `UNIQUE (provider, provider_price_id)` — a provider price id maps to
--     exactly one row, so a webhook that names a price can never be applied
--     against two mappings.
--
-- Data ownership: Rust-owned platform state. Provider price ids are opaque
-- handles; amounts are integer minor units (cents), never floats. No provider
-- secret or key is stored here.
--
-- It must NOT:
--   • float the amount — money is integer minor units with an explicit
--     currency;
--   • let one provider price id map to two plans;
--   • be readable cross-tenant (FORCE RLS below is the control).

CREATE TABLE plan_prices (
    id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id         uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    plan_id           uuid NOT NULL REFERENCES plans (id) ON DELETE CASCADE,
    provider          text NOT NULL CHECK (char_length(provider) BETWEEN 1 AND 32),
    -- Opaque provider handle (`price_…`). Globally unique: a webhook naming
    -- this price must resolve to exactly one mapping.
    provider_price_id text NOT NULL CHECK (char_length(provider_price_id) BETWEEN 1 AND 128),
    currency          text NOT NULL DEFAULT 'usd'
                      CHECK (currency ~ '^[a-z]{3}$'),
    -- Integer minor units (cents). NULL when the provider does not expose an
    -- amount to the platform; never fabricated as zero.
    unit_amount       bigint NULL CHECK (unit_amount IS NULL OR unit_amount >= 0),
    billing_interval  text NOT NULL
                      CHECK (billing_interval IN ('month', 'year', 'one_time')),
    active            boolean NOT NULL DEFAULT true,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, provider, plan_id, billing_interval)
);

CREATE UNIQUE INDEX plan_prices_provider_price_key
    ON plan_prices (provider, provider_price_id);
CREATE INDEX plan_prices_tenant_plan_idx
    ON plan_prices (tenant_id, plan_id, active);

CREATE TRIGGER plan_prices_updated_at BEFORE UPDATE ON plan_prices
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE plan_prices ENABLE ROW LEVEL SECURITY;
ALTER TABLE plan_prices FORCE ROW LEVEL SECURITY;
CREATE POLICY plan_prices_tenant_isolation ON plan_prices
    USING (
        tenant_id = nullif(current_setting('app.current_tenant', true), '')::uuid
    )
    WITH CHECK (
        tenant_id = nullif(current_setting('app.current_tenant', true), '')::uuid
    );

COMMENT ON TABLE plan_prices IS
    'Tenant-scoped plan ↔ provider price catalogue. Amounts are integer minor '
    'units; provider price ids are opaque handles, globally unique.';
