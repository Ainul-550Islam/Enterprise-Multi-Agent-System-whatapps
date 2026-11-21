-- PURPOSE: Migration 0057 — the tenant ↔ billing-provider customer mapping.
--
-- The checkout flow and the webhook path meet at exactly this table. The
-- checkout side creates the provider customer for a tenant; the webhook side
-- resolves *which tenant* a provider event belongs to by looking up the
-- provider customer id in this table — server-side truth, never a tenant id
-- from the event body.
--
-- Two uniqueness constraints, two different guarantees:
--   • `UNIQUE (tenant_id, provider)` — a tenant has at most one customer per
--     provider, so checkout is idempotent;
--   • `UNIQUE (provider, provider_customer_id)` — a provider customer maps to
--     exactly one tenant, so an event can never be applied to two tenants.
--
-- Data ownership: Rust-owned platform state. Provider identifiers are opaque
-- handles (`cus_…`); **no provider secret, API key, card number or customer
-- credential is ever stored here**.
--
-- It must NOT:
--   • trust a customer id that arrived in an unverified body — the lookup
--     happens only after signature verification;
--   • allow one provider customer to be linked to two tenants;
--   • be readable cross-tenant (FORCE RLS below is the control).

CREATE TABLE billing_customers (
    id                   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id            uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    provider             text NOT NULL CHECK (char_length(provider) BETWEEN 1 AND 32),
    -- Opaque provider handle. Unique across tenants: this is the anti-
    -- cross-tenant control on the webhook resolution path.
    provider_customer_id text NOT NULL CHECK (char_length(provider_customer_id) BETWEEN 1 AND 128),
    email                text NULL CHECK (email IS NULL OR char_length(email) BETWEEN 1 AND 320),
    created_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, provider)
);

CREATE UNIQUE INDEX billing_customers_provider_customer_key
    ON billing_customers (provider, provider_customer_id);
CREATE INDEX billing_customers_tenant_idx
    ON billing_customers (tenant_id, provider);

CREATE TRIGGER billing_customers_updated_at BEFORE UPDATE ON billing_customers
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE billing_customers ENABLE ROW LEVEL SECURITY;
ALTER TABLE billing_customers FORCE ROW LEVEL SECURITY;
CREATE POLICY billing_customers_tenant_isolation ON billing_customers
    USING (
        tenant_id = nullif(current_setting('app.current_tenant', true), '')::uuid
    )
    WITH CHECK (
        tenant_id = nullif(current_setting('app.current_tenant', true), '')::uuid
    );

COMMENT ON TABLE billing_customers IS
    'Tenant ↔ billing-provider customer mapping. The webhook path resolves a '
    'tenant only through this table, after signature verification.';

-- Pre-scope resolution: the webhook handler has verified the signature but
-- has no tenant scope yet. Following the 0015 philosophy — confine the
-- exception instead of widening the rule — the (provider, customer) -> tenant
-- lookup lives in exactly one SECURITY DEFINER function that exposes exactly
-- one owning tenant (and its organization, for the authoritative scope) and is
-- executable only by mas_app. No BYPASSRLS role is created, and the table's
-- grants are not widened. Zero rows mean "unknown customer"; the caller must
-- then reject the event — never fall back to a tenant id from the event body.
CREATE FUNCTION mas_resolve_billing_tenant(p_provider text, p_customer_id text)
RETURNS TABLE (tenant_id uuid, organization_id uuid)
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = pg_catalog, public
AS $$
    SELECT c.tenant_id, t.organization_id
    FROM billing_customers c
    JOIN tenants t ON t.id = c.tenant_id
    WHERE c.provider = p_provider AND c.provider_customer_id = p_customer_id
$$;

REVOKE ALL ON FUNCTION mas_resolve_billing_tenant(text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION mas_resolve_billing_tenant(text, text) TO mas_app;
