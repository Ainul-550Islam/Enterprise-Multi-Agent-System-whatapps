-- PURPOSE: Migration 0059 — durable billing checkout session records.
--
-- Every checkout the platform initiates is recorded here *before* the browser
-- is redirected to the provider: the idempotency key the caller supplied, the
-- provider session handle once one exists, and the plan/price the session was
-- opened for. This is what makes checkout exactly-once: a retried request with
-- the same idempotency key returns the recorded session instead of opening a
-- second one, and a webhook that later names the provider session resolves to
-- the row this platform owns.
--
-- Data ownership: Rust-owned platform state. `provider_session_id` is an
-- opaque handle (`cs_…`); no provider secret or card data is stored here.
--
-- It must NOT:
--   • open a second provider session for a repeated idempotency key — the
--     unique index below is the control;
--   • trust a session id from a webhook body without matching it to a row in
--     this table;
--   • be readable cross-tenant (FORCE RLS below is the control).

CREATE TABLE billing_checkout_sessions (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    -- Caller-supplied idempotency key. Unique per tenant: a retried checkout
    -- is the same checkout.
    idempotency_key     text NOT NULL CHECK (char_length(idempotency_key) BETWEEN 1 AND 200),
    plan_id             uuid NOT NULL REFERENCES plans (id),
    -- The plan's stable code at checkout time. Recorded here so the webhook
    -- effect can name a plan without trusting the event body or joining at
    -- apply time.
    plan_code           text NOT NULL CHECK (char_length(plan_code) BETWEEN 1 AND 64),
    plan_price_id       uuid NULL REFERENCES plan_prices (id),
    provider            text NOT NULL CHECK (char_length(provider) BETWEEN 1 AND 32),
    -- Opaque provider session handle, set once the provider accepted the
    -- session request. NULL while creation is in flight.
    provider_session_id text NULL
                        CHECK (provider_session_id IS NULL
                            OR char_length(provider_session_id) BETWEEN 1 AND 128),
    state               text NOT NULL DEFAULT 'created'
                        CHECK (state IN ('created', 'completed', 'expired', 'failed')),
    -- Where the browser returns. Free-form https URLs validated in the
    -- application layer; stored so the flow survives a process restart.
    success_url         text NOT NULL CHECK (char_length(success_url) BETWEEN 1 AND 2000),
    cancel_url          text NOT NULL CHECK (char_length(cancel_url) BETWEEN 1 AND 2000),
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, idempotency_key)
);

CREATE UNIQUE INDEX billing_checkout_sessions_provider_key
    ON billing_checkout_sessions (provider, provider_session_id)
    WHERE provider_session_id IS NOT NULL;
CREATE INDEX billing_checkout_sessions_tenant_idx
    ON billing_checkout_sessions (tenant_id, state, created_at DESC);

CREATE TRIGGER billing_checkout_sessions_updated_at BEFORE UPDATE ON billing_checkout_sessions
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE billing_checkout_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE billing_checkout_sessions FORCE ROW LEVEL SECURITY;
CREATE POLICY billing_checkout_sessions_tenant_isolation ON billing_checkout_sessions
    USING (
        tenant_id = nullif(current_setting('app.current_tenant', true), '')::uuid
    )
    WITH CHECK (
        tenant_id = nullif(current_setting('app.current_tenant', true), '')::uuid
    );

COMMENT ON TABLE billing_checkout_sessions IS
    'Durable checkout session records. Exactly-once per (tenant, idempotency '
    'key); provider session ids resolve only against rows this platform owns.';
