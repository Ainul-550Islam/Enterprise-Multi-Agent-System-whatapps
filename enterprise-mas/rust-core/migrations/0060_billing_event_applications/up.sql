-- PURPOSE: Migration 0060 — the durable per-tenant application claim for
-- billing provider events.
--
-- The two-sided exactly-once story:
--   • `billing_webhook_receipts` (0020) records that a provider event arrived
--     — platform-wide, before a tenant is known, append-only evidence;
--   • this table records that *this tenant* applied *this event* — under
--     tenant scope, after the tenant has been resolved from
--     `billing_customers`, with the effect summarised for audit.
--
-- The application step claims the row with `INSERT … ON CONFLICT DO NOTHING`
-- *before* mutating subscription state: if the claim inserts, the effect is
-- applied in the same transaction; if it conflicts, the event was already
-- applied to this tenant and the handler reports success without re-applying.
-- A replay therefore cannot apply a subscription change twice, even when the
-- platform receipt was finished by a different replica.
--
-- Data ownership: Rust-owned platform state. The effect column is a short
-- code the application layer writes ('subscription_upserted', 'ignored',
-- …) — never provider text.
--
-- It must NOT:
--   • let one (tenant, provider, event_id) apply twice — the unique index
--     below is the claim;
--   • be mutated after the claim (append-only trigger below);
--   • be readable cross-tenant (FORCE RLS below is the control).

CREATE TABLE billing_event_applications (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    provider    text NOT NULL CHECK (char_length(provider) BETWEEN 1 AND 32),
    event_id    text NOT NULL CHECK (char_length(event_id) BETWEEN 1 AND 200),
    -- The platform receipt this application was derived from. NULL when the
    -- receipt was recorded by an older build that did not link them.
    receipt_id  uuid NULL,
    event_type  text NOT NULL DEFAULT '' CHECK (char_length(event_type) <= 200),
    effect      text NOT NULL CHECK (char_length(effect) BETWEEN 1 AND 64),
    applied_at  timestamptz NOT NULL DEFAULT now(),
    -- The claim: one application per tenant and provider event.
    UNIQUE (tenant_id, provider, event_id)
);

CREATE INDEX billing_event_applications_tenant_idx
    ON billing_event_applications (tenant_id, applied_at DESC);

-- Append-only: the claim is evidence of a completed effect. Deleting or
-- editing it would let a replay re-apply.
CREATE TRIGGER billing_event_applications_no_delete BEFORE DELETE ON billing_event_applications
    FOR EACH ROW EXECUTE FUNCTION mas_forbid_mutation();

ALTER TABLE billing_event_applications ENABLE ROW LEVEL SECURITY;
ALTER TABLE billing_event_applications FORCE ROW LEVEL SECURITY;
CREATE POLICY billing_event_applications_tenant_isolation ON billing_event_applications
    USING (
        tenant_id = nullif(current_setting('app.current_tenant', true), '')::uuid
    )
    WITH CHECK (
        tenant_id = nullif(current_setting('app.current_tenant', true), '')::uuid
    );

COMMENT ON TABLE billing_event_applications IS
    'Durable per-tenant application claim for provider billing events. The '
    'unique (tenant_id, provider, event_id) makes the applied effect '
    'exactly-once; the row is append-only evidence.';
