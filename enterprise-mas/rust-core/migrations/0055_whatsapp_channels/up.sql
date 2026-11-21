-- PURPOSE: Migration 0055 — tenant-scoped WhatsApp channel configuration.
--
-- A "channel" is one WhatsApp Business phone number the tenant has connected:
-- its provider identifiers, the Graph API version to drive it with, and the
-- secret *reference* of the webhook verify token. The verify token itself
-- lives in the secret store; only its reference is stored here, exactly like
-- every other credential in the platform.
--
-- Data ownership: Rust-owned platform state. The Python orchestrator never
-- writes this table. Provider identifiers (`phone_number_id`, `waba_id`) are
-- opaque handles — never tokens, never keys.
--
-- It must NOT:
--   • store a WhatsApp access token or verify token — only references;
--   • allow two rows anywhere to claim the same phone number — webhook
--     delivery resolves the owning tenant from this handle, so ambiguity
--     would let one phone number address two tenants;
--   • be readable cross-tenant (FORCE RLS below is the control).

CREATE TABLE whatsapp_channels (
    id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id         uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    display_name      text NOT NULL CHECK (char_length(display_name) BETWEEN 1 AND 200),
    -- Provider identifiers. Opaque handles used to address the Graph API.
    -- `phone_number_id` is globally unique: it is the handle an inbound
    -- webhook names, and the platform must be able to resolve exactly one
    -- owning tenant from it before any tenant scope exists.
    phone_number_id   text NOT NULL CHECK (char_length(phone_number_id) BETWEEN 1 AND 64),
    waba_id           text NULL CHECK (waba_id IS NULL OR char_length(waba_id) BETWEEN 1 AND 64),
    api_version       text NOT NULL DEFAULT 'v21.0'
                      CHECK (char_length(api_version) BETWEEN 1 AND 32),
    status            text NOT NULL DEFAULT 'active'
                      CHECK (status IN ('active', 'paused', 'disconnected')),
    -- Secret reference for the webhook verify token (e.g. `env:MAS_WA_VERIFY_TOKEN`).
    verify_token_ref  text NOT NULL CHECK (char_length(verify_token_ref) BETWEEN 1 AND 300),
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    UNIQUE (phone_number_id)
);

CREATE INDEX whatsapp_channels_tenant_idx
    ON whatsapp_channels (tenant_id, status, created_at DESC);

CREATE TRIGGER whatsapp_channels_updated_at BEFORE UPDATE ON whatsapp_channels
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE whatsapp_channels ENABLE ROW LEVEL SECURITY;
ALTER TABLE whatsapp_channels FORCE ROW LEVEL SECURITY;
CREATE POLICY whatsapp_channels_tenant_isolation ON whatsapp_channels
    USING (
        tenant_id = nullif(current_setting('app.current_tenant', true), '')::uuid
    )
    WITH CHECK (
        tenant_id = nullif(current_setting('app.current_tenant', true), '')::uuid
    );

COMMENT ON TABLE whatsapp_channels IS
    'Tenant-scoped WhatsApp Business channel configuration. Secret material '
    'is referenced, never stored; provider ids are opaque handles.';

-- Pre-scope resolution: an inbound webhook names a `phone_number_id` before
-- any tenant scope exists (the public receiver has no authenticated
-- principal). Following the 0015 philosophy — confine the exception instead
-- of widening the rule — the lookup lives in exactly one SECURITY DEFINER
-- function that exposes exactly one owning tenant (and its organization, for
-- the authoritative scope) and is executable only by mas_app. No BYPASSRLS
-- role is created, and the table's grants are not widened. Zero rows mean
-- "unknown phone number"; the caller must then refuse the delivery.
CREATE FUNCTION mas_resolve_whatsapp_tenant(p_phone_number_id text)
RETURNS TABLE (tenant_id uuid, organization_id uuid)
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = pg_catalog, public
AS $$
    SELECT c.tenant_id, t.organization_id
    FROM whatsapp_channels c
    JOIN tenants t ON t.id = c.tenant_id
    WHERE c.phone_number_id = p_phone_number_id
$$;

REVOKE ALL ON FUNCTION mas_resolve_whatsapp_tenant(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION mas_resolve_whatsapp_tenant(text) TO mas_app;
