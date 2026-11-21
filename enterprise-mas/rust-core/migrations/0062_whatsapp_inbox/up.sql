-- PURPOSE: Migration 0062 — WhatsApp conversations and message templates.
--
-- Conversations are the inbox unit the dashboard addresses: one contact on
-- one channel, with human-handoff and STOP/START opt-out flags. Templates
-- are the only legal outbound after the 24-hour customer-care window.
--
-- Data ownership: Rust-owned platform state. Provider template names are
-- opaque handles. Media bytes are never stored here.
--
-- It must NOT:
--   • omit tenant_id (the repo pattern is a column, not a subquery);
--   • run without ENABLE + FORCE RLS;
--   • use a GUC other than app.current_tenant.

CREATE TABLE whatsapp_conversations (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    channel_id          uuid NOT NULL REFERENCES whatsapp_channels (id) ON DELETE CASCADE,
    contact             text NOT NULL CHECK (char_length(contact) BETWEEN 1 AND 64),
    handoff             boolean NOT NULL DEFAULT false,
    opt_out             boolean NOT NULL DEFAULT false,
    window_expires_at   timestamptz NULL,
    last_inbound_at     timestamptz NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, channel_id, contact)
);

CREATE INDEX whatsapp_conversations_tenant_channel_idx
    ON whatsapp_conversations (tenant_id, channel_id, updated_at DESC);

CREATE TRIGGER whatsapp_conversations_updated_at BEFORE UPDATE ON whatsapp_conversations
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE whatsapp_conversations ENABLE ROW LEVEL SECURITY;
ALTER TABLE whatsapp_conversations FORCE ROW LEVEL SECURITY;
CREATE POLICY whatsapp_conversations_tenant_isolation ON whatsapp_conversations
    USING (
        tenant_id = nullif(current_setting('app.current_tenant', true), '')::uuid
    )
    WITH CHECK (
        tenant_id = nullif(current_setting('app.current_tenant', true), '')::uuid
    );

CREATE TABLE whatsapp_templates (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id    uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    channel_id   uuid NOT NULL REFERENCES whatsapp_channels (id) ON DELETE CASCADE,
    name         text NOT NULL CHECK (char_length(name) BETWEEN 1 AND 512),
    language     text NOT NULL CHECK (char_length(language) BETWEEN 2 AND 16),
    status       text NOT NULL DEFAULT 'draft'
                 CHECK (status IN ('draft', 'pending', 'approved', 'rejected')),
    body         text NULL CHECK (body IS NULL OR char_length(body) <= 1024),
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, channel_id, name, language)
);

CREATE INDEX whatsapp_templates_tenant_channel_idx
    ON whatsapp_templates (tenant_id, channel_id, name);

CREATE TRIGGER whatsapp_templates_updated_at BEFORE UPDATE ON whatsapp_templates
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE whatsapp_templates ENABLE ROW LEVEL SECURITY;
ALTER TABLE whatsapp_templates FORCE ROW LEVEL SECURITY;
CREATE POLICY whatsapp_templates_tenant_isolation ON whatsapp_templates
    USING (
        tenant_id = nullif(current_setting('app.current_tenant', true), '')::uuid
    )
    WITH CHECK (
        tenant_id = nullif(current_setting('app.current_tenant', true), '')::uuid
    );

GRANT SELECT, INSERT, UPDATE, DELETE ON whatsapp_conversations TO mas_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON whatsapp_templates TO mas_app;

-- Tenant immutability (0061's function): a conversation or template cannot
-- migrate between tenants by UPDATE.
CREATE TRIGGER whatsapp_conversations_tenant_immutable
    BEFORE UPDATE ON whatsapp_conversations
    FOR EACH ROW EXECUTE FUNCTION mas_forbid_tenant_reassignment();
CREATE TRIGGER whatsapp_templates_tenant_immutable
    BEFORE UPDATE ON whatsapp_templates
    FOR EACH ROW EXECUTE FUNCTION mas_forbid_tenant_reassignment();

COMMENT ON TABLE whatsapp_conversations IS
    'Tenant-scoped WhatsApp inbox conversations (one contact per channel).';
COMMENT ON TABLE whatsapp_templates IS
    'Tenant-scoped WhatsApp message templates used after the 24h window.';
