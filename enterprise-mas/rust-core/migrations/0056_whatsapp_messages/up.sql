-- PURPOSE: Migration 0056 — tenant-scoped WhatsApp message log.
--
-- Every message that enters or leaves a WhatsApp channel is recorded here:
-- the provider message id (`wamid`), direction, the addresses on each side,
-- the declared content type and a bounded text body. The raw provider webhook
-- payload is kept in `payload` for incident review — it is *untrusted input*
-- and is never re-parsed into authoritative state after the initial
-- verification.
--
-- Data ownership: Rust-owned platform state. Inbound messages are written by
-- the webhook receiver after signature/verify-token checks; outbound rows are
-- written around the guarded Graph API call so the log reflects what the
-- platform actually sent.
--
-- It must NOT:
--   • store media bytes — media lives behind the provider's own storage and
--     is referenced by id inside `payload`;
--   • allow the same provider message id to be recorded twice for a channel
--     (the partial unique index below is the dedup control);
--   • be readable cross-tenant (FORCE RLS below is the control).

CREATE TABLE whatsapp_messages (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id    uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    channel_id   uuid NOT NULL REFERENCES whatsapp_channels (id) ON DELETE CASCADE,
    direction    text NOT NULL CHECK (direction IN ('inbound', 'outbound')),
    -- The provider's message id (wamid). NULL for rows the provider never
    -- acknowledged; the partial unique index keeps every real id unique.
    wamid        text NULL CHECK (wamid IS NULL OR char_length(wamid) BETWEEN 1 AND 200),
    from_addr    text NULL CHECK (from_addr IS NULL OR char_length(from_addr) BETWEEN 1 AND 64),
    to_addr      text NULL CHECK (to_addr IS NULL OR char_length(to_addr) BETWEEN 1 AND 64),
    message_type text NOT NULL DEFAULT 'text'
                 CHECK (message_type IN
                     ('text', 'image', 'audio', 'video', 'document', 'template',
                      'interactive', 'button', 'reaction', 'location', 'unknown')),
    body_text    text NULL CHECK (body_text IS NULL OR char_length(body_text) <= 65536),
    status       text NOT NULL DEFAULT 'received'
                 CHECK (status IN ('received', 'sent', 'delivered', 'read', 'failed')),
    -- Untrusted raw provider payload, preserved for evidence only.
    payload      jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at  timestamptz NOT NULL DEFAULT now(),
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- Dedup: one provider message id per channel. Rows without a wamid (failed
-- sends the provider never acknowledged) are exempt.
CREATE UNIQUE INDEX whatsapp_messages_channel_wamid_key
    ON whatsapp_messages (channel_id, wamid)
    WHERE wamid IS NOT NULL;
CREATE INDEX whatsapp_messages_tenant_channel_idx
    ON whatsapp_messages (tenant_id, channel_id, occurred_at DESC);
CREATE INDEX whatsapp_messages_tenant_direction_idx
    ON whatsapp_messages (tenant_id, direction, occurred_at DESC);

CREATE TRIGGER whatsapp_messages_updated_at BEFORE UPDATE ON whatsapp_messages
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE whatsapp_messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE whatsapp_messages FORCE ROW LEVEL SECURITY;
CREATE POLICY whatsapp_messages_tenant_isolation ON whatsapp_messages
    USING (
        tenant_id = nullif(current_setting('app.current_tenant', true), '')::uuid
    )
    WITH CHECK (
        tenant_id = nullif(current_setting('app.current_tenant', true), '')::uuid
    );

COMMENT ON TABLE whatsapp_messages IS
    'Tenant-scoped WhatsApp message log. `payload` holds untrusted provider '
    'JSON kept for evidence; it never becomes authoritative state.';
