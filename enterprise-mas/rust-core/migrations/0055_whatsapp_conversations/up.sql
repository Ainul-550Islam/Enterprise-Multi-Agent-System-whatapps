-- 0055_whatsapp_conversations up.sql
-- Creates conversation and messages tables with RLS and window expiry

-- WhatsApp conversations per (account, agent)
CREATE TABLE IF NOT EXISTS conversations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    whatsapp_account_id UUID NOT NULL,
    agent_id UUID NOT NULL,
    wamid UUID UNIQUE, -- the last/inbound message ID that started this conversation
    status TEXT NOT NULL DEFAULT 'open', -- open | closed | archived
    opt_in BOOLEAN NOT NULL DEFAULT FALSE, -- whether user opted in
    handoff_to_agent UUID, -- agent who took handoff, if any
    window_expires_at TIMESTAMPTZ NOT NULL, -- 24h from last inbound
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Foreign key to whatsapp_accounts (enforced via app logic + RLS)
    CONSTRAINT fk_conv_account FOREIGN KEY (whatsapp_account_id) REFERENCES whatsapp_accounts(id) ON DELETE CASCADE
);

-- Row-level security: tenants see only their own conversations
CREATE POLICY "tenant_conversations_select" ON conversations
    FOR SELECT USING (whatsapp_account_id IN (
        SELECT id FROM whatsapp_accounts WHERE tenant_id = current_setting('app.tenant_id')::UUID
    ));

CREATE POLICY "tenant_conversations_insert" ON conversations
    FOR INSERT WITH CHECK (whatsapp_account_id IN (
        SELECT id FROM whatsapp_accounts WHERE tenant_id = current_setting('app.tenant_id')::UUID
    ));

CREATE POLICY "tenant_conversations_update" ON conversations
    FOR UPDATE USING (whatsapp_account_id IN (
        SELECT id FROM whatsapp_accounts WHERE tenant_id = current_setting('app.tenant_id')::UUID
    ));

CREATE POLICY "tenant_conversations_delete" ON conversations
    FOR DELETE USING (whatsapp_account_id IN (
        SELECT id FROM whatsapp_accounts WHERE tenant_id = current_setting('app.tenant_id')::UUID
    ));

-- Indexes
CREATE INDEX IF NOT EXISTS ix_conversations_account ON conversations(whatsapp_account_id);
CREATE INDEX IF NOT EXISTS ix_conversations_agent ON conversations(agent_id);
CREATE INDEX IF NOT EXISTS ix_conversations_window ON conversations(window_expires_at);
CREATE INDEX IF NOT EXISTS ix_conversations_wamid ON conversations(wamid);

-- Messages table: every inbound/outbound message related to a conversation
CREATE TABLE IF NOT EXISTS messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id UUID NOT NULL,
    wamid UUID NOT NULL, -- WhatsApp message ID (unique per account + direction)
    direction TEXT NOT NULL DEFAULT 'inbound', -- inbound | outbound
    kind TEXT NOT NULL, -- text | image | audio | video | document | location | interactive | button | reaction | unsupported
    status TEXT NOT NULL DEFAULT 'pending', -- pending | sent | delivered | read | failed
    payload_json JSONB NOT NULL, -- raw Meta payload (stored for audit/debug)
    executed_at TIMESTAMPTZ, -- when outbound was sent (null for inbound)
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Ensure wamid is unique per (conversation_id, direction) — idempotent insert
    CONSTRAINT uq_wamid_per_conv_direction UNIQUE (conversation_id, direction, wamid),

    -- Foreign key
    CONSTRAINT fk_msg_conv FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
);

-- RLS policies for messages
CREATE POLICY "tenant_messages_select" ON messages
    FOR SELECT USING (conversation_id IN (
        SELECT id FROM conversations WHERE whatsapp_account_id IN (
            SELECT id FROM whatsapp_accounts WHERE tenant_id = current_setting('app.tenant_id')::UUID
        )
    ));

CREATE POLICY "tenant_messages_insert" ON messages
    FOR INSERT WITH CHECK (conversation_id IN (
        SELECT id FROM conversations WHERE whatsapp_account_id IN (
            SELECT id FROM whatsapp_accounts WHERE tenant_id = current_setting('app.tenant_id')::UUID
        )
    ));

CREATE POLICY "tenant_messages_update" ON messages
    FOR UPDATE USING (conversation_id IN (
        SELECT id FROM conversations WHERE whatsapp_account_id IN (
            SELECT id FROM whatsapp_accounts WHERE tenant_id = current_setting('app.tenant_id')::UUID
        )
    ));

CREATE POLICY "tenant_messages_delete" ON messages
    FOR DELETE USING (conversation_id IN (
        SELECT id FROM conversations WHERE whatsapp_account_id IN (
            SELECT id FROM whatsapp_accounts WHERE tenant_id = current_setting('app.tenant_id')::UUID
        )
    ));

-- Indexes
CREATE INDEX IF NOT EXISTS ix_messages_conversation ON messages(conversation_id);
CREATE INDEX IF NOT EXISTS ix_messages_wamid ON messages(wamid);
CREATE INDEX IF NOT EXISTS ix_messages_direction ON messages(direction);
CREATE INDEX IF NOT EXISTS ix_messages_status ON messages(status);
CREATE INDEX IF NOT EXISTS ix_messages_created ON messages(created_at);

-- Audit function
CREATE OR REPLACE FUNCTION conversations_audit()
RETURNS TRIGGER AS
$$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER conversations_updated_at_trigger
    BEFORE UPDATE ON conversations
    FOR EACH ROW EXECUTE FUNCTION conversations_audit();

CREATE TRIGGER messages_created_at_trigger
    BEFORE INSERT ON messages
    FOR EACH ROW EXECUTE FUNCTION conversations_audit();