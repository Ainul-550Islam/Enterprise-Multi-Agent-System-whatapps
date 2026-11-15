-- 0055_whatsapp_conversations down.sql

DROP TRIGGER IF EXISTS messages_created_at_trigger ON messages;
DROP TRIGGER IF EXISTS conversations_updated_at_trigger ON conversations;

DROP POLICY IF EXISTS tenant_messages_select ON messages;
DROP POLICY IF EXISTS tenant_messages_insert ON messages;
DROP POLICY IF EXISTS tenant_messages_update ON messages;
DROP POLICY IF EXISTS tenant_messages_delete ON messages;

DROP POLICY IF EXISTS tenant_conversations_select ON conversations;
DROP POLICY IF EXISTS tenant_conversations_insert ON conversations;
DROP POLICY IF EXISTS tenant_conversations_update ON conversations;
DROP POLICY IF EXISTS tenant_conversations_delete ON conversations;

DROP INDEX IF EXISTS ix_messages_created;
DROP INDEX IF EXISTS ix_messages_status;
DROP INDEX IF EXISTS ix_messages_wamid;
DROP INDEX IF EXISTS ix_messages_direction;
DROP INDEX IF EXISTS ix_messages_conversation;

DROP INDEX IF EXISTS ix_conversations_window;
DROP INDEX IF EXISTS ix_conversations_wamid;
DROP INDEX IF EXISTS ix_conversations_agent;
DROP INDEX IF EXISTS ix_conversations_account;

DROP TABLE IF EXISTS messages CASCADE;
DROP TABLE IF EXISTS conversations CASCADE;