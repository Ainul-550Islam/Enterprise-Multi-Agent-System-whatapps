-- Reverses 0062_whatsapp_inbox only after an explicit data-loss opt-in.

DO $$
BEGIN
    IF current_setting('mas.allow_data_loss', true) IS DISTINCT FROM 'true'
       AND (
            EXISTS (SELECT 1 FROM whatsapp_conversations LIMIT 1)
         OR EXISTS (SELECT 1 FROM whatsapp_templates LIMIT 1)
       ) THEN
        RAISE EXCEPTION
            '0062 down migration would delete inbox rows; set mas.allow_data_loss=true explicitly';
    END IF;
END
$$;

DROP TRIGGER IF EXISTS whatsapp_templates_tenant_immutable ON whatsapp_templates;
DROP TRIGGER IF EXISTS whatsapp_conversations_tenant_immutable ON whatsapp_conversations;
DROP POLICY IF EXISTS whatsapp_templates_tenant_isolation ON whatsapp_templates;
DROP POLICY IF EXISTS whatsapp_conversations_tenant_isolation ON whatsapp_conversations;
DROP TABLE IF EXISTS whatsapp_templates;
DROP TABLE IF EXISTS whatsapp_conversations;
