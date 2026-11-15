-- 0056_whatsapp_templates down.sql

DROP TRIGGER IF EXISTS whatsapp_templates_updated_at_trigger ON whatsapp_templates;
DROP FUNCTION IF EXISTS whatsapp_templates_audit() ON whatsapp_templates;

DROP POLICY IF EXISTS tenant_templates_select ON whatsapp_templates;
DROP POLICY IF EXISTS tenant_templates_insert ON whatsapp_templates;
DROP POLICY IF EXISTS tenant_templates_update ON whatsapp_templates;
DROP POLICY IF EXISTS tenant_templates_delete ON whatsapp_templates;

DROP INDEX IF EXISTS ix_templates_category;
DROP INDEX IF EXISTS ix_templates_status;
DROP INDEX IF EXISTS ix_templates_language;
DROP INDEX IF EXISTS ix_templates_name;

DROP TABLE IF EXISTS whatsapp_templates CASCADE;