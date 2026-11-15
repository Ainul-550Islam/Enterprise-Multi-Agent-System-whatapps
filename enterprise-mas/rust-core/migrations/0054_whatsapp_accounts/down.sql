-- 0054_whatsapp_accounts down.sql
-- Drop the whatsapp_accounts table and associated policies/triggers

DROP TRIGGER IF EXISTS whatsapp_accounts_updated_at_trigger ON whatsapp_accounts;
DROP FUNCTION IF EXISTS whatsapp_accounts_audit() ON whatsapp_accounts;

DROP POLICY IF EXISTS tenant_whatsapp_accounts_select ON whatsapp_accounts;
DROP POLICY IF EXISTS tenant_whatsapp_accounts_insert ON whatsapp_accounts;
DROP POLICY IF EXISTS tenant_whatsapp_accounts_update ON whatsapp_accounts;
DROP POLICY IF EXISTS tenant_whatsapp_accounts_delete ON whatsapp_accounts;

DROP INDEX IF EXISTS ix_whatsapp_accounts_route;
DROP INDEX IF EXISTS ix_whatsapp_accounts_phone;
DROP INDEX IF EXISTS ix_whatsapp_accounts_waba;
DROP INDEX IF EXISTS ix_whatsapp_accounts_tenant;

DROP TABLE IF EXISTS whatsapp_accounts CASCADE;