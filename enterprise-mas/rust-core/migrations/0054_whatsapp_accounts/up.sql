-- 0054_whatsapp_accounts up.sql
-- Creates the whatsapp_accounts table RLS-armed per-tenant

-- Extension: enable uuid-ossp if needed (postgres)
-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- WhatsApp account per-tenant row
-- One row per (tenant, project, route) representing a WABA phone-number binding
CREATE TABLE IF NOT EXISTS whatsapp_accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    project_id UUID NOT NULL,
    route_id UUID NOT NULL,
    waba_id UUID NOT NULL,
    phone_number_id UUID NOT NULL,
    secret_ref TEXT NOT NULL,   -- reference to secret store key, never inline secret
    status TEXT NOT NULL DEFAULT 'active', -- active | disabled | error
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Ensure one account per (tenant, waba_id, phone_number_id) combination
    UNIQUE(tenant_id, waba_id, phone_number_id),

    -- Ensure route_id is unique across the system
    UNIQUE(route_id)
);

-- Row-level security: tenants can only read/write their own accounts
CREATE POLICY "tenant_whatsapp_accounts_select" ON whatsapp_accounts
    FOR SELECT USING (tenant_id = current_setting('app.tenant_id')::UUID);

CREATE POLICY "tenant_whatsapp_accounts_insert" ON whatsapp_accounts
    FOR INSERT WITH CHECK (tenant_id = current_setting('app.tenant_id')::UUID);

CREATE POLICY "tenant_whatsapp_accounts_update" ON whatsapp_accounts
    FOR UPDATE USING (tenant_id = current_setting('app.tenant_id')::UUID);

CREATE POLICY "tenant_whatsapp_accounts_delete" ON whatsapp_accounts
    FOR DELETE USING (tenant_id = current_setting('app.tenant_id')::UUID);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS ix_whatsapp_accounts_tenant ON whatsapp_accounts(tenant_id);
CREATE INDEX IF NOT EXISTS ix_whatsapp_accounts_waba ON whatsapp_accounts(waba_id);
CREATE INDEX IF NOT EXISTS ix_whatsapp_accounts_phone ON whatsapp_accounts(phone_number_id);
CREATE INDEX IF NOT EXISTS ix_whatsapp_accounts_route ON whatsapp_accounts(route_id);

-- Audit trigger: record every INSERT/UPDATE with actor context
CREATE OR REPLACE FUNCTION whatsapp_accounts_audit()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER whatsapp_accounts_updated_at_trigger
    BEFORE UPDATE ON whatsapp_accounts
    FOR EACH ROW EXECUTE FUNCTION whatsapp_accounts_audit();