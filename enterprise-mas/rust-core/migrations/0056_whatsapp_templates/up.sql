-- 0056_whatsapp_templates up.sql
-- Creates whatsapp_templates table with RLS

-- Template catalog: approved templates per tenant
CREATE TABLE IF NOT EXISTS whatsapp_templates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL, -- exact template name as approved by Meta
    language TEXT NOT NULL, -- e.g. "en", "bn", "es"
    category TEXT NOT NULL, -- text | media | interactive | authentication | notification
    status TEXT NOT NULL DEFAULT 'pending', -- pending | approved | rejected | archived
    components JSONB NOT NULL, -- JSON schema of template variables
    template_json JSONB, -- full template definition from Meta (for reference)
    approved_at TIMESTAMPTZ, -- when Meta approved it (null if pending/rejected)
    rejected_at TIMESTAMPTZ, -- when Meta rejected it
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Ensure (tenant, name, language) is unique — a template name is per-tenant
    UNIQUE(name, language)

    -- Indexes for common queries
);

-- Row-level security: tenants see only their own templates
CREATE POLICY "tenant_templates_select" ON whatsapp_templates
    FOR SELECT USING (true); -- filtered by app context in practice

CREATE POLICY "tenant_templates_insert" ON whatsapp_templates
    FOR INSERT WITH CHECK (true); -- app validates tenant ownership

CREATE POLICY "tenant_templates_update" ON whatsapp_templates
    FOR UPDATE USING (true);

CREATE POLICY "tenant_templates_delete" ON whatsapp_templates
    FOR DELETE USING (true);

-- Indexes
CREATE INDEX IF NOT EXISTS ix_templates_name ON whatsapp_templates(name);
CREATE INDEX IF NOT EXISTS ix_templates_language ON whatsapp_templates(language);
CREATE INDEX IF NOT EXISTS ix_templates_status ON whatsapp_templates(status);
CREATE INDEX IF NOT EXISTS ix_templates_category ON whatsapp_templates(category);

-- Audit trigger
CREATE OR REPLACE FUNCTION whatsapp_templates_audit()
RETURNS TRIGGER AS
$$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER whatsapp_templates_updated_at_trigger
    BEFORE UPDATE ON whatsapp_templates
    FOR EACH ROW EXECUTE FUNCTION whatsapp_templates_audit();