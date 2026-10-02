-- 0010_integrations: connectors, credentials (SecretReference-only!),
-- webhooks, webhook deliveries, notifications.

CREATE TABLE connectors (
    id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id  uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    name       text NOT NULL,
    kind       text NOT NULL, -- crm / ticketing / email / storage / db / messaging
    status     text NOT NULL DEFAULT 'configuring'
               CHECK (status IN ('configuring', 'active', 'degraded', 'disabled')),
    config     jsonb NOT NULL DEFAULT '{}'::jsonb, -- non-secret configuration only
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);
CREATE TRIGGER connectors_updated_at BEFORE UPDATE ON connectors
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE connectors ENABLE ROW LEVEL SECURITY;
ALTER TABLE connectors FORCE ROW LEVEL SECURITY;
CREATE POLICY connectors_isolation ON connectors
    USING (tenant_id::text = current_setting('app.current_tenant', true));

-- Credentials store SecretReference payloads pointing at the vault — never
-- raw secret material (platform rule).
CREATE TABLE credentials (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    connector_id uuid NULL REFERENCES connectors (id) ON DELETE SET NULL,
    name        text NOT NULL,
    purpose     text NOT NULL,
    secret_ref  jsonb NOT NULL, -- SecretReference { provider, path, key, version }
    status      text NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'rotating', 'revoked')),
    expires_at  timestamptz NULL,
    rotated_at  timestamptz NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);
CREATE TRIGGER credentials_updated_at BEFORE UPDATE ON credentials
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE credentials ENABLE ROW LEVEL SECURITY;
ALTER TABLE credentials FORCE ROW LEVEL SECURITY;
CREATE POLICY credentials_isolation ON credentials
    USING (tenant_id::text = current_setting('app.current_tenant', true));

CREATE TABLE webhooks (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    name        text NOT NULL,
    target_url  text NOT NULL, -- validated http(s) URL (SSRF-checked app-side)
    event_filter jsonb NOT NULL DEFAULT '[]'::jsonb, -- event type globs
    signing_ref jsonb NULL, -- SecretReference for the signing secret, if rotated in vault
    status      text NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'paused', 'disabled')),
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);
CREATE TRIGGER webhooks_updated_at BEFORE UPDATE ON webhooks
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE webhooks ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhooks FORCE ROW LEVEL SECURITY;
CREATE POLICY webhooks_isolation ON webhooks
    USING (tenant_id::text = current_setting('app.current_tenant', true));

CREATE TABLE webhook_deliveries (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    webhook_id  uuid NOT NULL REFERENCES webhooks (id) ON DELETE CASCADE,
    event_id    uuid NOT NULL,
    status      text NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'delivered', 'failed', 'exhausted')),
    attempts    integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    http_status integer NULL,
    last_error  text NULL,
    next_attempt_at timestamptz NOT NULL,
    delivered_at timestamptz NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (webhook_id, event_id)
);
CREATE INDEX webhook_deliveries_due_idx ON webhook_deliveries (next_attempt_at)
    WHERE status IN ('pending', 'failed');
CREATE TRIGGER webhook_deliveries_updated_at BEFORE UPDATE ON webhook_deliveries
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE webhook_deliveries ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_deliveries FORCE ROW LEVEL SECURITY;
CREATE POLICY webhook_deliveries_isolation ON webhook_deliveries
    USING (tenant_id::text = current_setting('app.current_tenant', true));

CREATE TABLE notifications (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    recipient_id uuid NOT NULL, -- user id (or channel key) receiving it
    channel     text NOT NULL,
    template    text NOT NULL,
    payload     jsonb NOT NULL DEFAULT '{}'::jsonb,
    status      text NOT NULL DEFAULT 'queued'
                CHECK (status IN ('queued', 'sent', 'failed', 'cancelled', 'read')),
    sent_at     timestamptz NULL,
    read_at     timestamptz NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX notifications_recipient_idx ON notifications (recipient_id, created_at DESC);
CREATE TRIGGER notifications_updated_at BEFORE UPDATE ON notifications
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE notifications ENABLE ROW LEVEL SECURITY;
ALTER TABLE notifications FORCE ROW LEVEL SECURITY;
CREATE POLICY notifications_isolation ON notifications
    USING (tenant_id::text = current_setting('app.current_tenant', true));
