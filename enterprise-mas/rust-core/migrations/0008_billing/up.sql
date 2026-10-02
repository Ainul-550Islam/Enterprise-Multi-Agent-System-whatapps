-- 0008_billing: subscriptions, entitlements, quotas, usage records.

CREATE TABLE subscriptions (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    plan        text NOT NULL,
    state       text NOT NULL DEFAULT 'trialing'
                CHECK (state IN ('trialing', 'active', 'past_due', 'suspended', 'cancelled')),
    period      jsonb NULL, -- { start, end } billing period payload
    spec        jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX subscriptions_tenant_active_key ON subscriptions (tenant_id)
    WHERE state IN ('trialing', 'active', 'past_due');
CREATE TRIGGER subscriptions_updated_at BEFORE UPDATE ON subscriptions
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE subscriptions ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscriptions FORCE ROW LEVEL SECURITY;
CREATE POLICY subscriptions_isolation ON subscriptions
    USING (tenant_id::text = current_setting('app.current_tenant', true));

CREATE TABLE entitlements (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    feature     text NOT NULL,
    enabled     boolean NOT NULL DEFAULT true,
    numeric_limit bigint NULL CHECK (numeric_limit IS NULL OR numeric_limit >= 0),
    source      text NOT NULL DEFAULT 'plan'
                CHECK (source IN ('plan', 'contract_override', 'manual_grant', 'trial')),
    expires_at  timestamptz NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, feature)
);
CREATE TRIGGER entitlements_updated_at BEFORE UPDATE ON entitlements
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE entitlements ENABLE ROW LEVEL SECURITY;
ALTER TABLE entitlements FORCE ROW LEVEL SECURITY;
CREATE POLICY entitlements_isolation ON entitlements
    USING (tenant_id::text = current_setting('app.current_tenant', true));

CREATE TABLE quotas (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    scope       text NOT NULL, -- e.g. tenant / project / agent
    resource    text NOT NULL, -- e.g. executions.minute, tokens.day
    max_value   bigint NOT NULL CHECK (max_value >= 0),
    window      text NOT NULL CHECK (window IN ('second', 'minute', 'hour', 'day', 'month', 'lifetime')),
    spec        jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, scope, resource, window)
);
CREATE TRIGGER quotas_updated_at BEFORE UPDATE ON quotas
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE quotas ENABLE ROW LEVEL SECURITY;
ALTER TABLE quotas FORCE ROW LEVEL SECURITY;
CREATE POLICY quotas_isolation ON quotas
    USING (tenant_id::text = current_setting('app.current_tenant', true));

-- Usage records are append-only metering facts.
CREATE TABLE usage_records (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    resource    text NOT NULL,
    quantity    bigint NOT NULL CHECK (quantity >= 0),
    unit        text NOT NULL,
    source_id   text NULL, -- execution/task id that produced the usage
    dimensions  jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX usage_records_metering_idx ON usage_records (tenant_id, resource, occurred_at);

ALTER TABLE usage_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE usage_records FORCE ROW LEVEL SECURITY;
CREATE POLICY usage_records_isolation ON usage_records
    USING (tenant_id::text = current_setting('app.current_tenant', true));
