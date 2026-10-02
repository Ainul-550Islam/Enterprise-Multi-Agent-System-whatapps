-- 0007_policy: policy definitions and the append-only decision log.

CREATE TABLE policies (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    name        text NOT NULL,
    kind        text NOT NULL,
    status      text NOT NULL DEFAULT 'draft'
                CHECK (status IN ('draft', 'active', 'disabled', 'archived')),
    spec        jsonb NOT NULL, -- rules/permit/deny/limits payload
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);
CREATE TRIGGER policies_updated_at BEFORE UPDATE ON policies
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE policies ENABLE ROW LEVEL SECURITY;
ALTER TABLE policies FORCE ROW LEVEL SECURITY;
CREATE POLICY policies_isolation ON policies
    USING (tenant_id::text = current_setting('app.current_tenant', true));

-- Decision log is append-only for tamper evidence.
CREATE TABLE policy_decisions (
    id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id      uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    policy_id      uuid NULL REFERENCES policies (id) ON DELETE SET NULL,
    subject        text NOT NULL,   -- who/what was evaluated
    action         text NOT NULL,   -- evaluated action
    outcome        text NOT NULL CHECK (outcome IN ('permit', 'deny', 'escalate')),
    rationale      text NOT NULL DEFAULT '',
    evaluation_ms  integer NOT NULL CHECK (evaluation_ms >= 0),
    context        jsonb NOT NULL DEFAULT '{}'::jsonb,
    correlation_id text NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX policy_decisions_tenant_idx ON policy_decisions (tenant_id, created_at DESC);
CREATE INDEX policy_decisions_correlation_idx ON policy_decisions (correlation_id);

CREATE TRIGGER policy_decisions_no_update BEFORE UPDATE ON policy_decisions
    FOR EACH ROW EXECUTE FUNCTION mas_forbid_mutation();
CREATE TRIGGER policy_decisions_no_delete BEFORE DELETE ON policy_decisions
    FOR EACH ROW EXECUTE FUNCTION mas_forbid_mutation();

ALTER TABLE policy_decisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE policy_decisions FORCE ROW LEVEL SECURITY;
CREATE POLICY policy_decisions_isolation ON policy_decisions
    USING (tenant_id::text = current_setting('app.current_tenant', true));
