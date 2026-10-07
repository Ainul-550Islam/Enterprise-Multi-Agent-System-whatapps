-- PURPOSE: Durable approval requests and one-vote-per-approver decisions.
--
-- The application policy engine remains the state-machine authority. These
-- tables persist its decisions across restarts and enforce tenant isolation,
-- duplicate-vote protection and terminal-state finality at the database layer.

CREATE TABLE approval_requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    organization_id UUID NOT NULL REFERENCES organizations (id) ON DELETE RESTRICT,
    policy_id UUID NULL REFERENCES policies (id) ON DELETE SET NULL,
    origin TEXT NOT NULL CHECK (char_length(origin) BETWEEN 1 AND 80),
    resource_type TEXT NOT NULL CHECK (char_length(resource_type) BETWEEN 1 AND 80),
    resource_id TEXT NOT NULL CHECK (char_length(resource_id) BETWEEN 1 AND 256),
    requested_by UUID NULL REFERENCES users (id) ON DELETE SET NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'approved', 'denied', 'expired', 'cancelled')),
    quorum INTEGER NOT NULL CHECK (quorum > 0),
    expires_at TIMESTAMPTZ NOT NULL,
    decided_at TIMESTAMPTZ NULL,
    decision_reason TEXT NULL CHECK (decision_reason IS NULL OR char_length(decision_reason) <= 1024),
    context JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(context) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id)
);

CREATE INDEX approval_requests_queue_idx
    ON approval_requests (tenant_id, status, expires_at, created_at);
CREATE TRIGGER approval_requests_updated_at BEFORE UPDATE ON approval_requests
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

CREATE OR REPLACE FUNCTION mas_guard_approval_terminal_state()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.status IN ('approved', 'denied', 'expired', 'cancelled')
       AND NEW.status IS DISTINCT FROM OLD.status THEN
        RAISE EXCEPTION 'terminal approval request state cannot be changed'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER approval_requests_terminal_guard
    BEFORE UPDATE OF status ON approval_requests
    FOR EACH ROW EXECUTE FUNCTION mas_guard_approval_terminal_state();

CREATE TABLE approval_votes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    request_id UUID NOT NULL,
    approver_id UUID NOT NULL REFERENCES users (id) ON DELETE RESTRICT,
    decision TEXT NOT NULL CHECK (decision IN ('approve', 'deny')),
    reason TEXT NOT NULL CHECK (char_length(reason) BETWEEN 1 AND 1024),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, request_id, approver_id),
    FOREIGN KEY (tenant_id, request_id)
        REFERENCES approval_requests (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX approval_votes_request_idx ON approval_votes (tenant_id, request_id, created_at);
CREATE TRIGGER approval_votes_no_update
    BEFORE UPDATE ON approval_votes
    FOR EACH ROW EXECUTE FUNCTION mas_forbid_mutation();
CREATE TRIGGER approval_votes_no_delete
    BEFORE DELETE ON approval_votes
    FOR EACH ROW EXECUTE FUNCTION mas_forbid_mutation();

ALTER TABLE approval_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE approval_requests FORCE ROW LEVEL SECURITY;
CREATE POLICY approval_requests_tenant_isolation ON approval_requests
    USING (tenant_id::text = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id::text = current_setting('app.current_tenant', true));

ALTER TABLE approval_votes ENABLE ROW LEVEL SECURITY;
ALTER TABLE approval_votes FORCE ROW LEVEL SECURITY;
CREATE POLICY approval_votes_tenant_isolation ON approval_votes
    USING (tenant_id::text = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id::text = current_setting('app.current_tenant', true));
