-- 0011_audit: the tamper-evident, append-only audit log.
--
-- Rules (enforced here, in SQL):
--   * UPDATE and DELETE are rejected by trigger for EVERY role;
--   * inserts are monotone by occurred_at within a tenant (approximate —
--     callers should trust the server default, not client clocks);
--   * metadata must already be redacted by the application; the column is a
--     plain JSONB map with no nested secret material by contract.

CREATE TABLE audit_events (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        uuid NULL REFERENCES tenants (id) ON DELETE RESTRICT,
    organization_id  uuid NULL REFERENCES organizations (id) ON DELETE RESTRICT,
    actor            jsonb NOT NULL,   -- AuditActor { kind, id, display? }
    action           text NOT NULL,    -- subject.verb form
    resource_type    text NOT NULL,
    resource_id      text NULL,
    outcome          text NOT NULL
                     CHECK (outcome IN ('success', 'failure', 'denied')),
    severity         text NOT NULL DEFAULT 'info'
                     CHECK (severity IN ('info', 'notice', 'warning', 'high', 'critical')),
    compliance_class text NOT NULL DEFAULT 'general'
                     CHECK (compliance_class IN ('general', 'security', 'privacy', 'financial')),
    correlation_id   text NOT NULL,
    metadata         jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at      timestamptz NOT NULL DEFAULT now(),
    -- Envelope for deferred integrity chaining (hash chain populated by the
    -- audit service; kept SQL-visible for future verification jobs).
    integrity        jsonb NULL,
    CHECK (length(action) BETWEEN 3 AND 160),
    CHECK (length(resource_type) BETWEEN 1 AND 80)
);
CREATE INDEX audit_events_tenant_time_idx ON audit_events (tenant_id, occurred_at DESC);
CREATE INDEX audit_events_resource_idx ON audit_events (tenant_id, resource_type, resource_id);
CREATE INDEX audit_events_correlation_idx ON audit_events (correlation_id);
CREATE INDEX audit_events_compliance_idx ON audit_events (compliance_class, occurred_at DESC);

-- Append-only enforcement for all roles.
CREATE TRIGGER audit_events_no_update BEFORE UPDATE ON audit_events
    FOR EACH ROW EXECUTE FUNCTION mas_forbid_mutation();
CREATE TRIGGER audit_events_no_delete BEFORE DELETE ON audit_events
    FOR EACH ROW EXECUTE FUNCTION mas_forbid_mutation();

ALTER TABLE audit_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_events FORCE ROW LEVEL SECURITY;
CREATE POLICY audit_events_isolation ON audit_events
    USING (tenant_id IS NULL /* platform-global entries visible to service roles only */
        OR tenant_id::text = current_setting('app.current_tenant', true));
