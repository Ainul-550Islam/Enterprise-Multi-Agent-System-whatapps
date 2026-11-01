-- PURPOSE: Additive indexes for bounded, tenant-scoped filtered audit keyset reads.
--
-- Bounded, filtered audit keyset reads. Every index is tenant-leading so
-- FORCE RLS and the explicit tenant predicate agree with the access path.
CREATE INDEX audit_events_tenant_keyset_idx
    ON audit_events (tenant_id, occurred_at DESC, id DESC);

CREATE INDEX audit_events_tenant_actor_keyset_idx
    ON audit_events (tenant_id, (actor->>'id'), occurred_at DESC, id DESC);

CREATE INDEX audit_events_tenant_action_keyset_idx
    ON audit_events (tenant_id, action text_pattern_ops, occurred_at DESC, id DESC);

CREATE INDEX audit_events_tenant_resource_keyset_idx
    ON audit_events (tenant_id, resource_type, occurred_at DESC, id DESC);

CREATE INDEX audit_events_tenant_outcome_keyset_idx
    ON audit_events (tenant_id, outcome, occurred_at DESC, id DESC);
