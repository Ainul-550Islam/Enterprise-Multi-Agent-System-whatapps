-- PURPOSE: Revert only the five indexes created by migration 0050.
DROP INDEX IF EXISTS audit_events_tenant_outcome_keyset_idx;
DROP INDEX IF EXISTS audit_events_tenant_resource_keyset_idx;
DROP INDEX IF EXISTS audit_events_tenant_action_keyset_idx;
DROP INDEX IF EXISTS audit_events_tenant_actor_keyset_idx;
DROP INDEX IF EXISTS audit_events_tenant_keyset_idx;
