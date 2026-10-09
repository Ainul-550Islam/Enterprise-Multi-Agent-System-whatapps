-- PURPOSE: Reverse the isolated inbound webhook endpoint registry introduced
-- by migration 0042.
--
-- Owning layer: database schema and public-ingress security boundary.
--
-- Production role: remove only the endpoint registry, its policy/index/trigger,
-- and the private allowlist validation function during a deliberate rollback.
--
-- Security boundary: revoke the narrow mas_inbound route-read grant before
-- dropping its table. Receipt/outbox facts from 0041 are intentionally retained
-- because they are independent durable delivery evidence.
--
-- Data ownership: this migration removes endpoint configuration only; it never
-- deletes tenant secret references, signing material, receipts, payloads, or
-- outbox events.
--
-- Prohibited responsibilities: deleting secret-manager bytes, weakening RLS on
-- shared tables, altering mas_inbound's receipt grants, or dropping a function
-- that another object still depends on.

REVOKE SELECT (
    id,
    tenant_id,
    organization_id,
    source,
    signing_secret_reference_id,
    required_key_version,
    accepted_key_versions,
    enabled
) ON inbound_webhook_endpoints FROM mas_inbound;
REVOKE SELECT, INSERT, UPDATE, DELETE ON inbound_webhook_endpoints FROM mas_app;

DROP POLICY IF EXISTS inbound_webhook_endpoints_isolation ON inbound_webhook_endpoints;
DROP TRIGGER IF EXISTS inbound_webhook_endpoints_updated_at ON inbound_webhook_endpoints;
DROP TABLE IF EXISTS inbound_webhook_endpoints;
DROP FUNCTION IF EXISTS mas_inbound_key_versions_valid(text[]);
