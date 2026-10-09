-- PURPOSE: Reverse migration 0049's tenant integration-catalogue keyset
-- pagination indexes during a coordinated rollback.
--
-- Owning layer: PostgreSQL request-plane performance boundary.
--
-- Production role: remove only indexes introduced for connector/webhook API
-- pagination; it does not alter catalogue facts, RLS, grants, or leases.
--
-- Security boundary: dropping these indexes cannot grant cross-tenant access
-- or expose signing/credential material.
--
-- Data ownership: table data remains PostgreSQL-owned and untouched.
--
-- Prohibited responsibilities: deleting rows, changing endpoint state,
-- modifying secret references, or changing database roles/policies.

DROP INDEX IF EXISTS connectors_tenant_created_id_desc_idx;
DROP INDEX IF EXISTS webhooks_tenant_created_id_desc_idx;
