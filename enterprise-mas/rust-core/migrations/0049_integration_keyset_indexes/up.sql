-- PURPOSE: Add keyset pagination indexes for tenant connector and outbound
-- webhook endpoint catalogues.
--
-- Owning layer: PostgreSQL request-plane performance boundary.
--
-- Production role: support bounded `created_at DESC, id DESC` tenant-scoped
-- connector and webhook API pages without offset scans or full catalogue
-- materialization as tenants grow.
--
-- Security boundary: indexes expose no new columns, privileges, policies, or
-- cross-tenant access paths; RLS and the request-scoped tenant predicate remain
-- the authority for every indexed query.
--
-- Data ownership: PostgreSQL owns index lifecycle alongside the existing
-- connector/webhook tables.
--
-- Prohibited responsibilities: changing endpoint/connector state, granting
-- roles, storing secret bytes, modifying signing references, or relaxing RLS.

CREATE INDEX IF NOT EXISTS connectors_tenant_created_id_desc_idx
    ON connectors (tenant_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS webhooks_tenant_created_id_desc_idx
    ON webhooks (tenant_id, created_at DESC, id DESC);
