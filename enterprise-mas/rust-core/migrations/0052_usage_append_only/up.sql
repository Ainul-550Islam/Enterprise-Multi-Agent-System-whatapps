-- PURPOSE: Migration 0052 — make usage facts append-only for the runtime role.
--
-- Owning layer: database schema.
--
-- Production responsibility: close the broad default table grants from 0001
-- for `usage_records`; runtime APIs may read and append metering facts but may
-- not rewrite or erase them. The worker's INSERT-only grant remains narrow.
--
-- Data ownership: tenant-scoped usage facts owned by the metering pipeline.
--
-- MUST NOT: revoke the runtime role's SELECT or INSERT privileges, or widen
-- any worker-plane privilege.

REVOKE UPDATE, DELETE ON usage_records FROM mas_app;
