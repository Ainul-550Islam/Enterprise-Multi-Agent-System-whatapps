-- PURPOSE: Forward-only acknowledgement for OAuth opaque secret locators.
--
-- Owning layer: database OAuth credential/reference security boundary.
--
-- Production role: make rollback intent explicit: prior Vault routes are not
-- retained in PostgreSQL and therefore cannot be reconstructed safely.
--
-- Security boundary: a rollback must not reintroduce Vault paths into database
-- metadata or legacy credential JSON.
--
-- Data ownership: opaque UUID locators remain canonical until a coordinated
-- application rollback with an operator-approved migration is designed.
--
-- Prohibited responsibilities: fabricating Vault paths, weakening grants, or
-- changing externally stored OAuth material.

SELECT 1;
