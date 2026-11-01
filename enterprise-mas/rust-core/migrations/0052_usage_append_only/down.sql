-- PURPOSE: Roll back migration 0052 — restore the prior runtime table grants.
--
-- Owning layer: database schema.
--
-- Production responsibility: exactly reverse the append-only privilege change
-- from 0052 without changing table data or other role grants.
--
-- Data ownership: none; privileges only.
--
-- MUST NOT: alter usage rows or widen privileges for any role other than
-- `mas_app`.

GRANT UPDATE, DELETE ON usage_records TO mas_app;
