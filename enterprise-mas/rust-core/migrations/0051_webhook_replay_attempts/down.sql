-- PURPOSE: Roll back migration 0051 — drop exactly the objects 0051
-- created (the append-only webhook_replay_attempts evidence table and the
-- tenant-uniqueness index on webhook_deliveries). No delivery rows, dead
-- letters, or other schema objects are touched.
--
-- MUST NOT: alter webhook_deliveries data or drop any index not created by
-- 0051 up.sql.

DROP TABLE IF EXISTS webhook_replay_attempts;
DROP INDEX IF EXISTS webhook_deliveries_tenant_id_unique;
