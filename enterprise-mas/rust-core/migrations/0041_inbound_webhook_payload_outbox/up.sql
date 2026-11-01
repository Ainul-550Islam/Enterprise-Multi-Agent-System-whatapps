-- PURPOSE: Add canonical authenticated inbound-webhook payload and ingress-role
-- support without mutating the already-released enterprise-plane migration.
--
-- Owning layer: database schema and ingress security boundary.
--
-- Production role: retain only signature-verified bounded JSON necessary to
-- release deferred ordered receipts into the transactional outbox, preserve the
-- organization selected by server-owned route configuration, and isolate public
-- ingress writes behind a minimal NOLOGIN callback role.
--
-- Security boundary: `mas_inbound` can insert/update receipt ordering facts and
-- insert transactional outbox events, but cannot read tenants, credentials,
-- OAuth state, secret references, connector configuration, or existing outbox
-- payloads. Application code must verify a signature before using this pool.
--
-- Data ownership: receipt rows own verified provider event evidence and the
-- selected organization; outbox rows own event relay state; providers own the
-- original event and secret managers own signing key material.
--
-- Prohibited responsibilities: storing signing secrets, admitting unsigned
-- data, allowing arbitrary outbox reads, or granting generic tenant access.

ALTER TABLE inbound_webhook_receipts
    ADD COLUMN IF NOT EXISTS organization_id uuid NULL
        REFERENCES organizations (id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS payload jsonb NULL;

-- No canonical inbound processor existed before this migration. Preserve any
-- historical receipt evidence as an empty JSON object rather than inventing a
-- provider payload. The new adapter writes a bounded parsed JSON value before
-- it can mark a receipt processable.
UPDATE inbound_webhook_receipts
SET payload = '{}'::jsonb
WHERE payload IS NULL;

ALTER TABLE inbound_webhook_receipts
    ALTER COLUMN payload SET NOT NULL,
    ALTER COLUMN payload SET DEFAULT '{}'::jsonb,
    DROP CONSTRAINT IF EXISTS inbound_webhook_receipts_payload_json_check,
    ADD CONSTRAINT inbound_webhook_receipts_payload_json_check
        CHECK (jsonb_typeof(payload) IS NOT NULL),
    DROP CONSTRAINT IF EXISTS inbound_webhook_receipts_tenant_org_scope_fk,
    ADD CONSTRAINT inbound_webhook_receipts_tenant_org_scope_fk
        FOREIGN KEY (tenant_id, organization_id)
        REFERENCES tenants (id, organization_id)
        ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS inbound_webhook_receipts_source_ready_order_idx
    ON inbound_webhook_receipts (tenant_id, source, sequence_no ASC, id ASC)
    WHERE processing_state IN ('accepted', 'deferred');

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mas_inbound') THEN
        CREATE ROLE mas_inbound NOLOGIN BYPASSRLS;
    END IF;
END
$$;
ALTER ROLE mas_inbound NOLOGIN BYPASSRLS;
GRANT USAGE ON SCHEMA public TO mas_inbound;
GRANT SELECT, INSERT, UPDATE ON inbound_webhook_receipts TO mas_inbound;
GRANT SELECT, INSERT, UPDATE ON inbound_webhook_order_cursors TO mas_inbound;
GRANT INSERT ON outbox_events TO mas_inbound;

COMMENT ON ROLE mas_inbound IS
    'Authenticated inbound webhook receipt writer. NOLOGIN/BYPASSRLS is used '
    'only after application HMAC verification because a public callback has no '
    'tenant session. It may write receipt/order facts and insert outbox events '
    'but cannot read credentials, OAuth state, secret references, connectors, '
    'tenants, or existing outbox rows.';
