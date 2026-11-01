-- PURPOSE: 0051_webhook_replay_attempts — append-only evidence for manual DLQ replay.
--
-- Stores only ids, payload digest, and the signing-key version reference; the
-- payload body and secret material remain owned by their existing stores.

CREATE UNIQUE INDEX webhook_deliveries_tenant_id_unique
    ON webhook_deliveries (tenant_id, id);

CREATE TABLE webhook_replay_attempts (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    webhook_id uuid NOT NULL,
    delivery_id uuid NOT NULL,
    event_id uuid NOT NULL,
    payload_id uuid NULL,
    payload_hash text NOT NULL CHECK (payload_hash ~ '^[A-Fa-f0-9]{64}$'),
    signing_key_version text NOT NULL CHECK (char_length(signing_key_version) BETWEEN 1 AND 128),
    requested_by uuid NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT webhook_replay_attempts_endpoint_scope_fk
        FOREIGN KEY (tenant_id, webhook_id)
        REFERENCES webhooks (tenant_id, id) ON DELETE CASCADE,
    CONSTRAINT webhook_replay_attempts_delivery_scope_fk
        FOREIGN KEY (tenant_id, delivery_id)
        REFERENCES webhook_deliveries (tenant_id, id) ON DELETE CASCADE,
    CONSTRAINT webhook_replay_attempts_payload_scope_fk
        FOREIGN KEY (tenant_id, payload_id)
        REFERENCES webhook_payloads (tenant_id, id) ON DELETE RESTRICT,
    CONSTRAINT webhook_replay_attempts_dead_letter_fk
        FOREIGN KEY (delivery_id) REFERENCES webhook_dead_letters (delivery_id) ON DELETE CASCADE,
    CONSTRAINT webhook_replay_attempts_delivery_unique UNIQUE (tenant_id, delivery_id)
);
CREATE INDEX webhook_replay_attempts_tenant_created_idx
    ON webhook_replay_attempts (tenant_id, created_at DESC, id DESC);
REVOKE UPDATE, DELETE ON TABLE webhook_replay_attempts FROM PUBLIC, mas_app;
ALTER TABLE webhook_replay_attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_replay_attempts FORCE ROW LEVEL SECURITY;
CREATE POLICY webhook_replay_attempts_isolation ON webhook_replay_attempts
    USING (tenant_id::text = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id::text = current_setting('app.current_tenant', true));
