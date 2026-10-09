-- PURPOSE: Migration 0039 creates the durable enterprise integration plane.
--
-- Owning layer: PostgreSQL persistence/security layer.
--
-- Production role: evolve the legacy integration tables into one canonical
-- tenant-scoped connector/credential/webhook/notification model; retain exact
-- webhook payloads durably; provide transactional inbound receipts and order
-- cursors; provide outbound delivery leases, monotonic fences and dead-letter
-- evidence; and persist OAuth state only as references to external secret
-- manager material.
--
-- Security boundary: tenant request traffic remains FORCE RLS under mas_app.
-- A distinct, NOLOGIN, BYPASSRLS `mas_integrations` relay role receives only
-- the minimum webhook rows it must claim and settle. It does not receive the
-- credentials table and cannot be used as the normal task worker plane.
--
-- Data ownership: integrations owns all tables introduced here. Raw signing,
-- OAuth, and credential secret bytes are prohibited; only vault/secret-manager
-- references, version identifiers, hashes, and bounded encrypted-at-rest
-- payload bytes may be stored.
--
-- Prohibited responsibilities: this migration does not grant mas_worker access
-- to connectors, credentials, or webhooks; does not create a private-network
-- exception; does not make old direct in-process webhook delivery production;
-- and does not weaken any existing RLS policy.

-- ===========================================================================
-- 1. Canonical connector and credential-reference lifecycle
-- ===========================================================================

-- Canonical metadata for a pointer into a real secret manager. This table
-- contains no material: a path/version alone cannot authenticate to a manager.
CREATE TABLE IF NOT EXISTS secret_references (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    provider text NOT NULL CHECK (provider IN (
        'vault', 'kms', 'aws_secrets_manager', 'gcp_secret_manager',
        'azure_key_vault', 'environment'
    )),
    path text NOT NULL CHECK (char_length(path) BETWEEN 1 AND 1024),
    key_version text NULL CHECK (key_version IS NULL OR char_length(key_version) BETWEEN 1 AND 128),
    rotation_state text NOT NULL DEFAULT 'current'
        CHECK (rotation_state IN ('current', 'rotating', 'retired')),
    availability text NOT NULL DEFAULT 'available'
        CHECK (availability IN ('available', 'unavailable', 'deleted')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id)
);
CREATE INDEX IF NOT EXISTS secret_references_tenant_state_idx
    ON secret_references (tenant_id, availability, rotation_state, id);
CREATE TRIGGER secret_references_updated_at BEFORE UPDATE ON secret_references
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();
ALTER TABLE secret_references ENABLE ROW LEVEL SECURITY;
ALTER TABLE secret_references FORCE ROW LEVEL SECURITY;
CREATE POLICY secret_references_isolation ON secret_references
    USING (tenant_id::text = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id::text = current_setting('app.current_tenant', true));

-- Backfill only legacy JSON that is already a complete typed SecretReference.
-- Invalid/partial old metadata stays unbound and is never guessed into an
-- operational secret path.
INSERT INTO secret_references (
    id, tenant_id, provider, path, key_version, rotation_state, availability
)
SELECT
    (credential.secret_ref ->> 'id')::uuid,
    credential.tenant_id,
    credential.secret_ref ->> 'provider',
    credential.secret_ref ->> 'path',
    NULLIF(credential.secret_ref ->> 'key_version', ''),
    COALESCE(NULLIF(credential.secret_ref ->> 'rotation_state', ''), 'current'),
    COALESCE(NULLIF(credential.secret_ref ->> 'availability', ''), 'available')
FROM credentials AS credential
WHERE credential.secret_ref ? 'id'
  AND credential.secret_ref ? 'provider'
  AND credential.secret_ref ? 'path'
  AND (credential.secret_ref ->> 'id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  AND credential.secret_ref ->> 'provider' IN (
        'vault', 'kms', 'aws_secrets_manager', 'gcp_secret_manager',
        'azure_key_vault', 'environment'
  )
  AND char_length(credential.secret_ref ->> 'path') BETWEEN 1 AND 1024
ON CONFLICT (id) DO NOTHING;

-- Existing credentials already contain only `secret_ref`. Add explicit
-- lifecycle/key-version evidence rather than introducing another secret model.
ALTER TABLE credentials
    ADD COLUMN IF NOT EXISTS secret_reference_id uuid NULL,
    ADD COLUMN IF NOT EXISTS secret_version text NULL,
    ADD COLUMN IF NOT EXISTS last_validated_at timestamptz NULL,
    ADD COLUMN IF NOT EXISTS last_validation_error text NULL,
    ADD COLUMN IF NOT EXISTS revoked_at timestamptz NULL;

-- Reconcile old connector credential bindings before making the stronger
-- three-column relation mandatory. Existing unbound credential records remain
-- valid for a connector that is still in OAuth setup.
UPDATE credentials AS credential
SET connector_id = connector.id
FROM connectors AS connector
WHERE credential.tenant_id = connector.tenant_id
  AND credential.id = connector.credential_id
  AND credential.connector_id IS NULL;

UPDATE credentials
SET revoked_at = COALESCE(revoked_at, updated_at)
WHERE status = 'revoked';

UPDATE credentials AS credential
SET secret_reference_id = reference.id
FROM secret_references AS reference
WHERE reference.tenant_id = credential.tenant_id
  AND credential.secret_reference_id IS NULL
  AND credential.secret_ref ? 'id'
  AND (credential.secret_ref ->> 'id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  AND reference.id = (credential.secret_ref ->> 'id')::uuid;

ALTER TABLE credentials
    ADD CONSTRAINT credentials_secret_reference_scope_fk
        FOREIGN KEY (tenant_id, secret_reference_id)
        REFERENCES secret_references (tenant_id, id)
        ON DELETE RESTRICT;

-- The referenced tuple must be unique before the connector FK is installed.
-- It is intentionally nullable: OAuth setup can create a connector before
-- binding a credential.
ALTER TABLE credentials
    ADD CONSTRAINT credentials_tenant_id_connector_id_unique
        UNIQUE (tenant_id, id, connector_id),
    DROP CONSTRAINT IF EXISTS credentials_secret_version_check,
    ADD CONSTRAINT credentials_secret_version_check
        CHECK (secret_version IS NULL OR char_length(secret_version) BETWEEN 1 AND 128),
    DROP CONSTRAINT IF EXISTS credentials_last_validation_error_check,
    ADD CONSTRAINT credentials_last_validation_error_check
        CHECK (last_validation_error IS NULL OR char_length(last_validation_error) <= 1024),
    DROP CONSTRAINT IF EXISTS credentials_lifecycle_check,
    ADD CONSTRAINT credentials_lifecycle_check
        CHECK (
            (status IN ('active', 'rotating') AND revoked_at IS NULL)
            OR (status = 'revoked' AND revoked_at IS NOT NULL)
        );

CREATE INDEX IF NOT EXISTS credentials_tenant_connector_lifecycle_idx
    ON credentials (tenant_id, connector_id, status, updated_at DESC);

-- A connector must only use a credential record that belongs to both the same
-- tenant and that connector (the earlier tenant-only FK remains useful for
-- pre-existing NULL connector_id rows).
ALTER TABLE connectors
    ADD CONSTRAINT connectors_credential_binding_fk
        FOREIGN KEY (tenant_id, credential_id, id)
        REFERENCES credentials (tenant_id, id, connector_id)
        DEFERRABLE INITIALLY DEFERRED;

-- ===========================================================================
-- 2. Canonical OAuth authorization-code state (no raw verifier/token bytes)
-- ===========================================================================

CREATE TABLE IF NOT EXISTS oauth_authorization_states (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    connector_id uuid NULL,
    provider text NOT NULL CHECK (provider ~ '^[A-Za-z0-9._-]{1,64}$'),
    -- SHA-256 hex of the public OAuth state value; raw state is never stored.
    state_hash text NOT NULL CHECK (state_hash ~ '^[A-Fa-f0-9]{64}$'),
    -- Legacy JSON reference retained for forward compatibility. New paths use
    -- pkce_verifier_secret_id, a tenant-scoped foreign key into the canonical
    -- secret-reference lifecycle table; raw verifier bytes never persist here.
    pkce_verifier_ref jsonb NULL,
    pkce_verifier_secret_id uuid NOT NULL,
    redirect_uri text NOT NULL CHECK (char_length(redirect_uri) BETWEEN 1 AND 2048),
    requested_scopes text[] NOT NULL DEFAULT '{}'::text[],
    expires_at timestamptz NOT NULL,
    consumed_at timestamptz NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT oauth_authorization_states_connector_scope_fk
        FOREIGN KEY (tenant_id, connector_id)
        REFERENCES connectors (tenant_id, id)
        ON DELETE CASCADE,
    CONSTRAINT oauth_authorization_states_pkce_secret_scope_fk
        FOREIGN KEY (tenant_id, pkce_verifier_secret_id)
        REFERENCES secret_references (tenant_id, id)
        ON DELETE RESTRICT,
    CONSTRAINT oauth_authorization_states_expiry_check
        CHECK (expires_at > created_at)
);
CREATE UNIQUE INDEX IF NOT EXISTS oauth_authorization_states_state_hash_unique
    ON oauth_authorization_states (state_hash);
CREATE INDEX IF NOT EXISTS oauth_authorization_states_tenant_expiry_idx
    ON oauth_authorization_states (tenant_id, expires_at)
    WHERE consumed_at IS NULL;

ALTER TABLE oauth_authorization_states ENABLE ROW LEVEL SECURITY;
ALTER TABLE oauth_authorization_states FORCE ROW LEVEL SECURITY;
CREATE POLICY oauth_authorization_states_isolation ON oauth_authorization_states
    USING (tenant_id::text = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id::text = current_setting('app.current_tenant', true));

-- ===========================================================================
-- 3. Outbound endpoint/payload/delivery relay with leases and fencing
-- ===========================================================================

-- Keep legacy columns readable during a rolling upgrade, but new repositories
-- exclusively read/write the canonical columns below.
ALTER TABLE webhooks
    ADD COLUMN IF NOT EXISTS url text NULL,
    ADD COLUMN IF NOT EXISTS description text NULL,
    ADD COLUMN IF NOT EXISTS subscribed_events text[] NOT NULL DEFAULT '{}'::text[],
    ADD COLUMN IF NOT EXISTS signature_algorithm text NOT NULL DEFAULT 'hmac_sha256',
    ADD COLUMN IF NOT EXISTS signing_secret_id uuid NULL,
    ADD COLUMN IF NOT EXISTS signing_key_version text NULL,
    ADD COLUMN IF NOT EXISTS enabled boolean NOT NULL DEFAULT true,
    ADD COLUMN IF NOT EXISTS retry_max_attempts integer NOT NULL DEFAULT 6,
    ADD COLUMN IF NOT EXISTS retry_initial_backoff_ms bigint NOT NULL DEFAULT 1000,
    ADD COLUMN IF NOT EXISTS consecutive_failures integer NOT NULL DEFAULT 0;

UPDATE webhooks AS endpoint
SET signing_secret_id = reference.id
FROM secret_references AS reference
WHERE endpoint.tenant_id = reference.tenant_id
  AND endpoint.signing_secret_id IS NULL
  AND endpoint.signing_ref ? 'id'
  AND (endpoint.signing_ref ->> 'id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  AND reference.id = (endpoint.signing_ref ->> 'id')::uuid;

-- Legacy endpoints without a fully resolvable reference are disabled rather
-- than allowed to send unsigned traffic.
UPDATE webhooks
SET enabled = false,
    status = CASE WHEN status = 'active' THEN 'disabled' ELSE status END
WHERE signing_secret_id IS NULL;

UPDATE webhooks
SET url = COALESCE(url, target_url),
    subscribed_events = CASE
        WHEN cardinality(subscribed_events) > 0 THEN subscribed_events
        WHEN jsonb_typeof(event_filter) = 'array' THEN ARRAY(
            SELECT jsonb_array_elements_text(event_filter)
        )
        ELSE '{}'::text[]
    END,
    enabled = enabled AND status = 'active'
WHERE url IS NULL
   OR cardinality(subscribed_events) = 0;

ALTER TABLE webhooks
    ALTER COLUMN url SET NOT NULL,
    DROP CONSTRAINT IF EXISTS webhooks_signature_algorithm_check,
    ADD CONSTRAINT webhooks_signature_algorithm_check
        CHECK (signature_algorithm = 'hmac_sha256'),
    DROP CONSTRAINT IF EXISTS webhooks_retry_max_attempts_check,
    ADD CONSTRAINT webhooks_retry_max_attempts_check
        CHECK (retry_max_attempts BETWEEN 1 AND 64),
    DROP CONSTRAINT IF EXISTS webhooks_retry_initial_backoff_check,
    ADD CONSTRAINT webhooks_retry_initial_backoff_check
        CHECK (retry_initial_backoff_ms BETWEEN 1 AND 300000),
    DROP CONSTRAINT IF EXISTS webhooks_consecutive_failures_check,
    ADD CONSTRAINT webhooks_consecutive_failures_check
        CHECK (consecutive_failures >= 0),
    DROP CONSTRAINT IF EXISTS webhooks_signing_key_version_check,
    ADD CONSTRAINT webhooks_signing_key_version_check
        CHECK (signing_key_version IS NULL OR char_length(signing_key_version) BETWEEN 1 AND 128),
    ADD CONSTRAINT webhooks_signing_secret_scope_fk
        FOREIGN KEY (tenant_id, signing_secret_id)
        REFERENCES secret_references (tenant_id, id)
        ON DELETE RESTRICT;

ALTER TABLE webhooks
    ADD CONSTRAINT webhooks_tenant_id_unique UNIQUE (tenant_id, id);

CREATE INDEX IF NOT EXISTS webhooks_tenant_enabled_idx
    ON webhooks (tenant_id, id)
    WHERE enabled;

-- Exact payload bytes are retained separately from delivery state. Bodies are
-- bounded so an event fanout cannot turn the database into unbounded blob
-- storage. The SHA-256 hex check lets repositories verify body integrity before
-- handoff to the guarded HTTP transport.
CREATE TABLE IF NOT EXISTS webhook_payloads (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    event_id uuid NOT NULL,
    event_type text NOT NULL CHECK (char_length(event_type) BETWEEN 1 AND 128),
    body bytea NOT NULL CHECK (octet_length(body) BETWEEN 1 AND 1048576),
    body_sha256 text NOT NULL CHECK (body_sha256 ~ '^[A-Fa-f0-9]{64}$'),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, event_id),
    UNIQUE (tenant_id, id)
);
CREATE INDEX IF NOT EXISTS webhook_payloads_retention_idx
    ON webhook_payloads (created_at);

ALTER TABLE webhook_payloads ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_payloads FORCE ROW LEVEL SECURITY;
CREATE POLICY webhook_payloads_isolation ON webhook_payloads
    USING (tenant_id::text = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id::text = current_setting('app.current_tenant', true));

ALTER TABLE webhook_deliveries
    ADD COLUMN IF NOT EXISTS payload_id uuid NULL,
    ADD COLUMN IF NOT EXISTS payload_hash text NULL,
    ADD COLUMN IF NOT EXISTS signing_key_version text NULL,
    ADD COLUMN IF NOT EXISTS lease_owner text NULL,
    ADD COLUMN IF NOT EXISTS lease_expires_at timestamptz NULL,
    ADD COLUMN IF NOT EXISTS fence bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS dead_lettered_at timestamptz NULL;

ALTER TABLE webhook_deliveries
    DROP CONSTRAINT IF EXISTS webhook_deliveries_status_check,
    ADD CONSTRAINT webhook_deliveries_status_check
        CHECK (status IN ('pending', 'leased', 'delivered', 'failed', 'dead_lettered', 'cancelled', 'exhausted')),
    DROP CONSTRAINT IF EXISTS webhook_deliveries_payload_hash_check,
    ADD CONSTRAINT webhook_deliveries_payload_hash_check
        CHECK (payload_hash IS NULL OR payload_hash ~ '^[A-Fa-f0-9]{64}$'),
    DROP CONSTRAINT IF EXISTS webhook_deliveries_fence_check,
    ADD CONSTRAINT webhook_deliveries_fence_check
        CHECK (fence >= 0),
    DROP CONSTRAINT IF EXISTS webhook_deliveries_lease_check,
    ADD CONSTRAINT webhook_deliveries_lease_check
        CHECK (
            (status = 'leased' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
            OR (status <> 'leased' AND lease_owner IS NULL AND lease_expires_at IS NULL)
        ),
    ADD CONSTRAINT webhook_deliveries_payload_scope_fk
        FOREIGN KEY (tenant_id, payload_id)
        REFERENCES webhook_payloads (tenant_id, id)
        ON DELETE RESTRICT;

-- Legacy scheduled rows cannot safely be signed/replayed because their old
-- event payload is not present in the integrations schema. Preserve them as
-- transparent terminal evidence instead of inventing a payload or fake send.
UPDATE webhook_deliveries
SET status = 'exhausted',
    last_error = COALESCE(last_error, 'legacy delivery lacks durable payload; not replayed'),
    dead_lettered_at = COALESCE(dead_lettered_at, now())
WHERE payload_id IS NULL
  AND status IN ('pending', 'failed');

-- One payload may legitimately fan out to many endpoints, but each endpoint
-- can receive an event only once in the durable delivery stream.
CREATE UNIQUE INDEX IF NOT EXISTS webhook_deliveries_endpoint_event_unique
    ON webhook_deliveries (webhook_id, event_id);
CREATE INDEX IF NOT EXISTS webhook_deliveries_claim_idx
    ON webhook_deliveries (status, next_attempt_at ASC, id ASC)
    WHERE status IN ('pending', 'failed', 'leased');
CREATE INDEX IF NOT EXISTS webhook_deliveries_lease_expiry_idx
    ON webhook_deliveries (lease_expires_at ASC)
    WHERE status = 'leased';
CREATE INDEX IF NOT EXISTS webhook_deliveries_dead_letter_idx
    ON webhook_deliveries (tenant_id, dead_lettered_at DESC, id DESC)
    WHERE status IN ('dead_lettered', 'exhausted');

-- A dead letter is an immutable operator/replay record. The delivery row keeps
-- the fast query state; this table keeps the terminal reason and fence at the
-- time it became terminal.
CREATE TABLE IF NOT EXISTS webhook_dead_letters (
    delivery_id uuid PRIMARY KEY REFERENCES webhook_deliveries (id) ON DELETE CASCADE,
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    webhook_id uuid NOT NULL REFERENCES webhooks (id) ON DELETE CASCADE,
    event_id uuid NOT NULL,
    payload_id uuid NULL,
    payload_hash text NULL CHECK (payload_hash IS NULL OR payload_hash ~ '^[A-Fa-f0-9]{64}$'),
    final_status integer NULL CHECK (final_status BETWEEN 100 AND 599),
    attempt_count integer NOT NULL CHECK (attempt_count >= 0),
    error text NOT NULL CHECK (char_length(error) BETWEEN 1 AND 1024),
    final_fence bigint NOT NULL CHECK (final_fence >= 0),
    dead_lettered_at timestamptz NOT NULL DEFAULT now(),
    replayed_at timestamptz NULL,
    replayed_by uuid NULL,
    CONSTRAINT webhook_dead_letters_payload_scope_fk
        FOREIGN KEY (tenant_id, payload_id)
        REFERENCES webhook_payloads (tenant_id, id)
        ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS webhook_dead_letters_tenant_cursor_idx
    ON webhook_dead_letters (tenant_id, dead_lettered_at DESC, delivery_id DESC);

ALTER TABLE webhook_dead_letters ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_dead_letters FORCE ROW LEVEL SECURITY;
CREATE POLICY webhook_dead_letters_isolation ON webhook_dead_letters
    USING (tenant_id::text = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id::text = current_setting('app.current_tenant', true));

-- ===========================================================================
-- 4. Inbound verified receipt/outbox ordering facts
-- ===========================================================================

CREATE TABLE IF NOT EXISTS inbound_webhook_receipts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    source text NOT NULL CHECK (source ~ '^[A-Za-z0-9._:-]{1,128}$'),
    external_event_id text NOT NULL CHECK (char_length(external_event_id) BETWEEN 1 AND 256),
    sequence_no bigint NULL CHECK (sequence_no >= 0),
    payload_sha256 text NOT NULL CHECK (payload_sha256 ~ '^[A-Fa-f0-9]{64}$'),
    signature_key_version text NULL CHECK (char_length(signature_key_version) BETWEEN 1 AND 128),
    received_at timestamptz NOT NULL DEFAULT now(),
    -- accepted is ready for outbox processing; deferred has a sequence gap;
    -- processed is terminal and idempotent. Rejected evidence is never created
    -- before signature verification succeeds.
    processing_state text NOT NULL DEFAULT 'accepted'
        CHECK (processing_state IN ('accepted', 'deferred', 'processed')),
    outbox_event_id uuid NULL,
    processed_at timestamptz NULL,
    UNIQUE (tenant_id, source, external_event_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS inbound_webhook_receipts_source_sequence_unique
    ON inbound_webhook_receipts (tenant_id, source, sequence_no)
    WHERE sequence_no IS NOT NULL;
CREATE INDEX IF NOT EXISTS inbound_webhook_receipts_ready_idx
    ON inbound_webhook_receipts (received_at ASC, id ASC)
    WHERE processing_state = 'accepted';
CREATE INDEX IF NOT EXISTS inbound_webhook_receipts_deferred_idx
    ON inbound_webhook_receipts (tenant_id, source, sequence_no ASC)
    WHERE processing_state = 'deferred';

ALTER TABLE inbound_webhook_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE inbound_webhook_receipts FORCE ROW LEVEL SECURITY;
CREATE POLICY inbound_webhook_receipts_isolation ON inbound_webhook_receipts
    USING (tenant_id::text = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id::text = current_setting('app.current_tenant', true));

CREATE TABLE IF NOT EXISTS inbound_webhook_order_cursors (
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    source text NOT NULL CHECK (source ~ '^[A-Za-z0-9._:-]{1,128}$'),
    last_processed_sequence bigint NOT NULL DEFAULT -1,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, source)
);

ALTER TABLE inbound_webhook_order_cursors ENABLE ROW LEVEL SECURITY;
ALTER TABLE inbound_webhook_order_cursors FORCE ROW LEVEL SECURITY;
CREATE POLICY inbound_webhook_order_cursors_isolation ON inbound_webhook_order_cursors
    USING (tenant_id::text = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id::text = current_setting('app.current_tenant', true));

-- ===========================================================================
-- 5. Durable recipient notifications and lifecycle/deduplication facts
-- ===========================================================================

ALTER TABLE notifications
    ADD COLUMN IF NOT EXISTS deduplication_key text NULL,
    ADD COLUMN IF NOT EXISTS destination text NULL,
    ADD COLUMN IF NOT EXISTS subject text NULL,
    ADD COLUMN IF NOT EXISTS body text NULL,
    ADD COLUMN IF NOT EXISTS delivery_state text NULL,
    ADD COLUMN IF NOT EXISTS attempts integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS max_attempts integer NOT NULL DEFAULT 6,
    ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz NULL,
    ADD COLUMN IF NOT EXISTS last_error text NULL,
    ADD COLUMN IF NOT EXISTS archived_at timestamptz NULL,
    ADD COLUMN IF NOT EXISTS delivered_at timestamptz NULL,
    ADD COLUMN IF NOT EXISTS lease_owner text NULL,
    ADD COLUMN IF NOT EXISTS lease_expires_at timestamptz NULL,
    ADD COLUMN IF NOT EXISTS fence bigint NOT NULL DEFAULT 0;

-- Historical template/payload rows are converted to safe, bounded evidence;
-- a migration never attempts to render or deliver an old template implicitly.
UPDATE notifications
SET deduplication_key = COALESCE(deduplication_key, 'legacy:' || id::text),
    destination = COALESCE(destination, 'legacy'),
    subject = COALESCE(subject, left(template, 512)),
    body = COALESCE(body, left(payload::text, 65536)),
    delivery_state = COALESCE(
        delivery_state,
        CASE status
            WHEN 'sent' THEN 'delivered'
            WHEN 'cancelled' THEN 'cancelled'
            WHEN 'failed' THEN 'dead_lettered'
            ELSE 'pending'
        END
    ),
    delivered_at = CASE WHEN status = 'sent' THEN COALESCE(delivered_at, created_at) ELSE delivered_at END
WHERE deduplication_key IS NULL
   OR destination IS NULL
   OR subject IS NULL
   OR body IS NULL
   OR delivery_state IS NULL;

UPDATE notifications
SET channel = 'in_app'
WHERE channel NOT IN ('email', 'webhook', 'slack', 'in_app');

ALTER TABLE notifications
    ALTER COLUMN deduplication_key SET NOT NULL,
    ALTER COLUMN destination SET NOT NULL,
    ALTER COLUMN subject SET NOT NULL,
    ALTER COLUMN body SET NOT NULL,
    ALTER COLUMN delivery_state SET NOT NULL,
    DROP CONSTRAINT IF EXISTS notifications_channel_check,
    ADD CONSTRAINT notifications_channel_check
        CHECK (channel IN ('email', 'webhook', 'slack', 'in_app')),
    DROP CONSTRAINT IF EXISTS notifications_deduplication_key_check,
    ADD CONSTRAINT notifications_deduplication_key_check
        CHECK (char_length(deduplication_key) BETWEEN 1 AND 256),
    DROP CONSTRAINT IF EXISTS notifications_subject_check,
    ADD CONSTRAINT notifications_subject_check
        CHECK (char_length(subject) BETWEEN 1 AND 512),
    DROP CONSTRAINT IF EXISTS notifications_body_check,
    ADD CONSTRAINT notifications_body_check
        CHECK (char_length(body) BETWEEN 1 AND 65536),
    DROP CONSTRAINT IF EXISTS notifications_delivery_state_check,
    ADD CONSTRAINT notifications_delivery_state_check
        CHECK (delivery_state IN ('pending', 'sending', 'delivered', 'failed', 'dead_lettered', 'cancelled')),
    DROP CONSTRAINT IF EXISTS notifications_attempts_check,
    ADD CONSTRAINT notifications_attempts_check
        CHECK (attempts >= 0 AND max_attempts BETWEEN 1 AND 64 AND attempts <= max_attempts),
    DROP CONSTRAINT IF EXISTS notifications_lifecycle_check,
    ADD CONSTRAINT notifications_lifecycle_check
        CHECK (
            (delivery_state = 'failed' AND next_attempt_at IS NOT NULL)
            OR (delivery_state <> 'failed')
        ),
    DROP CONSTRAINT IF EXISTS notifications_lease_check,
    ADD CONSTRAINT notifications_lease_check
        CHECK (
            (delivery_state = 'sending' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
            OR (delivery_state <> 'sending' AND lease_owner IS NULL AND lease_expires_at IS NULL)
        ),
    DROP CONSTRAINT IF EXISTS notifications_fence_check,
    ADD CONSTRAINT notifications_fence_check CHECK (fence >= 0);

CREATE UNIQUE INDEX IF NOT EXISTS notifications_tenant_recipient_dedup_unique
    ON notifications (tenant_id, recipient_id, channel, deduplication_key);
CREATE INDEX IF NOT EXISTS notifications_recipient_lifecycle_cursor_idx
    ON notifications (tenant_id, recipient_id, archived_at, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS notifications_claim_idx
    ON notifications (delivery_state, next_attempt_at ASC, id ASC)
    WHERE delivery_state IN ('pending', 'failed', 'sending');
CREATE INDEX IF NOT EXISTS notifications_lease_expiry_idx
    ON notifications (lease_expires_at ASC)
    WHERE delivery_state = 'sending';

-- ===========================================================================
-- 6. Minimum grants for the dedicated durable integrations relay
-- ===========================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mas_integrations') THEN
        CREATE ROLE mas_integrations NOLOGIN BYPASSRLS;
    END IF;
END
$$;
ALTER ROLE mas_integrations NOLOGIN BYPASSRLS;

GRANT USAGE ON SCHEMA public TO mas_integrations;
-- The relay gets endpoint metadata, payload bytes and delivery state only. It
-- deliberately gets no SELECT on credentials, OAuth states, users, API keys,
-- arbitrary outbox rows, or the task-worker tables.
GRANT SELECT, UPDATE ON webhooks TO mas_integrations;
GRANT SELECT ON secret_references TO mas_integrations;
GRANT SELECT ON webhook_payloads TO mas_integrations;
GRANT SELECT, UPDATE ON webhook_deliveries TO mas_integrations;
GRANT SELECT, INSERT, UPDATE ON webhook_dead_letters TO mas_integrations;

COMMENT ON ROLE mas_integrations IS
    'Durable outbound integrations relay. NOLOGIN/BYPASSRLS is necessary only '
    'to claim webhook deliveries across tenants with SKIP LOCKED. Grants are '
    'limited to webhooks, webhook payloads, delivery state and dead letters; '
    'it cannot read credentials, OAuth state, API keys, task queues or user data.';

-- OAuth callbacks arrive before a tenant can be resolved from state. A separate
-- NOLOGIN role can consume only hashed one-shot state and reference metadata;
-- it cannot read credential rows or general tenant data.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mas_oauth') THEN
        CREATE ROLE mas_oauth NOLOGIN BYPASSRLS;
    END IF;
END
$$;
ALTER ROLE mas_oauth NOLOGIN BYPASSRLS;
GRANT USAGE ON SCHEMA public TO mas_oauth;
GRANT SELECT, UPDATE ON oauth_authorization_states TO mas_oauth;
GRANT SELECT ON secret_references TO mas_oauth;
COMMENT ON ROLE mas_oauth IS
    'OAuth callback state consumer. NOLOGIN/BYPASSRLS exists only because a '
    'callback state is intentionally tenant-opaque before its one-shot hash is '
    'consumed. Grants are OAuth state SELECT/UPDATE and secret-reference '
    'metadata SELECT; it cannot read credentials, tokens, API keys or tasks.';

-- Notification delivery likewise scans all tenant queues, but is isolated
-- from integration/OAuth workers because it can read recipient message bodies.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mas_notifications') THEN
        CREATE ROLE mas_notifications NOLOGIN BYPASSRLS;
    END IF;
END
$$;
ALTER ROLE mas_notifications NOLOGIN BYPASSRLS;
GRANT USAGE ON SCHEMA public TO mas_notifications;
GRANT SELECT, UPDATE ON notifications TO mas_notifications;
COMMENT ON ROLE mas_notifications IS
    'Durable notification delivery plane. NOLOGIN/BYPASSRLS is limited to '
    'claiming and settling notification rows across tenants; it has no grants '
    'on credentials, connectors, webhooks, OAuth state, API keys or tasks.';

-- Existing tables predate the default-privileges declaration in some deployed
-- databases; make the request-role grants explicit and auditable.
GRANT SELECT, INSERT, UPDATE, DELETE ON secret_references TO mas_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON oauth_authorization_states TO mas_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON webhook_payloads TO mas_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON webhook_dead_letters TO mas_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON inbound_webhook_receipts TO mas_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON inbound_webhook_order_cursors TO mas_app;
