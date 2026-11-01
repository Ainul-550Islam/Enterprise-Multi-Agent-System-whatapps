-- PURPOSE: Durable delivery receipts and one-time internal-authentication nonces.
--
-- Both records are deliberately global operational facts rather than tenant
-- data. A broker receipt proves one queue delivery across worker restarts;
-- an internal receipt proves a signed service request has been used once
-- across API/orchestrator/scheduler replicas. Neither table stores payloads,
-- secrets, bearer tokens, or tenant authority.

-- One-time v2 internal-service signature nonces. `expires_at` supports bounded
-- cleanup; uniqueness remains enough for the signed freshness window, and an
-- expired nonce's timestamp is rejected before this table is consulted.
CREATE TABLE IF NOT EXISTS internal_request_receipts (
    service_name text NOT NULL CHECK (service_name ~ '^[A-Za-z0-9_-]{1,64}$'),
    nonce text NOT NULL CHECK (nonce ~ '^[A-Za-z0-9_-]{22,128}$'),
    signed_at_ms bigint NOT NULL,
    expires_at timestamptz NOT NULL,
    consumed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (service_name, nonce)
);
CREATE INDEX IF NOT EXISTS internal_request_receipts_expiry_idx
    ON internal_request_receipts (expires_at);

-- API/runtime and explicitly provisioned machine-plane connections must be
-- able to atomically insert their own receipt. The table is non-tenant and
-- carries no user data, so it intentionally has no RLS policy.
GRANT INSERT, SELECT, DELETE ON internal_request_receipts TO mas_app;
GRANT INSERT, SELECT, DELETE ON internal_request_receipts TO mas_worker;
GRANT INSERT, SELECT, DELETE ON internal_request_receipts TO mas_scheduler;

-- Persistent broker configuration lets durable delivery retain its ack policy
-- across a broker process restart. A differing re-registration is a hard
-- configuration conflict in the repository, not an implicit policy change.
CREATE TABLE IF NOT EXISTS broker_consumers (
    consumer_name text PRIMARY KEY CHECK (consumer_name ~ '^[A-Za-z0-9_-]{1,128}$'),
    ack_wait_ms integer NOT NULL CHECK (ack_wait_ms BETWEEN 10 AND 300000),
    max_deliver integer NOT NULL CHECK (max_deliver BETWEEN 1 AND 64),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- A receipt is allocated before delivery and becomes in-flight atomically with
-- the lifecycle claim. The identity sequence is the BrokerPort sequence: it is
-- database-generated, restart-safe, and cannot collide like a UUID prefix.
CREATE TABLE IF NOT EXISTS broker_delivery_receipts (
    receipt_sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    consumer_name text NOT NULL REFERENCES broker_consumers (consumer_name) ON DELETE CASCADE,
    task_id uuid NOT NULL REFERENCES tasks (id) ON DELETE CASCADE,
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    attempt_id uuid NULL REFERENCES task_attempts (id) ON DELETE SET NULL,
    worker_id text NULL,
    delivery_count integer NOT NULL CHECK (delivery_count BETWEEN 1 AND 64),
    status text NOT NULL CHECK (status IN ('reserved', 'in_flight', 'acked', 'nacked', 'terminated', 'expired')),
    lease_expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    settled_at timestamptz NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS broker_delivery_one_active_task
    ON broker_delivery_receipts (task_id)
    WHERE status IN ('reserved', 'in_flight');
CREATE INDEX IF NOT EXISTS broker_delivery_active_expiry_idx
    ON broker_delivery_receipts (lease_expires_at)
    WHERE status IN ('reserved', 'in_flight');
CREATE INDEX IF NOT EXISTS broker_delivery_consumer_history_idx
    ON broker_delivery_receipts (consumer_name, task_id, receipt_sequence DESC);

GRANT SELECT, INSERT, UPDATE, DELETE ON broker_consumers TO mas_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON broker_delivery_receipts TO mas_worker;
GRANT USAGE, SELECT ON SEQUENCE broker_delivery_receipts_receipt_sequence_seq TO mas_worker;

-- Stable, database-generated broker message identity. This is distinct from
-- a delivery receipt: one task message may be delivered several times, while
-- every delivery receives its own receipt above. It replaces UUID-prefix
-- truncation, which collides for UUIDv7 values created in the same millisecond.
CREATE TABLE IF NOT EXISTS broker_task_messages (
    message_sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_id uuid NOT NULL UNIQUE REFERENCES tasks (id) ON DELETE CASCADE,
    created_at timestamptz NOT NULL DEFAULT now()
);
GRANT SELECT, INSERT ON broker_task_messages TO mas_worker;
GRANT USAGE, SELECT ON SEQUENCE broker_task_messages_message_sequence_seq TO mas_worker;
