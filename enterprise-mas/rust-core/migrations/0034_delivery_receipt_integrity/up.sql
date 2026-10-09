-- PURPOSE: Harden the delivery-receipt schema introduced by 0033 without
-- rewriting that already-versioned migration.
--
-- PostgreSQL migrations are checksum-pinned by `mas_schema_migrations`.
-- Existing deployments may therefore have applied 0033 already; changing it
-- would make the next boot fail closed with a checksum mismatch. This follow-up
-- migration installs the same invariants for every deployed database.
--
-- The receipt is a cross-replica ownership record. Its task, tenant and
-- attached attempt must name one coherent durable run; otherwise a malformed
-- or stale receipt could be attached across tenant/task boundaries and later
-- be used to settle an unrelated broker delivery.

-- Composite foreign keys require unique targets. `id` is already each table's
-- primary key; the composite keys deliberately repeat tenant/task dimensions
-- so PostgreSQL can enforce the association in the receipt itself.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'tasks_id_tenant_unique'
          AND conrelid = 'tasks'::regclass
    ) THEN
        ALTER TABLE tasks
            ADD CONSTRAINT tasks_id_tenant_unique UNIQUE (id, tenant_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'task_attempts_id_task_tenant_unique'
          AND conrelid = 'task_attempts'::regclass
    ) THEN
        ALTER TABLE task_attempts
            ADD CONSTRAINT task_attempts_id_task_tenant_unique
            UNIQUE (id, task_id, tenant_id);
    END IF;
END $$;

-- An attempt must belong to the receipt's exact task and tenant, not merely
-- exist somewhere in the attempt ledger. NULL remains valid while a reserved
-- receipt has not reached durable claim creation.
ALTER TABLE broker_delivery_receipts
    ADD CONSTRAINT broker_delivery_receipts_task_tenant_fk
    FOREIGN KEY (task_id, tenant_id)
    REFERENCES tasks (id, tenant_id)
    ON DELETE CASCADE;

ALTER TABLE broker_delivery_receipts
    ADD CONSTRAINT broker_delivery_receipts_attempt_task_tenant_fk
    FOREIGN KEY (attempt_id, task_id, tenant_id)
    REFERENCES task_attempts (id, task_id, tenant_id);

-- A receipt must be live for a positive interval and terminal receipts carry a
-- settlement timestamp. `now()` cannot appear in a PostgreSQL CHECK (it is
-- non-immutable), so current-time liveness remains fenced in the repository
-- UPDATE predicates; this schema check prevents structurally impossible rows.
ALTER TABLE broker_delivery_receipts
    ADD CONSTRAINT broker_delivery_receipts_positive_lease_check
    CHECK (lease_expires_at > created_at),
    ADD CONSTRAINT broker_delivery_receipts_settlement_shape_check
    CHECK (
        (status IN ('reserved', 'in_flight') AND settled_at IS NULL)
        OR
        (status IN ('acked', 'nacked', 'terminated', 'expired') AND settled_at IS NOT NULL)
    );

-- Plane roles need only the operations their repositories issue. In
-- particular, neither a worker nor a request-serving service should delete
-- audit receipts or mutate an established durable-consumer policy.
REVOKE SELECT, DELETE, UPDATE ON internal_request_receipts
    FROM mas_app, mas_worker, mas_scheduler;
GRANT INSERT ON internal_request_receipts
    TO mas_app, mas_worker, mas_scheduler;

REVOKE UPDATE, DELETE ON broker_consumers FROM mas_worker;
GRANT SELECT, INSERT ON broker_consumers TO mas_worker;

REVOKE DELETE ON broker_delivery_receipts FROM mas_worker;
GRANT SELECT, INSERT, UPDATE ON broker_delivery_receipts TO mas_worker;
