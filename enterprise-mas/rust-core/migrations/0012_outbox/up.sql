-- 0012_outbox: transactional outbox for domain events.
--
-- State changes and event inserts commit atomically; the dispatcher publishes
-- after commit and marks rows as it goes. `envelope` carries the full
-- EventEnvelope JSON (including tenant_id, making the rows self-scoping even
-- when fetched by the cross-tenant dispatcher role).

CREATE TABLE outbox_events (
    event_id        uuid PRIMARY KEY,
    envelope        jsonb NOT NULL,
    tenant_id       uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    event_type      text NOT NULL,
    aggregate_type  text NOT NULL,
    aggregate_id    text NOT NULL,
    status          text NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'published', 'delivered', 'failed', 'dead_lettered')),
    attempts        integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error      text NULL,
    enqueued_at     timestamptz NOT NULL DEFAULT now(),
    published_at    timestamptz NULL
);

-- Dispatcher hot path: due pending/failed rows, oldest first.
CREATE INDEX outbox_events_due_idx ON outbox_events (next_attempt_at)
    WHERE status IN ('pending', 'failed');
CREATE INDEX outbox_events_tenant_idx ON outbox_events (tenant_id, enqueued_at DESC);
CREATE INDEX outbox_events_aggregate_idx ON outbox_events (aggregate_type, aggregate_id);

ALTER TABLE outbox_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE outbox_events FORCE ROW LEVEL SECURITY;
-- The dispatcher runs under a dedicated BYPASSRLS service role (it must fetch
-- across tenants); ordinary mas_app traffic is confined to its own tenant.
CREATE POLICY outbox_events_isolation ON outbox_events
    USING (tenant_id::text = current_setting('app.current_tenant', true));
