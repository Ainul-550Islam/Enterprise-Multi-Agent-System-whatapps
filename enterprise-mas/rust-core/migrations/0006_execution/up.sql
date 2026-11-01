-- 0006_execution: executions, execution steps, tasks, task attempts.

CREATE TABLE executions (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    organization_id uuid NOT NULL REFERENCES organizations (id) ON DELETE RESTRICT,
    project_id      uuid NULL REFERENCES projects (id) ON DELETE SET NULL,
    workflow_id     uuid NULL REFERENCES workflows (id) ON DELETE SET NULL,
    agent_id        uuid NULL REFERENCES agents (id) ON DELETE SET NULL,
    parent_id       uuid NULL REFERENCES executions (id) ON DELETE SET NULL,
    status          text NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'running', 'paused', 'completed', 'failed', 'cancelled')),
    input           jsonb NOT NULL DEFAULT '{}'::jsonb,
    output          jsonb NULL, -- redacted execution output
    error           jsonb NULL, -- classified failure record
    state           jsonb NOT NULL DEFAULT '{}'::jsonb, -- checkpoints/cursors
    correlation_id  text NOT NULL,
    started_at      timestamptz NULL,
    finished_at     timestamptz NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX executions_tenant_status_idx ON executions (tenant_id, status);
CREATE INDEX executions_correlation_idx ON executions (correlation_id);
CREATE TRIGGER executions_updated_at BEFORE UPDATE ON executions
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE executions ENABLE ROW LEVEL SECURITY;
ALTER TABLE executions FORCE ROW LEVEL SECURITY;
CREATE POLICY executions_isolation ON executions
    USING (tenant_id::text = current_setting('app.current_tenant', true));

CREATE TABLE execution_steps (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id    uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    execution_id uuid NOT NULL REFERENCES executions (id) ON DELETE CASCADE,
    node_key     text NOT NULL,
    ordinal      integer NOT NULL CHECK (ordinal >= 0),
    status       text NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'running', 'waiting_approval', 'completed',
                                   'failed', 'skipped', 'cancelled')),
    attempt      integer NOT NULL DEFAULT 1 CHECK (attempt > 0),
    detail       jsonb NOT NULL DEFAULT '{}'::jsonb, -- inputs/outputs (redacted)
    started_at   timestamptz NULL,
    finished_at  timestamptz NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (execution_id, ordinal)
);
CREATE INDEX execution_steps_execution_idx ON execution_steps (execution_id);
CREATE TRIGGER execution_steps_updated_at BEFORE UPDATE ON execution_steps
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE execution_steps ENABLE ROW LEVEL SECURITY;
ALTER TABLE execution_steps FORCE ROW LEVEL SECURITY;
CREATE POLICY execution_steps_isolation ON execution_steps
    USING (tenant_id::text = current_setting('app.current_tenant', true));

CREATE TABLE tasks (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    organization_id uuid NOT NULL REFERENCES organizations (id) ON DELETE RESTRICT,
    project_id      uuid NULL REFERENCES projects (id) ON DELETE SET NULL,
    execution_id    uuid NULL REFERENCES executions (id) ON DELETE SET NULL,
    idempotency_key text NOT NULL,
    kind            text NOT NULL,
    status          text NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'queued', 'running', 'completed',
                                      'failed', 'cancelled', 'dead_lettered')),
    priority        text NOT NULL DEFAULT 'normal'
                    CHECK (priority IN ('low', 'normal', 'high', 'critical')),
    payload         jsonb NOT NULL DEFAULT '{}'::jsonb,
    result          jsonb NULL,
    not_before      timestamptz NOT NULL DEFAULT now(),
    deadline_at     timestamptz NULL,
    correlation_id  text NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, idempotency_key)
);
CREATE INDEX tasks_dispatch_idx ON tasks (status, priority, not_before)
    WHERE status IN ('pending', 'queued');
CREATE TRIGGER tasks_updated_at BEFORE UPDATE ON tasks
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE tasks ENABLE ROW LEVEL SECURITY;
ALTER TABLE tasks FORCE ROW LEVEL SECURITY;
CREATE POLICY tasks_isolation ON tasks
    USING (tenant_id::text = current_setting('app.current_tenant', true));

CREATE TABLE task_attempts (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    task_id       uuid NOT NULL REFERENCES tasks (id) ON DELETE CASCADE,
    attempt       integer NOT NULL CHECK (attempt > 0),
    worker_id     text NOT NULL,
    lease_expires timestamptz NULL,
    status        text NOT NULL DEFAULT 'running'
                  CHECK (status IN ('running', 'succeeded', 'failed', 'expired', 'cancelled')),
    detail        jsonb NOT NULL DEFAULT '{}'::jsonb,
    started_at    timestamptz NOT NULL DEFAULT now(),
    finished_at   timestamptz NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (task_id, attempt)
);
CREATE INDEX task_attempts_task_idx ON task_attempts (task_id);
CREATE INDEX task_attempts_lease_idx ON task_attempts (lease_expires)
    WHERE status = 'running';
CREATE TRIGGER task_attempts_updated_at BEFORE UPDATE ON task_attempts
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE task_attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE task_attempts FORCE ROW LEVEL SECURITY;
CREATE POLICY task_attempts_isolation ON task_attempts
    USING (tenant_id::text = current_setting('app.current_tenant', true));
