-- 0009_schedules: cron/interval schedules and their run history.

CREATE TABLE schedules (
    id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id      uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    organization_id uuid NOT NULL REFERENCES organizations (id) ON DELETE RESTRICT,
    name           text NOT NULL,
    target         jsonb NOT NULL, -- what runs: workflow/agent + input template
    rule           jsonb NOT NULL, -- cron | interval | one-shot payload
    status         text NOT NULL DEFAULT 'active'
                   CHECK (status IN ('active', 'paused', 'disabled', 'completed')),
    timezone       text NOT NULL DEFAULT 'UTC',
    next_run_at    timestamptz NULL,
    last_run_at    timestamptz NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);
CREATE INDEX schedules_due_idx ON schedules (next_run_at)
    WHERE status = 'active' AND next_run_at IS NOT NULL;
CREATE TRIGGER schedules_updated_at BEFORE UPDATE ON schedules
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();

ALTER TABLE schedules ENABLE ROW LEVEL SECURITY;
ALTER TABLE schedules FORCE ROW LEVEL SECURITY;
CREATE POLICY schedules_isolation ON schedules
    USING (tenant_id::text = current_setting('app.current_tenant', true));

CREATE TABLE schedule_runs (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id    uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    schedule_id  uuid NOT NULL REFERENCES schedules (id) ON DELETE CASCADE,
    planned_at   timestamptz NOT NULL,
    execution_id uuid NULL REFERENCES executions (id) ON DELETE SET NULL,
    outcome      text NOT NULL DEFAULT 'dispatched'
                 CHECK (outcome IN ('dispatched', 'skipped_catchup', 'skipped_overlap', 'disabled')),
    detail       jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (schedule_id, planned_at) -- one run per planned tick, enforced
);
CREATE INDEX schedule_runs_schedule_idx ON schedule_runs (schedule_id, planned_at DESC);

ALTER TABLE schedule_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE schedule_runs FORCE ROW LEVEL SECURITY;
CREATE POLICY schedule_runs_isolation ON schedule_runs
    USING (tenant_id::text = current_setting('app.current_tenant', true));
