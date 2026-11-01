-- PURPOSE: Tenant-scoped hourly execution and usage rollups for the dashboard.
--
-- Raw events remain authoritative. This table is a rebuildable read model; the
-- refresh function bounds every call to one tenant and a finite time window.

CREATE TABLE execution_rollups_hourly (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    agent_id UUID NULL,
    bucket TIMESTAMPTZ NOT NULL,
    tasks_completed BIGINT NOT NULL DEFAULT 0 CHECK (tasks_completed >= 0),
    tasks_failed BIGINT NOT NULL DEFAULT 0 CHECK (tasks_failed >= 0),
    tokens BIGINT NOT NULL DEFAULT 0 CHECK (tokens >= 0),
    cost_micros BIGINT NOT NULL DEFAULT 0 CHECK (cost_micros >= 0),
    p95_ms BIGINT NOT NULL DEFAULT 0 CHECK (p95_ms >= 0),
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE NULLS NOT DISTINCT (tenant_id, agent_id, bucket)
);

CREATE INDEX execution_rollups_hourly_tenant_bucket_idx
    ON execution_rollups_hourly (tenant_id, bucket DESC);
CREATE INDEX execution_rollups_hourly_agent_bucket_idx
    ON execution_rollups_hourly (tenant_id, agent_id, bucket DESC)
    WHERE agent_id IS NOT NULL;

ALTER TABLE execution_rollups_hourly ENABLE ROW LEVEL SECURITY;
ALTER TABLE execution_rollups_hourly FORCE ROW LEVEL SECURITY;
CREATE POLICY execution_rollups_hourly_tenant_isolation
    ON execution_rollups_hourly
    USING (tenant_id::text = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id::text = current_setting('app.current_tenant', true));

CREATE OR REPLACE FUNCTION refresh_execution_rollups_hourly(
    requested_tenant UUID,
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ
) RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
DECLARE
    affected BIGINT;
BEGIN
    IF requested_tenant IS NULL OR window_start IS NULL OR window_end IS NULL
       OR window_end <= window_start
       OR window_end - window_start > INTERVAL '93 days' THEN
        RAISE EXCEPTION 'invalid or unbounded analytics refresh window'
            USING ERRCODE = '22023';
    END IF;

    INSERT INTO execution_rollups_hourly (
        tenant_id, agent_id, bucket, tasks_completed, tasks_failed,
        tokens, cost_micros, p95_ms, refreshed_at
    )
    SELECT
        requested_tenant,
        grouped.agent_id,
        grouped.bucket,
        grouped.tasks_completed,
        grouped.tasks_failed,
        COALESCE(usage.tokens, 0),
        COALESCE(usage.cost_micros, 0),
        grouped.p95_ms,
        now()
    FROM (
        SELECT
            e.agent_id,
            date_trunc('hour', e.finished_at) AS bucket,
            count(*) FILTER (WHERE e.status = 'completed')::bigint AS tasks_completed,
            count(*) FILTER (WHERE e.status = 'failed')::bigint AS tasks_failed,
            COALESCE(
                percentile_cont(0.95) WITHIN GROUP (
                    ORDER BY greatest(
                        0,
                        (extract(epoch FROM (e.finished_at - e.started_at)) * 1000)::bigint
                    )
                ) FILTER (WHERE e.started_at IS NOT NULL AND e.finished_at IS NOT NULL),
                0
            )::bigint AS p95_ms
        FROM executions AS e
        WHERE e.tenant_id = requested_tenant
          AND e.finished_at >= window_start
          AND e.finished_at < window_end
          AND e.status IN ('completed', 'failed')
        GROUP BY e.agent_id, date_trunc('hour', e.finished_at)
    ) AS grouped
    LEFT JOIN LATERAL (
        SELECT
            COALESCE(sum(u.quantity) FILTER (WHERE u.resource = 'tokens'), 0)::bigint AS tokens,
            COALESCE(sum(u.cost_micros), 0)::bigint AS cost_micros
        FROM usage_records AS u
        WHERE u.tenant_id = requested_tenant
          AND u.occurred_at >= grouped.bucket
          AND u.occurred_at < grouped.bucket + INTERVAL '1 hour'
          AND (u.dimensions ->> 'agent_id') IS NOT DISTINCT FROM grouped.agent_id::text
    ) AS usage ON true
    ON CONFLICT (tenant_id, agent_id, bucket) DO UPDATE SET
        tasks_completed = EXCLUDED.tasks_completed,
        tasks_failed = EXCLUDED.tasks_failed,
        tokens = EXCLUDED.tokens,
        cost_micros = EXCLUDED.cost_micros,
        p95_ms = EXCLUDED.p95_ms,
        refreshed_at = EXCLUDED.refreshed_at;

    GET DIAGNOSTICS affected = ROW_COUNT;
    RETURN affected;
END;
$$;

REVOKE ALL ON FUNCTION refresh_execution_rollups_hourly(UUID, TIMESTAMPTZ, TIMESTAMPTZ)
    FROM PUBLIC;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mas_worker') THEN
        GRANT EXECUTE ON FUNCTION refresh_execution_rollups_hourly(UUID, TIMESTAMPTZ, TIMESTAMPTZ)
            TO mas_worker;
    END IF;
END
$$;
