-- PURPOSE: Align Rust-owned Data Hub metadata with its aggregate state machines.
--
-- Migration 0028 introduced the isolated vector plane and metadata tables but
-- predates the Rust application aggregates. This additive follow-up persists
-- source cursors and stable failure codes, the document chunk count, and the
-- lifecycle vocabulary enforced by the domain.

ALTER TABLE data_sources
    ADD COLUMN sync_cursor TEXT NULL,
    ADD COLUMN last_failure_code TEXT NULL
        CHECK (last_failure_code IS NULL OR last_failure_code ~ '^[A-Za-z0-9_-]{1,128}$');

UPDATE data_sources
SET status = CASE status
    WHEN 'paused' THEN 'disabled'
    WHEN 'failed' THEN 'error'
    WHEN 'archived' THEN 'disabled'
    ELSE status
END;

ALTER TABLE data_sources DROP CONSTRAINT IF EXISTS data_sources_status_check;
ALTER TABLE data_sources
    ADD CONSTRAINT data_sources_status_check
    CHECK (status IN ('draft', 'active', 'syncing', 'error', 'disabled'));

CREATE INDEX data_sources_tenant_sync_idx
    ON data_sources (tenant_id, status, updated_at DESC);

ALTER TABLE documents
    ADD COLUMN chunk_count INTEGER NOT NULL DEFAULT 0 CHECK (chunk_count >= 0),
    ADD COLUMN failure_code TEXT NULL
        CHECK (failure_code IS NULL OR failure_code ~ '^[A-Za-z0-9_-]{1,128}$');

UPDATE documents
SET failure_code = 'unknown_failure'
WHERE status = 'failed' AND failure_code IS NULL;

ALTER TABLE documents
    ADD CONSTRAINT documents_failure_state_consistency
    CHECK ((status = 'failed') = (failure_code IS NOT NULL));

CREATE INDEX documents_tenant_project_status_idx
    ON documents (tenant_id, project_id, status, created_at DESC);
