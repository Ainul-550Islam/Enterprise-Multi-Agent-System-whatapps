-- Durable RAG-plane ownership and deletion fences. The RAG role still cannot
-- access Rust-owned catalogue tables: the one-time backfill runs as migrator.
-- A migration login unable to bypass FORCE RLS must fail, not silently copy an
-- incomplete subset. Uncatalogued legacy vectors are deliberately not claimed.
SET LOCAL row_security = off;
CREATE TABLE rag_document_fences (
    tenant_id UUID NOT NULL,
    document_id UUID NOT NULL,
    organization_id UUID NOT NULL,
    project_id UUID NOT NULL,
    source_id UUID NOT NULL,
    deleted BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, document_id)
);
INSERT INTO rag_document_fences (tenant_id, document_id, organization_id, project_id, source_id, deleted)
SELECT d.tenant_id, d.id, p.organization_id, d.project_id, d.source_id, d.status = 'deleted'
FROM documents AS d
JOIN projects AS p ON p.id = d.project_id AND p.tenant_id = d.tenant_id;
ALTER TABLE rag_document_fences ENABLE ROW LEVEL SECURITY;
ALTER TABLE rag_document_fences FORCE ROW LEVEL SECURITY;
CREATE POLICY rag_document_fences_tenant_isolation ON rag_document_fences
    USING (tenant_id = nullif(current_setting('app.current_tenant', true), '')::uuid)
    WITH CHECK (tenant_id = nullif(current_setting('app.current_tenant', true), '')::uuid);
REVOKE ALL ON rag_document_fences FROM PUBLIC;
REVOKE ALL ON rag_document_fences FROM mas_app;
GRANT SELECT, INSERT, UPDATE ON rag_document_fences TO mas_rag;
