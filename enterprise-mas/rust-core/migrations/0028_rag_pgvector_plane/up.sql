-- PURPOSE: Migration 0028 — isolate the Python RAG vector plane from Rust-owned metadata.
--
-- The vector plane contains only derived chunks, embeddings, and retrieval
-- filters. Source, document, project, membership, and permission metadata stay
-- in the Rust-owned tables and are never writable from this role.
--
-- This migration is additive. The production Python DSN must authenticate as
-- a deployment login that assumes only `mas_rag`; the role itself is NOLOGIN,
-- receives no BYPASSRLS, and has no access to any Rust-owned table.

CREATE EXTENSION IF NOT EXISTS vector;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mas_rag') THEN
        CREATE ROLE mas_rag NOLOGIN NOBYPASSRLS;
    END IF;
END
$$;

CREATE TABLE data_sources (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    project_id UUID NOT NULL REFERENCES projects (id) ON DELETE CASCADE,
    name TEXT NOT NULL CHECK (char_length(name) BETWEEN 1 AND 200),
    kind TEXT NOT NULL CHECK (kind IN ('upload', 'object_storage', 'web')),
    status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'paused', 'syncing', 'failed', 'archived')),
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    credential_id UUID NULL REFERENCES credentials (id) ON DELETE RESTRICT,
    created_by UUID NOT NULL REFERENCES users (id) ON DELETE RESTRICT,
    last_synced_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, project_id, name)
);
CREATE INDEX data_sources_tenant_project_idx ON data_sources (tenant_id, project_id, status);
CREATE TRIGGER data_sources_updated_at BEFORE UPDATE ON data_sources
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();
ALTER TABLE data_sources ENABLE ROW LEVEL SECURITY;
ALTER TABLE data_sources FORCE ROW LEVEL SECURITY;
CREATE POLICY data_sources_tenant_isolation ON data_sources
    USING (tenant_id::text = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id::text = current_setting('app.current_tenant', true));

CREATE TABLE documents (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    project_id UUID NOT NULL REFERENCES projects (id) ON DELETE CASCADE,
    source_id UUID NOT NULL,
    title TEXT NOT NULL CHECK (char_length(title) BETWEEN 1 AND 512),
    mime_type TEXT NOT NULL CHECK (char_length(mime_type) BETWEEN 1 AND 255),
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    content_sha256 TEXT NOT NULL CHECK (content_sha256 ~ '^[a-f0-9]{64}$'),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'ready', 'failed', 'deleted')),
    access_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by UUID NOT NULL REFERENCES users (id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id, source_id)
        REFERENCES data_sources (tenant_id, id) ON DELETE CASCADE
);
CREATE INDEX documents_tenant_source_idx ON documents (tenant_id, source_id, status);
CREATE TRIGGER documents_updated_at BEFORE UPDATE ON documents
    FOR EACH ROW EXECUTE FUNCTION mas_set_updated_at();
ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
ALTER TABLE documents FORCE ROW LEVEL SECURITY;
CREATE POLICY documents_tenant_isolation ON documents
    USING (tenant_id::text = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id::text = current_setting('app.current_tenant', true));

CREATE TABLE document_chunks (
    tenant_id UUID NOT NULL,
    document_id UUID NOT NULL,
    chunk_id TEXT NOT NULL CHECK (char_length(chunk_id) BETWEEN 1 AND 128),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    start_char INTEGER NOT NULL CHECK (start_char >= 0),
    end_char INTEGER NOT NULL CHECK (end_char >= start_char),
    chunk_text TEXT NOT NULL CHECK (char_length(chunk_text) BETWEEN 1 AND 8000),
    embedding vector(1536) NOT NULL,
    embedding_model TEXT NOT NULL CHECK (char_length(embedding_model) BETWEEN 1 AND 255),
    title TEXT NOT NULL DEFAULT '' CHECK (char_length(title) <= 512),
    source_uri TEXT CHECK (source_uri IS NULL OR char_length(source_uri) <= 2048),
    required_roles TEXT[] NOT NULL DEFAULT '{}',
    denied_principals TEXT[] NOT NULL DEFAULT '{}',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, document_id, chunk_id)
);

CREATE INDEX document_chunks_tenant_document_idx
    ON document_chunks (tenant_id, document_id, ordinal);
CREATE INDEX document_chunks_embedding_hnsw_idx
    ON document_chunks USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

ALTER TABLE document_chunks ENABLE ROW LEVEL SECURITY;
ALTER TABLE document_chunks FORCE ROW LEVEL SECURITY;
CREATE POLICY document_chunks_tenant_isolation ON document_chunks
    USING (
        tenant_id = nullif(current_setting('app.current_tenant', true), '')::uuid
    )
    WITH CHECK (
        tenant_id = nullif(current_setting('app.current_tenant', true), '')::uuid
    );

-- 0001 grants default privileges to mas_app; remove that grant explicitly so
-- only the dedicated RAG role can access the derived vector table.
REVOKE ALL ON document_chunks FROM PUBLIC;
REVOKE ALL ON document_chunks FROM mas_app;
GRANT USAGE ON SCHEMA public TO mas_rag;
GRANT SELECT, INSERT, UPDATE, DELETE ON document_chunks TO mas_rag;
REVOKE CREATE ON SCHEMA public FROM mas_rag;
