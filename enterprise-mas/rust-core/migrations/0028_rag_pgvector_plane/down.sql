-- Reverses 0028_data_hub_rag_plane only after an explicit data-loss opt-in.
-- Set `mas.allow_data_loss = 'true'` in the migration session to drop any
-- remaining Data Hub/RAG data. The pgvector extension is intentionally kept:
-- extensions may be shared by other database objects.

DO $$
BEGIN
    IF current_setting('mas.allow_data_loss', true) IS DISTINCT FROM 'true'
       AND (
           EXISTS (SELECT 1 FROM data_sources LIMIT 1)
           OR EXISTS (SELECT 1 FROM documents LIMIT 1)
           OR EXISTS (SELECT 1 FROM document_chunks LIMIT 1)
       ) THEN
        RAISE EXCEPTION
            '0028 down migration would delete Data Hub/RAG data; set mas.allow_data_loss=true explicitly';
    END IF;
END
$$;

DROP TABLE IF EXISTS document_chunks;
DROP TABLE IF EXISTS documents;
DROP TABLE IF EXISTS data_sources;

REVOKE ALL ON SCHEMA public FROM mas_rag;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mas_rag') THEN
        DROP ROLE mas_rag;
    END IF;
END
$$;
