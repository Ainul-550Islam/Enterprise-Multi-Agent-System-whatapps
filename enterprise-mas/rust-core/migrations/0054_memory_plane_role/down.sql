-- Reverses 0054_memory_plane_role only after an explicit data-loss opt-in.
-- Set `mas.allow_data_loss = 'true'` in the migration session to drop stored
-- memory records.

DO $$
BEGIN
    IF current_setting('mas.allow_data_loss', true) IS DISTINCT FROM 'true'
       AND EXISTS (SELECT 1 FROM memory_records LIMIT 1) THEN
        RAISE EXCEPTION
            '0054 down migration would delete memory records; set mas.allow_data_loss=true explicitly';
    END IF;
END
$$;

DROP TABLE IF EXISTS memory_records;

REVOKE ALL ON SCHEMA public FROM mas_memory;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mas_memory') THEN
        DROP ROLE mas_memory;
    END IF;
END
$$;
