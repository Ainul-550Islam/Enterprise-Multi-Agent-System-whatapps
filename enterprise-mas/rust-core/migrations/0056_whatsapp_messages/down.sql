-- Reverses 0056_whatsapp_messages only after an explicit data-loss opt-in.
-- Set `mas.allow_data_loss = 'true'` in the migration session to drop stored
-- messages.

DO $$
BEGIN
    IF current_setting('mas.allow_data_loss', true) IS DISTINCT FROM 'true'
       AND EXISTS (SELECT 1 FROM whatsapp_messages LIMIT 1) THEN
        RAISE EXCEPTION
            '0056 down migration would delete whatsapp messages; set mas.allow_data_loss=true explicitly';
    END IF;
END
$$;

DROP TABLE IF EXISTS whatsapp_messages;
