-- Reverses 0055_whatsapp_channels only after an explicit data-loss opt-in.
-- Set `mas.allow_data_loss = 'true'` in the migration session to drop stored
-- channel configuration.

DO $$
BEGIN
    IF current_setting('mas.allow_data_loss', true) IS DISTINCT FROM 'true'
       AND EXISTS (SELECT 1 FROM whatsapp_channels LIMIT 1) THEN
        RAISE EXCEPTION
            '0055 down migration would delete whatsapp channels; set mas.allow_data_loss=true explicitly';
    END IF;
END
$$;

DROP TABLE IF EXISTS whatsapp_channels;
DROP FUNCTION IF EXISTS mas_resolve_whatsapp_tenant(text);
