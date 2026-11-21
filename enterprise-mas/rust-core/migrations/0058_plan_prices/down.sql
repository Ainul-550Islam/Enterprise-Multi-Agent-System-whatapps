-- Reverses 0058_plan_prices only after an explicit data-loss opt-in.
-- Set `mas.allow_data_loss = 'true'` in the migration session to drop stored
-- price mappings.

DO $$
BEGIN
    IF current_setting('mas.allow_data_loss', true) IS DISTINCT FROM 'true'
       AND EXISTS (SELECT 1 FROM plan_prices LIMIT 1) THEN
        RAISE EXCEPTION
            '0058 down migration would delete plan prices; set mas.allow_data_loss=true explicitly';
    END IF;
END
$$;

DROP TABLE IF EXISTS plan_prices;
