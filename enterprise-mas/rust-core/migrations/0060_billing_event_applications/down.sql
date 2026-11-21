-- Reverses 0060_billing_event_applications only after an explicit data-loss
-- opt-in. Set `mas.allow_data_loss = 'true'` in the migration session to drop
-- stored application claims.

DO $$
BEGIN
    IF current_setting('mas.allow_data_loss', true) IS DISTINCT FROM 'true'
       AND EXISTS (SELECT 1 FROM billing_event_applications LIMIT 1) THEN
        RAISE EXCEPTION
            '0060 down migration would delete billing event applications; set mas.allow_data_loss=true explicitly';
    END IF;
END
$$;

DROP TABLE IF EXISTS billing_event_applications;
