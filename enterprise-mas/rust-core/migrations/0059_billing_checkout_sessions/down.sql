-- Reverses 0059_billing_checkout_sessions only after an explicit data-loss
-- opt-in. Set `mas.allow_data_loss = 'true'` in the migration session to drop
-- stored checkout records.

DO $$
BEGIN
    IF current_setting('mas.allow_data_loss', true) IS DISTINCT FROM 'true'
       AND EXISTS (SELECT 1 FROM billing_checkout_sessions LIMIT 1) THEN
        RAISE EXCEPTION
            '0059 down migration would delete checkout sessions; set mas.allow_data_loss=true explicitly';
    END IF;
END
$$;

DROP TABLE IF EXISTS billing_checkout_sessions;
