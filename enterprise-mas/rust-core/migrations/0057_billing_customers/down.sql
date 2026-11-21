-- Reverses 0057_billing_customers only after an explicit data-loss opt-in.
-- Set `mas.allow_data_loss = 'true'` in the migration session to drop stored
-- customer mappings.

DO $$
BEGIN
    IF current_setting('mas.allow_data_loss', true) IS DISTINCT FROM 'true'
       AND EXISTS (SELECT 1 FROM billing_customers LIMIT 1) THEN
        RAISE EXCEPTION
            '0057 down migration would delete billing customers; set mas.allow_data_loss=true explicitly';
    END IF;
END
$$;

DROP TABLE IF EXISTS billing_customers;
DROP FUNCTION IF EXISTS mas_resolve_billing_tenant(text, text);
