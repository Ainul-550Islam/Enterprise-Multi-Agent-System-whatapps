-- Rollback weakens the guarantee that newly written receipts were verified.
ALTER TABLE billing_webhook_receipts
    DROP CONSTRAINT billing_receipts_verified_new_rows;
