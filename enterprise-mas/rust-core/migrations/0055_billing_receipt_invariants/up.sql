-- Old deployments may have recorded unsigned events before verifying them.
-- Preserve those rows for investigation; enforce the rule for all future
-- inserts and updates without making this migration discard evidence.
ALTER TABLE billing_webhook_receipts
    ADD CONSTRAINT billing_receipts_verified_new_rows
    CHECK (signature_valid) NOT VALID;

-- A verified receipt's identity and initial facts never change. The only
-- mutation permitted is one transition from received to a terminal verdict;
-- final receipts, including ignored, cannot be rewritten or reopened.
CREATE OR REPLACE FUNCTION mas_billing_receipt_forward_only() RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.id IS DISTINCT FROM OLD.id
       OR NEW.provider IS DISTINCT FROM OLD.provider
       OR NEW.event_id IS DISTINCT FROM OLD.event_id
       OR NEW.event_type IS DISTINCT FROM OLD.event_type
       OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash
       OR NEW.signature_valid IS DISTINCT FROM OLD.signature_valid
       OR NEW.received_at IS DISTINCT FROM OLD.received_at THEN
        RAISE EXCEPTION 'billing receipt identity and verification are immutable';
    END IF;

    IF OLD.state <> 'received' THEN
        IF NEW IS DISTINCT FROM OLD THEN
            RAISE EXCEPTION 'terminal billing receipt is immutable';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.state = 'received' THEN
        IF NEW IS DISTINCT FROM OLD THEN
            RAISE EXCEPTION 'received billing receipt cannot be edited';
        END IF;
        RETURN NEW;
    END IF;

    IF NOT OLD.signature_valid OR NEW.state NOT IN ('applied', 'ignored', 'rejected') THEN
        RAISE EXCEPTION 'invalid billing receipt transition';
    END IF;
    IF OLD.tenant_id IS NOT NULL AND NEW.tenant_id IS DISTINCT FROM OLD.tenant_id THEN
        RAISE EXCEPTION 'billing receipt tenant cannot be changed';
    END IF;
    -- The database, not a caller-provided clock, stamps finalization once.
    NEW.processed_at := now();
    RETURN NEW;
END;
$$;
