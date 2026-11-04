-- Downgrading weakens receipt immutability and allows unverified inserts.
-- Stop receipt writers and review incident implications before rollback.
ALTER TABLE billing_webhook_receipts DROP CONSTRAINT billing_receipts_verified_new_rows;

CREATE OR REPLACE FUNCTION mas_billing_receipt_forward_only() RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    -- The verdict columns are immutable once written.
    IF NEW.signature_valid IS DISTINCT FROM OLD.signature_valid
       OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash
       OR NEW.provider IS DISTINCT FROM OLD.provider
       OR NEW.event_id IS DISTINCT FROM OLD.event_id THEN
        RAISE EXCEPTION
            'billing_webhook_receipts: the verdict (provider, event_id, payload_hash, signature_valid) is immutable';
    END IF;

    -- Only received/ignored may move, and only to a terminal state. A
    -- duplicate finish is a no-op, not a second state change.
    IF OLD.state IN ('applied', 'rejected') AND NEW.state IS DISTINCT FROM OLD.state THEN
        RAISE EXCEPTION
            'billing_webhook_receipts: state % is terminal and cannot become %',
            OLD.state, NEW.state;
    END IF;

    IF OLD.state = 'received' AND NEW.state NOT IN ('received', 'applied', 'ignored', 'rejected') THEN
        RAISE EXCEPTION
            'billing_webhook_receipts: unknown state %', NEW.state;
    END IF;

    -- processed_at is stamped once, on the transition out of `received`.
    IF NEW.state <> 'received' AND OLD.processed_at IS NULL THEN
        NEW.processed_at := now();
    END IF;

    RETURN NEW;
END;
$$;

