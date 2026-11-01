-- PURPOSE: Reverse of 0053 — revoke only the sequences 0053 newly granted
-- to the runtime role. Grants that predate 0053 (0021's sweep, 0033's
-- worker-plane grants) are left untouched.
REVOKE USAGE, SELECT ON SEQUENCE broker_delivery_receipts_receipt_sequence_seq FROM mas_app;
REVOKE USAGE, SELECT ON SEQUENCE broker_task_messages_message_sequence_seq FROM mas_app;
