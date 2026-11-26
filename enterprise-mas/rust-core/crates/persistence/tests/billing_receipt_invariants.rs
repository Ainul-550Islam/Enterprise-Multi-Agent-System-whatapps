//! Live PostgreSQL enforcement beyond the billing receipt adapter.

use mas_common::ids::BillingReceiptId;
use mas_common::timestamps::Timestamp;
use mas_persistence::repositories::control_plane::{PostgresBillingWebhookStore, ReceiptState};
use uuid::Uuid;

mod support;

#[tokio::test]
async fn database_rejects_unverified_new_receipts_and_terminal_rewrites() {
    let Some(pool) = support::live_pool("billing-receipt-invariants").await else {
        return;
    };
    let event = format!("evt_{}", Uuid::new_v4().simple());
    let unsigned = sqlx::query(
        "INSERT INTO billing_webhook_receipts (provider, event_id, event_type, payload_hash, signature_valid, state)
         VALUES ('stripe', $1, 'invoice.paid', $2, false, 'received')",
    ).bind(&event).bind("a".repeat(64)).execute(&pool).await;
    assert!(
        unsigned.is_err(),
        "unverified inserts must not reserve an event id"
    );

    let store = PostgresBillingWebhookStore::new(pool.clone());
    let receipt = store
        .claim(
            "stripe",
            &event,
            "invoice.paid",
            &"a".repeat(64),
            true,
            None,
        )
        .await
        .expect("new receipt")
        .expect("receipt is created");
    for sql in [
        "UPDATE billing_webhook_receipts SET event_type = 'invoice.failed' WHERE id = $1",
        "UPDATE billing_webhook_receipts SET received_at = received_at + interval '1 day' WHERE id = $1",
        "UPDATE billing_webhook_receipts SET tenant_id = gen_random_uuid() WHERE id = $1",
        "UPDATE billing_webhook_receipts SET processed_at = now() WHERE id = $1",
    ] {
        assert!(sqlx::query(sql).bind(receipt.id).execute(&pool).await.is_err(), "accepted {sql}");
    }
    let finalized = store
        .finish(
            BillingReceiptId::from_uuid(receipt.id),
            ReceiptState::Ignored,
            Some("unsupported"),
            None,
            &Timestamp::now(),
        )
        .await
        .expect("terminal transition");
    assert_eq!(finalized.state, "ignored");
    assert!(finalized.processed_at.is_some());
    for sql in [
        "UPDATE billing_webhook_receipts SET state = 'applied' WHERE id = $1",
        "UPDATE billing_webhook_receipts SET state = 'received' WHERE id = $1",
        "UPDATE billing_webhook_receipts SET rejection_reason = 'changed' WHERE id = $1",
        "UPDATE billing_webhook_receipts SET tenant_id = gen_random_uuid() WHERE id = $1",
        "UPDATE billing_webhook_receipts SET processed_at = now() + interval '1 day' WHERE id = $1",
    ] {
        assert!(
            sqlx::query(sql)
                .bind(receipt.id)
                .execute(&pool)
                .await
                .is_err(),
            "accepted {sql}"
        );
    }
    assert!(
        sqlx::query("UPDATE billing_webhook_receipts SET state = state WHERE id = $1")
            .bind(receipt.id)
            .execute(&pool)
            .await
            .is_ok()
    );
    let row = store
        .get("stripe", &event)
        .await
        .expect("receipt unchanged");
    assert_eq!(row.state, "ignored");
    assert_eq!(row.rejection_reason.as_deref(), Some("unsupported"));
    assert_eq!(row.processed_at, finalized.processed_at);
}
