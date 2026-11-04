//! Live PostgreSQL enforcement beyond the billing receipt adapter.

use mas_common::ids::BillingReceiptId;
use mas_common::timestamps::Timestamp;
use mas_persistence::repositories::control_plane::{
    BillingClaim, PostgresBillingWebhookStore, ReceiptState,
};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[tokio::test]
async fn database_rejects_unverified_new_receipts_and_terminal_rewrites() {
    let url =
        std::env::var("MAS_TEST_DATABASE_APP_URL").expect("MAS_TEST_DATABASE_APP_URL required");
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("app pool");
    let event = format!("evt_{}", Uuid::new_v4().simple());
    let unsigned = sqlx::query(
        "INSERT INTO billing_webhook_receipts (provider, event_id, event_type, payload_hash, signature_valid, state)
         VALUES ('stripe', $1, 'invoice.paid', $2, false, 'rejected')",
    ).bind(&event).bind("a".repeat(64)).execute(&pool).await;
    assert!(
        unsigned.is_err(),
        "unverified inserts must not reserve an event id"
    );

    let store = PostgresBillingWebhookStore::new(pool.clone());
    let BillingClaim::New(receipt) = store
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
    else {
        panic!("expected new")
    };
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

#[tokio::test]
async fn authenticated_stripe_envelope_can_claim_a_receipt_without_trusting_tenant_input() {
    use mas_integrations::stripe_webhooks::verify_stripe_event;
    use mas_integrations::webhooks::compute_signature;
    let url =
        std::env::var("MAS_TEST_DATABASE_APP_URL").expect("MAS_TEST_DATABASE_APP_URL required");
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("app pool");
    let store = PostgresBillingWebhookStore::new(pool);
    let id = format!("evt_{}", Uuid::new_v4().simple());
    let body = format!(
        r#"{{"id":"{id}","type":"invoice.paid","livemode":false,"account":"acct_test","metadata":{{"tenant_id":"untrusted"}}}}"#
    );
    let secret = b"whsec_test_0123456789abcdef";
    let now = Timestamp::now();
    let signature = compute_signature(now.to_unix_seconds(), body.as_bytes(), secret);
    let header = format!("t={},v1={signature}", now.to_unix_seconds());
    let event = verify_stripe_event(&header, body.as_bytes(), &[secret], &now, false)
        .expect("verify signed body");
    assert_eq!(event.account(), Some("acct_test"));
    let BillingClaim::New(row) = store
        .claim_verified_stripe(&event)
        .await
        .expect("claim verified")
    else {
        panic!("new event")
    };
    assert_eq!(row.event_id, id);
    assert_eq!(row.payload_hash, event.payload_hash());
    assert_eq!(
        row.tenant_id, None,
        "tenant is never resolved from provider JSON"
    );
    assert!(matches!(
        store
            .claim_verified_stripe(&event)
            .await
            .expect("duplicate"),
        BillingClaim::Existing(_)
    ));
    assert!(verify_stripe_event(&header, b"{}", &[secret], &now, false).is_err());
}
