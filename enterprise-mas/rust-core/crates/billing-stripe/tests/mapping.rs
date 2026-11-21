//! Event → domain mapping: the projections the billing service acts on must
//! carry exactly the documented fields, and nothing may be trusted blindly.

use mas_billing_stripe::{StripeEvent, SubscriptionObject};

#[test]
fn maps_checkout_completed() {
    let body = include_str!("fixtures/checkout_completed.json");
    let e = StripeEvent::parse(body.as_bytes()).expect("parses");
    let session = e.checkout_session().expect("checkout projection");
    assert_eq!(session.id, "cs_test_a1b2c3");
    assert_eq!(session.mode.as_deref(), Some("subscription"));
    assert_eq!(session.customer.as_deref(), Some("cus_TEST0001"));
    assert_eq!(session.subscription.as_deref(), Some("sub_TEST0001"));
    assert_eq!(session.status.as_deref(), Some("complete"));
    assert_eq!(session.payment_status.as_deref(), Some("paid"));
    // client_reference_id is carried, never trusted (reconciliation happens
    // in the billing service against rows this platform owns)
    assert_eq!(session.client_reference_id.as_deref(), Some("tenant-1000"));
}

#[test]
fn maps_subscription_items_to_the_first_price() {
    let body = include_str!("fixtures/subscription_updated.json");
    let e = StripeEvent::parse(body.as_bytes()).expect("parses");
    let sub: SubscriptionObject = e.subscription().expect("subscription projection");
    assert_eq!(sub.id, "sub_TEST0002");
    assert_eq!(sub.status.as_deref(), Some("active"));
    assert_eq!(sub.price_id.as_deref(), Some("price_PRO_MONTHLY"));
    assert_eq!(sub.cancel_at_period_end, Some(true));
}

#[test]
fn refuses_foreign_object_shapes() {
    // a cs_-prefixed id is the shape key: foreign shapes project as None
    assert!(SubscriptionObject::from_value(&serde_json::json!({"id": "cs_x"})).is_none());
    assert!(
        mas_billing_stripe::CheckoutSession::from_value(&serde_json::json!({"id": "sub_x"}))
            .is_none()
    );
    assert!(
        mas_billing_stripe::CheckoutSession::from_value(&serde_json::json!({"no": "id"})).is_none()
    );
}
