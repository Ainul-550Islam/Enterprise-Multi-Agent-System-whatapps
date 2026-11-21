//! Fixture-driven event parsing: every fixture under `tests/fixtures/` must
//! parse into the exact typed shape the billing service consumes.

use mas_billing_stripe::{StripeEvent, StripeEventType};

#[test]
fn parses_all_event_fixtures() {
    let checkout = include_str!("fixtures/checkout_completed.json");
    let subscription = include_str!("fixtures/subscription_updated.json");
    let invoice = include_str!("fixtures/invoice_paid.json");

    let e = StripeEvent::parse(checkout.as_bytes()).expect("checkout fixture parses");
    assert_eq!(e.id, "evt_1PABCcheckout0001");
    assert_eq!(e.event_type, StripeEventType::CheckoutSessionCompleted);
    assert_eq!(e.raw_type, "checkout.session.completed");
    assert_eq!(e.livemode, Some(false));

    let e = StripeEvent::parse(subscription.as_bytes()).expect("subscription fixture parses");
    assert_eq!(e.event_type, StripeEventType::CustomerSubscriptionUpdated);
    assert_eq!(e.created, Some(1_763_400_300));

    let e = StripeEvent::parse(invoice.as_bytes()).expect("invoice fixture parses");
    assert_eq!(e.event_type, StripeEventType::InvoicePaid);
    assert!(
        e.checkout_session().is_none(),
        "invoice is not a checkout session"
    );
}

#[test]
fn rejects_broken_envelopes() {
    assert!(StripeEvent::parse(b"not json").is_err());
    assert!(StripeEvent::parse(br#"{"type":"invoice.paid","data":{"object":{}}}"#).is_err());
    assert!(StripeEvent::parse(br#"{"id":"evt_x","data":{"object":{}}}"#).is_err());
}

#[test]
fn classifies_unknown_types_without_dropping_them() {
    let raw =
        br#"{"id":"evt_new","type":"radar.early_fraud_warning.created","data":{"object":{}}}"#;
    let e = StripeEvent::parse(raw).expect("parses");
    assert_eq!(
        e.event_type,
        StripeEventType::Unknown("radar.early_fraud_warning.created".into())
    );
    assert_eq!(e.raw_type, "radar.early_fraud_warning.created");
}
