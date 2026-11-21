//! Client behaviour through a mock transport: URL pinning to the Stripe API
//! host, auth header, event parsing and error mapping — no network involved.

use mas_billing_stripe::client::{
    HttpStripeClient, HttpTransport, StripeClient, TransportResponse, STRIPE_API_HOST,
};
use mas_common::result::Result;
use std::sync::{Arc, Mutex};

struct MockTransport {
    calls: Arc<Mutex<Vec<(String, String)>>>,
    respond: TransportResponse,
}

impl HttpTransport for MockTransport {
    fn get(&self, url: &str, authorization: &str) -> Result<TransportResponse> {
        self.calls
            .lock()
            .expect("lock")
            .push((url.to_owned(), authorization.to_owned()));
        Ok(self.respond.clone())
    }
}

fn transport(status: u16, body: &str) -> (Arc<Mutex<Vec<(String, String)>>>, MockTransport) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    (
        Arc::clone(&calls),
        MockTransport {
            calls,
            respond: TransportResponse {
                status,
                body: body.to_owned(),
            },
        },
    )
}

#[test]
fn mock_stripe_client_pins_the_api_host_and_parses_events() {
    let (calls, t) = transport(200, include_str!("fixtures/invoice_paid.json"));
    let client = HttpStripeClient::new(Box::new(t), "sk_test_abc");
    let event = client.fetch_event("evt_1PABCinvoice0003").expect("fetch");
    assert_eq!(event.id, "evt_1PABCinvoice0003");

    let sent = calls.lock().expect("lock");
    assert_eq!(sent.len(), 1);
    let (url, auth) = &sent[0];
    assert!(
        url.starts_with(&format!("https://{STRIPE_API_HOST}/")),
        "host pinned: {url}"
    );
    assert!(
        url.ends_with("/v1/events/evt_1PABCinvoice0003"),
        "url: {url}"
    );
    assert_eq!(auth, "Bearer sk_test_abc");
}

#[test]
fn mock_stripe_client_redacts_the_key_in_debug() {
    let (_calls, t) = transport(200, "{}");
    let client = HttpStripeClient::new(Box::new(t), "sk_live_secret");
    let dbg = format!("{client:?}");
    assert!(dbg.contains("[redacted]"), "key leaked: {dbg}");
    assert!(!dbg.contains("sk_live_secret"));
}

#[test]
fn mock_stripe_client_maps_provider_errors() {
    let (_calls, t) = transport(500, "oops");
    let client = HttpStripeClient::new(Box::new(t), "sk");
    let err = client
        .fetch_event("evt_anything")
        .expect_err("500 must fail");
    assert!(err.to_string().contains("stripe"), "got: {err}");
}

#[test]
fn mock_stripe_client_maps_402_and_429() {
    let (_calls, t) = transport(402, "card_declined");
    let client = HttpStripeClient::new(Box::new(t), "sk");
    let err = client.retrieve_subscription("sub_x").expect_err("402");
    assert_eq!(err.error_code(), "CONFLICT");

    let (_calls, t) = transport(429, "slow");
    let client = HttpStripeClient::new(Box::new(t), "sk");
    let err = client.retrieve_subscription("sub_x").expect_err("429");
    assert_eq!(err.error_code(), "RATE_LIMITED");
}

#[test]
fn mock_stripe_client_rejects_foreign_ids() {
    let (_calls, t) = transport(200, "{}");
    let client = HttpStripeClient::new(Box::new(t), "sk");
    assert!(client.fetch_event("not-an-event").is_err());
}
