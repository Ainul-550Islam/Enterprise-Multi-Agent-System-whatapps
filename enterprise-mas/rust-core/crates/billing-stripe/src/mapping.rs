//! Maps a verified Stripe event onto the subscription object the billing
//! service will apply.
//!
//! Out-of-order webhooks are the common case: `customer.subscription.updated`
//! can arrive after `deleted`, or a stale `updated` can follow a newer one.
//! When a client is provided this module re-fetches the live subscription
//! (`retrieve_subscription`) and uses that object instead of the event
//! payload. The event remains the receipt key; the live object is the state.

use mas_common::error::AppError;
use mas_common::result::Result;

use crate::client::StripeClient;
use crate::event::{StripeEvent, StripeEventType, SubscriptionObject};

/// Resolves the subscription the billing service should apply.
///
/// * When `client` is `Some`, a live retrieve wins over the event payload
///   (out-of-order protection).
/// * A retrieve failure on a `deleted` event falls back to the payload —
///   Stripe 404s a deleted subscription.
/// * Events that do not carry a subscription object return [`AppError::validation`].
pub fn resolve_subscription(
    event: &StripeEvent,
    client: Option<&dyn StripeClient>,
) -> Result<SubscriptionObject> {
    let from_event = event.subscription().ok_or_else(|| {
        AppError::validation(format!(
            "stripe event {} does not carry a subscription object",
            event.raw_type
        ))
    })?;
    let Some(client) = client else {
        return Ok(from_event);
    };
    match client.retrieve_subscription(&from_event.id) {
        Ok(live) => Ok(live),
        Err(error)
            if matches!(
                event.event_type,
                StripeEventType::CustomerSubscriptionDeleted
            ) =>
        {
            let _ = error;
            Ok(from_event)
        },
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{HttpTransport, StripeClient, TransportResponse};
    use mas_common::result::Result;
    use std::sync::{Arc, Mutex};

    struct Mock {
        body: String,
        status: u16,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl HttpTransport for Mock {
        fn get(&self, url: &str, _authorization: &str) -> Result<TransportResponse> {
            self.calls.lock().expect("lock").push(url.to_owned());
            Ok(TransportResponse {
                status: self.status,
                body: self.body.clone(),
            })
        }
    }

    impl StripeClient for Mock {
        fn fetch_event(&self, _event_id: &str) -> Result<StripeEvent> {
            Err(AppError::internal("not used"))
        }

        fn retrieve_subscription(&self, subscription_id: &str) -> Result<SubscriptionObject> {
            let auth = "Bearer x";
            let url = format!("https://api.stripe.com/v1/subscriptions/{subscription_id}");
            let response = self.get(&url, auth)?;
            if !(200..300).contains(&response.status) {
                return Err(AppError::external_service("stripe", "retrieve failed"));
            }
            let value: serde_json::Value = serde_json::from_str(&response.body).expect("json");
            SubscriptionObject::from_value(&value)
                .ok_or_else(|| AppError::external_service("stripe", "shape"))
        }
    }

    #[test]
    fn retrieve_wins_over_a_stale_event_payload() {
        let event = StripeEvent::parse(include_bytes!(
            "../tests/fixtures/subscription_updated.json"
        ))
        .expect("event");
        let stale_status = event.subscription().expect("sub").status.clone();
        assert_eq!(stale_status.as_deref(), Some("active"));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let client = Mock {
            body: r#"{"id":"sub_TEST0002","object":"subscription","status":"canceled","customer":"cus_TEST0001","items":{"data":[{"price":{"id":"price_PRO_MONTHLY"}}]}}"#.into(),
            status: 200,
            calls: Arc::clone(&calls),
        };
        let live = resolve_subscription(&event, Some(&client)).expect("resolve");
        assert_eq!(live.status.as_deref(), Some("canceled"));
        assert_eq!(calls.lock().expect("lock").len(), 1);
    }

    #[test]
    fn deleted_event_falls_back_when_retrieve_404s() {
        let raw = br#"{"id":"evt_del","type":"customer.subscription.deleted","data":{"object":{"id":"sub_TEST0002","status":"canceled","customer":"cus_x"}}}"#;
        let event = StripeEvent::parse(raw).expect("event");
        let client = Mock {
            body: "{}".into(),
            status: 404,
            calls: Arc::new(Mutex::new(Vec::new())),
        };
        let sub = resolve_subscription(&event, Some(&client)).expect("fallback");
        assert_eq!(sub.id, "sub_TEST0002");
        assert_eq!(sub.status.as_deref(), Some("canceled"));
    }
}
