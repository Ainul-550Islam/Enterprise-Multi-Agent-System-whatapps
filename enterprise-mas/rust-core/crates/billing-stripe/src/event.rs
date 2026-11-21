//! The Stripe webhook event envelope and the typed object projections the
//! billing service consumes.
//!
//! Parsing is deliberately shallow: the envelope's identity fields are
//! validated, and `data.object` is kept as raw JSON until a typed projection
//! ([`CheckoutSession`], [`SubscriptionObject`]) is requested. Stripe adds
//! fields freely, so unknown fields are ignored and unknown event types are
//! surfaced as [`StripeEventType::Unknown`] -- never guessed at or silently
//! treated as a known transition.

use mas_common::error::AppError;
use mas_common::result::Result;
use serde::Deserialize;

/// The event families the billing service knows how to act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StripeEventType {
    /// `checkout.session.completed` -- a checkout finished successfully.
    CheckoutSessionCompleted,
    /// `checkout.session.expired` -- a checkout lapsed before completion.
    CheckoutSessionExpired,
    /// `customer.subscription.created`
    CustomerSubscriptionCreated,
    /// `customer.subscription.updated`
    CustomerSubscriptionUpdated,
    /// `customer.subscription.deleted`
    CustomerSubscriptionDeleted,
    /// `invoice.paid`
    InvoicePaid,
    /// `invoice.payment_failed`
    InvoicePaymentFailed,
    /// Any event type this build does not act on. The raw type string is
    /// preserved for the receipt log and diagnostics.
    Unknown(String),
}

impl StripeEventType {
    /// Classifies a raw `type` string.
    #[must_use]
    pub fn classify(raw: &str) -> Self {
        match raw {
            "checkout.session.completed" => Self::CheckoutSessionCompleted,
            "checkout.session.expired" => Self::CheckoutSessionExpired,
            "customer.subscription.created" => Self::CustomerSubscriptionCreated,
            "customer.subscription.updated" => Self::CustomerSubscriptionUpdated,
            "customer.subscription.deleted" => Self::CustomerSubscriptionDeleted,
            "invoice.paid" => Self::InvoicePaid,
            "invoice.payment_failed" => Self::InvoicePaymentFailed,
            other => Self::Unknown(other.to_owned()),
        }
    }

    /// Whether this event family carries a checkout session object.
    #[must_use]
    pub fn is_checkout_session(&self) -> bool {
        matches!(
            self,
            Self::CheckoutSessionCompleted | Self::CheckoutSessionExpired
        )
    }

    /// Whether this event family carries a subscription object.
    #[must_use]
    pub fn is_subscription(&self) -> bool {
        matches!(
            self,
            Self::CustomerSubscriptionCreated
                | Self::CustomerSubscriptionUpdated
                | Self::CustomerSubscriptionDeleted
        )
    }
}

impl std::fmt::Display for StripeEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CheckoutSessionCompleted => f.write_str("checkout.session.completed"),
            Self::CheckoutSessionExpired => f.write_str("checkout.session.expired"),
            Self::CustomerSubscriptionCreated => f.write_str("customer.subscription.created"),
            Self::CustomerSubscriptionUpdated => f.write_str("customer.subscription.updated"),
            Self::CustomerSubscriptionDeleted => f.write_str("customer.subscription.deleted"),
            Self::InvoicePaid => f.write_str("invoice.paid"),
            Self::InvoicePaymentFailed => f.write_str("invoice.payment_failed"),
            Self::Unknown(raw) => f.write_str(raw),
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawEnvelope {
    id: String,
    #[serde(rename = "type")]
    event_type: String,
    created: Option<u64>,
    livemode: Option<bool>,
    data: RawData,
}

#[derive(Debug, Deserialize)]
struct RawData {
    object: serde_json::Value,
}

/// One signature-verified webhook event.
#[derive(Debug, Clone)]
pub struct StripeEvent {
    /// Stripe's event id (`evt_...`). This is the durable receipt key.
    pub id: String,
    /// Raw `type` string, preserved verbatim.
    pub raw_type: String,
    /// Classified event family.
    pub event_type: StripeEventType,
    /// Event creation time (unix seconds), when Stripe supplied it.
    pub created: Option<u64>,
    /// Whether the event came from live mode.
    pub livemode: Option<bool>,
    /// The event's `data.object`, untyped.
    pub data_object: serde_json::Value,
}

impl StripeEvent {
    /// Parses a raw webhook body that has **already** passed signature
    /// verification.
    ///
    /// # Errors
    ///
    /// [`AppError::validation`] when the body is not a JSON object with an
    /// `id`, a `type` and a `data.object`.
    pub fn parse(body: &[u8]) -> Result<Self> {
        let envelope: RawEnvelope = serde_json::from_slice(body)
            .map_err(|_| AppError::validation("stripe event body is not a valid event envelope"))?;
        if envelope.id.trim().is_empty() {
            return Err(AppError::validation("stripe event is missing an id"));
        }
        if envelope.event_type.trim().is_empty() {
            return Err(AppError::validation("stripe event is missing a type"));
        }
        Ok(Self {
            event_type: StripeEventType::classify(&envelope.event_type),
            id: envelope.id,
            raw_type: envelope.event_type,
            created: envelope.created,
            livemode: envelope.livemode,
            data_object: envelope.data.object,
        })
    }

    /// Projects `data.object` as a checkout session, when the shape matches.
    #[must_use]
    pub fn checkout_session(&self) -> Option<CheckoutSession> {
        CheckoutSession::from_value(&self.data_object)
    }

    /// Projects `data.object` as a subscription, when the shape matches.
    #[must_use]
    pub fn subscription(&self) -> Option<SubscriptionObject> {
        SubscriptionObject::from_value(&self.data_object)
    }
}

/// A `checkout.session` object projection.
///
/// Only the fields the billing flow acts on are read; everything else stays
/// in `data_object`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckoutSession {
    /// Session id (`cs_...`).
    pub id: String,
    /// Checkout mode: `payment`, `subscription` or `setup`.
    pub mode: Option<String>,
    /// Provider customer id (`cus_...`), once attached.
    pub customer: Option<String>,
    /// Provider subscription id (`sub_...`) for subscription-mode sessions.
    pub subscription: Option<String>,
    /// Session status: `open`, `complete` or `expired`.
    pub status: Option<String>,
    /// Payment status for payment-mode sessions.
    pub payment_status: Option<String>,
    /// The caller-supplied reference. The platform uses this to carry its own
    /// tenant/organization id -- it is **never** trusted until reconciled
    /// against a row this platform owns.
    pub client_reference_id: Option<String>,
}

impl CheckoutSession {
    /// Projects a `data.object` value when it has the session shape.
    #[must_use]
    pub fn from_value(value: &serde_json::Value) -> Option<Self> {
        let id = value.get("id")?.as_str()?.to_owned();
        if !id.starts_with("cs_") {
            return None;
        }
        let text = |key: &str| value.get(key).and_then(|v| v.as_str()).map(str::to_owned);
        Some(Self {
            id,
            mode: text("mode"),
            customer: text("customer"),
            subscription: text("subscription"),
            status: text("status"),
            payment_status: text("payment_status"),
            client_reference_id: text("client_reference_id"),
        })
    }
}

/// A `customer.subscription` object projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionObject {
    /// Subscription id (`sub_...`).
    pub id: String,
    /// Provider customer id (`cus_...`).
    pub customer: Option<String>,
    /// Subscription status: `trialing`, `active`, `past_due`, `canceled`, ...
    pub status: Option<String>,
    /// The price id of the first subscription item, when present.
    pub price_id: Option<String>,
    /// Whether the subscription cancels at the period end.
    pub cancel_at_period_end: Option<bool>,
}

impl SubscriptionObject {
    /// Projects a `data.object` value when it has the subscription shape.
    #[must_use]
    pub fn from_value(value: &serde_json::Value) -> Option<Self> {
        let id = value.get("id")?.as_str()?.to_owned();
        if !id.starts_with("sub_") {
            return None;
        }
        let text = |key: &str| value.get(key).and_then(|v| v.as_str()).map(str::to_owned);
        let price_id = value
            .pointer("/items/data/0/price/id")
            .and_then(|v| v.as_str())
            .map(str::to_owned);
        Some(Self {
            id,
            customer: text("customer"),
            status: text("status"),
            price_id,
            cancel_at_period_end: value
                .get("cancel_at_period_end")
                .and_then(serde_json::Value::as_bool),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const COMPLETED_SESSION: &str = r#"{
        "id": "evt_1",
        "type": "checkout.session.completed",
        "created": 1700000000,
        "livemode": false,
        "data": {
            "object": {
                "id": "cs_test_abc",
                "mode": "subscription",
                "customer": "cus_1",
                "subscription": "sub_1",
                "status": "complete",
                "payment_status": "paid",
                "client_reference_id": "tenant-1",
                "unknown_future_field": {"nested": true}
            }
        },
        "unknown_top_level": 42
    }"#;

    #[test]
    fn parses_a_completed_checkout_session() {
        let event = StripeEvent::parse(COMPLETED_SESSION.as_bytes()).expect("valid event");
        assert_eq!(event.id, "evt_1");
        assert_eq!(event.event_type, StripeEventType::CheckoutSessionCompleted);
        assert_eq!(event.created, Some(1_700_000_000));
        assert_eq!(event.livemode, Some(false));

        let session = event.checkout_session().expect("session projection");
        assert_eq!(session.id, "cs_test_abc");
        assert_eq!(session.mode.as_deref(), Some("subscription"));
        assert_eq!(session.customer.as_deref(), Some("cus_1"));
        assert_eq!(session.subscription.as_deref(), Some("sub_1"));
        assert_eq!(session.client_reference_id.as_deref(), Some("tenant-1"));
    }

    #[test]
    fn unknown_event_types_are_preserved_not_guessed() {
        let body = r#"{"id":"evt_2","type":"radar.early_fraud_warning.created",
            "data":{"object":{"id":"issfr_1"}}}"#;
        let event = StripeEvent::parse(body.as_bytes()).expect("valid event");
        assert_eq!(
            event.event_type,
            StripeEventType::Unknown("radar.early_fraud_warning.created".to_owned())
        );
        assert_eq!(
            event.event_type.to_string(),
            "radar.early_fraud_warning.created"
        );
        assert!(!event.event_type.is_checkout_session());
        assert!(!event.event_type.is_subscription());
        assert!(event.checkout_session().is_none());
        assert!(event.subscription().is_none());
    }

    #[test]
    fn projects_a_subscription() {
        let body = r#"{
            "id": "evt_3",
            "type": "customer.subscription.updated",
            "data": {
                "object": {
                    "id": "sub_9",
                    "customer": "cus_9",
                    "status": "active",
                    "cancel_at_period_end": true,
                    "items": {"data": [{"price": {"id": "price_9"}}]}
                }
            }
        }"#;
        let event = StripeEvent::parse(body.as_bytes()).expect("valid event");
        assert!(event.event_type.is_subscription());
        let subscription = event.subscription().expect("subscription projection");
        assert_eq!(subscription.id, "sub_9");
        assert_eq!(subscription.customer.as_deref(), Some("cus_9"));
        assert_eq!(subscription.status.as_deref(), Some("active"));
        assert_eq!(subscription.price_id.as_deref(), Some("price_9"));
        assert_eq!(subscription.cancel_at_period_end, Some(true));
    }

    #[test]
    fn rejects_a_body_that_is_not_an_event() {
        for bad in [
            "",
            "null",
            "[]",
            r#"{"id":"","type":"x","data":{"object":{}}}"#,
            r#"{"id":"evt","type":"","data":{"object":{}}}"#,
            r#"{"id":"evt","type":"x"}"#,
            r#"{"id":"evt","type":"x","data":{}}"#,
        ] {
            assert!(
                StripeEvent::parse(bad.as_bytes()).is_err(),
                "body {bad:?} must be rejected"
            );
        }
    }

    #[test]
    fn session_projection_requires_the_session_shape() {
        // An invoice object must not project as a checkout session.
        let body = r#"{"id":"evt_4","type":"invoice.paid",
            "data":{"object":{"id":"in_1","customer":"cus_1"}}}"#;
        let event = StripeEvent::parse(body.as_bytes()).expect("valid event");
        assert!(event.checkout_session().is_none());
        assert!(event.subscription().is_none());
    }

    #[test]
    fn classify_covers_the_billing_families() {
        assert_eq!(
            StripeEventType::classify("checkout.session.expired"),
            StripeEventType::CheckoutSessionExpired
        );
        assert_eq!(
            StripeEventType::classify("customer.subscription.deleted"),
            StripeEventType::CustomerSubscriptionDeleted
        );
        assert_eq!(
            StripeEventType::classify("invoice.payment_failed"),
            StripeEventType::InvoicePaymentFailed
        );
    }
}
