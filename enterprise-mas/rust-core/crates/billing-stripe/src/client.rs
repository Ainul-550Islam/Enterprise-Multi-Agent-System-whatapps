//! The Stripe API client seam.
//!
//! The billing flow only needs one read: re-fetching an event by id (for
//! durable receipt reconciliation after a crash). Everything else arrives
//! through the webhook. Two rules:
//!
//! * **Fixed host.** Requests go to `api.stripe.com` only; there is no config
//!   key that can point the client elsewhere.
//! * **Reference secrets.** The API key is injected and redacted from
//!   `Debug`; tests drive [`HttpTransport`] mocks, production binds `reqwest`
//!   at the composition root.

use mas_common::error::AppError;
use mas_common::result::Result;

use crate::event::{StripeEvent, SubscriptionObject};

/// Fixed Stripe API host.
pub const STRIPE_API_HOST: &str = "api.stripe.com";

/// The transport seam: GET plus optional form POST.
pub trait HttpTransport: Send + Sync {
    /// GETs `url` with `authorization` (already `Bearer …`).
    fn get(&self, url: &str, authorization: &str) -> Result<TransportResponse>;

    /// POSTs `body` to `url`. `idempotency_key` is sent as `Idempotency-Key`
    /// when present. Default: not supported.
    fn post(
        &self,
        url: &str,
        authorization: &str,
        body: &str,
        idempotency_key: Option<&str>,
    ) -> Result<TransportResponse> {
        let _ = (url, authorization, body, idempotency_key);
        Err(AppError::internal("transport does not support POST"))
    }
}

/// A raw transport response.
#[derive(Debug, Clone)]
pub struct TransportResponse {
    /// HTTP status.
    pub status: u16,
    /// Response body.
    pub body: String,
}

/// The read-only Stripe boundary the billing service depends on.
pub trait StripeClient: Send + Sync {
    /// Fetches one event by id (`evt_…`).
    fn fetch_event(&self, event_id: &str) -> Result<StripeEvent>;

    /// Fetches the current subscription object (`sub_…`). Used when a webhook
    /// arrived out of order and the event payload may be stale.
    fn retrieve_subscription(&self, subscription_id: &str) -> Result<SubscriptionObject>;
}

/// Production client over the fixed Stripe host.
pub struct HttpStripeClient {
    transport: Box<dyn HttpTransport>,
    api_key: String,
}

impl std::fmt::Debug for HttpStripeClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpStripeClient")
            .field("host", &STRIPE_API_HOST)
            .field("api_key", &"[redacted]")
            .finish()
    }
}

impl HttpStripeClient {
    /// Builds a client over the given transport with the given API key.
    pub fn new(transport: Box<dyn HttpTransport>, api_key: impl Into<String>) -> Self {
        Self {
            transport,
            api_key: api_key.into(),
        }
    }

    /// The URL for an event fetch (fixed host).
    pub fn event_url(event_id: &str) -> String {
        format!("https://{STRIPE_API_HOST}/v1/events/{event_id}")
    }

    /// The URL for a subscription fetch (fixed host).
    pub fn subscription_url(subscription_id: &str) -> String {
        format!("https://{STRIPE_API_HOST}/v1/subscriptions/{subscription_id}")
    }
}

impl StripeClient for HttpStripeClient {
    fn fetch_event(&self, event_id: &str) -> Result<StripeEvent> {
        if !event_id.starts_with("evt_") {
            return Err(AppError::validation("stripe event id must start with evt_"));
        }
        let auth = format!("Bearer {}", self.api_key);
        let response = self.transport.get(&Self::event_url(event_id), &auth)?;
        if !(200..300).contains(&response.status) {
            return Err(AppError::external_service(
                "stripe",
                format!("event fetch rejected with status {}", response.status),
            ));
        }
        StripeEvent::parse(response.body.as_bytes())
    }

    fn retrieve_subscription(&self, subscription_id: &str) -> Result<SubscriptionObject> {
        if !subscription_id.starts_with("sub_") {
            return Err(AppError::validation(
                "stripe subscription id must start with sub_",
            ));
        }
        let auth = format!("Bearer {}", self.api_key);
        let response = self
            .transport
            .get(&Self::subscription_url(subscription_id), &auth)?;
        if !(200..300).contains(&response.status) {
            return Err(map_stripe_status(response.status, &response.body));
        }
        let value: serde_json::Value = serde_json::from_str(&response.body)
            .map_err(|_| AppError::external_service("stripe", "subscription body is not JSON"))?;
        SubscriptionObject::from_value(&value).ok_or_else(|| {
            AppError::external_service("stripe", "subscription body is not a subscription object")
        })
    }
}

fn map_stripe_status(status: u16, body: &str) -> AppError {
    match status {
        402 => AppError::conflict(format!("stripe payment required: {body}")),
        429 => AppError::rate_limited(format!("stripe rate limited: {body}")),
        other => AppError::external_service("stripe", format!("rejected with status {other}")),
    }
}
