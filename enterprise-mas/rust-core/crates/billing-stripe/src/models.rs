/// Stripe API response models (read-only, protocol-only).
///
/// These models mirror the minimal fields needed for the billing layer's
/// subscription state machine and receipt mapping. They do not include
/// every Stripe field — only what the application layer uses.

use serde::Deserialize;

/// Stripe Customer response (minimal)
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct StripeCustomer {
    /// Stripe-assigned customer ID (e.g. "cus_123456789")
    pub id: String,
    /// Customer email address
    #[serde(rename = "email")]
    pub email_address: Option<String>,
    /// Stripe object type, always "customer"
    #[serde(rename = "object")]
    pub object: String,
}

/// Stripe Checkout Session response (minimal)
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct StripeCheckoutSession {
    /// Stripe-assigned session ID (e.g. "cs_123456789")
    pub id: String,
    /// URL to redirect the customer to after successful payment
    #[serde(rename = "url")]
    pub success_url: Option<String>,
    /// URL to redirect the customer to for cancellation
    #[serde(rename = "cancel_url")]
    pub cancel_url: Option<String>,
    /// Customer ID (e.g. "cus_123456789")
    pub customer: String,
    /// Session mode, always "subscription" for our use case
    pub mode: String,
    /// Session status: "open", "complete", "expired"
    pub status: String,
    /// Epoch seconds when the session expires
    #[serde(rename = "expires_at")]
    pub expires_at: Option<i64>,
    /// The price ID that this session was created for
    pub price_id: String,
}

/// Stripe Billing Portal Session response (minimal)
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct StripePortalSession {
    /// Stripe-assigned session ID
    pub id: String,
    /// URL to redirect the customer to the billing portal
    pub url: String,
}

/// Stripe Subscription response (minimal)
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct StripeSubscription {
    /// Stripe-assigned subscription ID (e.g. "sub_123456789")
    pub id: String,
    /// Customer ID (e.g. "cus_123456789")
    pub customer: String,
    /// Subscription status: "active", "paused", "canceled", "incomplete", etc.
    pub status: String,
    /// Epoch seconds when the current billing period starts
    #[serde(rename = "current_period_start")]
    pub current_period_start: Option<i64>,
    /// Epoch seconds when the current billing period ends
    #[serde(rename = "current_period_end")]
    pub current_period_end: Option<i64>,
    /// Epoch seconds when the subscription is scheduled to be canceled
    #[serde(rename = "cancel_at")]
    pub cancel_at: Option<i64>,
    /// True if the subscription will be canceled at the end of the current period
    #[serde(rename = "cancel_at_period_end")]
    pub cancel_at_period_end: bool,
    /// Subscription items (plans/quantities)
    #[serde(rename = "items")]
    pub items: Vec<StripeSubscriptionItem>,
}

/// Stripe Subscription Item (one line item within a subscription)
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct StripeSubscriptionItem {
    /// The ID of the plan/price associated with this item
    pub price: String,
    /// Quantity of the subscription item
    #[serde(rename = "quantity")]
    pub quantity: i64,
    /// Current status of this item: "active", "trialing", "past_due", etc.
    pub status: String,
}

/// Stripe Event response (minimal, for webhook processing)
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct StripeEvent {
    /// Always "event"
    #[serde(rename = "object")]
    pub object: String,
    /// Unique identifier for the event
    pub id: String,
    /// One of: checkout.session.completed, customer.subscription.created,
    /// customer.subscription.updated, customer.subscription.deleted,
    /// invoice.paid, invoice.payment_failed, customer.subscription.trial_will_end
    pub r#type: String,
    /// The Stripe object associated with the event, if applicable
    pub data: StripeEventData,
}

/// The data payload inside a Stripe Event
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct StripeEventData {
    /// The actual object (checkout.session, customer.subscription, invoice, etc.)
    pub object: String,
    /// The ID of the associated object
    pub id: String,
    /// Differs based on event type
    pub previous_attributes: Option<serde_json::Value>,
}

/// Stripe Invoice response (minimal)
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct StripeInvoice {
    /// Stripe-assigned invoice ID
    pub id: String,
    /// Customer ID
    pub customer: String,
    /// Invoice status: "paid", "open", "void", "paid_canceled"
    pub status: String,
    /// True if the invoice was charged successfully
    #[serde(rename = "amount_paid")]
    pub amount_paid: Option<i64>,
    /// Epoch seconds when the invoice was finalized
    #[serde(rename = "date")]
    pub date: Option<i64>,
    /// True if charged automatically
    #[serde(rename = "auto_draft")]
    pub auto_draft: Option<bool>,
}

/// Stripe Subscription Trial Settings (minimal)
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct StripeTrialSettings {
    /// True if trials are enabled
    pub enabled: bool,
    /// Duration in days
    pub days: Option<i64>,
    /// Duration in months
    #[serde(rename = "duration_months")]
    pub duration_months: Option<i64>,
}