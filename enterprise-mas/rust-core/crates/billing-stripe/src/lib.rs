/// mas-billing-stripe — protocol-only crate
///
/// No database dependencies. All types, models, and pure functions live here.
/// Integration into the control plane happens via:
///   • `crates/persistence` — repository implementations (billing customers, plan prices)
///   • `crates/application` — service layer that owns receipt lifecycle + audit
///   • `crates/api` — HTTP handlers + webhook ingress
///
/// Stripe protocol contract:
///   - POST /v1/billing/checkout-session   (protected, billing-admin role)
///   - POST /v1/billing/portal-session     (protected, billing-admin role)
///   - PUBLIC POST /v1/public/billing/stripe/webhook  (raw body + Stripe signature verify)
///   - GET  /v1/billing/subscription/{id}  (protected)
///
/// All secret values (api_key, webhook secrets) are referenced via `secret_ref`,
/// never inline.  Outbound egress is guarded by `crates/integrations::http_guard`
/// (pinned hosts, SSRF protection, DNS pinning).
///
/// Crate policy: zero DB, zero runtime deps beyond workspace deps + standard libs.
/// Any DB/state interaction lives in the application crate; this crate purely
/// defines the protocol shapes, verifiers, and client adapters.
mod config;
mod errors;
mod mapping;
mod models;
mod signature;
mod client;
mod events;

pub use config::StripeConfig;
pub use errors::StripeError;
pub use models::{StripeCustomer, StripeCheckoutSession, StripePortalSession, StripeSubscription, StripeSubscriptionItem, StripeEvent, StripeEventData, StripeTrialSettings};
pub use mapping::{stripe_status_to_subscription_state, price_id_to_plan, map_stripe_event, LocalSubscriptionState};
pub use signature::{
    verify_stripe_signature, parse_stripe_signature, StripeSignature, StripeSignatureVersion,
};
pub use events::{StripeEvent, stripe_event_from_raw, EventType};
pub use client::StripeClient;