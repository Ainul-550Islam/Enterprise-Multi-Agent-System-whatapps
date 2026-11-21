//! # mas-billing-stripe -- the Stripe provider boundary
//!
//! Everything that knows Stripe's wire format lives here and nowhere else:
//!
//! * [`signature`] -- `Stripe-Signature` verification over the **raw request
//!   body**: HMAC-SHA256 over `"{timestamp}.{payload}"`, a bounded timestamp
//!   tolerance, constant-time tag comparison, and signing-key rotation (any
//!   configured secret may match any `v1` tag).
//! * [`event`] -- the webhook event envelope and the small typed object
//!   projections (`checkout.session.completed`, subscription lifecycle) that
//!   `mas-application` services consume. Unknown fields are ignored; unknown
//!   event types are reported, never guessed at.
//!
//! Security boundary: this crate performs **no I/O and holds no secret**. It
//! verifies bytes a caller already has and returns typed errors. The webhook
//! receipt log and the durable claim of an event id belong to
//! `mas-persistence`; applying an event to platform state belongs to
//! `mas-application`. Signature verification always happens **before** any
//! receipt is written or any state changes.
//!
//! It must NOT:
//!   * trust a field of an event that has not been signature-verified;
//!   * compare HMAC tags with `==` (timing side channel) -- verification goes
//!     through [`hmac::Mac::verify_slice`], which is constant-time;
//!   * accept a timestamp unbounded by tolerance, or a future timestamp;
//!   * read the environment, configuration, or any secret store.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod client;
pub mod event;
pub mod mapping;
pub mod signature;

pub use client::{HttpStripeClient, StripeClient};
pub use event::{CheckoutSession, StripeEvent, StripeEventType, SubscriptionObject};
pub use mapping::resolve_subscription;
pub use signature::{
    compute_v1_signature, parse_header, verify_signature, ParsedHeader, DEFAULT_TOLERANCE_SECS,
    MAX_SIGNATURES_PER_HEADER, STRIPE_SIGNATURE_HEADER,
};
