//! PURPOSE: The billing boundary aggregates -- the tenant <-> provider customer
//! mapping, the plan <-> provider price catalogue, durable checkout sessions
//! and the per-tenant application claim for provider events.
//!
//! Owning layer: `mas-domain`.
//!
//! Production responsibility: keep the two halves of the billing story
//! expressible in types the application layer can compose: what the platform
//! knows about a customer (`BillingCustomer`), what a plan costs on a provider
//! (`PlanPrice`), what checkout flow is in flight (`CheckoutSessionRecord`),
//! and which provider event has already been applied to which tenant
//! (`EventApplication`). The provider wire format lives in `mas-billing-stripe`;
//! these aggregates are provider-agnostic -- `provider` is an opaque label and
//! every provider identifier is a handle, never a credential.
//!
//! Data ownership: Rust-owned platform state; PostgreSQL is authoritative.
//!
//! It must NOT:
//!   * hold a provider secret, API key, card number or signing key -- only
//!     opaque handles (`cus_...`, `price_...`, `cs_...`, `evt_...`);
//!   * carry a money amount as a float -- `unit_amount` is integer minor units
//!     with an explicit currency;
//!   * let an `EventApplication` claim be re-used: one (tenant, provider,
//!     event) applies exactly once.

use mas_common::ids::{
    BillingCustomerId, BillingReceiptId, CheckoutSessionId, EventApplicationId, PlanId,
    PlanPriceId, TenantId,
};
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;

string_enum! {
    /// How often a price recurs.
    BillingInterval {
        Month => "month",
        Year => "year",
        /// A single one-shot charge.
        OneTime => "one_time",
    }
}

string_enum! {
    /// Lifecycle of a checkout session record.
    CheckoutState {
        /// Created and handed to the provider; not yet concluded.
        Created => "created",
        /// The provider reported the checkout finished.
        Completed => "completed",
        /// The checkout lapsed before completion.
        Expired => "expired",
        /// The provider rejected the session request.
        Failed => "failed",
    }
}

/// The tenant <-> billing-provider customer mapping.
///
/// Two uniqueness guarantees live on the storage side: one customer per
/// (tenant, provider), and one tenant per (provider, customer). The second is
/// the cross-tenant control on the webhook path -- an event naming a customer
/// resolves to exactly one tenant, from platform truth, never from the body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BillingCustomer {
    /// Row id.
    pub id: BillingCustomerId,
    /// Owning tenant.
    pub tenant_id: TenantId,
    /// Provider label (`stripe`, ...), opaque to the domain.
    pub provider: String,
    /// Provider customer handle (`cus_...`).
    pub provider_customer_id: String,
    /// Contact email the provider knows, when one exists.
    pub email: Option<String>,
    /// Creation time.
    pub created_at: Timestamp,
    /// Last update time.
    pub updated_at: Timestamp,
}

/// One plan price on one billing provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanPrice {
    /// Row id.
    pub id: PlanPriceId,
    /// Owning tenant.
    pub tenant_id: TenantId,
    /// The platform plan this price charges for.
    pub plan_id: PlanId,
    /// The plan's stable code, resolved from the plan row at read time so a
    /// webhook can name a plan without a second lookup.
    pub plan_code: String,
    /// Provider label (`stripe`, ...).
    pub provider: String,
    /// Provider price handle (`price_...`).
    pub provider_price_id: String,
    /// ISO 4217 code, lowercase.
    pub currency: String,
    /// Integer minor units (cents). `None` when the provider does not expose
    /// an amount to the platform -- never fabricated as zero.
    pub unit_amount: Option<i64>,
    /// How often the price recurs.
    pub billing_interval: BillingInterval,
    /// Whether this mapping may be used for new checkouts.
    pub active: bool,
    /// Creation time.
    pub created_at: Timestamp,
    /// Last update time.
    pub updated_at: Timestamp,
}

/// A durable checkout session record.
///
/// Created before the browser is redirected to the provider, so a retried
/// request with the same idempotency key returns this record instead of
/// opening a second provider session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckoutSessionRecord {
    /// Row id.
    pub id: CheckoutSessionId,
    /// Owning tenant.
    pub tenant_id: TenantId,
    /// Caller-supplied idempotency key, unique per tenant.
    pub idempotency_key: String,
    /// The plan being purchased.
    pub plan_id: PlanId,
    /// The plan's stable code at checkout time. Platform truth recorded when
    /// the session was opened -- the event body never supplies it.
    pub plan_code: String,
    /// The price the session was opened with, when one was resolved.
    pub plan_price_id: Option<PlanPriceId>,
    /// Provider label.
    pub provider: String,
    /// Provider session handle (`cs_...`), once the provider accepted it.
    pub provider_session_id: Option<String>,
    /// Lifecycle state.
    pub state: CheckoutState,
    /// Browser return URL on success.
    pub success_url: String,
    /// Browser return URL on cancellation.
    pub cancel_url: String,
    /// Creation time.
    pub created_at: Timestamp,
    /// Last update time.
    pub updated_at: Timestamp,
}

/// The durable per-tenant application claim for one provider billing event.
///
/// Inserted with `ON CONFLICT DO NOTHING` *before* the effect is applied: if
/// the claim inserts, the effect is applied in the same transaction; if it
/// conflicts, the event was already applied and must not be applied again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventApplication {
    /// Row id.
    pub id: EventApplicationId,
    /// Tenant the event was applied to.
    pub tenant_id: TenantId,
    /// Provider label.
    pub provider: String,
    /// The provider's event id (`evt_...`).
    pub event_id: String,
    /// The platform receipt this application derived from, when linked.
    pub receipt_id: Option<BillingReceiptId>,
    /// The provider event type, preserved verbatim.
    pub event_type: String,
    /// Short effect code the application layer wrote
    /// (`subscription_upserted`, `ignored`, ...).
    pub effect: String,
    /// When the effect was applied.
    pub applied_at: Timestamp,
}
