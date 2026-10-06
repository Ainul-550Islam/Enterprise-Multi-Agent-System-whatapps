//! PURPOSE: The control-plane repositories — plan, subscription, entitlement,
//! override, policy, quota counter, reservation, usage and billing-webhook
//! persistence, all in one place.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: give the enterprise control plane a durable home
//! so that every number deciding whether a tenant may consume a resource lives
//! in a row the database can enforce, not in a `Mutex<BTreeMap<..>>` inside one
//! process. Two replicas reserving against the same ceiling must be arbitrated
//! by Postgres, because they cannot see each other's memory.
//!
//! Security boundary: every table here except `plans` (platform reference
//! data) and `billing_webhook_receipts` (written before a tenant is known) is
//! tenant-scoped and sits under FORCE RLS. Repositories bind `tenant_id` from
//! server-side scope; none of them accept a tenant id to filter on as a
//! convenience parameter.
//!
//! Data ownership: all of it is Rust-owned platform state. The Python
//! orchestrator reports model and tool usage through the bridge; it never
//! writes these tables, and no handler accepts a client-supplied entitlement,
//! plan or quota claim.
//!
//! It must NOT:
//!   • be bypassed "just for a read" — the in-memory implementations that
//!     ship alongside these ports are test fixtures, and wiring one into a
//!     production binary is the exact defect this module exists to prevent;
//!   • expose a method that writes without a tenant, an actor and (for
//!     overrides) a reason;
//!   • let a duplicate — of a reservation, a usage record, a webhook event, or
//!     a subscription transition — produce a second effect.

pub mod adapters;
pub mod billing;
pub mod counters;
pub mod entitlements;
pub(crate) mod executor;
pub mod plans;
pub mod policies;
pub mod reservations;
pub mod subscriptions;
pub mod usage;

pub use adapters::build_postgres_control_plane;
pub use billing::{BillingReceiptRow, PostgresBillingWebhookStore, ReceiptState};
pub use counters::{PostgresCounterStore, PostgresQuotaRegistry, QuotaRow};
pub use entitlements::{EntitlementRow, PostgresEntitlementStore, PostgresOverrideStore};
pub use mas_domain::reservation::Settlement;
pub use plans::{
    PlanEntitlementRow, PlanQuotaRow, PlanRow, PostgresPlanResolver, PostgresPlanStore,
};
pub use policies::{PolicyRow, PostgresPolicyStore};
pub use reservations::{PostgresReservationStore, ReservationRow};
pub use subscriptions::{PostgresSubscriptionStore, SubscriptionEventRow, SubscriptionRow};
pub use usage::{PostgresUsageLedger, UsageRow, UsageTotal, UsageWrite};

use sqlx::PgPool;

/// Every control-plane store, built from one pool.
///
/// Composition roots take this rather than eight separate handles, so a binary
/// cannot end up with a Postgres policy store and an in-memory quota registry
/// by accident.
#[derive(Debug, Clone)]
pub struct ControlPlaneStores {
    /// Distributed quota counters.
    pub counters: PostgresCounterStore,
    /// Quota definitions.
    pub quotas: PostgresQuotaRegistry,
    /// Reservations.
    pub reservations: PostgresReservationStore,
    /// Policies.
    pub policies: PostgresPolicyStore,
    /// Plan catalogue.
    pub plans: PostgresPlanStore,
    /// Plan resolution including quotas.
    pub plan_resolver: PostgresPlanResolver,
    /// Entitlements and the subscription binding.
    pub entitlements: PostgresEntitlementStore,
    /// Overrides.
    pub overrides: PostgresOverrideStore,
    /// Subscriptions and their history.
    pub subscriptions: PostgresSubscriptionStore,
    /// The one usage write path.
    pub usage: PostgresUsageLedger,
    /// Webhook receipts.
    pub billing: PostgresBillingWebhookStore,
}

impl ControlPlaneStores {
    /// Builds every store over one pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            counters: PostgresCounterStore::new(pool.clone()),
            quotas: PostgresQuotaRegistry::new(pool.clone()),
            reservations: PostgresReservationStore::new(pool.clone()),
            policies: PostgresPolicyStore::new(pool.clone()),
            plans: PostgresPlanStore::new(pool.clone()),
            plan_resolver: PostgresPlanResolver::new(pool.clone()),
            entitlements: PostgresEntitlementStore::new(pool.clone()),
            overrides: PostgresOverrideStore::new(pool.clone()),
            subscriptions: PostgresSubscriptionStore::new(pool.clone()),
            usage: PostgresUsageLedger::new(pool.clone()),
            billing: PostgresBillingWebhookStore::new(pool),
        }
    }
}
