//! PURPOSE: The concrete PostgreSQL repositories, and the split between them.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: keep two flavours distinct and explicit —
//! scoped repositories that take a tenant on every call, and unit-of-work
//! repositories that only ever run inside a caller's transaction. The split is
//! what stops a repository from quietly borrowing a second connection.
//!
//! It must NOT:
//!   • re-export a store without its tenant-scoped variant;
//!   • let a `*_in` method open its own transaction;
//!   • hide a connection checkout behind a convenience wrapper.

//! Concrete PostgreSQL repositories.
//!
//! Two flavors:
//!
//! * **Scoped repositories** (`scoped`) accept an explicit
//!   [`crate::rls::RlsContext`] per call — app traffic, fail-closed by RLS.
//! * **Service repositories** ([`outbox`], [`audit`]) are written for the
//!   dedicated BYPASSRLS dispatcher/audit roles documented in the migration
//!   set; their APIs make the cross-tenant intent explicit instead of hiding
//!   it behind convenience.

pub mod agents;
pub mod api_keys;
pub mod audit;
pub mod broker;
pub mod control_plane;
pub mod executions;
pub mod outbox;
pub mod schedules;
mod scoped;
pub mod services;
pub mod tasks;
pub mod tenancy;
pub mod workflows;

pub use crate::durable::PgDurableExecution;
pub use audit::PostgresAuditLog;
pub use control_plane::{
    build_postgres_control_plane, ControlPlaneStores, PostgresBillingWebhookStore,
    PostgresCounterStore, PostgresEntitlementStore, PostgresOverrideStore, PostgresPlanResolver,
    PostgresPlanStore, PostgresPolicyStore, PostgresQuotaRegistry, PostgresReservationStore,
    PostgresSubscriptionStore, PostgresUsageLedger,
};
pub use outbox::PostgresOutboxStore;
pub use scoped::{DocumentTable, ScopedStore};
pub use tenancy::{MembershipStore, OrganizationStore, ProjectStore, TenantStore, UserStore};
