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
pub mod executions;
pub mod outbox;
pub mod schedules;
mod scoped;
pub mod services;
pub mod tasks;
pub mod tenancy;
pub mod workflows;

pub use audit::PostgresAuditLog;
pub use outbox::PostgresOutboxStore;
pub use scoped::{DocumentTable, ScopedStore};
pub use tenancy::{MembershipStore, OrganizationStore, ProjectStore, TenantStore, UserStore};
