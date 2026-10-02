//! Tenancy crate: the multi-tenant isolation spine.
//!
//! # Invariants (normative)
//!
//! 1. **Hierarchy ownership is sacred**: an `Organization` belongs to exactly
//!    one `Tenant`, a `Project` belongs to exactly one (tenant, organization)
//!    pair. Cross-links are impossible at resolution time because every
//!    pairing is re-validated ([`HierarchyService`]).
//! 2. **Cross-tenant existence is invisible**: looking up another tenant's
//!    resource yields the same "not found" as a truly absent one
//!    ([`IsolationService`]) — never "forbidden" (which leaks presence).
//! 3. **Every request runs *resolved***: [`TenantResolver`] turns
//!    (principal, requested ids) into a [`ResolvedTenantContext`] with the
//!    caller's *effective* roles (org and tenant scopes intersected); a
//!    suspended tenant stops resolution, a suspended membership too.
//! 4. **Roles are capabilities, not labels**: [`MembershipRole`][mas_domain::MembershipRole] ranks
//!    encode the management lattice (`can_manage`), and privileged
//!    impersonation is representable but audited.
//! 5. **Entitlements gate features, never guard data**: plan/subscription
//!    failures return "upgrade required" errors; but suspended tenants fail
//!    isolation before entitlement checks run.

pub mod context;
pub mod entitlements;
pub mod hierarchy;
pub mod isolation;
pub mod resolution;

pub use context::{ResolvedTenantContext, TenantContextSource};
pub use entitlements::{EntitlementProfile, EntitlementService, EntitlementStorePort};
pub use hierarchy::{
    HierarchyService, InMemoryDirectory, InMemoryMemberships, MembershipDirectoryPort,
    TenantDirectoryPort,
};
pub use isolation::{IsolationReport, IsolationService};
pub use resolution::{MembershipView, TenantResolver};
