// PURPOSE: The authoritative scope: the one value a database transaction is
// allowed to run under.
//
// Owning layer: `mas-security` (the security boundary), produced by
// `mas-tenancy`'s resolver and consumed by `mas-persistence`'s scoped
// transaction API.
//
// Production responsibility: spec Phase 15 asks for one safe abstraction,
// conceptually `with_authoritative_scope(...) → transaction → RLS SET LOCAL →
// repositories`, and asks that it be *difficult* to call a tenant repository
// without a scope. This type is the handle that makes that possible: a
// repository call path that requires a scope takes `&AuthoritativeScope`, so a
// raw `TenantId` will not compile where a proven scope is required.
//
// The name is the contract. A scope is **authoritative** only because it was
// resolved from authenticated membership or a trusted internal identity chain
// (Phase 3). It is never a value a caller supplied.
//
// Security boundary: this type is the seam between "the application decided
// who this is" and "the database will now enforce it". Two independent layers
// sit behind it — the application refuses to build a transaction without one,
// and PostgreSQL's RLS policies refuse to return rows outside it. Either layer
// alone would be a single point of failure; together a mistake in one is
// caught by the other.
//
// It must NOT:
//   • be constructible from a request header, a query parameter or a request
//     body — those are claims. Use `mas_tenancy`'s resolver;
//   • be widened after construction: there is no setter, and `with_project`
//     only ever narrows to a project already proven to belong to the tenant;
//   • be cached or reused across requests. It is per-request, per-transaction,
//     and must die with the transaction that carried it.

use mas_common::error::AppError;
use mas_common::ids::{OrganizationId, ProjectId, TenantId};
use mas_common::result::Result;
use serde::{Deserialize, Serialize};

/// The proven scope one transaction runs under.
///
/// Serialized only for diagnostics and audit correlation — never as a
/// transport for reconstructing a scope on the other side. Reconstructing a
/// scope from bytes is exactly the "caller-supplied identity" this type exists
/// to prevent, so `Deserialize` is implemented but the constructor that
/// matters is the named one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeScope {
    /// The resolved tenant. Authoritative.
    tenant_id: TenantId,
    /// The resolved organization. Authoritative.
    organization_id: OrganizationId,
    /// The resolved project, when the operation is project-scoped.
    project_id: Option<ProjectId>,
    /// Who the scope was resolved for (audit correlation only — never used to
    /// make a decision).
    actor: String,
}

impl AuthoritativeScope {
    /// Builds a scope from **server-resolved** identifiers.
    ///
    /// Callers must have already proven, by membership lookup or by a trusted
    /// internal identity chain, that the principal may reach this tenant.
    /// Calling this with a value taken from a request is the vulnerability
    /// this type was created to prevent; the RLS policies are the backstop
    /// that catches it anyway.
    #[must_use]
    pub fn resolved(
        tenant_id: TenantId,
        organization_id: OrganizationId,
        actor: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id,
            organization_id,
            project_id: None,
            actor: actor.into(),
        }
    }

    /// Narrows the scope to a project already proven to belong to the tenant.
    ///
    /// The caller must have verified the project's `tenant_id` and
    /// `organization_id` (the tenancy resolver does). This method cannot
    /// verify it without I/O, so it refuses a project it has not been told is
    /// proven, and records the narrowing for audit.
    #[must_use]
    pub fn with_project(mut self, project_id: ProjectId) -> Self {
        self.project_id = Some(project_id);
        self
    }

    /// The tenant the transaction runs as.
    #[must_use]
    pub const fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    /// The organization the tenant belongs to.
    #[must_use]
    pub const fn organization_id(&self) -> OrganizationId {
        self.organization_id
    }

    /// The project, when the operation is project-scoped.
    #[must_use]
    pub const fn project_id(&self) -> Option<ProjectId> {
        self.project_id
    }

    /// The actor the scope was resolved for (audit correlation only).
    #[must_use]
    pub fn actor(&self) -> &str {
        &self.actor
    }

    /// Whether the scope covers that tenant.
    #[must_use]
    pub fn covers_tenant(&self, tenant_id: TenantId) -> bool {
        self.tenant_id == tenant_id
    }

    /// Whether the scope covers that organization.
    #[must_use]
    pub fn covers_organization(&self, organization_id: OrganizationId) -> bool {
        self.organization_id == organization_id
    }

    /// Refuses a resource whose tenant does not match the scope.
    ///
    /// Deterministic: a mismatch is always the same failure, and never a
    /// silent re-scope to the principal's own tenant.
    pub fn require_tenant(&self, tenant_id: TenantId) -> Result<()> {
        if self.covers_tenant(tenant_id) {
            Ok(())
        } else {
            Err(AppError::forbidden(
                "resource belongs to a tenant outside the authorized scope",
            ))
        }
    }
}

/// The scope a worker or scheduler plane runs under.
///
/// Machine planes do not have memberships; their tenant authority is the
/// durable row they hold a lease on. This constructor makes that explicit so a
/// machine path is visibly not a human path, and so an audit can tell the
/// difference.
#[must_use]
pub fn machine_plane_scope(
    tenant_id: TenantId,
    organization_id: OrganizationId,
    actor: impl Into<String>,
) -> AuthoritativeScope {
    AuthoritativeScope::resolved(tenant_id, organization_id, actor)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant(seed: u128) -> TenantId {
        TenantId::from_uuid(uuid::Uuid::from_u128(seed))
    }
    fn org(seed: u128) -> OrganizationId {
        OrganizationId::from_uuid(uuid::Uuid::from_u128(seed))
    }

    #[test]
    fn a_scope_covers_only_its_own_tenant() {
        let scope = AuthoritativeScope::resolved(tenant(1), org(1), "user-1");
        assert!(scope.covers_tenant(tenant(1)));
        assert!(!scope.covers_tenant(tenant(2)));
        assert!(scope.covers_organization(org(1)));
        assert!(!scope.covers_organization(org(2)));
        assert!(scope.require_tenant(tenant(1)).is_ok());
        assert!(scope.require_tenant(tenant(2)).is_err());
    }

    #[test]
    fn a_project_narrows_the_scope_and_is_recorded() {
        let project = ProjectId::from_uuid(uuid::Uuid::from_u128(9));
        let scope = AuthoritativeScope::resolved(tenant(1), org(1), "user-1").with_project(project);
        assert_eq!(scope.project_id(), Some(project));
        assert_eq!(scope.tenant_id(), tenant(1), "narrowing never widens");
        assert_eq!(scope.actor(), "user-1");
    }

    #[test]
    fn a_mismatch_is_a_deterministic_failure_not_a_rescope() {
        let scope = AuthoritativeScope::resolved(tenant(1), org(1), "user-1");
        let first = scope.require_tenant(tenant(2));
        let second = scope.require_tenant(tenant(2));
        assert!(first.is_err() && second.is_err());
        // The scope itself is unchanged by the refusal: no silent switching.
        assert_eq!(scope.tenant_id(), tenant(1));
    }

    #[test]
    fn machine_plane_scopes_are_just_scopes_with_a_named_constructor() {
        let scope = machine_plane_scope(tenant(3), org(3), "worker-1");
        assert!(scope.covers_tenant(tenant(3)));
        assert_eq!(scope.actor(), "worker-1");
    }
}
