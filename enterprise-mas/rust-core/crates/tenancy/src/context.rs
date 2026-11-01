//! The resolved request-time tenant context: tenant + organization +
//! optional project + effective roles — the ONE shape downstream crates
//! should accept.

use mas_common::enums::TenantStatus;
use mas_common::ids::{OrganizationId, ProjectId, TenantId};
use mas_domain::{MembershipRole, Organization, Project, Tenant};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Fully-resolved tenancy context for one request.
///
/// Invariants proven by construction time:
/// * tenant and organization are linked (ownership),
/// * project (when present) belongs to both,
/// * `roles` are the caller's effective memberships roles (never widened
///   after resolution).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedTenantContext {
    pub tenant_id: TenantId,
    pub organization_id: OrganizationId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<ProjectId>,
    /// Tenant status snapshot at resolution time.
    pub tenant_status: TenantStatus,
    /// Effective roles of the resolving principal inside this context.
    pub roles: BTreeSet<MembershipRole>,
    /// Whether the resolving principal is the platform system identity
    /// (service-to-service operations, no memberships by construction).
    pub is_system: bool,
}

impl ResolvedTenantContext {
    /// System-side resolution (no memberships, no projects in scope).
    #[must_use]
    pub fn system(tenant: &Tenant, organization: &Organization) -> Self {
        debug_assert_eq!(tenant.organization_id(), organization.id);
        Self {
            tenant_id: tenant.id,
            organization_id: organization.id,
            project_id: None,
            tenant_status: tenant.status,
            roles: BTreeSet::new(),
            is_system: true,
        }
    }

    #[must_use]
    pub fn const_with_project(mut self, project: &Project) -> Self {
        debug_assert_eq!(project.tenant_id, self.tenant_id);
        debug_assert_eq!(project.organization_id, self.organization_id);
        self.project_id = Some(project.id);
        self
    }

    /// Whether the caller holds at least `role` (rank-wise).
    #[must_use]
    pub fn has_any_role_at_least(&self, floor: MembershipRole) -> bool {
        self.roles.iter().any(|role| role.rank() >= floor.rank())
    }

    #[must_use]
    pub fn can_manage_role(&self, target: MembershipRole) -> bool {
        self.roles
            .iter()
            .map(MembershipRole::rank)
            .max()
            .is_some_and(|highest| highest > target.rank())
    }
}

/// Anything carrying a resolved tenant context (view models, adapters).
pub trait TenantContextSource {
    fn tenant_context(&self) -> &ResolvedTenantContext;
}

impl TenantContextSource for ResolvedTenantContext {
    fn tenant_context(&self) -> &ResolvedTenantContext {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::enums::Environment;
    use mas_domain::{IsolationMode, Slug};

    #[test]
    fn role_lattice_queries_work() {
        let organization =
            Organization::create("a", "a", Slug::new("a").expect("slug")).expect("org");
        let tenant = Tenant::create(
            organization.id,
            "prod",
            Slug::new("prod").expect("slug"),
            Environment::Production,
            IsolationMode::SharedRls,
        )
        .expect("tenant");
        let mut context = ResolvedTenantContext::system(&tenant, &organization);
        context.roles = BTreeSet::from([MembershipRole::Developer]);
        assert!(context.has_any_role_at_least(MembershipRole::Operator));
        assert!(!context.has_any_role_at_least(MembershipRole::Admin));
        assert!(context.can_manage_role(MembershipRole::Viewer));
        assert!(!context.can_manage_role(MembershipRole::Owner));
    }
}
