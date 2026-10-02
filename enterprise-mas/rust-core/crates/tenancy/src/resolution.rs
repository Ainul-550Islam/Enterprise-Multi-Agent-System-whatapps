//! Resolution: (principal, requested tenant + project) → a proven context.

use crate::context::ResolvedTenantContext;
use crate::hierarchy::{MembershipDirectoryPort, TenantDirectoryPort};
use mas_common::enums::TenantStatus;
use mas_common::error::AppError;
use mas_common::ids::{ProjectId, TenantId, UserId};
use mas_common::result::Result;
use mas_domain::{
    Membership, MembershipRole, MembershipStatus, Organization, OrganizationStatus, ProjectStatus,
    Tenant,
};
use mas_security::SecurityPrincipal;
use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

/// A membership's contribution to one resolution (audit view).
#[derive(Debug, Clone)]
pub struct MembershipView {
    pub organization_memberships: Vec<Membership>,
    pub tenant_memberships: Vec<Membership>,
    /// Union of effective roles.
    pub effective_roles: BTreeSet<MembershipRole>,
    /// Highest rank held (for convenience metrics/audits).
    pub highest_role: Option<MembershipRole>,
}

/// The resolution outcome payload returned to callers along with the context.
#[derive(Debug, Clone)]
pub struct ResolutionOutcome {
    pub context: ResolvedTenantContext,
    pub view: MembershipView,
}

/// Turns request-time identity + ids into a verified context.
pub struct TenantResolver {
    directory: Arc<dyn TenantDirectoryPort>,
    memberships: Arc<dyn MembershipDirectoryPort>,
}

impl fmt::Debug for TenantResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TenantResolver")
            .field("directory", &self.directory)
            .field("memberships", &self.memberships)
            .finish()
    }
}

impl TenantResolver {
    pub fn new(
        directory: Arc<dyn TenantDirectoryPort>,
        memberships: Arc<dyn MembershipDirectoryPort>,
    ) -> Self {
        Self {
            directory,
            memberships,
        }
    }

    fn tenant_missing() -> AppError {
        AppError::not_found("tenant", "requested tenant does not exist")
    }

    fn project_missing() -> AppError {
        AppError::not_found("project", "requested project does not exist")
    }

    /// Resolves a principal against `(tenant_id, project?)`.
    ///
    /// Fail-closed rules:
    /// * unknown tenant / organization / project → NotFound,
    /// * a principal bound to another tenant → NotFound (existence hidden),
    /// * suspended/archived tenant → Forbidden (`tenant not operational`),
    /// * suspended organization → Forbidden,
    /// * cross-linked project → NotFound,
    /// * archived project → Forbidden.
    pub async fn resolve(
        &self,
        principal: Option<&SecurityPrincipal>,
        tenant_id: TenantId,
        project_id: Option<ProjectId>,
    ) -> Result<ResolutionOutcome> {
        // 1. Tenant existence + principal binding (cross-tenant → hidden).
        let tenant = self
            .directory
            .get_tenant(tenant_id)
            .await?
            .ok_or_else(Self::tenant_missing)?;
        if let Some(p) = principal {
            if !p.is_usable() {
                return Err(AppError::unauthorized("credential is expired"));
            }
            if p.tenant_id.is_some() && !p.tenant_matches(tenant_id) {
                return Err(Self::tenant_missing()); // hide cross-tenant existence
            }
        }
        // 2. Tenant lifecycle gate — after binding so outsiders stay blind.
        if tenant.status != TenantStatus::Active {
            return Err(AppError::forbidden(format!(
                "tenant is {} and not operational",
                tenant.status
            )));
        }
        // 3. Organization (must exist by ownership invariant).
        let organization = self
            .directory
            .get_organization(tenant.organization_id())
            .await?
            .ok_or_else(|| {
                AppError::internal("tenant resolved with a missing owning organization")
            })?;
        if organization.status != OrganizationStatus::Active {
            return Err(AppError::forbidden(format!(
                "organization is {} and not operational",
                organization.status
            )));
        }
        // 4. Project coherence, when requested.
        let mut context: ResolvedTenantContext;
        if let Some(pid) = project_id {
            let project = self
                .directory
                .get_project(pid)
                .await?
                .ok_or_else(Self::project_missing)?;
            if project.tenant_id != tenant_id || project.organization_id != organization.id {
                // Cross-linked projects are invisible outside their tenant.
                return Err(Self::project_missing());
            }
            if project.status != ProjectStatus::Active {
                return Err(AppError::forbidden(format!(
                    "project is {} and not operational",
                    project.status
                )));
            }
            context =
                ResolvedTenantContext::system(&tenant, &organization).const_with_project(&project);
        } else {
            context = ResolvedTenantContext::system(&tenant, &organization);
        }

        // 5. Effective roles: the union of active memberships' roles
        //    (org-scoped + tenant-scoped), for user principals with a
        //    parseable user id; system principals stay role-less.
        let view = self
            .effective_membership(principal, &tenant, &organization)
            .await?;
        context.roles = view.effective_roles.clone();
        context.is_system = principal.is_none()
            || principal.is_some_and(|p| p.kind == mas_security::PrincipalKind::System);
        Ok(ResolutionOutcome { context, view })
    }

    async fn effective_membership(
        &self,
        principal: Option<&SecurityPrincipal>,
        tenant: &Tenant,
        organization: &Organization,
    ) -> Result<MembershipView> {
        let Some(principal) = principal else {
            return Ok(MembershipView::empty());
        };
        if matches!(
            principal.kind,
            mas_security::PrincipalKind::System | mas_security::PrincipalKind::Token
        ) {
            // System and token principals do not carry memberships here; the
            // token's own claims scope the action, the system identity is
            // break-glass by design.
            return Ok(MembershipView::empty());
        }
        let Ok(user_id) = UserId::from_str(&principal.actor_id) else {
            return Ok(MembershipView::empty()); // non-user key without user binding
        };
        let rows = self.memberships.memberships_for_user(user_id).await?;
        let mut org_rows = Vec::new();
        let mut tenant_rows = Vec::new();
        for row in rows {
            if row.organization_id != organization.id {
                continue;
            }
            if row.status != MembershipStatus::Active {
                continue; // invited/suspended contribute nothing
            }
            match row.tenant_id {
                None => org_rows.push(row),
                Some(matching) if matching == tenant.id => tenant_rows.push(row),
                Some(_) => {}, // another tenant: not in this context
            }
        }
        let mut effective_roles = BTreeSet::new();
        for row in org_rows.iter().chain(tenant_rows.iter()) {
            effective_roles.extend(row.roles.iter().copied());
        }
        let highest_role = effective_roles
            .iter()
            .copied()
            .max_by_key(MembershipRole::rank);
        Ok(MembershipView {
            organization_memberships: org_rows,
            tenant_memberships: tenant_rows,
            effective_roles,
            highest_role,
        })
    }
}

impl MembershipView {
    fn empty() -> Self {
        Self {
            organization_memberships: Vec::new(),
            tenant_memberships: Vec::new(),
            effective_roles: BTreeSet::new(),
            highest_role: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hierarchy::{InMemoryDirectory, InMemoryMemberships};
    use mas_common::enums::Environment;
    use mas_domain::Project;
    use mas_domain::{IsolationMode, Membership, MembershipStatus, Slug};
    use mas_security::ScopeSet;

    struct World {
        resolver: TenantResolver,
        tenant: Tenant,
        organization: Organization,
        project: Project,
        memberships: Arc<InMemoryMemberships>,
        users: (UserId, UserId),
    }

    async fn world() -> World {
        let directory = Arc::new(InMemoryDirectory::new());
        let memberships = Arc::new(InMemoryMemberships::new());
        let organization =
            Organization::create("acme", "acme", Slug::new("acme").expect("s")).expect("org");
        let tenant = Tenant::create(
            organization.id,
            "acme-prod",
            Slug::new("acme-prod").expect("s"),
            Environment::Production,
            IsolationMode::SharedRls,
        )
        .expect("tenant");
        // Tenant starts in Provisioning; activate via status mutation in domain
        let mut tenant = tenant;
        tenant.status = TenantStatus::Active;
        let project = Project::create(
            tenant.id,
            organization.id,
            "default",
            Slug::new("default").expect("s"),
        )
        .expect("project");
        directory.upsert_organization(&organization).await.unwrap();
        directory.upsert_tenant(&tenant).await.unwrap();
        directory.upsert_project(&project).await.unwrap();

        let dev = UserId::new();
        // org-wide admin/dev
        let mut m =
            Membership::invite(dev, organization.id, None, MembershipRole::Admin).expect("invite");
        m.status = MembershipStatus::Active;
        memberships.upsert(&m).await.unwrap();
        let viewer = UserId::new();

        let resolver = TenantResolver::new(directory, memberships.clone());
        World {
            resolver,
            tenant,
            organization,
            project,
            memberships,
            users: (dev, viewer),
        }
    }

    fn user_principal(user: UserId, tenant: TenantId) -> SecurityPrincipal {
        SecurityPrincipal::new(
            user.to_string(),
            mas_security::PrincipalKind::User,
            Some(tenant),
            ScopeSet::denied(),
        )
    }

    #[tokio::test]
    async fn resolves_roles_and_rejects_wrong_binding() {
        let world = world().await;
        let (dev, _viewer) = world.users;
        // happy path
        let outcome = world
            .resolver
            .resolve(
                Some(&user_principal(dev, world.tenant.id)),
                world.tenant.id,
                Some(world.project.id),
            )
            .await
            .expect("resolve");
        assert!(outcome.context.roles.contains(&MembershipRole::Admin));
        assert_eq!(outcome.context.project_id, Some(world.project.id));
        assert_eq!(outcome.context.organization_id, world.organization.id);
        // other-tenant binding hides existence
        let err = world
            .resolver
            .resolve(
                Some(&user_principal(dev, TenantId::new())),
                world.tenant.id,
                None,
            )
            .await
            .expect_err("hidden");
        assert_eq!(err.error_code(), "RESOURCE_NOT_FOUND");
    }

    #[tokio::test]
    async fn suspended_memberships_and_tenants_have_no_effect() {
        let world = world().await;
        let (_dev, viewer) = world.users;
        let mut m = Membership::invite(viewer, world.organization.id, None, MembershipRole::Viewer)
            .expect("invite");
        m.status = MembershipStatus::Suspended;
        world.memberships.upsert(&m).await.unwrap();
        let outcome = world
            .resolver
            .resolve(
                Some(&user_principal(viewer, world.tenant.id)),
                world.tenant.id,
                None,
            )
            .await
            .expect("resolve");
        assert!(outcome.view.effective_roles.is_empty());

        // Tenant suspension blocks resolution for everyone.
        let directory = Arc::new(InMemoryDirectory::new());
        let t2_org = organization_of("inch");
        let mut t2 = tenant_of(t2_org.id, "sp");
        t2.status = TenantStatus::Suspended;
        directory.upsert_organization(&t2_org).await.unwrap();
        directory.upsert_tenant(&t2).await.unwrap();
        let resolver = TenantResolver::new(directory, world.memberships);
        let err = resolver
            .resolve(None, t2.id, None)
            .await
            .expect_err("suspended");
        assert_eq!(err.error_code(), "FORBIDDEN");
    }

    fn organization_of(name: &str) -> Organization {
        Organization::create(name, name, Slug::new(name).expect("s")).expect("org")
    }
    fn tenant_of(org: mas_common::ids::OrganizationId, name: &str) -> Tenant {
        Tenant::create(
            org,
            name,
            Slug::new(name).expect("s"),
            Environment::Production,
            IsolationMode::SharedRls,
        )
        .expect("tenant")
    }
}
