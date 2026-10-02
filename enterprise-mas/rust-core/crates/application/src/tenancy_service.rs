//! Tenancy-management use-cases: organizations → tenants → projects,
//! plus user registration and membership invitations.
//!
//! Hierarchy invariants (from the domain: organizations own tenants,
//! tenants own projects, projects can never be cross-linked) are
//! *re-verified here at the use-case boundary*: the context's org/tenant
//! scope must match the parent row actually stored, otherwise the parent
//! "does not exist" (existence hiding, matching the tenancy plane).

use mas_common::enums::Environment;
use mas_common::error::AppError;
use mas_common::ids::UserId;
use mas_common::result::Result;
use mas_domain::value_objects::{Email, Slug};
use mas_domain::{IsolationMode, Membership, MembershipRole, Organization, Project, Tenant, User};

use crate::audit::{self, AuditSinkPort};
use crate::context::ServiceContext;
use crate::stores::{
    MembershipStorePort, OrganizationStorePort, ProjectStorePort, TenantStorePort, UserStorePort,
};

/// Use-cases over the tenancy hierarchy and identity aggregates.
#[derive(Debug)]
pub struct TenancyService {
    organizations: Box<dyn OrganizationStorePort>,
    tenants: Box<dyn TenantStorePort>,
    projects: Box<dyn ProjectStorePort>,
    users: Box<dyn UserStorePort>,
    memberships: Box<dyn MembershipStorePort>,
    audit: Box<dyn AuditSinkPort>,
}

impl TenancyService {
    /// Composes the service over store ports + the audit sink.
    pub fn new(
        organizations: Box<dyn OrganizationStorePort>,
        tenants: Box<dyn TenantStorePort>,
        projects: Box<dyn ProjectStorePort>,
        users: Box<dyn UserStorePort>,
        memberships: Box<dyn MembershipStorePort>,
        audit: Box<dyn AuditSinkPort>,
    ) -> Self {
        Self {
            organizations,
            tenants,
            projects,
            users,
            memberships,
            audit,
        }
    }

    /// Creates an organization (system-level use-case; the slug must be
    /// globally free).
    pub async fn register_organization(
        &self,
        ctx: &ServiceContext,
        legal_name: &str,
        display_name: &str,
        slug: &str,
    ) -> Result<Organization> {
        let slug = Slug::new(slug)?;
        if self
            .organizations
            .get_by_slug(slug.as_str())
            .await?
            .is_some()
        {
            return Err(AppError::conflict(format!(
                "organization slug '{slug}' is already taken",
                slug = slug.as_str()
            )));
        }
        let organization = Organization::create(legal_name, display_name, slug)?;
        self.organizations.save(&organization).await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "organization.register",
            "organization",
            Some(organization.id.to_string()),
            mas_domain::AuditOutcome::Success,
        )
        .await?;
        tracing::info!(organization = %organization.id, "organization registered");
        Ok(organization)
    }

    /// Creates a tenant inside the context's organization.
    pub async fn register_tenant(
        &self,
        ctx: &ServiceContext,
        name: &str,
        slug: &str,
        environment: Environment,
        isolation: IsolationMode,
    ) -> Result<Tenant> {
        let organization_id = ctx.require_organization()?;
        let organization = self.organizations.get(organization_id).await?;
        if organization.is_none() {
            return Err(AppError::not_found(
                "organization",
                organization_id.to_string(),
            ));
        }
        let slug = Slug::new(slug)?;
        if self
            .tenants
            .get_by_slug(organization_id, slug.as_str())
            .await?
            .is_some()
        {
            return Err(AppError::conflict(format!(
                "tenant slug '{slug}' is already taken in this organization",
                slug = slug.as_str()
            )));
        }
        let tenant = Tenant::create(organization_id, name, slug, environment, isolation)?;
        self.tenants.save(&tenant).await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "tenant.register",
            "tenant",
            Some(tenant.id.to_string()),
            mas_domain::AuditOutcome::Success,
        )
        .await?;
        Ok(tenant)
    }

    /// Creates a project inside the context's tenant. The stored tenant
    /// must belong to the context's organization — a tenant id from
    /// another organization "does not exist".
    pub async fn register_project(
        &self,
        ctx: &ServiceContext,
        name: &str,
        slug: &str,
    ) -> Result<Project> {
        let tenant_id = ctx.require_tenant()?;
        let organization_id = ctx.require_organization()?;
        let tenant = self
            .tenants
            .get(tenant_id)
            .await?
            .filter(|t| t.organization_id() == organization_id)
            .ok_or_else(|| AppError::not_found("tenant", tenant_id.to_string()))?;
        if matches!(tenant.status, mas_common::enums::TenantStatus::Suspended) {
            return Err(AppError::conflict(
                "tenant is suspended; project creation is blocked (fail-closed)",
            ));
        }
        let project = Project::create(tenant_id, organization_id, name, Slug::new(slug)?)?;
        self.projects.save(&project).await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "project.register",
            "project",
            Some(project.id.to_string()),
            mas_domain::AuditOutcome::Success,
        )
        .await?;
        Ok(project)
    }

    /// Registers a user account (open sign-up: any context may call, but
    /// the email is enforced unique globally).
    pub async fn register_user(
        &self,
        ctx: &ServiceContext,
        email: &str,
        display_name: &str,
    ) -> Result<User> {
        let email = Email::parse(email)?;
        let email_key = email.to_string();
        if self.users.get_by_email(&email_key).await?.is_some() {
            return Err(AppError::conflict(
                "an account with this email already exists",
            ));
        }
        let user = User::register(email, display_name)?;
        self.users.save(&user).await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "user.register",
            "user",
            Some(user.id.to_string()),
            mas_domain::AuditOutcome::Success,
        )
        .await?;
        Ok(user)
    }

    /// Invites `user` into the context's tenant with `role`. Re-inviting a
    /// user who already holds a membership (any status) in this tenant
    /// conflicts — updates go through role management, not re-invites.
    pub async fn invite_membership(
        &self,
        ctx: &ServiceContext,
        user: UserId,
        role: MembershipRole,
    ) -> Result<Membership> {
        let tenant_id = ctx.require_tenant()?;
        let organization_id = ctx.require_organization()?;
        if self.users.get(user).await?.is_none() {
            return Err(AppError::not_found("user", user.to_string()));
        }
        let existing = self.memberships.for_tenant(tenant_id).await?;
        if existing.iter().any(|m| m.user_id == user) {
            return Err(AppError::conflict(
                "user already holds a membership in this tenant",
            ));
        }
        let membership = Membership::invite(user, organization_id, Some(tenant_id), role)?;
        self.memberships.save(&membership).await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "membership.invite",
            "membership",
            Some(membership.id.to_string()),
            mas_domain::AuditOutcome::Success,
        )
        .await?;
        Ok(membership)
    }

    /// Lists memberships of a tenant (tenant-scoped operator view).
    pub async fn tenant_memberships(&self, ctx: &ServiceContext) -> Result<Vec<Membership>> {
        let tenant_id = ctx.require_tenant()?;
        self.memberships.for_tenant(tenant_id).await
    }

    /// Lists organizations (system-level catalog view).
    pub async fn list_organizations(&self, _ctx: &ServiceContext) -> Result<Vec<Organization>> {
        self.organizations.list().await
    }

    /// Lists tenants of the context's organization.
    pub async fn list_tenants(&self, ctx: &ServiceContext) -> Result<Vec<Tenant>> {
        let organization_id = ctx.require_organization()?;
        self.tenants.list(organization_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::InMemoryAuditSink;
    use crate::stores::InMemoryServices;
    use mas_common::timestamps::Timestamp;
    use std::sync::Arc;

    use mas_common::enums::Environment;
    use mas_common::ids::TenantId;

    struct Fixture {
        service: TenancyService,
        audit: Arc<InMemoryAuditSink>,
    }

    fn fixture() -> Fixture {
        let store = Arc::new(InMemoryServices::new());
        let audit = Arc::new(InMemoryAuditSink::new());
        let service = TenancyService::new(
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store),
            Box::new(audit.clone()),
        );
        Fixture { service, audit }
    }

    fn now() -> Timestamp {
        Timestamp::from_unix_seconds(1_700_000_000).expect("ts")
    }

    #[tokio::test]
    async fn hierarchy_coherence_and_uniqueness_are_enforced() {
        let fixture = fixture();
        let ctx = ServiceContext::system("bootstrap-1", &now()).expect("ctx");

        let organization = fixture
            .service
            .register_organization(&ctx, "Acme Ltd.", "Acme", "acme")
            .await
            .expect("org");
        assert!(
            fixture
                .service
                .register_organization(&ctx, "Acme Clone", "Acme Clone", "acme")
                .await
                .is_err(),
            "slugs are globally unique"
        );

        let org_ctx = ctx.clone().with_scope(TenantId::new(), organization.id);
        // Org-scoped principal with a forged tenant id: tenant registration
        // is org-level, the scope tenant is ignored and must not matter.
        let tenant = fixture
            .service
            .register_tenant(
                &org_ctx,
                "Production",
                "production",
                Environment::Production,
                IsolationMode::SharedRls,
            )
            .await
            .expect("tenant");
        assert!(
            fixture
                .service
                .register_tenant(
                    &org_ctx,
                    "Production 2",
                    "production",
                    Environment::Staging,
                    IsolationMode::SharedRls,
                )
                .await
                .is_err(),
            "tenant slug unique per organization"
        );

        let tenant_ctx = ctx.clone().with_scope(tenant.id, organization.id);
        let project = fixture
            .service
            .register_project(&tenant_ctx, "Service Core", "service-core")
            .await
            .expect("project");
        assert_eq!(project.organization_id, organization.id);

        // Cross-organization coherence: a principal of org B cannot mint a
        // project under org A's tenant — the tenant "does not exist" for B.
        let other_org = fixture
            .service
            .register_organization(&ctx, "Globex", "Globex", "globex")
            .await
            .expect("other org");
        let wrong_ctx = ctx.clone().with_scope(tenant.id, other_org.id);
        assert!(
            fixture
                .service
                .register_project(&wrong_ctx, "Hostile", "hostile")
                .await
                .is_err(),
            "cross-organization project creation is impossible"
        );

        assert_eq!(fixture.audit.len(), 4, "org×2 + tenant + project audited");
    }

    #[tokio::test]
    async fn users_and_memberships_roundtrip_with_conflicts() {
        let fixture = fixture();
        let now = now();
        let ctx = ServiceContext::system("bootstrap-2", &now).expect("ctx");
        let organization = fixture
            .service
            .register_organization(&ctx, "Acme Ltd.", "Acme", "acme")
            .await
            .expect("org");
        let org_ctx = ctx.clone().with_scope(TenantId::new(), organization.id);
        let tenant = fixture
            .service
            .register_tenant(
                &org_ctx,
                "Prod",
                "prod",
                Environment::Production,
                IsolationMode::SharedRls,
            )
            .await
            .expect("tenant");
        let tenant_ctx = ctx.clone().with_scope(tenant.id, organization.id);

        let user = fixture
            .service
            .register_user(&ctx, "jane@acme.example", "Jane Doe")
            .await
            .expect("user");
        assert!(
            fixture
                .service
                .register_user(&ctx, "jane@acme.example", "Jane Again")
                .await
                .is_err(),
            "emails are globally unique"
        );

        let membership = fixture
            .service
            .invite_membership(&tenant_ctx, user.id, MembershipRole::Developer)
            .await
            .expect("invite");
        assert!(matches!(
            membership.status,
            mas_domain::MembershipStatus::Invited
        ));
        assert!(
            fixture
                .service
                .invite_membership(&tenant_ctx, user.id, MembershipRole::Owner)
                .await
                .is_err(),
            "re-invites conflict — use role management"
        );
        assert!(
            fixture
                .service
                .invite_membership(&tenant_ctx, UserId::new(), MembershipRole::Developer)
                .await
                .is_err(),
            "inviting a ghost user rejects with NotFound"
        );
        assert_eq!(
            fixture
                .service
                .tenant_memberships(&tenant_ctx)
                .await
                .expect("list")
                .len(),
            1
        );
    }
}
