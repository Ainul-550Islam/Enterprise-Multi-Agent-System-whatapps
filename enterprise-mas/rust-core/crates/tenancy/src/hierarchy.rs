//! Organization → tenant → project hierarchy: directory port, in-memory
//! implementation, and the service enforcing ownership coherence.
//!
//! Model invariants (from the domain):
//! * an **Organization** is the root tenancy grouping,
//! * a **Tenant** is owned by exactly one organization, forever,
//! * a **Project** references a `(tenant, organization)` pair and must be
//!   consistent with its tenant's owning organization — a cross-linked
//!   project is rejected at write time *and* at every trust boundary.

use mas_common::error::AppError;
use mas_common::ids::{MembershipId, OrganizationId, ProjectId, TenantId, UserId};
use mas_common::result::Result;
use mas_domain::{Membership, Organization, Project, Tenant};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

/// Read/write directory of the tenancy hierarchy.
#[async_trait::async_trait]
pub trait TenantDirectoryPort: Send + Sync + fmt::Debug {
    async fn get_tenant(&self, id: TenantId) -> Result<Option<Tenant>>;
    async fn get_organization(&self, id: OrganizationId) -> Result<Option<Organization>>;
    async fn get_project(&self, id: ProjectId) -> Result<Option<Project>>;
    async fn list_tenants(&self, organization_id: OrganizationId) -> Result<Vec<Tenant>>;
    async fn list_projects(&self, tenant_id: TenantId) -> Result<Vec<Project>>;

    async fn upsert_organization(&self, organization: &Organization) -> Result<()>;
    async fn upsert_tenant(&self, tenant: &Tenant) -> Result<()>;
    async fn upsert_project(&self, project: &Project) -> Result<()>;
}

/// Membership directory.
#[async_trait::async_trait]
pub trait MembershipDirectoryPort: Send + Sync + fmt::Debug {
    /// All memberships of `user_id` (any organization/tenant, any status).
    async fn memberships_for_user(&self, user_id: UserId) -> Result<Vec<Membership>>;
    /// Membership rows scoped to one organization.
    async fn memberships_in_organization(
        &self,
        organization_id: OrganizationId,
    ) -> Result<Vec<Membership>>;
    async fn upsert(&self, membership: &Membership) -> Result<()>;
    async fn get(&self, id: MembershipId) -> Result<Option<Membership>>;
}

/// In-memory directory: deterministic and full-featured.
#[derive(Debug, Default)]
pub struct InMemoryDirectory {
    tenants: Mutex<BTreeMap<TenantId, Tenant>>,
    organizations: Mutex<BTreeMap<OrganizationId, Organization>>,
    projects: Mutex<BTreeMap<ProjectId, Project>>,
}

impl InMemoryDirectory {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl TenantDirectoryPort for InMemoryDirectory {
    async fn get_tenant(&self, id: TenantId) -> Result<Option<Tenant>> {
        Ok(self
            .tenants
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .cloned())
    }
    async fn get_organization(&self, id: OrganizationId) -> Result<Option<Organization>> {
        Ok(self
            .organizations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .cloned())
    }
    async fn get_project(&self, id: ProjectId) -> Result<Option<Project>> {
        Ok(self
            .projects
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .cloned())
    }
    async fn list_tenants(&self, organization_id: OrganizationId) -> Result<Vec<Tenant>> {
        Ok(self
            .tenants
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|tenant| tenant.organization_id() == organization_id)
            .cloned()
            .collect())
    }
    async fn list_projects(&self, tenant_id: TenantId) -> Result<Vec<Project>> {
        Ok(self
            .projects
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|project| project.tenant_id == tenant_id)
            .cloned()
            .collect())
    }

    async fn upsert_organization(&self, organization: &Organization) -> Result<()> {
        self.organizations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(organization.id, organization.clone());
        Ok(())
    }

    async fn upsert_tenant(&self, tenant: &Tenant) -> Result<()> {
        // Ownership invariants hold at write time: the owning org must exist.
        if self
            .organizations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&tenant.organization_id())
            .is_none()
        {
            return Err(AppError::invalid_field(
                "organization_id",
                "unknown_parent",
                "tenants must belong to an existing organization",
            ));
        }
        self.tenants
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(tenant.id, tenant.clone());
        Ok(())
    }

    async fn upsert_project(&self, project: &Project) -> Result<()> {
        // Project coherence: tenant exists + its owning org matches.
        let tenants = self.tenants.lock().unwrap_or_else(|e| e.into_inner());
        match tenants.get(&project.tenant_id) {
            Some(tenant) if tenant.organization_id() == project.organization_id => {},
            Some(_) => {
                return Err(AppError::invalid_field(
                    "organization_id",
                    "cross_link",
                    "project organization must equal the tenant's owning organization",
                ));
            },
            None => {
                return Err(AppError::invalid_field(
                    "tenant_id",
                    "unknown_parent",
                    "projects must belong to an existing tenant",
                ));
            },
        }
        drop(tenants);
        self.projects
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(project.id, project.clone());
        Ok(())
    }
}

/// In-memory membership directory.
#[derive(Debug, Default)]
pub struct InMemoryMemberships {
    memberships: Mutex<BTreeMap<MembershipId, Membership>>,
}

impl InMemoryMemberships {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl MembershipDirectoryPort for InMemoryMemberships {
    async fn memberships_for_user(&self, user_id: UserId) -> Result<Vec<Membership>> {
        Ok(self
            .memberships
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|m| m.user_id == user_id)
            .cloned()
            .collect())
    }
    async fn memberships_in_organization(
        &self,
        organization_id: OrganizationId,
    ) -> Result<Vec<Membership>> {
        Ok(self
            .memberships
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|m| m.organization_id == organization_id)
            .cloned()
            .collect())
    }
    async fn upsert(&self, membership: &Membership) -> Result<()> {
        self.memberships
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(membership.id, membership.clone());
        Ok(())
    }
    async fn get(&self, id: MembershipId) -> Result<Option<Membership>> {
        Ok(self
            .memberships
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .cloned())
    }
}

/// Write-path hierarchy coherence service.
#[derive(Debug)]
pub struct HierarchyService {
    directory: Arc<dyn TenantDirectoryPort>,
}

impl HierarchyService {
    pub fn new(directory: Arc<dyn TenantDirectoryPort>) -> Self {
        Self { directory }
    }

    /// Provisions a complete chain: organization + tenant + project, all
    /// ownership-consistent.
    pub async fn provision_tree(
        &self,
        organization: &Organization,
        tenant: &Tenant,
        project: &Project,
    ) -> Result<()> {
        if tenant.organization_id() != organization.id {
            return Err(AppError::invalid_field(
                "tenant.organization_id",
                "incoherent_scope",
                "tenant must be owned by the organization being provisioned",
            ));
        }
        if project.tenant_id != tenant.id || project.organization_id != organization.id {
            return Err(AppError::invalid_field(
                "project.scope",
                "incoherent_scope",
                "project must belong to the tenant and its owning organization",
            ));
        }
        self.directory.upsert_organization(organization).await?;
        self.directory.upsert_tenant(tenant).await?;
        self.directory.upsert_project(project).await?;
        Ok(())
    }

    /// Whether `tenant_id` is owned by `organization_id` (Option = unknown).
    pub async fn tenant_belongs_to(
        &self,
        organization_id: OrganizationId,
        tenant_id: TenantId,
    ) -> Result<Option<bool>> {
        Ok(self
            .directory
            .get_tenant(tenant_id)
            .await?
            .map(|tenant| tenant.organization_id() == organization_id))
    }

    /// Whether `project_id` belongs coherently to (tenant, organization).
    pub async fn project_belongs_to(
        &self,
        tenant_id: TenantId,
        organization_id: Option<OrganizationId>,
        project_id: ProjectId,
    ) -> Result<Option<bool>> {
        match self.directory.get_project(project_id).await? {
            None => Ok(None),
            Some(project) => Ok(Some(
                project.tenant_id == tenant_id
                    && organization_id.is_none_or(|org| project.organization_id == org),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::enums::Environment;
    use mas_domain::{IsolationMode, Slug};

    fn make_organization(name: &str) -> Organization {
        Organization::create(
            format!("{name} Inc"),
            name.to_owned(),
            Slug::new(name).expect("slug"),
        )
        .expect("organization")
    }

    fn tenant(org: OrganizationId, name: &str) -> Tenant {
        Tenant::create(
            org,
            name.to_owned(),
            Slug::new(name).expect("slug"),
            Environment::Production,
            IsolationMode::SharedRls,
        )
        .expect("tenant")
    }

    fn project(t: &Tenant, name: &str) -> Project {
        Project::create(
            t.id,
            t.organization_id(),
            name.to_owned(),
            Slug::new(name).expect("slug"),
        )
        .expect("project")
    }

    #[tokio::test]
    async fn provision_tree_enforces_coherence() {
        let directory = Arc::new(InMemoryDirectory::new());
        let service = HierarchyService::new(directory);
        let organization = make_organization("acme");
        let t = tenant(organization.id, "acme-prod");
        let project = project(&t, "default");
        service
            .provision_tree(&organization, &t, &project)
            .await
            .expect("happy path");

        // Re-provisioning against a different org must fail.
        let other_org = make_organization("globex");
        assert!(service
            .provision_tree(&other_org, &t, &project)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn cross_linked_projects_are_rejected_at_write_time() {
        let directory = Arc::new(InMemoryDirectory::new());
        let org_a = make_organization("acme");
        let org_b = make_organization("globex");
        directory.upsert_organization(&org_a).await.expect("org");
        directory.upsert_organization(&org_b).await.expect("org");
        let tenant_a = tenant(org_a.id, "a-prod");
        directory.upsert_tenant(&tenant_a).await.expect("tenant");
        // project references tenant A but the OTHER organization:
        let bad = Project::create(
            tenant_a.id,
            org_b.id,
            "spy".to_owned(),
            Slug::new("spy").expect("slug"),
        )
        .expect("project");
        assert!(directory.upsert_project(&bad).await.is_err());
    }

    #[tokio::test]
    async fn ownership_queries_answer() {
        let directory = Arc::new(InMemoryDirectory::new());
        let service = HierarchyService::new(directory);
        let organization = make_organization("acme");
        let t = tenant(organization.id, "acme-prod");
        let project = project(&t, "default");
        service
            .provision_tree(&organization, &t, &project)
            .await
            .expect("tree");
        assert_eq!(
            service
                .tenant_belongs_to(organization.id, t.id)
                .await
                .expect("q"),
            Some(true)
        );
        assert_eq!(
            service
                .tenant_belongs_to(OrganizationId::new(), t.id)
                .await
                .expect("q"),
            Some(true).map(|b| !b) // wrong org → known false
        );
        assert_eq!(
            service
                .tenant_belongs_to(organization.id, TenantId::new())
                .await
                .expect("q"),
            None // unknown tenant
        );
        assert_eq!(
            service
                .project_belongs_to(t.id, Some(organization.id), project.id)
                .await
                .expect("q"),
            Some(true)
        );
        assert_eq!(
            service
                .project_belongs_to(t.id, None, project.id)
                .await
                .expect("q"),
            Some(true)
        );
    }
}
