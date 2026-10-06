// PURPOSE: The tenancy directory for the in-memory development and test lane.
//
// Owning layer: `mas-api` (composition), adapting `mas-application`'s
// in-memory store to `mas-tenancy`'s directory ports.
//
// Production responsibility: none — this type never runs in production. Its
// job is to make the development lane behave like production: scope is still
// resolved from identity and the directory, never from a request header.
//
// That matters more than it sounds. The earlier development lane had no
// directory at all, so it would have had to resolve scope from claims — and a
// lane that resolves scope differently from production is a lane where
// authorization bugs cannot be reproduced until deploy. This adapter makes the
// same code path run in both.
//
// Security boundary: it is a test double and is labelled as one. It reads the
// very same `InMemoryServices` the tenancy service writes to, so a tenant
// registered through the service is visible to the resolver, and a tenant that
// was never registered is not.
//
// It must NOT:
//   • be reachable from `compose_production` — a grep for this module outside
//     `#[cfg(test)]`/dev composition is a defect;
//   • invent tenants on demand. A directory that answers "yes" for any id
//     would make cross-tenant tests pass for the wrong reason.

use mas_application::stores::{
    InMemoryServices, MembershipStorePort, OrganizationStorePort, ProjectStorePort, TenantStorePort,
};
use mas_common::ids::{MembershipId, OrganizationId, ProjectId, TenantId, UserId};
use mas_common::result::Result;
use mas_domain::{Membership, Organization, Project, Tenant};
use mas_tenancy::{MembershipDirectoryPort, TenantDirectoryPort};
use std::sync::Arc;

/// The in-memory directory, backed by the shared application store.
#[derive(Debug)]
pub struct InMemoryTenantDirectory {
    store: Arc<InMemoryServices>,
}

impl InMemoryTenantDirectory {
    /// Wraps the shared store.
    #[must_use]
    pub fn new(store: Arc<InMemoryServices>) -> Self {
        Self { store }
    }
}

#[async_trait::async_trait]
impl TenantDirectoryPort for InMemoryTenantDirectory {
    async fn get_tenant(&self, id: TenantId) -> Result<Option<Tenant>> {
        TenantStorePort::get(self.store.as_ref(), id).await
    }

    async fn get_organization(&self, id: OrganizationId) -> Result<Option<Organization>> {
        OrganizationStorePort::get(self.store.as_ref(), id).await
    }

    async fn get_project(&self, id: ProjectId) -> Result<Option<Project>> {
        ProjectStorePort::get(self.store.as_ref(), id).await
    }

    async fn list_tenants(&self, organization_id: OrganizationId) -> Result<Vec<Tenant>> {
        TenantStorePort::list(self.store.as_ref(), organization_id).await
    }

    async fn list_projects(&self, tenant_id: TenantId) -> Result<Vec<Project>> {
        ProjectStorePort::list(self.store.as_ref(), tenant_id).await
    }

    async fn upsert_organization(&self, organization: &Organization) -> Result<()> {
        OrganizationStorePort::save(self.store.as_ref(), organization).await
    }

    async fn upsert_tenant(&self, tenant: &Tenant) -> Result<()> {
        TenantStorePort::save(self.store.as_ref(), tenant).await
    }

    async fn upsert_project(&self, project: &Project) -> Result<()> {
        ProjectStorePort::save(self.store.as_ref(), project).await
    }
}

/// The in-memory membership directory over the same store.
#[derive(Debug)]
pub struct InMemoryMembershipDirectory {
    store: Arc<InMemoryServices>,
}

impl InMemoryMembershipDirectory {
    /// Wraps the shared store.
    #[must_use]
    pub fn new(store: Arc<InMemoryServices>) -> Self {
        Self { store }
    }
}

#[async_trait::async_trait]
impl MembershipDirectoryPort for InMemoryMembershipDirectory {
    async fn memberships_for_user(&self, user_id: UserId) -> Result<Vec<Membership>> {
        MembershipStorePort::for_user(self.store.as_ref(), user_id).await
    }

    /// Memberships scoped to one organization.
    ///
    /// The application store indexes memberships by user and by tenant, so
    /// this is computed as the union over the organization's tenants. The
    /// resolver does not call it; it is implemented for completeness and for
    /// any future organization-level authorization check, and doing it this
    /// way avoids adding an index to a store that exists only for tests.
    async fn memberships_in_organization(
        &self,
        organization_id: OrganizationId,
    ) -> Result<Vec<Membership>> {
        let mut out = Vec::new();
        for tenant in TenantStorePort::list(self.store.as_ref(), organization_id).await? {
            let mut rows = MembershipStorePort::for_tenant(self.store.as_ref(), tenant.id).await?;
            out.append(&mut rows);
        }
        Ok(out)
    }

    async fn upsert(&self, membership: &Membership) -> Result<()> {
        MembershipStorePort::save(self.store.as_ref(), membership).await
    }

    async fn get(&self, id: MembershipId) -> Result<Option<Membership>> {
        MembershipStorePort::get(self.store.as_ref(), id).await
    }
}
