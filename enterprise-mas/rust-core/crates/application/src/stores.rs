//! Application-facing store ports, one per aggregate the use-cases
//! compose, plus a unified in-memory implementation (`InMemoryServices`)
//! used by tests, the CLI demo harness and local development.
//!
//! Port rules:
//! * lookups return `Option` — use-cases decide between `NotFound`,
//!   hidden-existence `NotFound`, and `Conflict`;
//! * writes are upserts keyed by the aggregate id; secondary uniqueness
//!   (slugs, emails, idempotency keys) is modelled explicitly by the port,
//!   not by trailing lookups through ten layers;
//! * every trait is object-safe so services hold `Box<dyn …Port>`.

use async_trait::async_trait;
use mas_common::ids::{
    AgentId, AgentVersionId, ExecutionId, MembershipId, OrganizationId, ProjectId, ScheduleId,
    TenantId, UserId, WorkflowId,
};
use mas_common::result::Result;
use mas_domain::{
    Agent, AgentVersion, Execution, Membership, Organization, Project, Schedule, Tenant, User,
    Workflow,
};
use std::collections::BTreeMap;
use std::sync::Mutex;

#[async_trait]
pub trait OrganizationStorePort: std::fmt::Debug + Send + Sync {
    async fn save(&self, organization: &Organization) -> Result<()>;
    async fn get(&self, id: OrganizationId) -> Result<Option<Organization>>;
    async fn get_by_slug(&self, slug: &str) -> Result<Option<Organization>>;
    async fn list(&self) -> Result<Vec<Organization>>;
}

#[async_trait]
pub trait TenantStorePort: std::fmt::Debug + Send + Sync {
    async fn save(&self, tenant: &Tenant) -> Result<()>;
    async fn get(&self, id: TenantId) -> Result<Option<Tenant>>;
    async fn get_by_slug(&self, organization: OrganizationId, slug: &str)
        -> Result<Option<Tenant>>;
    async fn list(&self, organization: OrganizationId) -> Result<Vec<Tenant>>;
}

#[async_trait]
pub trait ProjectStorePort: std::fmt::Debug + Send + Sync {
    async fn save(&self, project: &Project) -> Result<()>;
    async fn get(&self, id: ProjectId) -> Result<Option<Project>>;
    async fn list(&self, tenant: TenantId) -> Result<Vec<Project>>;
}

#[async_trait]
pub trait UserStorePort: std::fmt::Debug + Send + Sync {
    async fn save(&self, user: &User) -> Result<()>;
    async fn get(&self, id: UserId) -> Result<Option<User>>;
    async fn get_by_email(&self, email: &str) -> Result<Option<User>>;
}

#[async_trait]
pub trait MembershipStorePort: std::fmt::Debug + Send + Sync {
    async fn save(&self, membership: &Membership) -> Result<()>;
    async fn get(&self, id: MembershipId) -> Result<Option<Membership>>;
    async fn for_user(&self, user: UserId) -> Result<Vec<Membership>>;
    async fn for_tenant(&self, tenant: TenantId) -> Result<Vec<Membership>>;
}

#[async_trait]
pub trait AgentStorePort: std::fmt::Debug + Send + Sync {
    async fn save(&self, agent: &Agent) -> Result<()>;
    async fn get(&self, id: AgentId) -> Result<Option<Agent>>;
    async fn get_by_slug(&self, project: ProjectId, slug: &str) -> Result<Option<Agent>>;
    async fn list(&self, project: ProjectId) -> Result<Vec<Agent>>;
}

#[async_trait]
pub trait AgentVersionStorePort: std::fmt::Debug + Send + Sync {
    async fn save(&self, version: &AgentVersion) -> Result<()>;
    async fn get(&self, id: AgentVersionId) -> Result<Option<AgentVersion>>;
    async fn list(&self, agent: AgentId) -> Result<Vec<AgentVersion>>;
}

#[async_trait]
pub trait WorkflowStorePort: std::fmt::Debug + Send + Sync {
    async fn save(&self, workflow: &Workflow) -> Result<()>;
    async fn get(&self, id: WorkflowId) -> Result<Option<Workflow>>;
    async fn list(&self, project: ProjectId) -> Result<Vec<Workflow>>;
}

#[async_trait]
pub trait ExecutionStorePort: std::fmt::Debug + Send + Sync {
    async fn save(&self, execution: &Execution) -> Result<()>;
    async fn get(&self, id: ExecutionId) -> Result<Option<Execution>>;
    async fn list(&self, tenant: TenantId) -> Result<Vec<Execution>>;
    async fn idempotency_get(&self, tenant: TenantId, key: &str) -> Result<Option<ExecutionId>>;
    async fn idempotency_put(
        &self,
        tenant: TenantId,
        key: &str,
        execution: ExecutionId,
    ) -> Result<()>;
}

#[async_trait]
pub trait ScheduleStorePort: std::fmt::Debug + Send + Sync {
    async fn save(&self, schedule: &Schedule) -> Result<()>;
    async fn get(&self, id: ScheduleId) -> Result<Option<Schedule>>;
    async fn list(&self, tenant: TenantId) -> Result<Vec<Schedule>>;
}

/// Unified in-memory state behind one lock.
#[derive(Debug, Default)]
struct StoreState {
    organizations: BTreeMap<OrganizationId, Organization>,
    tenants: BTreeMap<TenantId, Tenant>,
    projects: BTreeMap<ProjectId, Project>,
    users: BTreeMap<UserId, User>,
    memberships: BTreeMap<MembershipId, Membership>,
    agents: BTreeMap<AgentId, Agent>,
    agent_versions: BTreeMap<AgentVersionId, AgentVersion>,
    workflows: BTreeMap<WorkflowId, Workflow>,
    executions: BTreeMap<ExecutionId, Execution>,
    execution_idempotency: BTreeMap<(TenantId, String), ExecutionId>,
    schedules: BTreeMap<ScheduleId, Schedule>,
}

/// Single-struct in-memory implementation of every port. Shares one lock
/// so cross-aggregate invariants (e.g. project coherence checks) are
/// atomic with respect to concurrent service calls in tests.
#[derive(Debug, Default)]
pub struct InMemoryServices {
    state: Mutex<StoreState>,
}

impl InMemoryServices {
    /// Empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Total number of stored aggregates (diagnostics).
    #[must_use]
    pub fn total_rows(&self) -> usize {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.organizations.len()
            + state.tenants.len()
            + state.projects.len()
            + state.users.len()
            + state.memberships.len()
            + state.agents.len()
            + state.agent_versions.len()
            + state.workflows.len()
            + state.executions.len()
            + state.schedules.len()
    }

    /// Whether the store is completely empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.total_rows() == 0
    }
}

#[async_trait]
impl OrganizationStorePort for InMemoryServices {
    async fn save(&self, organization: &Organization) -> Result<()> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .organizations
            .insert(organization.id, organization.clone());
        Ok(())
    }

    async fn get(&self, id: OrganizationId) -> Result<Option<Organization>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .organizations
            .get(&id)
            .cloned())
    }

    async fn get_by_slug(&self, slug: &str) -> Result<Option<Organization>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .organizations
            .values()
            .find(|o| o.slug.as_str() == slug)
            .cloned())
    }

    async fn list(&self) -> Result<Vec<Organization>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .organizations
            .values()
            .cloned()
            .collect())
    }
}

#[async_trait]
impl TenantStorePort for InMemoryServices {
    async fn save(&self, tenant: &Tenant) -> Result<()> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .tenants
            .insert(tenant.id, tenant.clone());
        Ok(())
    }

    async fn get(&self, id: TenantId) -> Result<Option<Tenant>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .tenants
            .get(&id)
            .cloned())
    }

    async fn get_by_slug(
        &self,
        organization: OrganizationId,
        slug: &str,
    ) -> Result<Option<Tenant>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .tenants
            .values()
            .find(|t| t.organization_id() == organization && t.slug.as_str() == slug)
            .cloned())
    }

    async fn list(&self, organization: OrganizationId) -> Result<Vec<Tenant>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .tenants
            .values()
            .filter(|t| t.organization_id() == organization)
            .cloned()
            .collect())
    }
}

#[async_trait]
impl ProjectStorePort for InMemoryServices {
    async fn save(&self, project: &Project) -> Result<()> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .projects
            .insert(project.id, project.clone());
        Ok(())
    }

    async fn get(&self, id: ProjectId) -> Result<Option<Project>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .projects
            .get(&id)
            .cloned())
    }

    async fn list(&self, tenant: TenantId) -> Result<Vec<Project>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .projects
            .values()
            .filter(|p| p.tenant_id == tenant)
            .cloned()
            .collect())
    }
}

#[async_trait]
impl UserStorePort for InMemoryServices {
    async fn save(&self, user: &User) -> Result<()> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .users
            .insert(user.id, user.clone());
        Ok(())
    }

    async fn get(&self, id: UserId) -> Result<Option<User>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .users
            .get(&id)
            .cloned())
    }

    async fn get_by_email(&self, email: &str) -> Result<Option<User>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .users
            .values()
            .find(|u| u.email.to_string() == email)
            .cloned())
    }
}

#[async_trait]
impl MembershipStorePort for InMemoryServices {
    async fn save(&self, membership: &Membership) -> Result<()> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .memberships
            .insert(membership.id, membership.clone());
        Ok(())
    }

    async fn get(&self, id: MembershipId) -> Result<Option<Membership>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .memberships
            .get(&id)
            .cloned())
    }

    async fn for_user(&self, user: UserId) -> Result<Vec<Membership>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .memberships
            .values()
            .filter(|m| m.user_id == user)
            .cloned()
            .collect())
    }

    async fn for_tenant(&self, tenant: TenantId) -> Result<Vec<Membership>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .memberships
            .values()
            .filter(|m| m.tenant_id == Some(tenant))
            .cloned()
            .collect())
    }
}

#[async_trait]
impl AgentStorePort for InMemoryServices {
    async fn save(&self, agent: &Agent) -> Result<()> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .agents
            .insert(agent.id, agent.clone());
        Ok(())
    }

    async fn get(&self, id: AgentId) -> Result<Option<Agent>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .agents
            .get(&id)
            .cloned())
    }

    async fn get_by_slug(&self, project: ProjectId, slug: &str) -> Result<Option<Agent>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .agents
            .values()
            .find(|a| a.project_id == project && a.slug.as_str() == slug)
            .cloned())
    }

    async fn list(&self, project: ProjectId) -> Result<Vec<Agent>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .agents
            .values()
            .filter(|a| a.project_id == project)
            .cloned()
            .collect())
    }
}

#[async_trait]
impl AgentVersionStorePort for InMemoryServices {
    async fn save(&self, version: &AgentVersion) -> Result<()> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .agent_versions
            .insert(version.id(), version.clone());
        Ok(())
    }

    async fn get(&self, id: AgentVersionId) -> Result<Option<AgentVersion>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .agent_versions
            .get(&id)
            .cloned())
    }

    async fn list(&self, agent: AgentId) -> Result<Vec<AgentVersion>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .agent_versions
            .values()
            .filter(|v| v.agent_id() == agent)
            .cloned()
            .collect())
    }
}

#[async_trait]
impl WorkflowStorePort for InMemoryServices {
    async fn save(&self, workflow: &Workflow) -> Result<()> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .workflows
            .insert(workflow.id, workflow.clone());
        Ok(())
    }

    async fn get(&self, id: WorkflowId) -> Result<Option<Workflow>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .workflows
            .get(&id)
            .cloned())
    }

    async fn list(&self, project: ProjectId) -> Result<Vec<Workflow>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .workflows
            .values()
            .filter(|w| w.project_id == project)
            .cloned()
            .collect())
    }
}

#[async_trait]
impl ExecutionStorePort for InMemoryServices {
    async fn save(&self, execution: &Execution) -> Result<()> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .executions
            .insert(execution.id, execution.clone());
        Ok(())
    }

    async fn get(&self, id: ExecutionId) -> Result<Option<Execution>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .executions
            .get(&id)
            .cloned())
    }

    async fn list(&self, tenant: TenantId) -> Result<Vec<Execution>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .executions
            .values()
            .filter(|e2| e2.tenant_id == tenant)
            .cloned()
            .collect())
    }

    async fn idempotency_get(&self, tenant: TenantId, key: &str) -> Result<Option<ExecutionId>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .execution_idempotency
            .get(&(tenant, key.to_owned()))
            .copied())
    }

    async fn idempotency_put(
        &self,
        tenant: TenantId,
        key: &str,
        execution: ExecutionId,
    ) -> Result<()> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .execution_idempotency
            .insert((tenant, key.to_owned()), execution);
        Ok(())
    }
}

#[async_trait]
impl ScheduleStorePort for InMemoryServices {
    async fn save(&self, schedule: &Schedule) -> Result<()> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .schedules
            .insert(schedule.id, schedule.clone());
        Ok(())
    }

    async fn get(&self, id: ScheduleId) -> Result<Option<Schedule>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .schedules
            .get(&id)
            .cloned())
    }

    async fn list(&self, tenant: TenantId) -> Result<Vec<Schedule>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .schedules
            .values()
            .filter(|s| s.tenant_id == tenant)
            .cloned()
            .collect())
    }
}

// ---------------------------------------------------------------------
// `Arc` delegation — services hold `Box<dyn …Port>`; tests/harnesses
// share one `Arc<InMemoryServices>` across every dependency slot.
// ---------------------------------------------------------------------
macro_rules! arc_delegate {
    ($trait:ident => $inner:ty { $(fn $method:ident($($arg:ident: $ty:ty),*) -> $ret:ty;)* }) => {
        #[async_trait]
        impl $trait for std::sync::Arc<$inner> {
            $(async fn $method(&self, $($arg: $ty),*) -> $ret {
                $trait::$method(self.as_ref(), $($arg),*).await
            })*
        }
    };
}

arc_delegate!(OrganizationStorePort => InMemoryServices {
    fn save(organization: &Organization) -> Result<()>;
    fn get(id: OrganizationId) -> Result<Option<Organization>>;
    fn get_by_slug(slug: &str) -> Result<Option<Organization>>;
    fn list() -> Result<Vec<Organization>>;
});
arc_delegate!(TenantStorePort => InMemoryServices {
    fn save(tenant: &Tenant) -> Result<()>;
    fn get(id: TenantId) -> Result<Option<Tenant>>;
    fn get_by_slug(organization: OrganizationId, slug: &str) -> Result<Option<Tenant>>;
    fn list(organization: OrganizationId) -> Result<Vec<Tenant>>;
});
arc_delegate!(ProjectStorePort => InMemoryServices {
    fn save(project: &Project) -> Result<()>;
    fn get(id: ProjectId) -> Result<Option<Project>>;
    fn list(tenant: TenantId) -> Result<Vec<Project>>;
});
arc_delegate!(UserStorePort => InMemoryServices {
    fn save(user: &User) -> Result<()>;
    fn get(id: UserId) -> Result<Option<User>>;
    fn get_by_email(email: &str) -> Result<Option<User>>;
});
arc_delegate!(MembershipStorePort => InMemoryServices {
    fn save(membership: &Membership) -> Result<()>;
    fn get(id: MembershipId) -> Result<Option<Membership>>;
    fn for_user(user: UserId) -> Result<Vec<Membership>>;
    fn for_tenant(tenant: TenantId) -> Result<Vec<Membership>>;
});
arc_delegate!(AgentStorePort => InMemoryServices {
    fn save(agent: &Agent) -> Result<()>;
    fn get(id: AgentId) -> Result<Option<Agent>>;
    fn get_by_slug(project: ProjectId, slug: &str) -> Result<Option<Agent>>;
    fn list(project: ProjectId) -> Result<Vec<Agent>>;
});
arc_delegate!(AgentVersionStorePort => InMemoryServices {
    fn save(version: &AgentVersion) -> Result<()>;
    fn get(id: AgentVersionId) -> Result<Option<AgentVersion>>;
    fn list(agent: AgentId) -> Result<Vec<AgentVersion>>;
});
arc_delegate!(WorkflowStorePort => InMemoryServices {
    fn save(workflow: &Workflow) -> Result<()>;
    fn get(id: WorkflowId) -> Result<Option<Workflow>>;
    fn list(project: ProjectId) -> Result<Vec<Workflow>>;
});
arc_delegate!(ExecutionStorePort => InMemoryServices {
    fn save(execution: &Execution) -> Result<()>;
    fn get(id: ExecutionId) -> Result<Option<Execution>>;
    fn list(tenant: TenantId) -> Result<Vec<Execution>>;
    fn idempotency_get(tenant: TenantId, key: &str) -> Result<Option<ExecutionId>>;
    fn idempotency_put(tenant: TenantId, key: &str, execution: ExecutionId) -> Result<()>;
});
arc_delegate!(ScheduleStorePort => InMemoryServices {
    fn save(schedule: &Schedule) -> Result<()>;
    fn get(id: ScheduleId) -> Result<Option<Schedule>>;
    fn list(tenant: TenantId) -> Result<Vec<Schedule>>;
});

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::enums::Environment;
    use mas_domain::value_objects::Slug;
    use mas_domain::IsolationMode;

    #[tokio::test]
    async fn unified_store_roundtrips_aggregates() {
        let store = InMemoryServices::new();
        assert!(store.is_empty());

        let organization =
            Organization::create("Acme Ltd.", "Acme", Slug::new("acme").expect("slug"))
                .expect("org");
        OrganizationStorePort::save(&store, &organization)
            .await
            .expect("save org");
        let fetched = OrganizationStorePort::get_by_slug(&store, "acme")
            .await
            .expect("get")
            .expect("present");
        assert_eq!(fetched.id, organization.id);

        let tenant = Tenant::create(
            organization.id,
            "Production",
            Slug::new("production").expect("slug"),
            Environment::Production,
            IsolationMode::SharedRls,
        )
        .expect("tenant");
        TenantStorePort::save(&store, &tenant)
            .await
            .expect("save tenant");
        assert_eq!(
            TenantStorePort::get_by_slug(&store, organization.id, "production")
                .await
                .expect("get")
                .expect("present")
                .id,
            tenant.id
        );
        assert_eq!(
            TenantStorePort::list(&store, organization.id)
                .await
                .expect("list")
                .len(),
            1
        );

        let execution = Execution::new(
            tenant.id,
            organization.id,
            ProjectId::new(),
            None,
            Some(AgentId::new()),
            serde_json::json!({"seed": true}),
            "corr-1",
            "bootstrap",
        )
        .expect("execution");
        ExecutionStorePort::save(&store, &execution)
            .await
            .expect("save execution");
        ExecutionStorePort::idempotency_put(&store, tenant.id, "idem-1", execution.id)
            .await
            .expect("put");
        assert_eq!(
            ExecutionStorePort::idempotency_get(&store, tenant.id, "idem-1")
                .await
                .expect("get"),
            Some(execution.id)
        );
        assert_eq!(
            ExecutionStorePort::list(&store, tenant.id)
                .await
                .expect("list")
                .len(),
            1
        );

        assert_eq!(store.total_rows(), 3);
        assert!(!store.is_empty());
    }
}
