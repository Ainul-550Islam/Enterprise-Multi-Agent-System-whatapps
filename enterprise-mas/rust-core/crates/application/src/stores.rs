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
    AgentId, AgentVersionId, ApprovalRequestId, ConnectorId, DataSourceId, DocumentId, ExecutionId,
    ExecutionStepId, MembershipId, OrganizationId, PolicyId, ProjectId, RollbackRecordId,
    ScheduleId, TenantId, ToolId, UsageRecordId, UserId, WebhookId, WorkflowId, WorkflowVersionId,
};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{
    Agent, AgentVersion, Connector, DataSource, Document, Execution, ExecutionStep, Membership,
    Notification, Organization, Policy, Project, RollbackRecord, RollbackResourceKind, Schedule,
    Tenant, Tool, UsageRecord, User, WebhookEndpoint, Workflow, WorkflowVersion,
};
use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::notification_service::{NotificationCursor, NotificationLifecycleStorePort};

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

#[async_trait]
pub trait ToolStorePort: std::fmt::Debug + Send + Sync {
    async fn save(&self, tool: &Tool) -> Result<()>;
    async fn get(&self, id: ToolId) -> Result<Option<Tool>>;
    async fn list(&self, tenant: TenantId, project: Option<ProjectId>) -> Result<Vec<Tool>>;
}

#[async_trait]
pub trait PolicyStorePort: std::fmt::Debug + Send + Sync {
    async fn save(&self, policy: &Policy) -> Result<()>;
    async fn get(&self, id: PolicyId) -> Result<Option<Policy>>;
    async fn list(&self, tenant: TenantId) -> Result<Vec<Policy>>;
}

#[async_trait]
pub trait ConnectorStorePort: std::fmt::Debug + Send + Sync {
    async fn save(&self, connector: &Connector) -> Result<()>;
    async fn get(&self, id: ConnectorId) -> Result<Option<Connector>>;
    async fn list(&self, tenant: TenantId) -> Result<Vec<Connector>>;
}

/// Stable descending keyset position for outbound endpoint metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WebhookEndpointCursor {
    pub created_at: Timestamp,
    pub id: WebhookId,
}

#[async_trait]
pub trait WebhookStorePort: std::fmt::Debug + Send + Sync {
    async fn save(&self, endpoint: &WebhookEndpoint) -> Result<()>;
    async fn get(&self, id: WebhookId) -> Result<Option<WebhookEndpoint>>;
    /// Legacy unbounded store helper. Public API handlers must use
    /// [`Self::list_page`] instead of materializing a tenant's full catalogue.
    async fn list(&self, tenant: TenantId) -> Result<Vec<WebhookEndpoint>>;
    /// Bounded stable page ordered `created_at DESC, id DESC`.
    async fn list_page(
        &self,
        tenant: TenantId,
        after: Option<WebhookEndpointCursor>,
        limit: u16,
    ) -> Result<Vec<WebhookEndpoint>>;
}

#[async_trait]
pub trait UsageStorePort: std::fmt::Debug + Send + Sync {
    /// Usage is append-only; duplicate IDs are conflicts.
    async fn append(&self, record: &UsageRecord) -> Result<()>;
    async fn get(&self, id: UsageRecordId) -> Result<Option<UsageRecord>>;
    async fn list(
        &self,
        tenant: TenantId,
        from: Timestamp,
        to: Timestamp,
    ) -> Result<Vec<UsageRecord>>;
}

#[async_trait]
pub trait NotificationStorePort: std::fmt::Debug + Send + Sync {
    async fn save(&self, notification: &Notification) -> Result<()>;
    async fn get(&self, id: uuid::Uuid) -> Result<Option<Notification>>;
    async fn list(&self, tenant: TenantId) -> Result<Vec<Notification>>;
}

#[async_trait]
pub trait WorkflowVersionStorePort: std::fmt::Debug + Send + Sync {
    /// Inserts an immutable snapshot; a workflow version number is unique.
    async fn append(&self, version: &WorkflowVersion) -> Result<()>;
    /// Atomically persists a published workflow pointer and its immutable snapshot.
    async fn publish_snapshot(&self, workflow: &Workflow, version: &WorkflowVersion) -> Result<()>;
    async fn get(&self, id: WorkflowVersionId) -> Result<Option<WorkflowVersion>>;
    async fn get_number(
        &self,
        workflow: WorkflowId,
        version_number: u64,
    ) -> Result<Option<WorkflowVersion>>;
    async fn list(&self, workflow: WorkflowId) -> Result<Vec<WorkflowVersion>>;
}

#[async_trait]
pub trait RollbackStorePort: std::fmt::Debug + Send + Sync {
    /// Appends a rollback record; idempotency-key uniqueness is tenant-scoped.
    async fn append(&self, record: &RollbackRecord) -> Result<()>;
    async fn get(&self, id: RollbackRecordId) -> Result<Option<RollbackRecord>>;
    async fn get_by_idempotency(
        &self,
        tenant: TenantId,
        idempotency_key: &str,
    ) -> Result<Option<RollbackRecord>>;
    async fn list(&self, tenant: TenantId, limit: u16) -> Result<Vec<RollbackRecord>>;
    /// Atomically inserts an agent rollback version, advances the agent, and
    /// appends the idempotency/audit history row.
    async fn commit_agent_rollback(
        &self,
        agent: &Agent,
        version: &AgentVersion,
        record: &RollbackRecord,
    ) -> Result<()>;
    /// Atomically inserts a workflow rollback snapshot, advances the workflow,
    /// and appends the idempotency/audit history row.
    async fn commit_workflow_rollback(
        &self,
        workflow: &Workflow,
        version: &WorkflowVersion,
        record: &RollbackRecord,
    ) -> Result<()>;
}

#[async_trait]
pub trait DataSourceStorePort: std::fmt::Debug + Send + Sync {
    async fn save(&self, source: &DataSource) -> Result<()>;
    async fn get(&self, id: DataSourceId) -> Result<Option<DataSource>>;
    async fn list(&self, tenant: TenantId, project: ProjectId) -> Result<Vec<DataSource>>;
}

#[async_trait]
pub trait DocumentStorePort: std::fmt::Debug + Send + Sync {
    async fn save(&self, document: &Document) -> Result<()>;
    async fn get(&self, id: DocumentId) -> Result<Option<Document>>;
    async fn list(&self, tenant: TenantId, source: DataSourceId) -> Result<Vec<Document>>;
}

#[async_trait]
pub trait ExecutionStepStorePort: std::fmt::Debug + Send + Sync {
    async fn save(&self, step: &ExecutionStep) -> Result<()>;
    async fn get(&self, id: ExecutionStepId) -> Result<Option<ExecutionStep>>;
    async fn list(&self, execution: ExecutionId) -> Result<Vec<ExecutionStep>>;
}

/// Data returned by the analytics read port. Each member is already scoped to
/// one tenant and bounded by the requested current/previous time window.
#[derive(Debug, Clone, Default)]
pub struct AnalyticsSnapshot {
    /// Tenant agents, used for counts and baseline time-saved estimates.
    pub agents: Vec<Agent>,
    /// Executions observed during either requested comparison window.
    pub executions: Vec<Execution>,
    /// Usage observations observed during either requested comparison window.
    pub usage_records: Vec<UsageRecord>,
}

#[async_trait]
pub trait AnalyticsStorePort: std::fmt::Debug + Send + Sync {
    async fn snapshot(
        &self,
        tenant: TenantId,
        previous_from: Timestamp,
        current_to: Timestamp,
    ) -> Result<AnalyticsSnapshot>;
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
    tools: BTreeMap<ToolId, Tool>,
    policies: BTreeMap<PolicyId, Policy>,
    approval_requests: BTreeMap<ApprovalRequestId, mas_policy::approval::ApprovalRequest>,
    connectors: BTreeMap<ConnectorId, Connector>,
    webhooks: BTreeMap<WebhookId, WebhookEndpoint>,
    usage_records: BTreeMap<UsageRecordId, UsageRecord>,
    notifications: BTreeMap<uuid::Uuid, Notification>,
    workflow_versions: BTreeMap<WorkflowVersionId, WorkflowVersion>,
    rollback_records: BTreeMap<RollbackRecordId, RollbackRecord>,
    data_sources: BTreeMap<DataSourceId, DataSource>,
    documents: BTreeMap<DocumentId, Document>,
    execution_steps: BTreeMap<ExecutionStepId, ExecutionStep>,
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
            + state.tools.len()
            + state.policies.len()
            + state.approval_requests.len()
            + state.connectors.len()
            + state.webhooks.len()
            + state.usage_records.len()
            + state.notifications.len()
            + state.workflow_versions.len()
            + state.rollback_records.len()
            + state.data_sources.len()
            + state.documents.len()
            + state.execution_steps.len()
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

#[async_trait]
impl ToolStorePort for InMemoryServices {
    async fn save(&self, tool: &Tool) -> Result<()> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .tools
            .insert(tool.id, tool.clone());
        Ok(())
    }

    async fn get(&self, id: ToolId) -> Result<Option<Tool>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .tools
            .get(&id)
            .cloned())
    }

    async fn list(&self, tenant: TenantId, project: Option<ProjectId>) -> Result<Vec<Tool>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .tools
            .values()
            .filter(|tool| {
                tool.tenant_id == tenant
                    && project.is_none_or(|project| tool.project_id == Some(project))
            })
            .cloned()
            .collect())
    }
}

#[async_trait]
impl PolicyStorePort for InMemoryServices {
    async fn save(&self, policy: &Policy) -> Result<()> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .policies
            .insert(policy.id, policy.clone());
        Ok(())
    }

    async fn get(&self, id: PolicyId) -> Result<Option<Policy>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .policies
            .get(&id)
            .cloned())
    }

    async fn list(&self, tenant: TenantId) -> Result<Vec<Policy>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .policies
            .values()
            .filter(|policy| policy.tenant_id == Some(tenant))
            .cloned()
            .collect())
    }
}

#[async_trait]
impl mas_policy::approval::ApprovalStorePort for InMemoryServices {
    async fn insert(&self, request: &mas_policy::approval::ApprovalRequest) -> Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.approval_requests.contains_key(&request.id) {
            return Err(mas_common::error::AppError::conflict(format!(
                "approval request {} already exists",
                request.id
            )));
        }
        state.approval_requests.insert(request.id, request.clone());
        Ok(())
    }

    async fn update(&self, request: &mas_policy::approval::ApprovalRequest) -> Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.approval_requests.contains_key(&request.id) {
            return Err(mas_common::error::AppError::not_found(
                "approval request",
                request.id.to_string(),
            ));
        }
        state.approval_requests.insert(request.id, request.clone());
        Ok(())
    }

    async fn get(
        &self,
        id: ApprovalRequestId,
    ) -> Result<Option<mas_policy::approval::ApprovalRequest>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .approval_requests
            .get(&id)
            .cloned())
    }

    async fn list_pending(
        &self,
        tenant_id: TenantId,
    ) -> Result<Vec<mas_policy::approval::ApprovalRequest>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .approval_requests
            .values()
            .filter(|request| {
                request.tenant_id == tenant_id
                    && request.status == mas_policy::approval::ApprovalStatus::Pending
            })
            .cloned()
            .collect())
    }

    async fn list_for_tenant(
        &self,
        tenant_id: TenantId,
    ) -> Result<Vec<mas_policy::approval::ApprovalRequest>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .approval_requests
            .values()
            .filter(|request| request.tenant_id == tenant_id)
            .cloned()
            .collect())
    }
}

#[async_trait]
impl ConnectorStorePort for InMemoryServices {
    async fn save(&self, connector: &Connector) -> Result<()> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .connectors
            .insert(connector.id, connector.clone());
        Ok(())
    }

    async fn get(&self, id: ConnectorId) -> Result<Option<Connector>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .connectors
            .get(&id)
            .cloned())
    }

    async fn list(&self, tenant: TenantId) -> Result<Vec<Connector>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .connectors
            .values()
            .filter(|connector| connector.tenant_id == tenant)
            .cloned()
            .collect())
    }
}

#[async_trait]
impl WebhookStorePort for InMemoryServices {
    async fn save(&self, endpoint: &WebhookEndpoint) -> Result<()> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .webhooks
            .insert(endpoint.id, endpoint.clone());
        Ok(())
    }

    async fn get(&self, id: WebhookId) -> Result<Option<WebhookEndpoint>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .webhooks
            .get(&id)
            .cloned())
    }

    async fn list(&self, tenant: TenantId) -> Result<Vec<WebhookEndpoint>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .webhooks
            .values()
            .filter(|endpoint| endpoint.tenant_id == tenant)
            .cloned()
            .collect())
    }

    async fn list_page(
        &self,
        tenant: TenantId,
        after: Option<WebhookEndpointCursor>,
        limit: u16,
    ) -> Result<Vec<WebhookEndpoint>> {
        let mut endpoints = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .webhooks
            .values()
            .filter(|endpoint| endpoint.tenant_id == tenant)
            .filter(|endpoint| {
                after.is_none_or(|cursor| {
                    endpoint.created_at < cursor.created_at
                        || (endpoint.created_at == cursor.created_at && endpoint.id < cursor.id)
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        endpoints.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| right.id.cmp(&left.id))
        });
        endpoints.truncate(usize::from(limit.clamp(1, 201)));
        Ok(endpoints)
    }
}

#[async_trait]
impl UsageStorePort for InMemoryServices {
    async fn append(&self, record: &UsageRecord) -> Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.usage_records.contains_key(&record.id) {
            return Err(mas_common::error::AppError::conflict(format!(
                "usage record {} already exists",
                record.id
            )));
        }
        state.usage_records.insert(record.id, record.clone());
        Ok(())
    }

    async fn get(&self, id: UsageRecordId) -> Result<Option<UsageRecord>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .usage_records
            .get(&id)
            .cloned())
    }

    async fn list(
        &self,
        tenant: TenantId,
        from: Timestamp,
        to: Timestamp,
    ) -> Result<Vec<UsageRecord>> {
        let mut records = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .usage_records
            .values()
            .filter(|record| {
                record.tenant_id == tenant && record.occurred_at >= from && record.occurred_at < to
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by_key(|record| record.occurred_at);
        Ok(records)
    }
}

#[async_trait]
impl NotificationStorePort for InMemoryServices {
    async fn save(&self, notification: &Notification) -> Result<()> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .notifications
            .insert(notification.id, notification.clone());
        Ok(())
    }

    async fn get(&self, id: uuid::Uuid) -> Result<Option<Notification>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .notifications
            .get(&id)
            .cloned())
    }

    async fn list(&self, tenant: TenantId) -> Result<Vec<Notification>> {
        let mut notifications = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .notifications
            .values()
            .filter(|notification| notification.tenant_id == tenant)
            .cloned()
            .collect::<Vec<_>>();
        notifications.sort_by_key(|notification| notification.created_at);
        Ok(notifications)
    }
}

/// In-memory implementation of the canonical notification lifecycle port.
///
/// This is test/dev-only state shared with the legacy notification store. It
/// deliberately mirrors the durable unique key and descending keyset behavior
/// so handler tests cannot accidentally rely on offset pagination semantics.
#[async_trait]
impl NotificationLifecycleStorePort for InMemoryServices {
    async fn create_or_get(&self, notification: &Notification) -> Result<Notification> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(existing) = state.notifications.values().find(|existing| {
            existing.tenant_id == notification.tenant_id
                && existing.recipient_id == notification.recipient_id
                && existing.channel == notification.channel
                && existing.deduplication_key == notification.deduplication_key
        }) {
            return Ok(existing.clone());
        }
        state
            .notifications
            .insert(notification.id, notification.clone());
        Ok(notification.clone())
    }

    async fn get(&self, tenant: TenantId, id: uuid::Uuid) -> Result<Option<Notification>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .notifications
            .get(&id)
            .filter(|notification| notification.tenant_id == tenant)
            .cloned())
    }

    async fn save(&self, notification: &Notification) -> Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        match state.notifications.get(&notification.id) {
            Some(existing) if existing.tenant_id == notification.tenant_id => {
                state
                    .notifications
                    .insert(notification.id, notification.clone());
                Ok(())
            },
            _ => Err(mas_common::error::AppError::not_found(
                "notification",
                notification.id.to_string(),
            )),
        }
    }

    async fn list_page(
        &self,
        tenant: TenantId,
        recipient: UserId,
        include_archived: bool,
        unread_only: bool,
        after: Option<NotificationCursor>,
        limit: u16,
    ) -> Result<Vec<Notification>> {
        if limit == 0 {
            return Err(mas_common::error::AppError::invalid_field(
                "limit",
                "out_of_range",
                "notification page limit must be positive",
            ));
        }
        let mut page = self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .notifications
            .values()
            .filter(|notification| {
                notification.tenant_id == tenant
                    && notification.recipient_id == recipient
                    && (include_archived || notification.archived_at.is_none())
                    && (!unread_only || notification.read_at.is_none())
                    && after.is_none_or(|cursor| {
                        notification.created_at < cursor.created_at
                            || (notification.created_at == cursor.created_at
                                && notification.id < cursor.id)
                    })
            })
            .cloned()
            .collect::<Vec<_>>();
        page.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| right.id.cmp(&left.id))
        });
        page.truncate(usize::from(limit));
        Ok(page)
    }
}

#[async_trait]
impl WorkflowVersionStorePort for InMemoryServices {
    async fn append(&self, version: &WorkflowVersion) -> Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.workflow_versions.contains_key(&version.id())
            || state.workflow_versions.values().any(|stored| {
                stored.workflow_id() == version.workflow_id()
                    && stored.version_number() == version.version_number()
            })
        {
            return Err(mas_common::error::AppError::conflict(format!(
                "workflow {} version {} already exists",
                version.workflow_id(),
                version.version_number()
            )));
        }
        state
            .workflow_versions
            .insert(version.id(), version.clone());
        Ok(())
    }

    async fn publish_snapshot(&self, workflow: &Workflow, version: &WorkflowVersion) -> Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if workflow.id != version.workflow_id()
            || workflow.tenant_id != version.tenant_id()
            || workflow.published_version != Some(version.version_number())
        {
            return Err(mas_common::error::AppError::invalid_field(
                "workflow_version",
                "scope_mismatch",
                "workflow pointer and published snapshot do not match",
            ));
        }
        let existing = state.workflows.get(&workflow.id).ok_or_else(|| {
            mas_common::error::AppError::not_found("workflow", workflow.id.to_string())
        })?;
        if existing.tenant_id != workflow.tenant_id
            || existing.published_version.unwrap_or(0).checked_add(1)
                != Some(version.version_number())
        {
            return Err(mas_common::error::AppError::conflict(
                "workflow version is stale or belongs to another tenant",
            ));
        }
        if state.workflow_versions.contains_key(&version.id())
            || state.workflow_versions.values().any(|stored| {
                stored.workflow_id() == version.workflow_id()
                    && stored.version_number() == version.version_number()
            })
        {
            return Err(mas_common::error::AppError::conflict(format!(
                "workflow {} version {} already exists",
                version.workflow_id(),
                version.version_number()
            )));
        }
        state
            .workflow_versions
            .insert(version.id(), version.clone());
        state.workflows.insert(workflow.id, workflow.clone());
        Ok(())
    }

    async fn get(&self, id: WorkflowVersionId) -> Result<Option<WorkflowVersion>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .workflow_versions
            .get(&id)
            .cloned())
    }

    async fn get_number(
        &self,
        workflow: WorkflowId,
        version_number: u64,
    ) -> Result<Option<WorkflowVersion>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .workflow_versions
            .values()
            .find(|version| {
                version.workflow_id() == workflow && version.version_number() == version_number
            })
            .cloned())
    }

    async fn list(&self, workflow: WorkflowId) -> Result<Vec<WorkflowVersion>> {
        let mut versions = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .workflow_versions
            .values()
            .filter(|version| version.workflow_id() == workflow)
            .cloned()
            .collect::<Vec<_>>();
        versions.sort_by_key(WorkflowVersion::version_number);
        Ok(versions)
    }
}

#[async_trait]
impl RollbackStorePort for InMemoryServices {
    async fn append(&self, record: &RollbackRecord) -> Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.rollback_records.contains_key(&record.id)
            || state.rollback_records.values().any(|stored| {
                stored.tenant_id == record.tenant_id
                    && stored.idempotency_key == record.idempotency_key
            })
        {
            return Err(mas_common::error::AppError::conflict(
                "rollback idempotency key or record id already exists",
            ));
        }
        state.rollback_records.insert(record.id, record.clone());
        Ok(())
    }

    async fn get(&self, id: RollbackRecordId) -> Result<Option<RollbackRecord>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .rollback_records
            .get(&id)
            .cloned())
    }

    async fn get_by_idempotency(
        &self,
        tenant: TenantId,
        idempotency_key: &str,
    ) -> Result<Option<RollbackRecord>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .rollback_records
            .values()
            .find(|record| record.tenant_id == tenant && record.idempotency_key == idempotency_key)
            .cloned())
    }

    async fn list(&self, tenant: TenantId, limit: u16) -> Result<Vec<RollbackRecord>> {
        let mut records = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .rollback_records
            .values()
            .filter(|record| record.tenant_id == tenant)
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| right.id.cmp(&left.id))
        });
        records.truncate(usize::from(limit));
        Ok(records)
    }

    async fn commit_agent_rollback(
        &self,
        agent: &Agent,
        version: &AgentVersion,
        record: &RollbackRecord,
    ) -> Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        validate_rollback_commit(
            &state,
            record,
            RollbackResourceKind::Agent,
            *agent.id.as_uuid(),
            agent.tenant_id,
            record.to_version,
        )?;
        let previous = state
            .agents
            .get(&agent.id)
            .ok_or_else(|| mas_common::error::AppError::not_found("agent", agent.id.to_string()))?;
        if previous.tenant_id != record.tenant_id
            || previous.current_version != Some(record.from_version)
        {
            return Err(mas_common::error::AppError::conflict(
                "agent changed while rollback was being committed",
            ));
        }
        if version.agent_id() != agent.id
            || version.version_number() != record.to_version
            || version.rollback_of().is_none()
            || agent.current_version != Some(record.to_version)
        {
            return Err(mas_common::error::AppError::invalid_field(
                "rollback",
                "incoherent_commit",
                "agent, version and rollback record do not describe one transition",
            ));
        }
        if state.agent_versions.contains_key(&version.id())
            || state.agent_versions.values().any(|stored| {
                stored.agent_id() == version.agent_id()
                    && stored.version_number() == version.version_number()
            })
        {
            return Err(mas_common::error::AppError::conflict(
                "agent version number already exists",
            ));
        }
        state.agent_versions.insert(version.id(), version.clone());
        state.agents.insert(agent.id, agent.clone());
        state.rollback_records.insert(record.id, record.clone());
        Ok(())
    }

    async fn commit_workflow_rollback(
        &self,
        workflow: &Workflow,
        version: &WorkflowVersion,
        record: &RollbackRecord,
    ) -> Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        validate_rollback_commit(
            &state,
            record,
            RollbackResourceKind::Workflow,
            *workflow.id.as_uuid(),
            workflow.tenant_id,
            record.to_version,
        )?;
        let previous = state.workflows.get(&workflow.id).ok_or_else(|| {
            mas_common::error::AppError::not_found("workflow", workflow.id.to_string())
        })?;
        if previous.tenant_id != record.tenant_id
            || previous.published_version != Some(record.from_version)
        {
            return Err(mas_common::error::AppError::conflict(
                "workflow changed while rollback was being committed",
            ));
        }
        if version.workflow_id() != workflow.id
            || version.tenant_id() != workflow.tenant_id
            || version.version_number() != record.to_version
            || version.rollback_of().is_none()
            || workflow.published_version != Some(record.to_version)
        {
            return Err(mas_common::error::AppError::invalid_field(
                "rollback",
                "incoherent_commit",
                "workflow, version and rollback record do not describe one transition",
            ));
        }
        if state.workflow_versions.contains_key(&version.id())
            || state.workflow_versions.values().any(|stored| {
                stored.workflow_id() == version.workflow_id()
                    && stored.version_number() == version.version_number()
            })
        {
            return Err(mas_common::error::AppError::conflict(
                "workflow version number already exists",
            ));
        }
        state
            .workflow_versions
            .insert(version.id(), version.clone());
        state.workflows.insert(workflow.id, workflow.clone());
        state.rollback_records.insert(record.id, record.clone());
        Ok(())
    }
}

fn validate_rollback_commit(
    state: &StoreState,
    record: &RollbackRecord,
    kind: RollbackResourceKind,
    resource_id: uuid::Uuid,
    tenant_id: TenantId,
    version: u64,
) -> Result<()> {
    if record.resource_kind != kind
        || record.resource_id != resource_id
        || record.tenant_id != tenant_id
        || record.to_version != version
    {
        return Err(mas_common::error::AppError::invalid_field(
            "rollback",
            "scope_mismatch",
            "rollback record does not match its resource transition",
        ));
    }
    if state.rollback_records.contains_key(&record.id)
        || state.rollback_records.values().any(|stored| {
            stored.tenant_id == record.tenant_id && stored.idempotency_key == record.idempotency_key
        })
    {
        return Err(mas_common::error::AppError::conflict(
            "rollback idempotency key or record id already exists",
        ));
    }
    Ok(())
}

#[async_trait]
impl DataSourceStorePort for InMemoryServices {
    async fn save(&self, source: &DataSource) -> Result<()> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .data_sources
            .insert(source.id, source.clone());
        Ok(())
    }

    async fn get(&self, id: DataSourceId) -> Result<Option<DataSource>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .data_sources
            .get(&id)
            .cloned())
    }

    async fn list(&self, tenant: TenantId, project: ProjectId) -> Result<Vec<DataSource>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .data_sources
            .values()
            .filter(|source| source.tenant_id == tenant && source.project_id == project)
            .cloned()
            .collect())
    }
}

#[async_trait]
impl DocumentStorePort for InMemoryServices {
    async fn save(&self, document: &Document) -> Result<()> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .documents
            .insert(document.id, document.clone());
        Ok(())
    }

    async fn get(&self, id: DocumentId) -> Result<Option<Document>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .documents
            .get(&id)
            .cloned())
    }

    async fn list(&self, tenant: TenantId, source: DataSourceId) -> Result<Vec<Document>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .documents
            .values()
            .filter(|document| document.tenant_id == tenant && document.source_id == source)
            .cloned()
            .collect())
    }
}

#[async_trait]
impl ExecutionStepStorePort for InMemoryServices {
    async fn save(&self, step: &ExecutionStep) -> Result<()> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .execution_steps
            .insert(step.id, step.clone());
        Ok(())
    }

    async fn get(&self, id: ExecutionStepId) -> Result<Option<ExecutionStep>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .execution_steps
            .get(&id)
            .cloned())
    }

    async fn list(&self, execution: ExecutionId) -> Result<Vec<ExecutionStep>> {
        let mut steps = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .execution_steps
            .values()
            .filter(|step| step.execution_id == execution)
            .cloned()
            .collect::<Vec<_>>();
        steps.sort_by_key(|step| step.sequence);
        Ok(steps)
    }
}

#[async_trait]
impl AnalyticsStorePort for InMemoryServices {
    async fn snapshot(
        &self,
        tenant: TenantId,
        previous_from: Timestamp,
        current_to: Timestamp,
    ) -> Result<AnalyticsSnapshot> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let from_ms = previous_from.to_unix_ms();
        let to_ms = current_to.to_unix_ms();
        let agents = state
            .agents
            .values()
            .filter(|agent| agent.tenant_id == tenant)
            .cloned()
            .collect();
        let executions = state
            .executions
            .values()
            .filter(|execution| {
                let observed_at = execution.finished_at.unwrap_or(execution.created_at);
                execution.tenant_id == tenant
                    && observed_at.to_unix_ms() >= from_ms
                    && observed_at.to_unix_ms() < to_ms
            })
            .cloned()
            .collect();
        let usage_records = state
            .usage_records
            .values()
            .filter(|record| {
                record.tenant_id == tenant
                    && record.occurred_at.to_unix_ms() >= from_ms
                    && record.occurred_at.to_unix_ms() < to_ms
            })
            .cloned()
            .collect();
        Ok(AnalyticsSnapshot {
            agents,
            executions,
            usage_records,
        })
    }
}

// ---------------------------------------------------------------------
// `Arc` delegation — services hold `Box<dyn …Port>`; tests/harnesses
// share one `Arc<InMemoryServices>` across every dependency slot.
// ---------------------------------------------------------------------
macro_rules! arc_delegate {
    ($trait:ident => $inner:ty { $(fn $method:ident($($arg:ident: $ty:ty),* $(,)?) -> $ret:ty;)* }) => {
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
arc_delegate!(ToolStorePort => InMemoryServices {
    fn save(tool: &Tool) -> Result<()>;
    fn get(id: ToolId) -> Result<Option<Tool>>;
    fn list(tenant: TenantId, project: Option<ProjectId>) -> Result<Vec<Tool>>;
});
arc_delegate!(PolicyStorePort => InMemoryServices {
    fn save(policy: &Policy) -> Result<()>;
    fn get(id: PolicyId) -> Result<Option<Policy>>;
    fn list(tenant: TenantId) -> Result<Vec<Policy>>;
});
arc_delegate!(ConnectorStorePort => InMemoryServices {
    fn save(connector: &Connector) -> Result<()>;
    fn get(id: ConnectorId) -> Result<Option<Connector>>;
    fn list(tenant: TenantId) -> Result<Vec<Connector>>;
});
arc_delegate!(WebhookStorePort => InMemoryServices {
    fn save(endpoint: &WebhookEndpoint) -> Result<()>;
    fn get(id: WebhookId) -> Result<Option<WebhookEndpoint>>;
    fn list(tenant: TenantId) -> Result<Vec<WebhookEndpoint>>;
    fn list_page(tenant: TenantId, after: Option<WebhookEndpointCursor>, limit: u16) -> Result<Vec<WebhookEndpoint>>;
});
arc_delegate!(UsageStorePort => InMemoryServices {
    fn append(record: &UsageRecord) -> Result<()>;
    fn get(id: UsageRecordId) -> Result<Option<UsageRecord>>;
    fn list(tenant: TenantId, from: Timestamp, to: Timestamp) -> Result<Vec<UsageRecord>>;
});
arc_delegate!(NotificationStorePort => InMemoryServices {
    fn save(notification: &Notification) -> Result<()>;
    fn get(id: uuid::Uuid) -> Result<Option<Notification>>;
    fn list(tenant: TenantId) -> Result<Vec<Notification>>;
});
arc_delegate!(NotificationLifecycleStorePort => InMemoryServices {
    fn create_or_get(notification: &Notification) -> Result<Notification>;
    fn get(tenant: TenantId, id: uuid::Uuid) -> Result<Option<Notification>>;
    fn save(notification: &Notification) -> Result<()>;
    fn list_page(
        tenant: TenantId,
        recipient: UserId,
        include_archived: bool,
        unread_only: bool,
        after: Option<NotificationCursor>,
        limit: u16,
    ) -> Result<Vec<Notification>>;
});
arc_delegate!(WorkflowVersionStorePort => InMemoryServices {
    fn append(version: &WorkflowVersion) -> Result<()>;
    fn publish_snapshot(workflow: &Workflow, version: &WorkflowVersion) -> Result<()>;
    fn get(id: WorkflowVersionId) -> Result<Option<WorkflowVersion>>;
    fn get_number(workflow: WorkflowId, version_number: u64) -> Result<Option<WorkflowVersion>>;
    fn list(workflow: WorkflowId) -> Result<Vec<WorkflowVersion>>;
});
arc_delegate!(RollbackStorePort => InMemoryServices {
    fn append(record: &RollbackRecord) -> Result<()>;
    fn get(id: RollbackRecordId) -> Result<Option<RollbackRecord>>;
    fn get_by_idempotency(tenant: TenantId, idempotency_key: &str) -> Result<Option<RollbackRecord>>;
    fn list(tenant: TenantId, limit: u16) -> Result<Vec<RollbackRecord>>;
    fn commit_agent_rollback(agent: &Agent, version: &AgentVersion, record: &RollbackRecord) -> Result<()>;
    fn commit_workflow_rollback(workflow: &Workflow, version: &WorkflowVersion, record: &RollbackRecord) -> Result<()>;
});
arc_delegate!(DataSourceStorePort => InMemoryServices {
    fn save(source: &DataSource) -> Result<()>;
    fn get(id: DataSourceId) -> Result<Option<DataSource>>;
    fn list(tenant: TenantId, project: ProjectId) -> Result<Vec<DataSource>>;
});
arc_delegate!(DocumentStorePort => InMemoryServices {
    fn save(document: &Document) -> Result<()>;
    fn get(id: DocumentId) -> Result<Option<Document>>;
    fn list(tenant: TenantId, source: DataSourceId) -> Result<Vec<Document>>;
});
arc_delegate!(ExecutionStepStorePort => InMemoryServices {
    fn save(step: &ExecutionStep) -> Result<()>;
    fn get(id: ExecutionStepId) -> Result<Option<ExecutionStep>>;
    fn list(execution: ExecutionId) -> Result<Vec<ExecutionStep>>;
});
arc_delegate!(AnalyticsStorePort => InMemoryServices {
    fn snapshot(tenant: TenantId, previous_from: Timestamp, current_to: Timestamp) -> Result<AnalyticsSnapshot>;
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
