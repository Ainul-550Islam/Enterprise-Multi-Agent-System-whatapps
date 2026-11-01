//! PURPOSE:
//! Resolves a tenant-authorized agent and immutable published version for the
//! production Rust-to-Python AgentBridge request.
//!
//! Rust owns the agent catalog. This read-only use case proves tenant,
//! organization, project, lifecycle, and version ownership before building the
//! sanitized snapshot that the worker signs into the existing AgentBridge
//! request. Python never substitutes its process-local built-in catalog for a
//! Rust-owned agent id.

use mas_common::error::AppError;
use mas_common::ids::{AgentId, AgentVersionId, OrganizationId, ProjectId, TenantId};
use mas_common::result::Result;
use mas_domain::{Agent, AgentVersion};
use mas_execution::agent_bridge::AgentRuntimeSnapshot;

use crate::stores::{AgentStorePort, AgentVersionStorePort};

/// Read-only application service for resolving one executable agent snapshot.
#[derive(Debug)]
pub struct AgentRuntimeCatalogService {
    agents: Box<dyn AgentStorePort>,
    versions: Box<dyn AgentVersionStorePort>,
}

impl AgentRuntimeCatalogService {
    /// Composes the resolver over the tenant-scoped agent and version stores.
    #[must_use]
    pub fn new(agents: Box<dyn AgentStorePort>, versions: Box<dyn AgentVersionStorePort>) -> Self {
        Self { agents, versions }
    }

    /// Resolves an executable agent and either its requested immutable version
    /// or its current published version.
    ///
    /// The caller must already be inside an RLS-armed transaction. All
    /// identifiers are then rechecked against the loaded aggregate so a stale
    /// or malformed task cannot use a different tenant's agent or project.
    pub async fn resolve(
        &self,
        tenant_id: TenantId,
        organization_id: OrganizationId,
        project_id: ProjectId,
        agent_id: AgentId,
        requested_version: Option<AgentVersionId>,
    ) -> Result<AgentRuntimeSnapshot> {
        let agent = self
            .agents
            .get(agent_id)
            .await?
            .filter(|agent| {
                agent.tenant_id == tenant_id
                    && agent.organization_id == organization_id
                    && agent.project_id == project_id
                    && agent.id == agent_id
                    && agent.is_executable()
            })
            .ok_or_else(|| AppError::not_found("agent", agent_id.to_string()))?;

        let versions = self.versions.list(agent_id).await?;
        let selected = select_version(&agent, versions, requested_version)?;
        AgentRuntimeSnapshot::from_domain(&agent, &selected)
    }
}

fn select_version(
    agent: &Agent,
    versions: Vec<AgentVersion>,
    requested_version: Option<AgentVersionId>,
) -> Result<AgentVersion> {
    let selected = if let Some(version_id) = requested_version {
        versions
            .into_iter()
            .find(|version| version.id() == version_id)
    } else {
        let current = agent
            .current_version
            .ok_or_else(|| AppError::not_found("agent", agent.id.to_string()))?;
        versions
            .into_iter()
            .find(|version| version.version_number() == current)
    };

    selected
        .filter(|version| version.agent_id() == agent.id)
        .ok_or_else(|| AppError::not_found("agent version", agent.id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stores::{AgentStorePort, AgentVersionStorePort, InMemoryServices};
    use mas_common::ids::{OrganizationId, ProjectId, TenantId, UserId};
    use mas_domain::{AgentKind, AgentVersion, Slug};
    use std::sync::Arc;

    const CHECKSUM_V1: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    const CHECKSUM_V2: &str = "2222222222222222222222222222222222222222222222222222222222222222";

    struct Fixture {
        store: Arc<InMemoryServices>,
        catalog: AgentRuntimeCatalogService,
        tenant: TenantId,
        organization: OrganizationId,
        project: ProjectId,
        agent: Agent,
        version_one: AgentVersion,
        version_two: AgentVersion,
    }

    async fn fixture() -> Fixture {
        let store = Arc::new(InMemoryServices::new());
        let tenant = TenantId::new();
        let organization = OrganizationId::new();
        let project = ProjectId::new();
        let mut agent = Agent::create(
            tenant,
            organization,
            project,
            "Catalog Agent",
            Slug::new("catalog-agent").expect("slug"),
            AgentKind::Standard,
        )
        .expect("agent");
        agent.enable().expect("activate agent");
        agent
            .record_published_version(1)
            .expect("record first version");

        let version_one = AgentVersion::publish(
            agent.id,
            1,
            CHECKSUM_V1,
            serde_json::json!({
                "model_hint": "provider/model-v1",
                "system_prompt": "version one instructions"
            }),
            UserId::new(),
        )
        .expect("publish first version");
        AgentStorePort::save(store.as_ref(), &agent)
            .await
            .expect("save agent");
        AgentVersionStorePort::save(store.as_ref(), &version_one)
            .await
            .expect("save first version");

        agent
            .record_published_version(2)
            .expect("record second version");
        let version_two = AgentVersion::publish(
            agent.id,
            2,
            CHECKSUM_V2,
            serde_json::json!({
                "model_hint": "provider/model-v2",
                "system_prompt": "version two instructions"
            }),
            UserId::new(),
        )
        .expect("publish second version");
        AgentStorePort::save(store.as_ref(), &agent)
            .await
            .expect("save updated agent");
        AgentVersionStorePort::save(store.as_ref(), &version_two)
            .await
            .expect("save second version");

        let catalog =
            AgentRuntimeCatalogService::new(Box::new(store.clone()), Box::new(store.clone()));
        Fixture {
            store,
            catalog,
            tenant,
            organization,
            project,
            agent,
            version_one,
            version_two,
        }
    }

    #[tokio::test]
    async fn current_resolution_uses_the_latest_immutable_version_and_real_uuid_ids() {
        let fixture = fixture().await;
        let snapshot = fixture
            .catalog
            .resolve(
                fixture.tenant,
                fixture.organization,
                fixture.project,
                fixture.agent.id,
                None,
            )
            .await
            .expect("resolve current version");

        assert_eq!(snapshot.agent_id, fixture.agent.id);
        assert_eq!(snapshot.tenant_id, fixture.tenant);
        assert_eq!(snapshot.organization_id, fixture.organization);
        assert_eq!(snapshot.project_id, fixture.project);
        assert_eq!(snapshot.agent_version_id, fixture.version_two.id());
        assert_eq!(snapshot.version_number, 2);
        assert_eq!(snapshot.configuration_checksum, CHECKSUM_V2);
        assert_eq!(
            snapshot.configuration.model_hint.as_deref(),
            Some("provider/model-v2")
        );
    }

    #[tokio::test]
    async fn an_explicit_older_version_selects_that_exact_snapshot() {
        let fixture = fixture().await;
        let snapshot = fixture
            .catalog
            .resolve(
                fixture.tenant,
                fixture.organization,
                fixture.project,
                fixture.agent.id,
                Some(fixture.version_one.id()),
            )
            .await
            .expect("resolve pinned version");

        assert_eq!(snapshot.agent_version_id, fixture.version_one.id());
        assert_eq!(snapshot.version_number, 1);
        assert_eq!(snapshot.configuration_checksum, CHECKSUM_V1);
        assert_eq!(
            snapshot.configuration.system_prompt,
            "version one instructions"
        );
    }

    #[tokio::test]
    async fn a_missing_agent_is_not_resolvable() {
        let fixture = fixture().await;
        let result = fixture
            .catalog
            .resolve(
                fixture.tenant,
                fixture.organization,
                fixture.project,
                AgentId::new(),
                None,
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn a_foreign_tenant_or_project_cannot_resolve_the_agent() {
        let fixture = fixture().await;

        let wrong_tenant = fixture
            .catalog
            .resolve(
                TenantId::new(),
                fixture.organization,
                fixture.project,
                fixture.agent.id,
                None,
            )
            .await;
        let wrong_project = fixture
            .catalog
            .resolve(
                fixture.tenant,
                fixture.organization,
                ProjectId::new(),
                fixture.agent.id,
                None,
            )
            .await;

        assert!(wrong_tenant.is_err());
        assert!(wrong_project.is_err());
    }

    #[tokio::test]
    async fn a_disabled_agent_is_not_resolvable_even_if_it_has_versions() {
        let fixture = fixture().await;
        let mut disabled = fixture.agent.clone();
        disabled.disable().expect("disable agent");
        AgentStorePort::save(fixture.store.as_ref(), &disabled)
            .await
            .expect("persist disabled status");

        let result = fixture
            .catalog
            .resolve(
                fixture.tenant,
                fixture.organization,
                fixture.project,
                fixture.agent.id,
                None,
            )
            .await;

        assert!(result.is_err());
    }
}
