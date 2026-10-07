//! Agent lifecycle use-cases: registration (per-project slug uniqueness,
//! project coherence), immutable version publishing with checksums, and
//! scoped reads (`get`/list never leak foreign aggregates).

use mas_common::error::AppError;
use mas_common::ids::{AgentId, ProjectId, UserId};
use mas_common::result::Result;
use mas_contracts::agent::AgentResponse;
use mas_contracts::rollback::{RollbackRequest, RollbackResponseDto};
use mas_domain::value_objects::Slug;
use mas_domain::{Agent, AgentKind, AgentVersion, RollbackRecord, RollbackResourceKind};

use crate::audit::{self, AuditSinkPort};
use crate::context::ServiceContext;
use crate::dto;
use crate::stores::{AgentStorePort, AgentVersionStorePort, ProjectStorePort, RollbackStorePort};

/// Use-cases over agents and their immutable versions.
#[derive(Debug)]
pub struct AgentService {
    projects: Box<dyn ProjectStorePort>,
    agents: Box<dyn AgentStorePort>,
    versions: Box<dyn AgentVersionStorePort>,
    rollbacks: Option<Box<dyn RollbackStorePort>>,
    audit: Box<dyn AuditSinkPort>,
}

impl AgentService {
    /// Composes the service over store ports + the audit sink.
    pub fn new(
        projects: Box<dyn ProjectStorePort>,
        agents: Box<dyn AgentStorePort>,
        versions: Box<dyn AgentVersionStorePort>,
        audit: Box<dyn AuditSinkPort>,
    ) -> Self {
        Self {
            projects,
            agents,
            versions,
            rollbacks: None,
            audit,
        }
    }

    /// Attaches the append-only rollback history port.
    #[must_use]
    pub fn with_rollback_store(mut self, rollbacks: Box<dyn RollbackStorePort>) -> Self {
        self.rollbacks = Some(rollbacks);
        self
    }

    /// Registers a new agent in `Draft`, enforcing:
    /// * scope-proven tenant/org from the context,
    /// * the target project exists and belongs to this scope,
    /// * per-project slug uniqueness.
    pub async fn register(
        &self,
        ctx: &ServiceContext,
        project: ProjectId,
        name: &str,
        slug: &str,
        kind: AgentKind,
    ) -> Result<Agent> {
        let tenant_id = ctx.require_tenant()?;
        let organization_id = ctx.require_organization()?;
        self.project_in_scope(ctx, project).await?;

        let slug = Slug::new(slug)?;
        if self
            .agents
            .get_by_slug(project, slug.as_str())
            .await?
            .is_some()
        {
            return Err(AppError::conflict(format!(
                "agent slug '{slug}' is already taken in this project",
                slug = slug.as_str()
            )));
        }
        let agent = Agent::create(tenant_id, organization_id, project, name, slug, kind)?;
        self.agents.save(&agent).await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "agent.register",
            "agent",
            Some(agent.id.to_string()),
            mas_domain::AuditOutcome::Success,
        )
        .await?;
        Ok(agent)
    }

    /// Publishes the next immutable version: monotonic numbering from the
    /// agent's `current_version`, domain checksum/format validation, and a
    /// synchronized `record_published_version` on the agent aggregate.
    pub async fn publish_version(
        &self,
        ctx: &ServiceContext,
        agent_id: AgentId,
        configuration_checksum: &str,
        configuration_snapshot: serde_json::Value,
        published_by: UserId,
    ) -> Result<AgentVersion> {
        let mut agent = self.scope_checked(ctx, agent_id).await?;
        let next_number = agent.current_version.unwrap_or(0) + 1;
        let version = AgentVersion::publish(
            agent_id,
            next_number,
            configuration_checksum,
            configuration_snapshot,
            published_by,
        )?;
        agent.record_published_version(next_number)?;
        self.versions.save(&version).await?;
        self.agents.save(&agent).await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "agent.publish_version",
            "agent_version",
            Some(version.id().to_string()),
            mas_domain::AuditOutcome::Success,
        )
        .await?;
        Ok(version)
    }

    /// Scope-checked fetch (tenant/organization hiding).
    pub async fn get(&self, ctx: &ServiceContext, agent_id: AgentId) -> Result<Agent> {
        self.scope_checked(ctx, agent_id).await
    }

    /// Fetch mapped to the wire DTO (what api processes return).
    pub async fn get_response(
        &self,
        ctx: &ServiceContext,
        agent_id: AgentId,
    ) -> Result<AgentResponse> {
        Ok(dto::agent_response(
            &self.scope_checked(ctx, agent_id).await?,
        ))
    }

    /// Lists agents of a project the context may see.
    pub async fn list(&self, ctx: &ServiceContext, project: ProjectId) -> Result<Vec<Agent>> {
        self.project_in_scope(ctx, project).await?;
        self.agents.list(project).await
    }

    /// Published versions, newest first.
    pub async fn list_versions(
        &self,
        ctx: &ServiceContext,
        agent_id: AgentId,
    ) -> Result<Vec<AgentVersion>> {
        self.scope_checked(ctx, agent_id).await?;
        let mut versions = self.versions.list(agent_id).await?;
        versions.sort_by_key(mas_domain::AgentVersion::version_number);
        versions.reverse();
        Ok(versions)
    }

    /// Publishes a new monotonic version from an earlier immutable snapshot.
    /// Repeating the same tenant-scoped idempotency key returns the original
    /// receipt only when the resource, target version, and reason also match.
    pub async fn rollback_version(
        &self,
        ctx: &ServiceContext,
        agent_id: AgentId,
        request: &RollbackRequest,
    ) -> Result<RollbackResponseDto> {
        request.validate()?;
        let tenant_id = ctx.require_tenant()?;
        let mut agent = self.scope_checked(ctx, agent_id).await?;
        agent.ensure_api_mutable("roll back")?;
        if agent.status == mas_common::enums::AgentStatus::Archived {
            return Err(AppError::conflict("archived agents cannot be rolled back"));
        }
        let rollback_store = self
            .rollbacks
            .as_ref()
            .ok_or_else(|| AppError::internal("agent rollback history store is not configured"))?;
        if let Some(existing) = rollback_store
            .get_by_idempotency(tenant_id, &request.idempotency_key)
            .await?
        {
            return self.replay_rollback(&existing, agent_id, request).await;
        }

        let current_version = agent.current_version.ok_or_else(|| {
            AppError::conflict("an unpublished agent has no version to roll back")
        })?;
        let versions = self.versions.list(agent_id).await?;
        let source = versions
            .iter()
            .find(|version| version.version_number() == request.target_version_number)
            .ok_or_else(|| {
                AppError::not_found(
                    "agent version",
                    format!("{}:{}", agent_id, request.target_version_number),
                )
            })?;
        if source.version_number() >= current_version {
            return Err(AppError::conflict(
                "rollback target must be older than the current agent version",
            ));
        }
        let next_version = current_version
            .checked_add(1)
            .ok_or_else(|| AppError::conflict("agent version counter is exhausted"))?;
        let publisher = authenticated_user(ctx)?;
        let version = AgentVersion::publish(
            agent_id,
            next_version,
            source.configuration_checksum().to_owned(),
            source.configuration_snapshot().clone(),
            publisher,
        )?
        .as_rollback_of(source.id());
        let record = RollbackRecord::new(
            tenant_id,
            RollbackResourceKind::Agent,
            *agent.id.as_uuid(),
            current_version,
            next_version,
            ctx.actor.clone(),
            request.reason.clone(),
            request.idempotency_key.clone(),
        )?;
        agent.record_published_version(next_version)?;

        if let Err(error) = rollback_store
            .commit_agent_rollback(&agent, &version, &record)
            .await
        {
            if let Some(existing) = rollback_store
                .get_by_idempotency(tenant_id, &request.idempotency_key)
                .await?
            {
                return self.replay_rollback(&existing, agent_id, request).await;
            }
            return Err(error);
        }
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "agent.rollback_version",
            "agent",
            Some(agent_id.to_string()),
            mas_domain::AuditOutcome::Success,
        )
        .await?;
        Ok(rollback_response(&record, request.target_version_number))
    }

    async fn replay_rollback(
        &self,
        record: &RollbackRecord,
        agent_id: AgentId,
        request: &RollbackRequest,
    ) -> Result<RollbackResponseDto> {
        if record.resource_kind != RollbackResourceKind::Agent
            || record.resource_id != *agent_id.as_uuid()
            || record.reason != request.reason
        {
            return Err(AppError::conflict(
                "rollback idempotency key was already used for a different operation",
            ));
        }
        let versions = self.versions.list(agent_id).await?;
        let created = versions
            .iter()
            .find(|version| version.version_number() == record.to_version)
            .ok_or_else(|| AppError::database("rollback record has no published agent version"))?;
        let source_id = created
            .rollback_of()
            .ok_or_else(|| AppError::database("rollback version is missing its source link"))?;
        let source = versions
            .iter()
            .find(|version| version.id() == source_id)
            .ok_or_else(|| AppError::database("rollback source version is missing"))?;
        if source.version_number() != request.target_version_number {
            return Err(AppError::conflict(
                "rollback idempotency key was already used with a different target version",
            ));
        }
        Ok(rollback_response(record, source.version_number()))
    }

    async fn scope_checked(&self, ctx: &ServiceContext, agent_id: AgentId) -> Result<Agent> {
        let tenant_id = ctx.require_tenant()?;
        let organization_id = ctx.require_organization()?;
        self.agents
            .get(agent_id)
            .await?
            .filter(|a| a.tenant_id == tenant_id && a.organization_id == organization_id)
            .ok_or_else(|| AppError::not_found("agent", agent_id.to_string()))
    }

    async fn project_in_scope(
        &self,
        ctx: &ServiceContext,
        project: ProjectId,
    ) -> Result<mas_domain::Project> {
        let tenant_id = ctx.require_tenant()?;
        let organization_id = ctx.require_organization()?;
        self.projects
            .get(project)
            .await?
            .filter(|p| p.tenant_id == tenant_id && p.organization_id == organization_id)
            .ok_or_else(|| AppError::not_found("project", project.to_string()))
    }
}

fn authenticated_user(ctx: &ServiceContext) -> Result<UserId> {
    if ctx.actor.kind != mas_domain::AuditActorKind::User {
        return Err(AppError::forbidden(
            "agent rollback requires an authenticated user principal",
        ));
    }
    let user = UserId::parse_str(&ctx.actor.id)
        .map_err(|_| AppError::forbidden("authenticated user id is invalid"))?;
    if user.is_nil() {
        return Err(AppError::forbidden("authenticated user id is invalid"));
    }
    Ok(user)
}

fn rollback_response(record: &RollbackRecord, source_version: u64) -> RollbackResponseDto {
    RollbackResponseDto {
        rollback_record_id: record.id,
        resource_kind: record.resource_kind.to_string(),
        resource_id: record.resource_id.to_string(),
        from_version: record.from_version,
        to_version: record.to_version,
        source_version,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::InMemoryAuditSink;
    use crate::stores::InMemoryServices;
    use crate::tenancy_service::TenancyService;
    use mas_common::enums::Environment;
    use mas_common::ids::{OrganizationId, TenantId};
    use mas_common::timestamps::Timestamp;
    use mas_domain::IsolationMode;
    use std::sync::Arc;

    struct Fixture {
        service: AgentService,
        tenant_ctx: ServiceContext,
        project: ProjectId,
        audit: Arc<InMemoryAuditSink>,
        publisher: UserId,
    }

    fn checksum(tag: &str) -> String {
        format!("{:0>64}", tag)
    }

    async fn fixture() -> Fixture {
        let now = Timestamp::from_unix_seconds(1_700_000_000).expect("ts");
        let store = Arc::new(InMemoryServices::new());
        let audit = Arc::new(InMemoryAuditSink::new());
        let tenancy = TenancyService::new(
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(audit.clone()),
            Box::new(crate::control_plane::build_in_memory_control_plane().entitlement_store()),
        );
        let service = AgentService::new(
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(audit.clone()),
        )
        .with_rollback_store(Box::new(store.clone()));
        let system = ServiceContext::system("setup", &now).expect("ctx");
        let organization = tenancy
            .register_organization(&system, "Acme Ltd.", "Acme", "acme-agents")
            .await
            .expect("org");
        let org_scoped = system.clone().with_scope(TenantId::new(), organization.id);
        let tenant = tenancy
            .register_tenant(
                &org_scoped,
                "Prod",
                "prod-agents",
                Environment::Production,
                IsolationMode::SharedRls,
            )
            .await
            .expect("tenant");
        let tenant_ctx = system.clone().with_scope(tenant.id, organization.id);
        let project = tenancy
            .register_project(&tenant_ctx, "Core", "core-agents")
            .await
            .expect("project");
        let publisher = UserId::new();
        Fixture {
            service,
            tenant_ctx,
            project: project.id,
            audit,
            publisher,
        }
    }

    #[tokio::test]
    async fn rollback_publishes_a_forward_version_and_replays_idempotently() {
        let fixture = fixture().await;
        let agent = fixture
            .service
            .register(
                &fixture.tenant_ctx,
                fixture.project,
                "Rollback target",
                "rollback-target",
                AgentKind::Standard,
            )
            .await
            .expect("register");
        fixture
            .service
            .publish_version(
                &fixture.tenant_ctx,
                agent.id,
                &checksum("a1"),
                serde_json::json!({"model": "one"}),
                fixture.publisher,
            )
            .await
            .expect("first version");
        fixture
            .service
            .publish_version(
                &fixture.tenant_ctx,
                agent.id,
                &checksum("b2"),
                serde_json::json!({"model": "two"}),
                fixture.publisher,
            )
            .await
            .expect("second version");
        let user_ctx = ServiceContext::new(
            mas_domain::AuditActor::new(
                mas_domain::AuditActorKind::User,
                fixture.publisher.to_string(),
            )
            .expect("user actor"),
            fixture.tenant_ctx.tenant_id,
            fixture.tenant_ctx.organization_id,
            "agent-rollback-test",
            &Timestamp::now(),
        )
        .expect("scoped context");
        let request = RollbackRequest {
            target_version_number: 1,
            reason: "revert a regression".to_owned(),
            idempotency_key: "rollback-agent-1".to_owned(),
        };
        let first = fixture
            .service
            .rollback_version(&user_ctx, agent.id, &request)
            .await
            .expect("rollback");
        assert_eq!(first.from_version, 2);
        assert_eq!(first.to_version, 3);
        assert_eq!(first.source_version, 1);
        let replay = fixture
            .service
            .rollback_version(&user_ctx, agent.id, &request)
            .await
            .expect("idempotent replay");
        assert_eq!(replay.rollback_record_id, first.rollback_record_id);
        assert_eq!(
            fixture
                .service
                .get(&user_ctx, agent.id)
                .await
                .expect("agent")
                .current_version,
            Some(3)
        );
        let conflicting = RollbackRequest {
            target_version_number: 2,
            ..request
        };
        assert!(fixture
            .service
            .rollback_version(&user_ctx, agent.id, &conflicting)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn register_enforces_project_scope_and_slug_uniqueness() {
        let fixture = fixture().await;
        let ctx = &fixture.tenant_ctx;

        let agent = fixture
            .service
            .register(
                ctx,
                fixture.project,
                "Copilot",
                "copilot",
                AgentKind::Standard,
            )
            .await
            .expect("register");
        assert_eq!(agent.name, "Copilot");
        assert!(
            fixture
                .service
                .register(
                    ctx,
                    fixture.project,
                    "Copilot 2",
                    "copilot",
                    AgentKind::Standard
                )
                .await
                .is_err(),
            "per-project slug uniqueness"
        );

        // A foreign tenant sees nothing — not even the project or the agent.
        let foreign_ctx = ServiceContext::system(
            "foreign",
            &Timestamp::from_unix_seconds(1_700_000_000).expect("ts"),
        )
        .expect("ctx")
        .with_scope(mas_common::ids::TenantId::new(), OrganizationId::new());
        assert!(
            fixture.service.get(&foreign_ctx, agent.id).await.is_err(),
            "existence hidden"
        );
        assert!(fixture
            .service
            .list(&foreign_ctx, fixture.project)
            .await
            .is_err());

        // Ghost project rejects registration.
        assert!(
            fixture
                .service
                .register(ctx, ProjectId::new(), "Ghost", "ghost", AgentKind::Standard)
                .await
                .is_err(),
            "project must exist in scope"
        );
        assert!(!fixture.audit.is_empty(), "registration audited");
    }

    #[tokio::test]
    async fn versions_are_monotonic_immutable_and_audited() {
        let fixture = fixture().await;
        let ctx = &fixture.tenant_ctx;
        let agent = fixture
            .service
            .register(
                ctx,
                fixture.project,
                "Planner",
                "planner",
                AgentKind::Supervisor,
            )
            .await
            .expect("register");

        let first = fixture
            .service
            .publish_version(
                ctx,
                agent.id,
                &checksum("a1"),
                serde_json::json!({"model_ref": "gpt-x", "temperature": 0.2}),
                fixture.publisher,
            )
            .await
            .expect("v1");
        let second = fixture
            .service
            .publish_version(
                ctx,
                agent.id,
                &checksum("b2"),
                serde_json::json!({"model_ref": "gpt-y", "temperature": 0.1}),
                fixture.publisher,
            )
            .await
            .expect("v2");
        assert_eq!(first.version_number(), 1);
        assert_eq!(second.version_number(), 2);
        assert!(second.checksum_matches(&checksum("b2")));

        let listed = fixture
            .service
            .list_versions(ctx, agent.id)
            .await
            .expect("list");
        assert_eq!(listed[0].version_number(), 2, "newest first");
        let upgraded = fixture.service.get(ctx, agent.id).await.expect("get");
        assert_eq!(upgraded.current_version, Some(2));

        // Invalid checksums never get near the monotonic counter.
        assert!(fixture
            .service
            .publish_version(
                ctx,
                agent.id,
                "not-hex!",
                serde_json::json!({}),
                fixture.publisher
            )
            .await
            .is_err());
        let still = fixture.service.get(ctx, agent.id).await.expect("get");
        assert_eq!(
            still.current_version,
            Some(2),
            "failed publish does not advance"
        );

        let audited = fixture.audit.events();
        assert!(
            audited.iter().any(|e| e.action == "agent.publish_version"),
            "publishes are audited"
        );
    }
}
