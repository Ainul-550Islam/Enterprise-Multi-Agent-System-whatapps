//! Execution lifecycle use-cases: idempotent submission (resource
//! existence pre-computed *before* creation, so idempotency collisions
//! only ever replay — never fail), legal state-machine transitions, and
//! reads that hide foreign tenants entirely.

use mas_common::error::AppError;
use mas_common::ids::{AgentId, ExecutionId, ProjectId, WorkflowId};
use mas_common::result::Result;
use mas_contracts::execution::ExecutionResponse;
use mas_domain::{AuditOutcome, Execution};

use crate::audit::{self, AuditSinkPort};
use crate::context::ServiceContext;
use crate::dto;
use crate::stores::{AgentStorePort, ExecutionStorePort, ProjectStorePort, WorkflowStorePort};

/// Result of a submission: either a freshly created execution or a replay
/// of a previously submitted one (idempotency hit).
#[derive(Debug, Clone)]
pub struct SubmissionOutcome {
    /// The execution (fresh or replayed).
    pub execution: Execution,
    /// `true` when the idempotency key matched an earlier submission.
    pub replayed: bool,
}

/// Use-cases over executions.
#[derive(Debug)]
pub struct ExecutionService {
    executions: Box<dyn ExecutionStorePort>,
    projects: Box<dyn ProjectStorePort>,
    workflows: Box<dyn WorkflowStorePort>,
    agents: Box<dyn AgentStorePort>,
    audit: Box<dyn AuditSinkPort>,
}

impl ExecutionService {
    /// Composes the service over store ports + the audit sink.
    pub fn new(
        executions: Box<dyn ExecutionStorePort>,
        projects: Box<dyn ProjectStorePort>,
        workflows: Box<dyn WorkflowStorePort>,
        agents: Box<dyn AgentStorePort>,
        audit: Box<dyn AuditSinkPort>,
    ) -> Self {
        Self {
            executions,
            projects,
            workflows,
            agents,
            audit,
        }
    }

    /// Idempotent submission: all referenced resources (project, workflow
    /// and/or agent) must exist *in scope* before the execution is
    /// created. On an idempotency hit the previously created execution is
    /// replayed as-is (`replayed = true`) — the original input wins and no
    /// second domain event is emitted.
    #[allow(clippy::too_many_arguments)]
    pub async fn submit(
        &self,
        ctx: &ServiceContext,
        project: ProjectId,
        workflow: Option<WorkflowId>,
        agent: Option<AgentId>,
        input: serde_json::Value,
        idempotency_key: Option<&str>,
    ) -> Result<SubmissionOutcome> {
        let tenant_id = ctx.require_tenant()?;
        let organization_id = ctx.require_organization()?;

        // ── Resource existence pre-checks (existence hidden across
        //    tenants/organizations). ───────────────────────────────────
        self.projects
            .get(project)
            .await?
            .filter(|p| p.tenant_id == tenant_id && p.organization_id == organization_id)
            .ok_or_else(|| AppError::not_found("project", project.to_string()))?;
        if let Some(workflow_id) = workflow {
            self.workflows
                .get(workflow_id)
                .await?
                .filter(|w| w.tenant_id == tenant_id && w.organization_id == organization_id)
                .ok_or_else(|| AppError::not_found("workflow", workflow_id.to_string()))?;
        }
        if let Some(agent_id) = agent {
            let agent = self
                .agents
                .get(agent_id)
                .await?
                .filter(|a| a.tenant_id == tenant_id && a.organization_id == organization_id)
                .ok_or_else(|| AppError::not_found("agent", agent_id.to_string()))?;
            if !agent.is_executable() {
                return Err(AppError::conflict(
                    "agent is not in an executable state (must be Active with a published version)",
                ));
            }
        }

        // ── Idempotency: replay original submission on key hit. ────────
        if let Some(key) = idempotency_key {
            if let Some(existing) = self.executions.idempotency_get(tenant_id, key).await? {
                let execution = self.executions.get(existing).await?.ok_or_else(|| {
                    AppError::internal("idempotency reference to vanished execution")
                })?;
                tracing::info!(execution = %existing, "idempotency replay: returning original execution");
                return Ok(SubmissionOutcome {
                    execution,
                    replayed: true,
                });
            }
        }

        let execution = Execution::new(
            tenant_id,
            organization_id,
            project,
            workflow,
            agent,
            input,
            &ctx.correlation_id,
            &ctx.actor.id,
        )?;
        self.executions.save(&execution).await?;
        if let Some(key) = idempotency_key {
            self.executions
                .idempotency_put(tenant_id, key, execution.id)
                .await?;
        }
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "execution.submit",
            "execution",
            Some(execution.id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(SubmissionOutcome {
            execution,
            replayed: false,
        })
    }

    /// Starts a `Pending` execution (legal transition enforced by the
    /// domain state machine).
    pub async fn start(
        &self,
        ctx: &ServiceContext,
        execution_id: ExecutionId,
    ) -> Result<Execution> {
        self.transition(ctx, execution_id, Execution::start, "execution.start")
            .await
    }

    /// Pauses a `Running` execution.
    pub async fn pause(
        &self,
        ctx: &ServiceContext,
        execution_id: ExecutionId,
    ) -> Result<Execution> {
        self.transition(ctx, execution_id, Execution::pause, "execution.pause")
            .await
    }

    /// Resumes a `Paused` execution.
    pub async fn resume(
        &self,
        ctx: &ServiceContext,
        execution_id: ExecutionId,
    ) -> Result<Execution> {
        self.transition(ctx, execution_id, Execution::resume, "execution.resume")
            .await
    }

    /// Cancels (driven by the cancellation flag — the flag must have been
    /// requested first, mirroring the safe-stop loop).
    pub async fn cancel(
        &self,
        ctx: &ServiceContext,
        execution_id: ExecutionId,
    ) -> Result<Execution> {
        self.transition(ctx, execution_id, Execution::cancel, "execution.cancel")
            .await
    }

    /// Requests cancellation (idempotent signal consumed by the loop).
    pub async fn request_cancellation(
        &self,
        ctx: &ServiceContext,
        execution_id: ExecutionId,
    ) -> Result<Execution> {
        let mut execution = self.scope_checked(ctx, execution_id).await?;
        execution.request_cancellation()?;
        self.executions.save(&execution).await?;
        Ok(execution)
    }

    /// Completes with an optional output.
    pub async fn complete(
        &self,
        ctx: &ServiceContext,
        execution_id: ExecutionId,
        output: Option<serde_json::Value>,
    ) -> Result<Execution> {
        let mut execution = self.scope_checked(ctx, execution_id).await?;
        execution.complete(output)?;
        self.persist_and_audit(ctx, &execution, "execution.complete")
            .await?;
        Ok(execution)
    }

    /// Fails with a human/orchestrator-readable reason (truncated to 4096
    /// chars by the aggregate; never carry secrets in here).
    pub async fn fail(
        &self,
        ctx: &ServiceContext,
        execution_id: ExecutionId,
        reason: impl Into<String> + Send,
    ) -> Result<Execution> {
        let mut execution = self.scope_checked(ctx, execution_id).await?;
        execution.fail(reason)?;
        self.persist_and_audit(ctx, &execution, "execution.fail")
            .await?;
        Ok(execution)
    }

    /// Scope-checked fetch (existence hiding).
    pub async fn get(&self, ctx: &ServiceContext, execution_id: ExecutionId) -> Result<Execution> {
        self.scope_checked(ctx, execution_id).await
    }

    /// Fetch mapped to the wire DTO.
    pub async fn get_response(
        &self,
        ctx: &ServiceContext,
        execution_id: ExecutionId,
    ) -> Result<ExecutionResponse> {
        Ok(dto::execution_response(
            &self.scope_checked(ctx, execution_id).await?,
        ))
    }

    /// Lists executions the context's tenant may see.
    pub async fn list(&self, ctx: &ServiceContext) -> Result<Vec<Execution>> {
        let tenant_id = ctx.require_tenant()?;
        self.executions.list(tenant_id).await
    }

    async fn transition(
        &self,
        ctx: &ServiceContext,
        execution_id: ExecutionId,
        apply: fn(&mut Execution) -> Result<()>,
        action: &'static str,
    ) -> Result<Execution> {
        let mut execution = self.scope_checked(ctx, execution_id).await?;
        apply(&mut execution)?;
        self.persist_and_audit(ctx, &execution, action).await?;
        Ok(execution)
    }

    async fn persist_and_audit(
        &self,
        ctx: &ServiceContext,
        execution: &Execution,
        action: &'static str,
    ) -> Result<()> {
        self.executions.save(execution).await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            action,
            "execution",
            Some(execution.id.to_string()),
            AuditOutcome::Success,
        )
        .await
    }

    async fn scope_checked(
        &self,
        ctx: &ServiceContext,
        execution_id: ExecutionId,
    ) -> Result<Execution> {
        let tenant_id = ctx.require_tenant()?;
        let organization_id = ctx.require_organization()?;
        self.executions
            .get(execution_id)
            .await?
            .filter(|e| e.tenant_id == tenant_id && e.organization_id == organization_id)
            .ok_or_else(|| AppError::not_found("execution", execution_id.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::InMemoryAuditSink;
    use crate::stores::InMemoryServices;
    use mas_common::enums::ExecutionStatus;
    use mas_common::ids::{OrganizationId, TenantId};
    use mas_common::timestamps::Timestamp;

    struct Fixture {
        service: ExecutionService,
        tenant_ctx: ServiceContext,
        foreign_ctx: ServiceContext,
        agent: AgentId,
        workflow: WorkflowId,
        project: ProjectId,
    }

    async fn fixture() -> Fixture {
        let now = Timestamp::from_unix_seconds(1_700_000_000).expect("ts");
        let store = Arc::new(InMemoryServices::new());
        let audit = Arc::new(InMemoryAuditSink::new());
        let tenant = TenantId::new();
        let organization = OrganizationId::new();

        // Seed scope directly through the stores (application::register_*
        // exercises project creation elsewhere).
        use crate::stores::{AgentStorePort, ProjectStorePort, WorkflowStorePort};
        use mas_domain::value_objects::Slug;
        use mas_domain::{Agent, AgentKind, Project, Workflow};
        let project_row = Project::create(
            tenant,
            organization,
            "Core",
            Slug::new("core-exec").expect("slug"),
        )
        .expect("project");
        ProjectStorePort::save(&store, &project_row)
            .await
            .expect("seed project");
        let workflow_row =
            Workflow::create(tenant, organization, project_row.id, "main-flow").expect("workflow");
        WorkflowStorePort::save(&store, &workflow_row)
            .await
            .expect("seed workflow");
        let agent_row = Agent::create(
            tenant,
            organization,
            project_row.id,
            "Runner",
            Slug::new("runner-exec").expect("slug"),
            AgentKind::Standard,
        )
        .expect("agent");
        AgentStorePort::save(&store, &agent_row)
            .await
            .expect("seed agent");

        let service = ExecutionService::new(
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store),
            Box::new(audit),
        );
        let tenant_ctx = ServiceContext::system("svc-1", &now)
            .expect("ctx")
            .with_scope(tenant, organization);
        let foreign_ctx = ServiceContext::system("foreign", &now)
            .expect("ctx")
            .with_scope(TenantId::new(), OrganizationId::new());
        Fixture {
            service,
            tenant_ctx,
            foreign_ctx,
            agent: agent_row.id,
            workflow: workflow_row.id,
            project: project_row.id,
        }
    }

    use std::sync::Arc;

    #[tokio::test]
    async fn idempotent_submission_replays_exactly_once() {
        let fixture = fixture().await;
        let ctx = &fixture.tenant_ctx;

        // Draft agents are NOT executable — submission refuses them.
        assert!(
            fixture
                .service
                .submit(
                    ctx,
                    fixture.project,
                    None,
                    Some(fixture.agent),
                    serde_json::json!({"n": 1}),
                    Some("key-draft"),
                )
                .await
                .is_err(),
            "draft agent cannot run"
        );

        let first = fixture
            .service
            .submit(
                ctx,
                fixture.project,
                Some(fixture.workflow),
                None,
                serde_json::json!({"n": 1}),
                Some("run-1"),
            )
            .await
            .expect("first submit");
        assert!(!first.replayed);
        let second = fixture
            .service
            .submit(
                ctx,
                fixture.project,
                Some(fixture.workflow),
                None,
                serde_json::json!({"n": 999}),
                Some("run-1"),
            )
            .await
            .expect("replay");
        assert!(second.replayed, "same key replays");
        assert_eq!(
            second.execution.id, first.execution.id,
            "original execution returned"
        );
        assert_eq!(
            second.execution.input,
            serde_json::json!({"n": 1}),
            "original input wins"
        );
        assert_eq!(
            fixture.service.list(ctx).await.expect("list").len(),
            1,
            "no duplicate rows"
        );

        // Existence-hidden resources fail *before* idempotency handles them.
        assert!(fixture
            .service
            .submit(
                ctx,
                ProjectId::new(),
                Some(fixture.workflow),
                None,
                serde_json::json!(null),
                Some("k")
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn state_machine_transitions_and_hiding() {
        let fixture = fixture().await;
        let ctx = &fixture.tenant_ctx;
        let outcome = fixture
            .service
            .submit(
                ctx,
                fixture.project,
                Some(fixture.workflow),
                None,
                serde_json::json!({"job": "docker"}),
                None,
            )
            .await
            .expect("submit");
        let execution_id = outcome.execution.id;

        // Illegal first: completing a Pending execution (no start yet).
        assert!(fixture
            .service
            .complete(ctx, execution_id, None)
            .await
            .is_err());

        let started = fixture
            .service
            .start(ctx, execution_id)
            .await
            .expect("start");
        let paused = fixture
            .service
            .pause(ctx, execution_id)
            .await
            .expect("pause");
        assert!(matches!(paused.status, ExecutionStatus::Paused));
        let resumed = fixture
            .service
            .resume(ctx, execution_id)
            .await
            .expect("resume");
        assert!(matches!(resumed.status, ExecutionStatus::Running));

        fixture
            .service
            .request_cancellation(ctx, execution_id)
            .await
            .expect("flag");
        let cancelled = fixture
            .service
            .cancel(ctx, execution_id)
            .await
            .expect("cancel");
        assert!(cancelled.status.is_terminal());
        // Terminal executions do not transition further.
        assert!(fixture
            .service
            .complete(ctx, execution_id, None)
            .await
            .is_err());
        let _ = started;

        let response = fixture
            .service
            .get_response(ctx, execution_id)
            .await
            .expect("response");
        assert!(response.cancellation_requested);

        // Foreign tenant: NotFound, never "exists but forbidden".
        let err = fixture
            .service
            .get(&fixture.foreign_ctx, execution_id)
            .await
            .expect_err("hidden");
        assert!(matches!(err, AppError::NotFound { .. }));
        assert!(fixture
            .service
            .list(&fixture.foreign_ctx)
            .await
            .expect("list")
            .is_empty());
    }
}
