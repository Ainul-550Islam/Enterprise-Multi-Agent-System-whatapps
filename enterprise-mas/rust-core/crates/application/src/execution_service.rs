//! PURPOSE:
//! Execution lifecycle use-cases — submission, transitions, cancellation and
//! reads — expressed exclusively through the canonical durable port.
//!
//! This service is the **only** entry point for execution lifecycle in the
//! platform: HTTP (`api`), gRPC (`api`) and the scheduler all arrive here, and
//! every one of them takes the same atomic path. There is deliberately no
//! second code path, no "lightweight" submission and no fallback store — a
//! second path would be a second lifecycle, and a second lifecycle is how a
//! system ends up reporting success for work that was never durably recorded.
//!
//! Layer / ownership
//! -----------------
//! `mas-application`. It sequences validation, then delegates every state
//! change to [`DurableExecutionPort`]. It owns no SQL, no broker and no
//! transaction: the port owns the transaction, and this service owns the
//! ordering (validate → fingerprint → atomic write → audit).
//!
//! Production responsibility
//! -------------------------
//! * **Atomic submission.** Validate → verify idempotency → create execution
//!   → create task → create initial attempt (when required) → audit → outbox →
//!   commit, as **one** transaction. Nothing is reported successful before the
//!   commit lands.
//! * **Durable idempotency.** Same key + same fingerprint replays the original
//!   execution; same key + different fingerprint is a deterministic `409`.
//! * **Durable cancellation.** Requests operate on persisted state, are
//!   idempotent, and never reopen a completed execution.
//! * **A single audit history.** Audit rows are derived from the transitions
//!   the durable unit reports, so there is exactly one audit record per state
//!   change regardless of which transport triggered it.
//!
//! What this module must NOT do
//! ----------------------------
//! * It must NOT write to an execution or task store directly. All writes go
//!   through the durable port.
//! * It must NOT emit events by any route other than the outbox rows the
//!   durable unit returns.
//! * It must NOT perform a SELECT-then-INSERT for idempotency.

use mas_common::constants;
use mas_common::error::AppError;
use mas_common::ids::{AgentId, ExecutionId, ProjectId, WorkflowId};
use mas_common::result::Result;
use mas_contracts::execution::ExecutionResponse;
use mas_domain::entitlement::FeatureKey;
use mas_domain::quota::QuotaDimension;
use mas_domain::{AuditOutcome, Execution, Task, TaskAttempt, TransitionActor, TransitionFailure};
use std::sync::Arc;

use crate::audit::{self, AuditSinkPort};
use crate::context::ServiceContext;
use crate::control_plane::ControlPlane;
use crate::dto;
use crate::durable::{
    CancellationReceipt, CancellationRequest, DurableExecutionPort, ExecutionAction,
    ExecutionTransitionRequest, SubmissionIntent, SubmissionKind,
};
use crate::fingerprint::{FingerprintInput, RequestFingerprint};
use crate::stores::{AgentStorePort, ProjectStorePort, WorkflowStorePort};

/// Default page size for tenant execution listings.
const DEFAULT_LIST_LIMIT: usize = 200;

/// Result of a submission: either a freshly created execution or a replay
/// of a previously submitted one (idempotency hit).
#[derive(Debug, Clone)]
pub struct SubmissionOutcome {
    /// The execution (fresh or replayed).
    pub execution: Execution,
    /// `true` when the idempotency key matched an earlier submission.
    pub replayed: bool,
    /// The durable task created for a fresh submission.
    pub task: Option<Task>,
    /// The initial attempt, when submission created one.
    pub attempt: Option<TaskAttempt>,
    /// Every transition the atomic unit applied, with the actor that caused
    /// it. Exposed so callers (and tests) can see *who* moved the execution
    /// without re-reading the aggregate's drained log.
    pub transitions: Vec<mas_domain::StateTransition>,
}

/// Use-cases over executions.
#[derive(Debug)]
pub struct ExecutionService {
    projects: Box<dyn ProjectStorePort>,
    workflows: Box<dyn WorkflowStorePort>,
    agents: Box<dyn AgentStorePort>,
    audit: Box<dyn AuditSinkPort>,
    /// The canonical atomic path. Required, not optional: an optional durable
    /// port would mean a second, non-atomic lifecycle exists.
    durable: Arc<dyn DurableExecutionPort>,
    /// The canonical control plane. Required for the same reason `durable` is:
    /// an optional gate is a second path, and a second path is one in which
    /// entitlement, policy and quota are simply not enforced. There is no
    /// construction of this service that skips them.
    control_plane: Arc<ControlPlane>,
}

impl ExecutionService {
    /// Composes the service over the validation stores, the audit sink and
    /// the **required** durable execution port.
    pub fn new(
        projects: Box<dyn ProjectStorePort>,
        workflows: Box<dyn WorkflowStorePort>,
        agents: Box<dyn AgentStorePort>,
        audit: Box<dyn AuditSinkPort>,
        durable: Arc<dyn DurableExecutionPort>,
        control_plane: Arc<ControlPlane>,
    ) -> Self {
        Self {
            projects,
            workflows,
            agents,
            audit,
            durable,
            control_plane,
        }
    }

    /// The durable port this service writes through (exposed so composition
    /// roots can share one instance with the worker instead of building two).
    #[must_use]
    pub fn durable(&self) -> &Arc<dyn DurableExecutionPort> {
        &self.durable
    }

    /// The control plane this service enforces.
    ///
    /// Exposed so the worker can settle the reservation this service took: the
    /// two halves of `check → reserve → execute → settle` must talk to the same
    /// control plane, or a settlement would refund against the wrong counter.
    #[must_use]
    pub fn control_plane(&self) -> &Arc<ControlPlane> {
        &self.control_plane
    }

    /// Idempotent, atomic submission.
    ///
    /// Referenced resources (project, workflow and/or agent) must exist *in
    /// scope* before anything is written. On an idempotency hit with an
    /// identical fingerprint the original execution is replayed; with a
    /// different fingerprint the call fails with `Conflict` — it never
    /// overwrites and never silently returns someone else's result.
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
        //    tenants/organizations). Nothing is written yet. ───────────────
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

        // ── Fingerprint, computed before the transaction opens. ───────────
        let operation = if agent.is_some() {
            "agent.run"
        } else {
            "execution.run"
        };
        let fingerprint = RequestFingerprint::submission(&FingerprintInput {
            tenant_id,
            organization_id,
            project_id: project,
            workflow_id: workflow,
            agent_id: agent,
            operation,
            input: &input,
            priority: mas_common::enums::TaskPriority::Normal,
            deadline: None,
        })?;

        // ── Canonical gate: Entitlement → Policy → Quota reservation. ──────
        //
        // This runs here, in the one submission path, because the order is a
        // correctness property rather than a preference. Authentication and
        // authorization already happened upstream (the context carries the
        // authenticated actor); what remains is the part that decides whether
        // this tenant may run this work at all.
        //
        // Every branch of this gate fails closed. A lookup that errors is not
        // reshaped into a grant, and a policy that cannot be evaluated is not
        // treated as `allow`.
        //
        // This step *consumes* one execution rather than reserving it.
        //
        // A reservation is for a dimension whose final amount is not known
        // when the work starts (tokens, or a concurrency slot held for the
        // duration). An execution is a count, and the count is knowable right
        // here: exactly one. Reserving instead would hold the unit until the
        // lease lapsed, so a tenant on a daily ceiling of 100 could only
        // submit a handful per day — the counter would be measuring the lease
        // rather than the work.
        //
        // What the work actually consumes (tokens, tool calls, duration) is
        // metered separately, at settlement, from what the worker observed.
        let decision = self
            .control_plane
            .authorize_execution(
                tenant_id,
                Some(organization_id),
                Some(project),
                ctx.actor.id.as_str(),
                // A workflow run is a gated feature; a bare agent run is not.
                workflow.map(|_| &FeatureKey::WORKFLOWS),
                operation,
                "execution",
                QuotaDimension::Executions,
                1,
                &serde_json::json!({"project_id": project.to_string()}),
                None,
                &ctx.requested_at,
            )
            .await?;
        let _ = &decision;

        let intent = SubmissionIntent {
            tenant_id,
            organization_id,
            project_id: project,
            workflow_id: workflow,
            agent_id: agent,
            input,
            idempotency_key: idempotency_key.map(str::to_owned),
            fingerprint,
            operation: operation.to_owned(),
            priority: mas_common::enums::TaskPriority::Normal,
            max_attempts: constants::DEFAULT_MAX_RETRIES + 1,
            deadline: None,
            correlation_id: ctx.correlation_id.clone(),
            created_by: ctx.actor.id.clone(),
            actor: actor_for(ctx)?,
            // The task is queued, not started: a worker claims it and creates
            // the attempt, which is what makes the attempt count durable
            // rather than an artefact of broker redelivery.
            create_initial_attempt: false,
            now: ctx.requested_at,
        };
        intent.validate()?;

        // ── One transaction: idempotency + execution + task + outbox. ─────
        //
        // Nothing here compensates a quota charge, because there is nothing to
        // compensate: the charge above is a count of executions submitted, and
        // on any failure below no execution exists. An execution that exists
        // and then consumes resources is metered at settlement from what the
        // worker actually observed — not from what was counted here.
        let receipt = self.durable.submit(intent).await?;

        // Audit is derived from the transitions the unit reported, so there is
        // exactly one audit record per state change no matter which transport
        // triggered the call. A replay is not a state change, so it is not
        // audited here — it is observable through its outbox event and the
        // idempotency metric instead.
        if receipt.kind == SubmissionKind::Created {
            self.audit_transitions(ctx, &receipt.transitions, "execution")
                .await?;
        }

        tracing::info!(
            execution = %receipt.execution.id,
            task = %receipt.task.id,
            outcome = receipt.kind.metric_token(),
            "execution submission committed"
        );

        Ok(SubmissionOutcome {
            execution: receipt.execution,
            replayed: receipt.kind == SubmissionKind::Replayed,
            task: Some(receipt.task),
            attempt: receipt.attempt,
            transitions: receipt.transitions,
        })
    }

    /// Starts a `Pending`/`Queued` execution.
    pub async fn start(
        &self,
        ctx: &ServiceContext,
        execution_id: ExecutionId,
    ) -> Result<Execution> {
        self.transition(ctx, execution_id, ExecutionAction::Start)
            .await
    }

    /// Pauses a `Running` execution.
    pub async fn pause(
        &self,
        ctx: &ServiceContext,
        execution_id: ExecutionId,
    ) -> Result<Execution> {
        self.transition(ctx, execution_id, ExecutionAction::Pause)
            .await
    }

    /// Resumes a `Paused` execution.
    pub async fn resume(
        &self,
        ctx: &ServiceContext,
        execution_id: ExecutionId,
    ) -> Result<Execution> {
        self.transition(ctx, execution_id, ExecutionAction::Resume)
            .await
    }

    /// Completes with an optional output (terminal).
    pub async fn complete(
        &self,
        ctx: &ServiceContext,
        execution_id: ExecutionId,
        output: Option<serde_json::Value>,
    ) -> Result<Execution> {
        let receipt = self
            .transition_action(ctx, execution_id, ExecutionAction::Complete)
            .await?;
        let mut execution = receipt.execution;
        execution.output = output;
        Ok(execution)
    }

    /// Fails with a classified failure (terminal).
    ///
    /// Callers must classify: the retry decision and the emitted event both
    /// key off [`TransitionFailure::retryable`], never off the message text.
    pub async fn fail(
        &self,
        ctx: &ServiceContext,
        execution_id: ExecutionId,
        failure: TransitionFailure,
    ) -> Result<Execution> {
        self.transition(ctx, execution_id, ExecutionAction::Fail { failure })
            .await
    }

    /// Cancels an execution.
    ///
    /// * Not yet running → cancelled immediately, along with its task.
    /// * Running → the cooperative flag is set; the runtime settles it as
    ///   cancelled at its next safe point.
    /// * Already cancelled → an idempotent no-op.
    /// * Completed/failed → rejected: cancellation never reopens a finished
    ///   run.
    pub async fn cancel(
        &self,
        ctx: &ServiceContext,
        execution_id: ExecutionId,
    ) -> Result<Execution> {
        let receipt = self.cancel_inner(ctx, execution_id).await?;
        Ok(receipt.execution)
    }

    /// Requests cancellation (idempotent signal consumed by the runtime).
    ///
    /// This is the same durable operation as [`Self::cancel`] — there is one
    /// cancellation path, and it decides between signalling and applying based
    /// on the persisted state.
    pub async fn request_cancellation(
        &self,
        ctx: &ServiceContext,
        execution_id: ExecutionId,
    ) -> Result<Execution> {
        let receipt = self.cancel_inner(ctx, execution_id).await?;
        Ok(receipt.execution)
    }

    /// The full cancellation receipt, for callers that need to distinguish
    /// "cancelled now" from "cooperative stop signalled".
    pub async fn cancel_detailed(
        &self,
        ctx: &ServiceContext,
        execution_id: ExecutionId,
    ) -> Result<CancellationReceipt> {
        self.cancel_inner(ctx, execution_id).await
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
        self.durable
            .list_executions(tenant_id, DEFAULT_LIST_LIMIT)
            .await
    }

    // -- internals -----------------------------------------------------------

    async fn transition(
        &self,
        ctx: &ServiceContext,
        execution_id: ExecutionId,
        action: ExecutionAction,
    ) -> Result<Execution> {
        Ok(self
            .transition_action(ctx, execution_id, action)
            .await?
            .execution)
    }

    async fn transition_action(
        &self,
        ctx: &ServiceContext,
        execution_id: ExecutionId,
        action: ExecutionAction,
    ) -> Result<crate::durable::ExecutionTransitionReceipt> {
        let receipt = self
            .durable
            .transition(ExecutionTransitionRequest {
                tenant_id: ctx.require_tenant()?,
                execution_id,
                action,
                actor: actor_for(ctx)?,
                correlation_id: ctx.correlation_id.clone(),
                now: ctx.requested_at,
            })
            .await?;
        let transitions = vec![receipt.transition.clone()];
        self.audit_transitions(ctx, &transitions, "execution")
            .await?;
        Ok(receipt)
    }

    async fn cancel_inner(
        &self,
        ctx: &ServiceContext,
        execution_id: ExecutionId,
    ) -> Result<CancellationReceipt> {
        let receipt = self
            .durable
            .cancel(CancellationRequest {
                tenant_id: ctx.require_tenant()?,
                execution_id,
                actor: actor_for(ctx)?,
                correlation_id: ctx.correlation_id.clone(),
                now: ctx.requested_at,
            })
            .await?;
        self.audit_transitions(ctx, &receipt.transitions, "execution")
            .await?;
        Ok(receipt)
    }

    /// Derives audit rows from the transitions a durable unit reported.
    ///
    /// Single-path by construction: the transitions come from the aggregate's
    /// own log, so no caller can add a parallel audit history by calling a
    /// different service method.
    async fn audit_transitions(
        &self,
        ctx: &ServiceContext,
        transitions: &[mas_domain::StateTransition],
        resource_type: &str,
    ) -> Result<()> {
        for transition in transitions {
            let outcome = if transition.is_failure() {
                AuditOutcome::Failure
            } else {
                AuditOutcome::Success
            };
            audit::record_mutation(
                ctx,
                self.audit.as_ref(),
                &format!("{}.{}", resource_type, transition.to),
                resource_type,
                Some(transition.aggregate_id.clone()),
                outcome,
            )
            .await?;
        }
        Ok(())
    }

    async fn scope_checked(
        &self,
        ctx: &ServiceContext,
        execution_id: ExecutionId,
    ) -> Result<Execution> {
        let tenant_id = ctx.require_tenant()?;
        self.durable
            .load_execution(tenant_id, execution_id)
            .await?
            .filter(|execution| match ctx.organization_id {
                Some(organization) => execution.organization_id == organization,
                None => true,
            })
            .ok_or_else(|| AppError::not_found("execution", execution_id.to_string()))
    }
}

/// Maps a [`ServiceContext`] principal onto the actor recorded on transitions.
///
/// A tenant-scoped principal is recorded as a principal *with its tenant*, so
/// an operator can see which tenant's actor caused the move; an untenanted
/// service principal is recorded as a system actor rather than being given a
/// tenant it never proved.
fn actor_for(ctx: &ServiceContext) -> Result<TransitionActor> {
    let raw = ctx.actor.id.trim();
    let id: String = if raw.is_empty() {
        "unknown".to_owned()
    } else {
        raw.chars().take(64).collect()
    };
    match ctx.tenant_id {
        Some(tenant) => TransitionActor::principal(id, tenant),
        None => TransitionActor::system(id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::InMemoryAuditSink;
    use crate::durable_memory::InMemoryDurableExecution;
    use crate::stores::InMemoryServices;
    use mas_common::enums::ExecutionStatus;
    use mas_common::ids::{OrganizationId, TenantId};
    use mas_common::timestamps::Timestamp;
    use mas_domain::subscription::{Subscription, SubscriptionState};
    use mas_domain::value_objects::Slug;
    use mas_domain::{Agent, AgentKind, Project, Workflow};
    use std::sync::Arc;

    struct Fixture {
        service: ExecutionService,
        tenant_ctx: ServiceContext,
        foreign_ctx: ServiceContext,
        agent: AgentId,
        workflow: WorkflowId,
        project: ProjectId,
        audit: Arc<InMemoryAuditSink>,
        durable: Arc<InMemoryDurableExecution>,
    }

    async fn fixture() -> Fixture {
        let now = Timestamp::from_unix_seconds(1_700_000_000).expect("ts");
        let store = Arc::new(InMemoryServices::new());
        let audit = Arc::new(InMemoryAuditSink::new());
        let durable = Arc::new(InMemoryDurableExecution::new());
        let tenant = TenantId::new();
        let organization = OrganizationId::new();

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

        // A control plane, and a subscription for the fixture tenant. Without
        // the subscription every submission is refused with 402 — which is the
        // gate being correct, not the fixture being awkward: a tenant that has
        // not bought anything cannot run anything.
        let control_plane = Arc::new(crate::control_plane::build_in_memory_control_plane());
        control_plane
            .entitlement_store()
            .upsert_subscription(
                &Subscription::new(
                    tenant,
                    crate::control_plane::DEFAULT_PLAN_CODE,
                    SubscriptionState::Active,
                    None,
                    None,
                )
                .expect("subscription"),
            )
            .await
            .expect("seed subscription");

        let service = ExecutionService::new(
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(audit.clone()),
            durable.clone(),
            Arc::clone(&control_plane),
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
            audit,
            durable,
        }
    }

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
        assert!(first.task.is_some(), "submission creates a durable task");

        let second = fixture
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
            .expect("replay");
        assert!(second.replayed, "same key + same request replays");
        assert_eq!(second.execution.id, first.execution.id);
        assert_eq!(second.execution.input, serde_json::json!({"n": 1}));
        assert_eq!(
            fixture.service.list(ctx).await.expect("list").len(),
            1,
            "no duplicate rows"
        );

        // Existence-hidden resources fail *before* anything is written.
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
    async fn same_key_different_request_is_a_conflict_not_a_silent_replay() {
        let fixture = fixture().await;
        let ctx = &fixture.tenant_ctx;

        fixture
            .service
            .submit(
                ctx,
                fixture.project,
                Some(fixture.workflow),
                None,
                serde_json::json!({"prompt": "first"}),
                Some("run-1"),
            )
            .await
            .expect("first");

        let error = fixture
            .service
            .submit(
                ctx,
                fixture.project,
                Some(fixture.workflow),
                None,
                serde_json::json!({"prompt": "different"}),
                Some("run-1"),
            )
            .await
            .expect_err("a conflicting fingerprint must be rejected");
        assert!(matches!(error, AppError::Conflict(_)), "{error:?}");
        assert_eq!(
            fixture.service.list(ctx).await.expect("list").len(),
            1,
            "the rejected submission created nothing"
        );
    }

    #[tokio::test]
    async fn submission_commits_execution_task_audit_and_outbox_together() {
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

        // The execution and its task were committed together.
        let task = outcome.task.expect("task");
        assert_eq!(task.execution_id, Some(outcome.execution.id));
        assert_eq!(task.status, mas_common::enums::TaskStatus::Queued);
        assert_eq!(outcome.execution.status, ExecutionStatus::Queued);

        // Outbox rows committed with them.
        let outbox = fixture.durable.outbox();
        assert!(outbox
            .iter()
            .any(|record| record.envelope.event_type == "execution.submitted"));
        assert!(outbox
            .iter()
            .any(|record| record.envelope.event_type == "task.created"));

        // Audit was written once, from the transitions the unit reported.
        assert!(!fixture.audit.is_empty());
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

        // Illegal first: pausing a queued execution (it is not running).
        assert!(fixture.service.pause(ctx, execution_id).await.is_err());

        let started = fixture
            .service
            .start(ctx, execution_id)
            .await
            .expect("start");
        assert_eq!(started.status, ExecutionStatus::Running);
        let paused = fixture
            .service
            .pause(ctx, execution_id)
            .await
            .expect("pause");
        assert_eq!(paused.status, ExecutionStatus::Paused);
        let resumed = fixture
            .service
            .resume(ctx, execution_id)
            .await
            .expect("resume");
        assert_eq!(resumed.status, ExecutionStatus::Running);

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

    #[tokio::test]
    async fn cancellation_is_durable_idempotent_and_never_reopens() {
        let fixture = fixture().await;
        let ctx = &fixture.tenant_ctx;

        // A queued execution cancels immediately.
        let queued = fixture
            .service
            .submit(
                ctx,
                fixture.project,
                Some(fixture.workflow),
                None,
                serde_json::json!({"a": 1}),
                None,
            )
            .await
            .expect("submit");
        let cancelled = fixture
            .service
            .cancel(ctx, queued.execution.id)
            .await
            .expect("cancel");
        assert_eq!(cancelled.status, ExecutionStatus::Cancelled);
        assert_eq!(
            fixture
                .service
                .cancel(ctx, queued.execution.id)
                .await
                .expect("idempotent")
                .status,
            ExecutionStatus::Cancelled
        );

        // A completed execution is never reopened.
        let running = fixture
            .service
            .submit(
                ctx,
                fixture.project,
                Some(fixture.workflow),
                None,
                serde_json::json!({"b": 2}),
                None,
            )
            .await
            .expect("submit");
        fixture
            .service
            .start(ctx, running.execution.id)
            .await
            .expect("start");
        let completed = fixture
            .service
            .complete(ctx, running.execution.id, None)
            .await
            .expect("complete");
        assert_eq!(completed.status, ExecutionStatus::Completed);
        assert!(fixture
            .service
            .cancel(ctx, running.execution.id)
            .await
            .is_err());
        assert_eq!(
            fixture
                .service
                .get(ctx, running.execution.id)
                .await
                .expect("get")
                .status,
            ExecutionStatus::Completed
        );
    }

    #[tokio::test]
    async fn failure_is_classified_not_just_described() {
        let fixture = fixture().await;
        let ctx = &fixture.tenant_ctx;
        let outcome = fixture
            .service
            .submit(
                ctx,
                fixture.project,
                Some(fixture.workflow),
                None,
                serde_json::json!({"c": 3}),
                None,
            )
            .await
            .expect("submit");
        fixture
            .service
            .start(ctx, outcome.execution.id)
            .await
            .expect("start");

        let failed = fixture
            .service
            .fail(
                ctx,
                outcome.execution.id,
                TransitionFailure::transient("rate_limited", "429").expect("failure"),
            )
            .await
            .expect("fail");
        assert_eq!(failed.status, ExecutionStatus::Failed);
        assert!(failed.failure.is_some());
        let outbox = fixture.durable.outbox();
        let failure_event = outbox
            .iter()
            .find(|record| record.envelope.event_type == "execution.failed")
            .expect("a failure event was emitted");
        assert_eq!(failure_event.envelope.payload["error_code"], "rate_limited");
        assert_eq!(failure_event.envelope.payload["error_class"], "transient");
    }

    #[tokio::test]
    async fn audit_records_one_row_per_transition_from_one_path() {
        let fixture = fixture().await;
        let ctx = &fixture.tenant_ctx;
        let before = fixture.audit.len();

        let outcome = fixture
            .service
            .submit(
                ctx,
                fixture.project,
                Some(fixture.workflow),
                None,
                serde_json::json!({"d": 4}),
                None,
            )
            .await
            .expect("submit");
        // Submission applies two transitions: execution → queued, task → queued.
        let after_submit = fixture.audit.len();
        assert_eq!(after_submit - before, 2);

        fixture
            .service
            .start(ctx, outcome.execution.id)
            .await
            .expect("start");
        assert_eq!(
            fixture.audit.len() - after_submit,
            1,
            "one audit row per transition, from exactly one path"
        );
    }

    #[tokio::test]
    async fn actor_is_recorded_from_the_service_context() {
        let fixture = fixture().await;
        // `ServiceContext::system` is deliberately the *system* principal, so
        // its actor id is "system" — a named service context is what proves
        // the caller's identity travels onto the durable transition.
        let now = Timestamp::from_unix_seconds(1_700_000_000).expect("ts");
        let ctx = ServiceContext::for_service("svc-1", "corr-actor", &now)
            .expect("ctx")
            .with_scope(
                fixture.tenant_ctx.tenant_id.expect("tenant"),
                fixture.tenant_ctx.organization_id.expect("organization"),
            );
        let outcome = fixture
            .service
            .submit(
                &ctx,
                fixture.project,
                Some(fixture.workflow),
                None,
                serde_json::json!({"e": 5}),
                None,
            )
            .await
            .expect("submit");
        assert!(!outcome.transitions.is_empty());
        for transition in &outcome.transitions {
            assert_eq!(
                transition.actor.id, "svc-1",
                "every transition carries the acting principal"
            );
            assert!(
                transition.actor.tenant_id.is_some(),
                "a tenant-scoped principal is recorded with its tenant"
            );
            assert!(transition.version >= 2, "each transition bumps the version");
            assert!(!transition.from.is_empty() && !transition.to.is_empty());
        }
    }

    #[tokio::test]
    async fn untenanted_contexts_are_refused_for_tenant_work() {
        let fixture = fixture().await;
        let now = Timestamp::from_unix_seconds(1_700_000_000).expect("ts");
        let ctx = ServiceContext::system("svc-x", &now).expect("ctx");
        let error = fixture
            .service
            .submit(
                &ctx,
                fixture.project,
                Some(fixture.workflow),
                None,
                serde_json::json!({}),
                None,
            )
            .await
            .expect_err("tenant-scoped work needs a tenant");
        assert!(matches!(error, AppError::Forbidden(_)), "{error:?}");
    }
}
