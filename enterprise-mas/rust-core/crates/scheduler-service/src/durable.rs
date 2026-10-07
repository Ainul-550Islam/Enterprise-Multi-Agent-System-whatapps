//! PURPOSE: The production scheduler dispatcher: due schedules become durable
//! executions and queued tasks through the real submission pipeline.
//!
//! Owning layer: `mas-scheduler-service`.
//!
//! Production responsibility: make a fired tick idempotent — the key is
//! derived from the schedule and the planned instant, so a replayed or
//! duplicated tick is answered by the pipeline's own idempotency rather than
//! creating a second run.
//!
//! It must NOT:
//!   • submit outside the canonical pipeline;
//!   • create a task directly instead of through a submission;
//!   • treat "the tick fired" as "the work ran".

//! Production dispatcher: due runs become durable pending executions +
//! queued task rows through the *real* application submit pipeline.
//!
//! Design:
//! * Idempotency: `sched:<schedule_id>:<planned_unix_ms>`; a replayed tick
//!   returns `Started` of the SAME execution (the scheduler driver is
//!   at-least-once by design; the run journal stays singleton-true through
//!   the `schedule_runs` unique key + this key).
//! * Overlap bookkeeping rides `schedule_runs.detail`: `{"finished": true}`
//!   is stamped by [`DurableDispatcher::mark_finished`]; `is_running`
//!   reports dispatched-but-unfinished runs.
//! * Payload template contract (`schedules.target.task_template`):
//!   `{operation: "execution.run", input?: …, project_id: uuid,
//!    workflow_id?: uuid, agent_id?: uuid, priority?: …}` — any other
//!   operation class is a configuration bug and fails loudly per run.

use std::sync::Arc;

use mas_application::context::ServiceContext;
use mas_application::execution_service::ExecutionService;
use mas_common::error::AppError;
use mas_common::ids::{AgentId, OrganizationId, ProjectId, ScheduleId, TenantId, WorkflowId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_persistence::repositories::audit::PostgresAuditLog;
use mas_persistence::repositories::services::PgServices;
use mas_persistence::repositories::PgDurableExecution;
use mas_scheduling::due::DueWork;
use mas_scheduling::runner::{DispatchAck, DispatchPort};
use mas_security::AuthoritativeScope;
use sqlx::PgPool;
use uuid::Uuid;

/// Dispatches due runs into the durable execution pipeline.
///
/// Three pools, because the dispatcher sits on both sides of the tenancy
/// boundary:
///
/// * `plane` — `mas_scheduler`. Used only to read a schedule's own
///   `tenant_id` / `organization_id` before a scope exists. SELECT on
///   `schedules` and `schedule_runs`, nothing else (migration 0019).
/// * `runtime` — `mas_app`. Every submission, every overlap query and every
///   write-back runs here, inside `with_authoritative_scope` for the tenant
///   the schedule row named.
/// * `audit` — `mas_audit`. Audit events are written on their own connection
///   so a rolled-back tick still leaves the record that the tick was
///   attempted; the audit role holds INSERT and nothing else.
///
/// It must NOT:
///   • submit outside the canonical pipeline;
///   • create a task directly instead of through a submission;
///   • treat "the tick fired" as "the work ran";
///   • take the tenant from configuration — the schedule row is the only
///     authority for which tenant a fired run belongs to.
#[derive(Debug)]
pub struct DurableDispatcher {
    /// `mas_scheduler` — tenancy resolution only.
    plane: PgPool,
    /// `mas_app` — all scoped work.
    runtime: PgPool,
    /// `mas_audit` — append-only audit lane.
    audit: PgPool,
    replica_id: String,
}

impl DurableDispatcher {
    /// Binds a dispatcher to the three plane pools + replica identity.
    #[must_use]
    pub fn new(
        plane: PgPool,
        runtime: PgPool,
        audit: PgPool,
        replica_id: impl Into<String>,
    ) -> Self {
        Self {
            plane,
            runtime,
            audit,
            replica_id: replica_id.into(),
        }
    }

    /// The scope a fired run runs under: the schedule row's own tenant and
    /// organization, read from the plane before any scope exists.
    ///
    /// This is the authoritative step. A due run belongs to the tenant that
    /// owns the schedule, and nothing else — not the completion callback, not
    /// configuration, not whatever the tick happened to be holding. Reading it
    /// from the row is what makes "the scheduler cannot be pointed at another
    /// tenant" a property rather than an intention.
    ///
    /// # Errors
    /// `not_found` when the schedule no longer exists (deleted between the
    /// due-scan and the dispatch); the mapped database error otherwise.
    async fn scope_for(&self, schedule: ScheduleId) -> Result<AuthoritativeScope> {
        let row: Option<(Uuid, Uuid)> =
            sqlx::query_as("SELECT tenant_id, organization_id FROM schedules WHERE id = $1")
                .bind(Uuid::from(schedule))
                .fetch_optional(&self.plane)
                .await
                .map_err(crate::squash_db)?;
        let (tenant, organization) =
            row.ok_or_else(|| AppError::not_found("schedule", schedule.to_string()))?;
        Ok(AuthoritativeScope::resolved(
            TenantId::from_uuid(tenant),
            OrganizationId::from_uuid(organization),
            crate::plane_store::SCHEDULER_ACTOR,
        ))
    }

    /// Parses the schedule's materialization template.
    fn parse_template(template: &serde_json::Value) -> Result<ParsedTemplate> {
        let operation = template
            .get("operation")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        if operation != "execution.run" {
            return Err(AppError::validation(format!(
                "schedule template operation '{operation}' unsupported (expected execution.run)"
            )));
        }
        let uuid_field = |key: &str| -> Result<Option<Uuid>> {
            match template.get(key).and_then(serde_json::Value::as_str) {
                None => Ok(None),
                Some(raw) => raw
                    .parse::<Uuid>()
                    .map(Some)
                    .map_err(|_| AppError::validation(format!("template {key} is not a uuid"))),
            }
        };
        Ok(ParsedTemplate {
            project: uuid_field("project_id")?,
            workflow: uuid_field("workflow_id")?,
            agent: uuid_field("agent_id")?,
            input: template
                .get("input")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        })
    }

    /// Idempotency key for one planned tick (stable across replicas).
    #[must_use]
    pub fn run_idempotency_key(work: &DueWork) -> String {
        format!(
            "sched:{}:{}",
            work.schedule_id.as_uuid(),
            work.planned_at.to_unix_ms()
        )
    }
}

#[derive(Debug)]
struct ParsedTemplate {
    project: Option<Uuid>,
    workflow: Option<Uuid>,
    agent: Option<Uuid>,
    input: serde_json::Value,
}

#[async_trait::async_trait]
impl DispatchPort for DurableDispatcher {
    /// Submits one fired tick through the canonical pipeline, inside the
    /// schedule tenant's own scoped transaction.
    async fn dispatch(&self, work: &DueWork) -> Result<DispatchAck> {
        let parsed = Self::parse_template(&work.task_template)?;
        let Some(project) = parsed.project else {
            return Err(AppError::validation(
                "schedule template requires project_id (the run's scope)",
            ));
        };
        let scope = self.scope_for(work.schedule_id).await?;
        let key = Self::run_idempotency_key(work);

        let runtime = self.runtime.clone();
        // A second clone for the closure: the outer binding is borrowed for
        // the duration of `with_authoritative_scope`, so the closure cannot
        // move it.
        let inner = runtime.clone();
        let audit = self.audit.clone();
        let replica_id = self.replica_id.clone();
        let project = ProjectId::from(project);
        let workflow = parsed.workflow.map(WorkflowId::from);
        let agent = parsed.agent.map(AgentId::from);
        let input = parsed.input.clone();
        let now = Timestamp::now();
        let tenant = scope.tenant_id();
        let organization = scope.organization_id();

        // One transaction, one tenant, one submission. The scope is armed by
        // `SET LOCAL` inside this transaction, so the pipeline below reads and
        // writes exactly one tenant's rows — and a failure anywhere rolls the
        // whole dispatch back rather than leaving a half-created run.
        mas_persistence::scoped_request::with_authoritative_scope(
            &runtime,
            scope,
            move || async move {
                let ctx: ServiceContext =
                    ServiceContext::for_service("mas-scheduler", &replica_id, &now)?
                        .with_scope(tenant, organization);
                // One control plane per scheduler tick, over the same pool as
                // the durable port.
                let control_plane = Arc::new(
                    mas_persistence::repositories::control_plane::build_postgres_control_plane(
                        inner.clone(),
                    ),
                );
                let execution_service = ExecutionService::new(
                    Box::new(PgServices::new(inner.clone())),
                    Box::new(PgServices::new(inner.clone())),
                    Box::new(PgServices::new(inner.clone())),
                    Box::new(PgServices::new(inner.clone())),
                    Box::new(PostgresAuditLog::new(audit)),
                    Arc::new(PgDurableExecution::new(inner)),
                    control_plane,
                );
                let outcome = execution_service
                    .submit(&ctx, project, workflow, agent, input, Some(&key))
                    .await?;
                Ok(DispatchAck::Started {
                    execution: outcome.execution.id,
                })
            },
        )
        .await
    }

    /// Whether a dispatched run has not yet reported completion.
    ///
    /// Answered inside the schedule's scope. A schedule id that belongs to
    /// another tenant resolves to a scope that cannot see its runs, so the
    /// answer is `false` — the same answer "does not exist" gives.
    async fn is_running(&self, schedule: ScheduleId) -> Result<bool> {
        let scope = self.scope_for(schedule).await?;
        let runtime = self.runtime.clone();
        mas_persistence::scoped_request::with_authoritative_scope(&runtime, scope, || async move {
            let mut connection = mas_persistence::scoped_request::connection().await?;
            let running: bool = sqlx::query_scalar(
                "SELECT EXISTS(
                    SELECT 1 FROM schedule_runs
                    WHERE schedule_id = $1
                      AND outcome = 'dispatched'
                      AND (detail ->> 'finished') IS NULL
                 )",
            )
            .bind(Uuid::from(schedule))
            .fetch_one(connection.connection().as_mut())
            .await
            .map_err(crate::squash_db)?;
            Ok(running)
        })
        .await
    }

    /// Stamps a completed run, inside the schedule's scope.
    ///
    /// Zero rows affected is not an error: it means the run was already
    /// stamped by another replica, or the schedule was deleted. Completion is
    /// idempotent by design — the tick loop is at-least-once.
    async fn mark_finished(&self, schedule: ScheduleId) -> Result<()> {
        let scope = self.scope_for(schedule).await?;
        let runtime = self.runtime.clone();
        mas_persistence::scoped_request::with_authoritative_scope(&runtime, scope, || async move {
            let mut connection = mas_persistence::scoped_request::connection().await?;
            sqlx::query(
                "UPDATE schedule_runs
                 SET detail = coalesce(detail, '{}'::jsonb) || jsonb_build_object('finished', true)
                 WHERE schedule_id = $1
                   AND outcome = 'dispatched'
                   AND (detail ->> 'finished') IS NULL",
            )
            .bind(Uuid::from(schedule))
            .execute(connection.connection().as_mut())
            .await
            .map_err(crate::squash_db)?;
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_work() -> DueWork {
        DueWork {
            schedule_id: ScheduleId::new(),
            tenant_id: mas_common::ids::TenantId::new(),
            planned_at: Timestamp::parse_rfc3339("2026-09-30T00:00:00.000Z").expect("ts"),
            task_template: serde_json::json!({"operation": "execution.run"}),
        }
    }

    #[test]
    fn idempotency_key_is_stable_and_distinguishing() {
        let work = sample_work();
        let key = DurableDispatcher::run_idempotency_key(&work);
        assert!(key.starts_with("sched:"));
        assert!(key.ends_with(":1790726400000"));
        let mut second = sample_work();
        second.planned_at = Timestamp::parse_rfc3339("2026-09-30T00:01:00.000Z").expect("ts");
        assert_ne!(DurableDispatcher::run_idempotency_key(&second), key);
    }

    #[test]
    fn template_parsing_rejects_non_execution_operations() {
        assert!(DurableDispatcher::parse_template(&serde_json::json!({
            "operation": "tool.invoke"
        }))
        .is_err());
        assert!(DurableDispatcher::parse_template(&serde_json::json!({
            "operation": "execution.run",
        }))
        .is_ok());
        let parsed = DurableDispatcher::parse_template(&serde_json::json!({
            "operation": "execution.run",
            "project_id": "00000000-0000-0000-0000-000000000099",
            "input": {"beat": 1},
        }))
        .expect("parsed");
        assert_eq!(
            parsed.project.unwrap().to_string(),
            "00000000-0000-0000-0000-000000000099"
        );
    }
}
