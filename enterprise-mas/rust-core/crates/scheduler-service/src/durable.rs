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

use mas_application::context::ServiceContext;
use mas_application::execution_service::ExecutionService;
use mas_common::error::AppError;
use mas_common::ids::{AgentId, OrganizationId, ProjectId, ScheduleId, WorkflowId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_persistence::repositories::audit::PostgresAuditLog;
use mas_persistence::repositories::services::PgServices;
use mas_scheduling::due::DueWork;
use mas_scheduling::runner::{DispatchAck, DispatchPort};
use sqlx::PgPool;
use uuid::Uuid;

/// Dispatches due runs into the durable execution pipeline.
#[derive(Debug)]
pub struct DurableDispatcher {
    pool: PgPool,
    replica_id: String,
}

impl DurableDispatcher {
    /// Binds a dispatcher to the shared pool + replica identity.
    #[must_use]
    pub fn new(pool: PgPool, replica_id: impl Into<String>) -> Self {
        Self {
            pool,
            replica_id: replica_id.into(),
        }
    }

    /// Org lookup for the run's scope (tenant FK holds the truth).
    async fn organization_of(&self, tenant: mas_common::ids::TenantId) -> Result<OrganizationId> {
        let org: Option<Uuid> =
            sqlx::query_scalar("SELECT organization_id FROM tenants WHERE id = $1")
                .bind(Uuid::from(tenant))
                .fetch_optional(&self.pool)
                .await
                .map_err(crate::squash_db)?;
        org.map(OrganizationId::from)
            .ok_or_else(|| AppError::validation("schedule dispatch: unknown tenant"))
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
    async fn dispatch(&self, work: &DueWork) -> Result<DispatchAck> {
        let parsed = Self::parse_template(&work.task_template)?;
        let Some(project) = parsed.project else {
            return Err(AppError::validation(
                "schedule template requires project_id (the run's scope)",
            ));
        };
        let organization = self.organization_of(work.tenant_id).await?;
        let ctx: ServiceContext =
            ServiceContext::for_service("mas-scheduler", &self.replica_id, &Timestamp::now())?
                .with_scope(work.tenant_id, organization);

        // Direct construction over THIS repo pool (service assembly is cheap;
        // each dispatch builds its own so transaction/context lifetimes stay
        // trivially correct).
        let execution_service = ExecutionService::new(
            Box::new(PgServices::new(self.pool.clone())),
            Box::new(PgServices::new(self.pool.clone())),
            Box::new(PgServices::new(self.pool.clone())),
            Box::new(PgServices::new(self.pool.clone())),
            Box::new(PostgresAuditLog::new(self.pool.clone())),
        );
        let key = Self::run_idempotency_key(work);
        let outcome = execution_service
            .submit(
                &ctx,
                ProjectId::from(project),
                parsed.workflow.map(WorkflowId::from),
                parsed.agent.map(AgentId::from),
                parsed.input,
                Some(&key),
            )
            .await?;
        Ok(DispatchAck::Started {
            execution: outcome.execution.id,
        })
    }

    async fn is_running(&self, schedule: ScheduleId) -> Result<bool> {
        let running: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM schedule_runs
                WHERE schedule_id = $1
                  AND outcome = 'dispatched'
                  AND (detail ->> 'finished') IS NULL
            )",
        )
        .bind(Uuid::from(schedule))
        .fetch_one(&self.pool)
        .await
        .map_err(crate::squash_db)?;
        Ok(running)
    }

    async fn mark_finished(&self, schedule: ScheduleId) -> Result<()> {
        sqlx::query(
            "UPDATE schedule_runs
             SET detail = coalesce(detail, '{}'::jsonb) || jsonb_build_object('finished', true)
             WHERE schedule_id = $1
               AND outcome = 'dispatched'
               AND (detail ->> 'finished') IS NULL",
        )
        .bind(Uuid::from(schedule))
        .execute(&self.pool)
        .await
        .map_err(crate::squash_db)?;
        Ok(())
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
