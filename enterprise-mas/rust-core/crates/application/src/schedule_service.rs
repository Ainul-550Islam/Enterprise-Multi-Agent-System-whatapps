//! Schedule lifecycle use-cases: registration (task templates must be
//! objects — stored procedures live in the scheduler plane), pause /
//! resume with idempotent resume, and tenant-scoped listings.

use mas_common::error::AppError;
use mas_common::ids::{ProjectId, ScheduleId};
use mas_common::result::Result;
use mas_domain::{Schedule, ScheduleKind};

use crate::audit::{self, AuditSinkPort};
use crate::context::ServiceContext;
use crate::stores::ScheduleStorePort;

/// Use-cases over schedules.
#[derive(Debug)]
pub struct ScheduleService {
    schedules: Box<dyn ScheduleStorePort>,
    audit: Box<dyn AuditSinkPort>,
}

impl ScheduleService {
    /// Composes the service over the schedule store port + audit sink.
    pub fn new(schedules: Box<dyn ScheduleStorePort>, audit: Box<dyn AuditSinkPort>) -> Self {
        Self { schedules, audit }
    }

    /// Registers a schedule under the context's tenant. The task template
    /// must be a JSON object (domain rule mirrored here for a type-safe
    /// external interface).
    pub async fn register(
        &self,
        ctx: &ServiceContext,
        project: Option<ProjectId>,
        name: &str,
        kind: ScheduleKind,
        task_template: serde_json::Value,
    ) -> Result<Schedule> {
        let tenant_id = ctx.require_tenant()?;
        if !task_template.is_object() {
            return Err(AppError::validation(
                "task_template must be a JSON object (e.g. {\"workflow_id\": \"…\"})",
            ));
        }
        let mut schedule = Schedule::new(tenant_id, project, name, kind, task_template)?;
        // Registration activates: domain schedules start Disabled; the
        // use-case contract is "registered == fires when due".
        schedule.enable()?;
        self.schedules.save(&schedule).await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "schedule.register",
            "schedule",
            Some(schedule.id.to_string()),
            mas_domain::AuditOutcome::Success,
        )
        .await?;
        Ok(schedule)
    }

    /// Pauses an enabled schedule (idempotent; legal transitions enforced
    /// by the aggregate).
    pub async fn pause(&self, ctx: &ServiceContext, schedule_id: ScheduleId) -> Result<Schedule> {
        let mut schedule = self.scope_checked(ctx, schedule_id).await?;
        schedule.pause()?;
        self.persist_and_audit(ctx, &schedule, "schedule.pause")
            .await?;
        Ok(schedule)
    }

    /// Resumes — returns to `Active` via the domain's `enable` verb
    /// (idempotent on already-active schedules, matching the scheduler
    /// service's resume semantics).
    pub async fn resume(&self, ctx: &ServiceContext, schedule_id: ScheduleId) -> Result<Schedule> {
        let mut schedule = self.scope_checked(ctx, schedule_id).await?;
        if !matches!(schedule.status, mas_common::enums::ScheduleStatus::Active) {
            schedule.enable()?;
        }
        self.persist_and_audit(ctx, &schedule, "schedule.resume")
            .await?;
        Ok(schedule)
    }

    /// Disables a schedule permanently (mirrors the aggregate verb).
    pub async fn disable(&self, ctx: &ServiceContext, schedule_id: ScheduleId) -> Result<Schedule> {
        let mut schedule = self.scope_checked(ctx, schedule_id).await?;
        schedule.disable()?;
        self.persist_and_audit(ctx, &schedule, "schedule.disable")
            .await?;
        Ok(schedule)
    }

    /// Scope-checked fetch (existence hiding).
    pub async fn get(&self, ctx: &ServiceContext, schedule_id: ScheduleId) -> Result<Schedule> {
        self.scope_checked(ctx, schedule_id).await
    }

    /// Lists schedules the context's tenant may see.
    pub async fn list(&self, ctx: &ServiceContext) -> Result<Vec<Schedule>> {
        let tenant_id = ctx.require_tenant()?;
        self.schedules.list(tenant_id).await
    }

    async fn persist_and_audit(
        &self,
        ctx: &ServiceContext,
        schedule: &Schedule,
        action: &'static str,
    ) -> Result<()> {
        self.schedules.save(schedule).await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            action,
            "schedule",
            Some(schedule.id.to_string()),
            mas_domain::AuditOutcome::Success,
        )
        .await
    }

    async fn scope_checked(
        &self,
        ctx: &ServiceContext,
        schedule_id: ScheduleId,
    ) -> Result<Schedule> {
        let tenant_id = ctx.require_tenant()?;
        self.schedules
            .get(schedule_id)
            .await?
            .filter(|s| s.tenant_id == tenant_id)
            .ok_or_else(|| AppError::not_found("schedule", schedule_id.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::InMemoryAuditSink;
    use crate::stores::InMemoryServices;
    use mas_common::enums::ScheduleStatus;
    use mas_common::ids::{OrganizationId, TenantId};
    use mas_common::timestamps::Timestamp;
    use std::sync::Arc;

    struct Fixture {
        service: ScheduleService,
        ctx: ServiceContext,
        audit: Arc<InMemoryAuditSink>,
    }

    fn fixture() -> Fixture {
        let now = Timestamp::from_unix_seconds(1_700_000_000).expect("ts");
        let store = Arc::new(InMemoryServices::new());
        let audit = Arc::new(InMemoryAuditSink::new());
        let service = ScheduleService::new(Box::new(store), Box::new(audit.clone()));
        let ctx = ServiceContext::system("sched-ctx", &now)
            .expect("ctx")
            .with_scope(TenantId::new(), OrganizationId::new());
        Fixture {
            service,
            ctx,
            audit,
        }
    }

    #[tokio::test]
    async fn register_pauses_resumes_and_hides() {
        let fixture = fixture();
        let scheduler = fixture.service;
        let ctx = &fixture.ctx;

        let schedule = scheduler
            .register(
                ctx,
                None,
                "nightly-retrain",
                ScheduleKind::Interval {
                    every_seconds: 86_400,
                },
                serde_json::json!({"workflow_id": "wf-1"}),
            )
            .await
            .expect("register");
        assert!(matches!(schedule.status, ScheduleStatus::Active));

        // Non-object templates are refused before reaching the aggregate.
        assert!(scheduler
            .register(
                ctx,
                None,
                "bad",
                ScheduleKind::Interval { every_seconds: 60 },
                serde_json::json!("not-an-object")
            )
            .await
            .is_err());

        let paused = scheduler.pause(ctx, schedule.id).await.expect("pause");
        assert!(matches!(paused.status, ScheduleStatus::Paused));
        let resumed = scheduler.resume(ctx, schedule.id).await.expect("resume");
        assert!(matches!(resumed.status, ScheduleStatus::Active));
        // Resume is idempotent per use-case semantics.
        let resumed_again = scheduler
            .resume(ctx, schedule.id)
            .await
            .expect("resume again");
        assert!(matches!(resumed_again.status, ScheduleStatus::Active));

        let listed = scheduler.list(ctx).await.expect("list");
        assert_eq!(listed.len(), 1);

        let other = ServiceContext::system(
            "other",
            &Timestamp::from_unix_seconds(1_700_000_001).expect("t"),
        )
        .expect("ctx")
        .with_scope(TenantId::new(), OrganizationId::new());
        assert!(
            scheduler.get(&other, schedule.id).await.is_err(),
            "tenant hiding"
        );
        assert!(
            fixture.audit.len() >= 3,
            "register + pause + resume audited (≥3)"
        );
    }
}
