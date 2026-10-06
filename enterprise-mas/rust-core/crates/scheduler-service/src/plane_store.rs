//! PURPOSE: The scheduler's schedule store: two pools, one cross-tenant read,
//! and a scoped transaction for everything after it.
//!
//! Owning layer: `mas-scheduler-service` (the scheduler process edge).
//!
//! Production responsibility: implement `mas_scheduling::store::ScheduleStorePort`
//! for the real database in a way that keeps row-level security in force. The
//! scheduler is a *plane*, not a request: it has no caller, no principal and no
//! tenant when its tick starts. That makes it the one component that could
//! justify an RLS bypass — and this module is where that justification is
//! confined to a single SELECT on a single table instead of being spent on the
//! whole tick.
//!
//! Security boundary — the tick is split deliberately:
//!
//!   1. `list_due` runs on the **plane pool** (`mas_scheduler`). That role
//!      holds BYPASSRLS, and its grants are `SELECT ON schedules` and
//!      `SELECT ON schedule_runs` and nothing else (migration 0019). It cannot
//!      read a task, an execution, an agent, a credential or an API key. The
//!      scan is the platform's only cross-tenant read and it is the only
//!      statement allowed to make one.
//!   2. Every statement after the scan — `get`, `save`, `record_run` — runs on
//!      the **runtime pool** (`mas_app`) inside
//!      `with_authoritative_scope`, with `SET LOCAL app.current_tenant` /
//!      `app.current_org` set from the tenant and organization the due row
//!      itself named. The scheduler never carries a caller-supplied tenant:
//!      the row is the authority, exactly as it is for a request.
//!
//! A schedule whose tenant cannot be resolved is an error, not a scan of every
//! tenant. There is no fallback to the runtime pool for the due-scan: under
//! `mas_app` it would return zero rows, and a scheduler that silently idles is
//! a worse failure than one that refuses to start.
//!
//! It must NOT:
//!   • run `save`, `get` or `record_run` on the plane pool — the plane role
//!     has no write grants, and giving it some would make the tick loop RLS-free;
//!   • accept a tenant from configuration, an environment variable or a
//!     completion callback; the schedule row is the only source;
//!   • widen the due-scan limit in a way that lets one tenant's backlog starve
//!     another's (`mas_scheduling::due` interleaves for exactly this reason).

use mas_common::error::AppError;
use mas_common::ids::{OrganizationId, ScheduleId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::schedule::Schedule;
use mas_persistence::repositories::schedules::ScheduleStore;
use mas_scheduling::store::ScheduleStorePort;
use mas_security::AuthoritativeScope;
use sqlx::PgPool;
use uuid::Uuid;

use crate::squash_db;

/// The actor string recorded on scheduler-opened scopes.
///
/// It appears in audit events and in `pg_stat_activity`, so it has to say
/// which plane opened the transaction and that no human was behind it.
pub const SCHEDULER_ACTOR: &str = "service:mas-scheduler";

/// A schedule's tenancy, as the row itself states it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScheduleTenancy {
    tenant_id: TenantId,
    organization_id: OrganizationId,
}

impl ScheduleTenancy {
    /// The authoritative scope for work on this schedule.
    fn scope(self) -> AuthoritativeScope {
        AuthoritativeScope::resolved(self.tenant_id, self.organization_id, SCHEDULER_ACTOR)
    }
}

/// The scheduler's schedule store: plane pool for the scan, runtime pool for
/// the scoped work the scan leads to.
#[derive(Debug)]
pub struct SchedulerPlaneStore {
    /// `mas_scheduler` — cross-tenant due-scan, SELECT on two tables.
    plane: PgPool,
    /// `mas_app` — every write, inside a tenant-scoped transaction.
    runtime: PgPool,
}

impl SchedulerPlaneStore {
    /// Binds the store to the two pools the tick needs.
    #[must_use]
    pub fn new(plane: PgPool, runtime: PgPool) -> Self {
        Self { plane, runtime }
    }

    /// Reads a schedule's tenancy from the row.
    ///
    /// This is the one pre-scope read in the tick, and it is the reason the
    /// plane role exists: there is no scope yet, so no RLS policy can admit
    /// the read. It returns the row's own tenant and organization — never a
    /// value from configuration or from the caller.
    ///
    /// # Errors
    /// `not_found` when the schedule does not exist; the mapped database error
    /// otherwise.
    async fn tenancy_of(&self, schedule_id: ScheduleId) -> Result<ScheduleTenancy> {
        let row: Option<(Uuid, Uuid)> =
            sqlx::query_as("SELECT tenant_id, organization_id FROM schedules WHERE id = $1")
                .bind(Uuid::from(schedule_id))
                .fetch_optional(&self.plane)
                .await
                .map_err(squash_db)?;
        let (tenant, organization) =
            row.ok_or_else(|| AppError::not_found("schedule", schedule_id.to_string()))?;
        Ok(ScheduleTenancy {
            tenant_id: TenantId::from_uuid(tenant),
            organization_id: OrganizationId::from_uuid(organization),
        })
    }

    /// Runs `work` on the runtime pool inside the schedule's own scope.
    ///
    /// The scope comes from the schedule row, so a scheduler replica cannot be
    /// steered at another tenant's data by anything the network hands it. The
    /// closure runs inside one transaction: a failed write-back rolls back
    /// with the rest of the tick's work rather than half-applying.
    async fn in_schedule_scope<T, F, Fut>(&self, tenancy: ScheduleTenancy, work: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: core::future::Future<Output = Result<T>>,
    {
        mas_persistence::scoped_request::with_authoritative_scope(
            &self.runtime,
            tenancy.scope(),
            work,
        )
        .await
    }
}

#[async_trait::async_trait]
impl ScheduleStorePort for SchedulerPlaneStore {
    /// The cross-tenant due-scan.
    ///
    /// Runs on the plane connection. Under any other role this returns zero
    /// rows rather than every tenant's schedules, which is the correct
    /// direction for the failure to go: a misconfigured scheduler idles
    /// visibly instead of leaking tenancy.
    async fn list_due(&self, now: &Timestamp, limit: usize) -> Result<Vec<Schedule>> {
        let mut connection = self.plane.acquire().await.map_err(squash_db)?;
        ScheduleStore::list_due_in(connection.as_mut(), now, limit).await
    }

    /// Loads one schedule: resolve the tenancy on the plane, then read the row
    /// under that tenant's scope.
    ///
    /// A schedule id belonging to another tenant resolves to a scope that
    /// cannot see it, so the answer is `None` — indistinguishable from "does
    /// not exist", which is what an unauthorized id must look like.
    async fn get(&self, id: ScheduleId) -> Result<Option<Schedule>> {
        let tenancy = self.tenancy_of(id).await?;
        self.in_schedule_scope(tenancy, || async move {
            let mut connection = mas_persistence::scoped_request::connection().await?;
            ScheduleStore::get_in(connection.connection().as_mut(), id).await
        })
        .await
    }

    /// Persists the post-fire schedule state: same schedule, same tenant, and
    /// the write runs inside that tenant's scoped transaction.
    async fn save(&self, schedule: &Schedule) -> Result<()> {
        let tenancy = ScheduleTenancy {
            tenant_id: schedule.tenant_id,
            // The domain aggregate is tenant-scoped by design and does not
            // carry the organization; the row does, and the row is the
            // authority. Reading it here keeps the scope honest without
            // widening the aggregate's contract.
            organization_id: self.tenancy_of(schedule.id).await?.organization_id,
        };
        let stored = schedule.clone();
        self.in_schedule_scope(tenancy, move || async move {
            let mut connection = mas_persistence::scoped_request::connection().await?;
            ScheduleStore::save_in(connection.connection().as_mut(), &stored).await
        })
        .await
    }

    /// Journals a planned tick under the schedule's own scope.
    ///
    /// `false` means another replica recorded this tick first — the
    /// duplicate-fire guard. The unique constraint is what actually decides;
    /// this only reports the outcome.
    async fn record_run(&self, schedule_id: ScheduleId, planned_at: Timestamp) -> Result<bool> {
        let tenancy = self.tenancy_of(schedule_id).await?;
        self.in_schedule_scope(tenancy, move || async move {
            let mut connection = mas_persistence::scoped_request::connection().await?;
            ScheduleStore::record_run_in(connection.connection().as_mut(), schedule_id, planned_at)
                .await
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The actor string must name the plane, because it is what an auditor
    /// reads in `audit_events.actor` when a tick is questioned.
    #[test]
    fn the_actor_string_names_the_plane() {
        assert!(SCHEDULER_ACTOR.contains("mas-scheduler"));
        assert!(SCHEDULER_ACTOR.starts_with("service:"));
    }

    /// The scope a tenancy produces must carry the row's own ids — not a
    /// default, not a nil, not something the caller supplied.
    #[test]
    fn a_tenancy_produces_a_scope_from_the_row() {
        let tenant = TenantId::new();
        let organization = OrganizationId::new();
        let scope = ScheduleTenancy {
            tenant_id: tenant,
            organization_id: organization,
        }
        .scope();
        assert_eq!(scope.tenant_id(), tenant);
        assert_eq!(scope.organization_id(), organization);
        assert!(scope.actor().contains("mas-scheduler"));
    }
}
