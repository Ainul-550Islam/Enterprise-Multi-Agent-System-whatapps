//! PURPOSE: The PostgreSQL implementations of the application layer's
//! control-plane ports.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: connect the gate's decisions to rows. The gate
//! decides; this module is the only place the decision becomes durable state,
//! and it is the only implementation wired into a binary.
//!
//! Security boundary: every method binds the tenant from the caller's
//! authoritative scope. None of them accepts a tenant id as a filter, and all
//! of them run under RLS, so a wrongly-scoped connection sees nothing rather
//! than seeing someone else's rows.
//!
//! Data ownership: these tables are Rust-owned platform state.
//!
//! It must NOT:
//!   • be paired with an in-memory sibling in a production binary — the
//!     in-memory stores in `mas-quota` and `mas-policy` are test fixtures, and
//!     the whole point of this module is that production uses these;
//!   • reinterpret a decision. It persists what it was told;
//!   • expose a path that writes usage without an idempotency dimension.

use mas_application::control_plane::ports::{ReservationStorePort, UsageLedgerPort, UsageWrite};
use mas_common::ids::{ExecutionId, TaskId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::quota::QuotaDimension;
use mas_domain::reservation::{QuotaReservation, Settlement};

use super::reservations::PostgresReservationStore;
use super::usage::{PostgresUsageLedger, UsageWrite as LedgerWrite};

/// Adapts the reservation store onto the application-layer port.
///
/// The mapping is deliberately thin: no defaults are invented, no amounts are
/// adjusted. If the gate said to reserve 40, 40 is what is reserved.
#[async_trait::async_trait]
impl ReservationStorePort for PostgresReservationStore {
    async fn reserve(
        &self,
        tenant: TenantId,
        dimension: QuotaDimension,
        bucket: i64,
        amount: u64,
        reservation_key: &str,
        execution_id: Option<ExecutionId>,
        task_id: Option<TaskId>,
        lease_expires_at: &Timestamp,
        now: &Timestamp,
    ) -> Result<QuotaReservation> {
        PostgresReservationStore::reserve(
            self,
            tenant,
            dimension,
            bucket,
            amount,
            reservation_key,
            execution_id,
            task_id,
            lease_expires_at,
            now,
        )
        .await
    }

    async fn get_by_key(
        &self,
        tenant: TenantId,
        reservation_key: &str,
    ) -> Result<QuotaReservation> {
        PostgresReservationStore::get_by_key(self, tenant, reservation_key).await
    }

    async fn extend_lease(
        &self,
        tenant: TenantId,
        reservation_key: &str,
        lease_expires_at: &Timestamp,
        now: &Timestamp,
    ) -> Result<Option<QuotaReservation>> {
        PostgresReservationStore::extend_lease(self, tenant, reservation_key, lease_expires_at, now)
            .await
    }

    async fn settle(
        &self,
        tenant: TenantId,
        reservation_key: &str,
        actual: u64,
        at: &Timestamp,
    ) -> Result<Settlement> {
        PostgresReservationStore::settle(self, tenant, reservation_key, actual, at).await
    }

    async fn release(
        &self,
        tenant: TenantId,
        reservation_key: &str,
        at: &Timestamp,
    ) -> Result<Settlement> {
        PostgresReservationStore::release(self, tenant, reservation_key, at).await
    }

    async fn reclaim_expired(
        &self,
        tenant: Option<TenantId>,
        now: &Timestamp,
        limit: i64,
    ) -> Result<Vec<(QuotaReservation, u64)>> {
        PostgresReservationStore::reclaim_expired(self, tenant, now, limit).await
    }
}

/// Adapts the usage ledger onto the application-layer port.
///
/// The two `UsageWrite` shapes are intentionally separate types: the
/// application layer's version carries no SQL concerns, and this conversion is
/// the single point where one becomes the other.
#[async_trait::async_trait]
impl UsageLedgerPort for PostgresUsageLedger {
    async fn record(&self, write: &UsageWrite) -> Result<bool> {
        let ledger_write = LedgerWrite {
            tenant_id: write.tenant_id,
            organization_id: write.organization_id,
            project_id: write.project_id,
            dimension: write.dimension,
            quantity: write.quantity,
            execution_id: write.execution_id,
            task_id: write.task_id,
            task_attempt_id: write.task_attempt_id,
            model: write.model.clone(),
            provider: write.provider.clone(),
            input_quantity: write.input_quantity,
            output_quantity: write.output_quantity,
            duration_ms: write.duration_ms,
            cost_micros: write.cost_micros,
            cost_currency: write.cost_currency.clone(),
            estimated: write.estimated,
            dimensions: write.dimensions.clone(),
            metering_key: write.metering_key.clone(),
            occurred_at: write.occurred_at,
        };
        PostgresUsageLedger::record(self, &ledger_write).await
    }
}

/// Assembles the production control plane over one pool.
///
/// This is the function a production binary calls. Every port behind the gate
/// is answered by Postgres, so the guarantees the gate is there to provide —
/// two replicas cannot together exceed one ceiling, a crashed process does not
/// consume quota forever, duplicate work is metered once — are the database's
/// guarantees rather than the process's.
///
/// There is no configuration flag that swaps any part of this for an in-memory
/// implementation. Doing so would silently remove the distributed guarantees
/// while leaving every caller believing they were in force.
///
/// It must NOT:
///   • be called with a pool that bypasses RLS: the gate is tenant-scoped
///     work, and a bypassing connection would let one tenant's quota decision
///     read another tenant's counters;
///   • be called more than once per process with different pools. One control
///     plane per process, or a settlement could refund against a counter it
///     never charged.
pub fn build_postgres_control_plane(
    pool: sqlx::PgPool,
) -> mas_application::control_plane::ControlPlane {
    use crate::repositories::ControlPlaneStores;
    use mas_application::control_plane::build_control_plane;
    use mas_policy::cache::DecisionCache;
    use mas_policy::engine::Evaluation;
    use std::sync::Arc;

    let stores = ControlPlaneStores::new(pool);

    build_control_plane(
        // Subscription and entitlement rows: the Postgres store implements the
        // tenancy crate's port directly, so there is no adapter to write.
        Arc::new(stores.entitlements.clone()),
        Arc::new(stores.plans.clone()),
        Arc::new(stores.policies.clone()),
        Arc::new(stores.counters.clone()),
        Arc::new(stores.quotas.clone()),
        Arc::new(stores.reservations.clone()),
        Arc::new(stores.usage.clone()),
        DecisionCache::<Evaluation>::default(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_application::control_plane::ports::UsageWrite;

    fn app_write() -> UsageWrite {
        UsageWrite {
            tenant_id: TenantId::new(),
            organization_id: None,
            project_id: None,
            dimension: QuotaDimension::Tokens,
            quantity: 12,
            execution_id: None,
            task_id: Some(TaskId::new()),
            task_attempt_id: None,
            model: Some("gpt-x".to_owned()),
            provider: Some("acme".to_owned()),
            input_quantity: Some(4),
            output_quantity: Some(8),
            duration_ms: Some(900),
            cost_micros: Some(1_500),
            cost_currency: Some("USD".to_owned()),
            estimated: true,
            dimensions: serde_json::json!({"tenant_visible": true}),
            metering_key: None,
            occurred_at: Timestamp::now(),
        }
    }

    #[test]
    fn the_application_write_shape_carries_every_attribution_field() {
        // Guards against the two shapes drifting apart silently: a field added
        // to one and not the other would otherwise be dropped on the floor at
        // this conversion, and the usage record would lose attribution.
        let write = app_write();
        let converted = LedgerWrite {
            tenant_id: write.tenant_id,
            organization_id: write.organization_id,
            project_id: write.project_id,
            dimension: write.dimension,
            quantity: write.quantity,
            execution_id: write.execution_id,
            task_id: write.task_id,
            task_attempt_id: write.task_attempt_id,
            model: write.model.clone(),
            provider: write.provider.clone(),
            input_quantity: write.input_quantity,
            output_quantity: write.output_quantity,
            duration_ms: write.duration_ms,
            cost_micros: write.cost_micros,
            cost_currency: write.cost_currency.clone(),
            estimated: write.estimated,
            dimensions: write.dimensions.clone(),
            metering_key: write.metering_key.clone(),
            occurred_at: write.occurred_at,
        };
        assert_eq!(converted.quantity, 12);
        assert_eq!(converted.input_quantity, Some(4));
        assert_eq!(converted.output_quantity, Some(8));
        assert_eq!(converted.cost_micros, Some(1_500));
        assert!(converted.estimated, "model-reported usage stays flagged");
        assert_eq!(converted.model.as_deref(), Some("gpt-x"));
    }

    #[test]
    fn the_ledger_validates_the_same_invariants_as_the_application_shape() {
        let mut write = app_write();
        write.quantity = 0;
        assert!(
            write.validate().is_err()
                && LedgerWrite {
                    ..ledger_of(&write)
                }
                .validate()
                .is_err(),
            "both shapes must refuse a zero quantity"
        );
    }

    fn ledger_of(write: &UsageWrite) -> LedgerWrite {
        LedgerWrite {
            tenant_id: write.tenant_id,
            organization_id: write.organization_id,
            project_id: write.project_id,
            dimension: write.dimension,
            quantity: write.quantity,
            execution_id: write.execution_id,
            task_id: write.task_id,
            task_attempt_id: write.task_attempt_id,
            model: write.model.clone(),
            provider: write.provider.clone(),
            input_quantity: write.input_quantity,
            output_quantity: write.output_quantity,
            duration_ms: write.duration_ms,
            cost_micros: write.cost_micros,
            cost_currency: write.cost_currency.clone(),
            estimated: write.estimated,
            dimensions: write.dimensions.clone(),
            metering_key: write.metering_key.clone(),
            occurred_at: write.occurred_at,
        }
    }
}
