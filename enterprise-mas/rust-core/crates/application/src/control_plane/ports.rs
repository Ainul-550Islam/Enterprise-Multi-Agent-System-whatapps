//! PURPOSE: The ports the control-plane gate depends on for its durable side.
//!
//! Owning layer: `mas-application`.
//!
//! Production responsibility: keep the dependency direction honest.
//! `mas-persistence` depends on `mas-application`, so the application layer
//! cannot name a `mas-persistence` type without creating a cycle — and should
//! not want to, because the gate is about *decisions* while persistence is
//! about *storage*. These traits let the gate say what it needs without
//! knowing which database answers.
//!
//! Security boundary: every method takes `tenant_id` from the caller's
//! authoritative scope. None of them accepts a tenant id to filter on, and
//! none of them will cross tenants for convenience.
//!
//! Data ownership: the implementations own platform state; the gate owns the
//! decision to change it.
//!
//! It must NOT:
//!   • grow a method that reads across tenants — the control plane is
//!     tenant-scoped work, and cross-tenant operations belong to the
//!     dispatcher/audit planes with their own dedicated roles;
//!   • let an implementation decide policy or quota semantics: these traits
//!     move numbers, they do not interpret them.

use mas_common::ids::{ExecutionId, TaskId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::quota::QuotaDimension;
use mas_domain::reservation::{QuotaReservation, Settlement};
use std::fmt;

/// Durable reservations: the recoverable half of a quota charge.
#[async_trait::async_trait]
pub trait ReservationStorePort: Send + Sync + fmt::Debug {
    /// Serializes a reservation key before the control plane reads it and
    /// charges quota. Production implementations hold this lock for the
    /// authoritative request transaction; without it two replicas can both
    /// miss a row, both charge, and only one can retain the reservation.
    ///
    /// In-memory test implementations may be a no-op because the control
    /// plane also holds a process-local gate around the same critical section.
    async fn lock_key(&self, tenant: TenantId, reservation_key: &str) -> Result<()>;

    /// Authorises `amount` units under `reservation_key`, idempotently.
    #[allow(clippy::too_many_arguments)]
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
        // `now` is the caller's clock, passed in rather than read from `now()`
        // so a caller operating on a fixed timestamp — a test, or a replay of
        // recorded traffic — gets the same answer the production path would,
        // and so the lease is validated against the same instant the rest of
        // the request used.
        now: &Timestamp,
    ) -> Result<QuotaReservation>;

    /// Reads one reservation by its idempotency key.
    async fn get_by_key(&self, tenant: TenantId, reservation_key: &str)
        -> Result<QuotaReservation>;

    /// Extends the lease on a still-held reservation.
    ///
    /// A run that lasts longer than its lease must be able to say so. Without
    /// this, the only way to hold a slot for a long run is to reserve it for
    /// the worst case up front — which is the difference between "a crashed
    /// worker holds this slot for a minute" and "it holds it for an hour".
    ///
    /// Returns `None` when the reservation does not exist or is no longer
    /// held: a lease cannot be extended on a reservation that has already
    /// been settled, released or reclaimed.
    async fn extend_lease(
        &self,
        tenant: TenantId,
        reservation_key: &str,
        lease_expires_at: &Timestamp,
        now: &Timestamp,
    ) -> Result<Option<QuotaReservation>>;

    /// Reconciles against actual consumption. Second call is a no-op.
    async fn settle(
        &self,
        tenant: TenantId,
        reservation_key: &str,
        actual: u64,
        at: &Timestamp,
    ) -> Result<Settlement>;

    /// Returns the reservation in full. Second call is a no-op.
    async fn release(
        &self,
        tenant: TenantId,
        reservation_key: &str,
        at: &Timestamp,
    ) -> Result<Settlement>;

    /// Reclaims lapsed reservations, returning (reservation, released units).
    async fn reclaim_expired(
        &self,
        tenant: Option<TenantId>,
        now: &Timestamp,
        limit: i64,
    ) -> Result<Vec<(QuotaReservation, u64)>>;
}

/// The one authoritative usage write path.
///
/// A single method on purpose. Usage is the number customers are billed from;
/// the fewer shapes that can produce one, the fewer places a double-count or a
/// fabricated quantity can hide.
#[async_trait::async_trait]
pub trait UsageLedgerPort: Send + Sync + fmt::Debug {
    /// Records one observation; `false` means it was already recorded.
    async fn record(&self, write: &UsageWrite) -> Result<bool>;
}

/// One usage observation, as the application layer expresses it.
///
/// Mirrors the persistence-layer write shape but carries no SQL concerns. The
/// `tenant_id` is the authenticated, server-resolved tenant — never a value
/// taken from a request body.
#[derive(Debug, Clone)]
pub struct UsageWrite {
    /// Owning tenant (server-side scope).
    pub tenant_id: TenantId,
    /// Owning organization.
    pub organization_id: Option<mas_common::ids::OrganizationId>,
    /// Project, when attributable.
    pub project_id: Option<mas_common::ids::ProjectId>,
    /// Metered dimension.
    pub dimension: QuotaDimension,
    /// Observed quantity.
    pub quantity: u64,
    /// Causing execution.
    pub execution_id: Option<ExecutionId>,
    /// Causing task (part of the idempotency dimension).
    pub task_id: Option<TaskId>,
    /// Causing attempt.
    pub task_attempt_id: Option<mas_common::ids::TaskAttemptId>,
    /// Model that consumed the resource.
    pub model: Option<String>,
    /// Provider that served it.
    pub provider: Option<String>,
    /// Input-side quantity (prompt tokens, request bytes).
    pub input_quantity: Option<u64>,
    /// Output-side quantity (completion tokens, response bytes).
    pub output_quantity: Option<u64>,
    /// Wall-clock duration.
    pub duration_ms: Option<u64>,
    /// Cost in integer micro-units — never a float.
    pub cost_micros: Option<i64>,
    /// Currency label for `cost_micros`.
    pub cost_currency: Option<String>,
    /// Whether the quantity is model-reported rather than measured.
    pub estimated: bool,
    /// Non-secret metadata (never prompts, never credentials).
    pub dimensions: serde_json::Value,
    /// Idempotency key for usage with no task.
    pub metering_key: Option<String>,
    /// When the consumption happened.
    pub occurred_at: Timestamp,
}

impl UsageWrite {
    /// Validates the invariants every caller must satisfy.
    ///
    /// Refusing a zero quantity is what keeps "no usage" distinguishable from
    /// "usage of zero". Refusing a write with no idempotency dimension at all
    /// is what stops a duplicated delivery from being metered twice.
    pub fn validate(&self) -> Result<()> {
        if self.quantity == 0 {
            return Err(mas_common::error::AppError::validation(
                "usage quantity must be positive; zero-quantity work is not a usage record",
            ));
        }
        if self.metering_key.is_none() && self.task_id.is_none() {
            return Err(mas_common::error::AppError::validation(concat!(
                "usage must carry a task_id or a metering_key: without one, a ",
                "duplicate delivery cannot be recognised and the same work ",
                "would be metered twice"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write() -> UsageWrite {
        UsageWrite {
            tenant_id: TenantId::new(),
            organization_id: None,
            project_id: None,
            dimension: QuotaDimension::Tokens,
            quantity: 10,
            execution_id: None,
            task_id: None,
            task_attempt_id: None,
            model: None,
            provider: None,
            input_quantity: None,
            output_quantity: None,
            duration_ms: None,
            cost_micros: None,
            cost_currency: None,
            estimated: false,
            dimensions: serde_json::json!({}),
            metering_key: Some("k".to_owned()),
            occurred_at: Timestamp::now(),
        }
    }

    #[test]
    fn a_zero_quantity_is_refused() {
        let mut bad = write();
        bad.quantity = 0;
        assert!(bad.validate().is_err());
    }

    #[test]
    fn usage_without_any_idempotency_dimension_is_refused() {
        let mut bad = write();
        bad.metering_key = None;
        bad.task_id = None;
        assert!(bad.validate().is_err());
        let mut good = write();
        good.metering_key = None;
        good.task_id = Some(TaskId::new());
        assert!(good.validate().is_ok());
    }
}
