//! PURPOSE: In-memory implementations of the control-plane ports, for tests and
//! single-process development only.
//!
//! Owning layer: `mas-application`.
//!
//! Production responsibility: **none.** This module exists so that the
//! in-memory composition root — the one that already uses
//! `InMemoryDurableExecution` — can still construct a `ControlPlane`, because
//! the gate is now a required dependency of `ExecutionService` and a required
//! dependency cannot be conditionally absent.
//!
//! That is the whole justification. There is deliberately no code path that
//! selects these over the Postgres stores based on configuration: a binary
//! either wires [`crate::control_plane::ControlPlane`] over Postgres or it is
//! not serving production traffic.
//!
//! Security boundary: single process, no persistence, no cross-tenant surface
//! (there are no tenants to confuse). Reservations and usage evaporate on
//! restart, which is exactly why this is not production.
//!
//! It must NOT:
//!   • be selected by any production configuration — the distributed
//!     guarantees (two replicas cannot exceed one ceiling, a crashed process
//!     does not consume quota forever) come from the database, and these
//!     provide neither;
//!   • be used to "make a test pass" that is supposed to be proving a
//!     concurrency or recovery property. Those belong in
//!     `crates/persistence/tests/control_plane_pg.rs` against real Postgres;
//!   • silently swallow a duplicate. The duplicate-detection semantics here
//!     mirror the Postgres store's, so a test that passes against memory and
//!     fails against Postgres is reporting a real difference, not a fixture
//!     quirk.

use mas_common::error::AppError;
use mas_common::ids::{ExecutionId, TaskId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::quota::QuotaDimension;
use mas_domain::reservation::{QuotaReservation, ReservationState, Settlement};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Mutex;

use super::ports::{ReservationStorePort, UsageLedgerPort, UsageWrite};

/// In-memory reservations.
#[derive(Debug, Default)]
pub struct InMemoryReservationStore {
    inner: Mutex<BTreeMap<String, QuotaReservation>>,
}

impl InMemoryReservationStore {
    /// Creates an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl ReservationStorePort for InMemoryReservationStore {
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
        let mut guard = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let key = format!("{tenant}/{reservation_key}");
        if let Some(existing) = guard.get(&key) {
            // Idempotent on the key, exactly as the Postgres store is: a
            // retried call re-uses the hold rather than charging twice.
            //
            // Only a *live* hold is re-used. A settled or released hold is
            // replaced in place, matching `ON CONFLICT … DO UPDATE WHERE state
            // <> 'held'` — otherwise the in-memory store would hand back a
            // finished reservation and the caller would charge the counter for
            // a hold that no settlement can ever refund.
            if existing.state == ReservationState::Held {
                return Ok(existing.clone());
            }
            guard.remove(&key);
        }
        let reservation = QuotaReservation::hold(
            tenant,
            dimension,
            bucket,
            amount,
            reservation_key,
            execution_id,
            task_id,
            *lease_expires_at,
            now,
        )?;
        guard.insert(key, reservation.clone());
        Ok(reservation)
    }

    async fn get_by_key(
        &self,
        tenant: TenantId,
        reservation_key: &str,
    ) -> Result<QuotaReservation> {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&format!("{tenant}/{reservation_key}"))
            .cloned()
            .ok_or_else(|| {
                AppError::not_found("quota_reservation", format!("{tenant}/{reservation_key}"))
            })
    }

    async fn extend_lease(
        &self,
        tenant: TenantId,
        reservation_key: &str,
        lease_expires_at: &Timestamp,
        now: &Timestamp,
    ) -> Result<Option<QuotaReservation>> {
        let mut guard = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let key = format!("{tenant}/{reservation_key}");
        let Some(reservation) = guard.get_mut(&key) else {
            return Ok(None);
        };
        if reservation.state.is_terminal() {
            return Ok(None);
        }
        reservation.extend_lease(lease_expires_at, now)?;
        Ok(Some(reservation.clone()))
    }

    async fn settle(
        &self,
        tenant: TenantId,
        reservation_key: &str,
        actual: u64,
        at: &Timestamp,
    ) -> Result<Settlement> {
        let mut guard = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let key = format!("{tenant}/{reservation_key}");
        let Some(reservation) = guard.get_mut(&key) else {
            return Err(AppError::not_found("quota_reservation", key));
        };
        if !reservation.state.is_open() {
            // A duplicate completion: nothing to meter, nothing to refund.
            return Ok(Settlement::duplicate(reservation.settled_amount));
        }
        reservation.settle(actual, at)?;
        Ok(Settlement::applied(
            reservation.settled_amount,
            reservation.unused(),
        ))
    }

    async fn release(
        &self,
        tenant: TenantId,
        reservation_key: &str,
        at: &Timestamp,
    ) -> Result<Settlement> {
        let mut guard = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let key = format!("{tenant}/{reservation_key}");
        let Some(reservation) = guard.get_mut(&key) else {
            return Err(AppError::not_found("quota_reservation", key));
        };
        if !reservation.state.is_open() {
            return Ok(Settlement::duplicate(0));
        }
        let released = reservation.release(at)?;
        Ok(Settlement::applied(0, released))
    }

    async fn reclaim_expired(
        &self,
        _tenant: Option<TenantId>,
        now: &Timestamp,
        _limit: i64,
    ) -> Result<Vec<(QuotaReservation, u64)>> {
        let mut guard = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let mut reclaimed = Vec::new();
        for reservation in guard.values_mut() {
            if reservation.is_reclaimable_at(now) {
                let released = reservation.expire(now).unwrap_or(0);
                reclaimed.push((reservation.clone(), released));
            }
        }
        Ok(reclaimed)
    }
}

/// In-memory usage ledger.
#[derive(Debug, Default)]
pub struct InMemoryUsageLedger {
    writes: Mutex<Vec<UsageWrite>>,
}

impl InMemoryUsageLedger {
    /// Creates an empty ledger.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every write recorded so far, for assertions.
    ///
    /// Returns a snapshot rather than a guard so a test can inspect the ledger
    /// without deciding how long to hold the lock.
    #[must_use]
    pub fn recorded(&self) -> Vec<UsageWrite> {
        self.writes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
}

#[async_trait::async_trait]
impl UsageLedgerPort for InMemoryUsageLedger {
    async fn record(&self, write: &UsageWrite) -> Result<bool> {
        write.validate()?;
        let mut guard = self
            .writes
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let duplicate = guard.iter().any(|existing| {
            existing.tenant_id == write.tenant_id
                && existing.dimension == write.dimension
                && existing.metering_key == write.metering_key
                && existing.task_id == write.task_id
        });
        if duplicate {
            return Ok(false);
        }
        guard.push(write.clone());
        Ok(true)
    }
}

/// Debug formatting that names the module's non-production status, so a log
/// line that mentions these stores says so.
impl fmt::Display for InMemoryUsageLedger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self
            .writes
            .lock()
            .map(|guard| guard.len())
            .unwrap_or_else(|error| error.into_inner().len());
        write!(
            formatter,
            "InMemoryUsageLedger({count} writes; not for production)"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(tenant: TenantId, task: Option<TaskId>) -> UsageWrite {
        UsageWrite {
            tenant_id: tenant,
            organization_id: None,
            project_id: None,
            dimension: QuotaDimension::Tokens,
            quantity: 7,
            execution_id: None,
            task_id: task,
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
            metering_key: None,
            occurred_at: Timestamp::now(),
        }
    }

    #[tokio::test]
    async fn the_same_observation_is_recorded_once() {
        let ledger = InMemoryUsageLedger::new();
        let tenant = TenantId::new();
        let task = TaskId::new();
        assert!(ledger
            .record(&write(tenant, Some(task)))
            .await
            .expect("first"));
        assert!(
            !ledger
                .record(&write(tenant, Some(task)))
                .await
                .expect("second"),
            "duplicate work is metered once"
        );
        assert_eq!(ledger.recorded().len(), 1);
    }

    #[tokio::test]
    async fn a_reservation_settles_once_and_releases_the_rest() {
        let store = InMemoryReservationStore::new();
        let tenant = TenantId::new();
        let now = Timestamp::now();
        let lease = now
            .checked_add(std::time::Duration::from_secs(60))
            .expect("lease");

        let held = store
            .reserve(
                tenant,
                QuotaDimension::Tokens,
                0,
                10,
                "k",
                None,
                None,
                &lease,
                &now,
            )
            .await
            .expect("reserve");
        assert_eq!(held.reserved_amount, 10);

        let replay = store
            .reserve(
                tenant,
                QuotaDimension::Tokens,
                0,
                10,
                "k",
                None,
                None,
                &lease,
                &now,
            )
            .await
            .expect("replay");
        assert_eq!(replay.id, held.id, "a retry re-uses the hold");

        let settled = store.settle(tenant, "k", 4, &now).await.expect("settle");
        assert!(settled.applied && settled.consumed == 4 && settled.released == 6);

        let again = store.settle(tenant, "k", 4, &now).await.expect("duplicate");
        assert!(!again.applied && again.released == 0);
    }

    #[tokio::test]
    async fn a_lapsed_lease_is_reclaimable_exactly_once() {
        let store = InMemoryReservationStore::new();
        let tenant = TenantId::new();
        let now = Timestamp::now();
        let lease = now
            .checked_add(std::time::Duration::from_secs(1))
            .expect("lease");
        store
            .reserve(
                tenant,
                QuotaDimension::Tokens,
                0,
                3,
                "k",
                None,
                None,
                &lease,
                &now,
            )
            .await
            .expect("reserve");

        let early = store
            .reclaim_expired(Some(tenant), &now, 10)
            .await
            .expect("early");
        assert!(early.is_empty(), "an un-expired lease is still in flight");

        let later = Timestamp::from_unix_seconds(now.to_unix_seconds() + 600).expect("later");
        let swept = store
            .reclaim_expired(Some(tenant), &later, 10)
            .await
            .expect("sweep");
        assert_eq!(swept.len(), 1);
        assert_eq!(swept[0].1, 3);

        let twice = store
            .reclaim_expired(Some(tenant), &later, 10)
            .await
            .expect("second sweep");
        assert!(twice.is_empty(), "reclaiming twice would refund twice");
    }
}
