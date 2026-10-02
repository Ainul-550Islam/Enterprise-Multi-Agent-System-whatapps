//! Concurrency limiter: leased execution slots per tenant.
//!
//! `ConcurrentRuns` is a gauge, not a periodic counter: slots are acquired
//! when an execution starts and released when it finishes (or when its lease
//! dies of old age — the sweeper reaps permits older than
//! [`Permit::expires_at`]). Permits carry an id, so double-release is a hard
//! error, not a silent accounting drift.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use mas_common::error::AppError;
use mas_common::ids::{TenantId, UsageRecordId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::quota::QuotaDimension;

use crate::counter::{CounterKey, CounterStorePort};

/// Default permit TTL — a run holding a slot for longer than a day is dead.
pub const DEFAULT_PERMIT_TTL: Duration = Duration::from_secs(86_400);

/// A held concurrency slot. Dropping it WITHOUT releasing is a bug; owners
/// call [`ConcurrencyLimiter::release`] on every exit path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Permit {
    /// Unique permit id.
    pub id: UsageRecordId,
    /// Owning tenant.
    pub tenant_id: TenantId,
    /// Dimension being leased (`ConcurrentRuns` typically).
    pub dimension: QuotaDimension,
    /// When the slot was taken.
    pub acquired_at: Timestamp,
    /// When the slot self-expires (crash fail-safe).
    pub expires_at: Timestamp,
}

impl Permit {
    /// Whether the permit auto-expired at `now`.
    #[must_use]
    pub fn is_expired(&self, now: &Timestamp) -> bool {
        !self.expires_at.is_after(now)
    }
}

/// Concurrency limiter over a counter store.
#[derive(Debug)]
pub struct ConcurrencyLimiter<S: CounterStorePort> {
    store: S,
    dimension: QuotaDimension,
    ledger: Mutex<BTreeMap<UsageRecordId, Permit>>,
}

impl<S: CounterStorePort> ConcurrencyLimiter<S> {
    /// Binds a limiter to one dimension (`ConcurrentRuns` by default).
    #[must_use]
    pub fn new(store: S, dimension: QuotaDimension) -> Self {
        Self {
            store,
            dimension,
            ledger: Mutex::new(BTreeMap::new()),
        }
    }

    /// `ConcurrentRuns` gauge limiter.
    #[must_use]
    pub fn runs(store: S) -> Self {
        Self::new(store, QuotaDimension::ConcurrentRuns)
    }

    /// Tries to take one slot for `tenant`, capped at `max`.
    ///
    /// Optimistic check: increment, then enforce — on overflow the counter
    /// is decremented back so rejected attempts never leak into the gauge.
    pub async fn acquire(&self, tenant: TenantId, max: u64, now: &Timestamp) -> Result<Permit> {
        if max == 0 {
            return Err(AppError::validation("concurrency max must be positive"));
        }
        let key = CounterKey::for_time(tenant, self.dimension, None, now);
        self.sweep_expired(tenant, now).await;
        let now_value = self.store.increment(key, 1).await?;
        if now_value > max {
            let _ = self.store.decrement(key, 1).await?;
            self.sweep_expired(tenant, now).await;
            // One more chance after sweeping stale permits.
            let after = self.store.increment(key, 1).await?;
            if after > max {
                let _ = self.store.decrement(key, 1).await?;
                return Err(AppError::rate_limited(format!(
                    "tenant {tenant} has no free {dimension} slots (max {max})",
                    dimension = self.dimension
                )));
            }
        }
        let expires_at = now
            .checked_add(DEFAULT_PERMIT_TTL)
            .ok_or_else(|| AppError::internal("permit TTL overflow"))?;
        let permit = Permit {
            id: UsageRecordId::new(),
            tenant_id: tenant,
            dimension: self.dimension,
            acquired_at: *now,
            expires_at,
        };
        self.ledger
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(permit.id, permit.clone());
        Ok(permit)
    }

    /// Returns a slot. Double-release and foreign-permit release are errors.
    pub async fn release(&self, permit: &Permit, now: &Timestamp) -> Result<()> {
        let removed = self
            .ledger
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&permit.id);
        match removed {
            None => Err(AppError::conflict(format!(
                "permit {} unknown (already released or expired-swept)",
                permit.id
            ))),
            Some(_) if permit.is_expired(now) => {
                // Already swept from the counter — releasing now would
                // double-subtract. Decline, don't corrupt the gauge.
                Err(AppError::conflict(format!(
                    "permit {} already expired and was reaped",
                    permit.id
                )))
            },
            Some(held) => {
                let key = CounterKey::for_time(held.tenant_id, self.dimension, None, now);
                let _ = self.store.decrement(key, 1).await?;
                Ok(())
            },
        }
    }

    /// Live slot count for a tenant (gauge read).
    pub async fn in_use(&self, tenant: TenantId, now: &Timestamp) -> Result<u64> {
        let key = CounterKey::for_time(tenant, self.dimension, None, now);
        self.store.get(key).await
    }

    /// Reaps permits whose TTL expired, decrementing their counters
    /// (idempotent via the ledger — each permit sweeps exactly once).
    pub async fn sweep_expired(&self, tenant: TenantId, now: &Timestamp) -> u64 {
        let expired: Vec<Permit> = {
            let mut ledger = self.ledger.lock().unwrap_or_else(|e| e.into_inner());
            let ids: Vec<UsageRecordId> = ledger
                .iter()
                .filter(|(_, p)| p.tenant_id == tenant && p.is_expired(now))
                .map(|(id, _)| *id)
                .collect();
            ids.iter().filter_map(|id| ledger.remove(id)).collect()
        };
        let mut swept = 0u64;
        for permit in expired {
            let key = CounterKey::for_time(permit.tenant_id, self.dimension, None, now);
            if self.store.decrement(key, 1).await.is_ok() {
                swept += 1;
            }
        }
        swept
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::counter::InMemoryCounterStore;
    use std::sync::Arc;

    #[tokio::test]
    async fn slots_cap_release_and_double_release_rejected() {
        let limiter = ConcurrencyLimiter::runs(InMemoryCounterStore::new());
        let tenant = TenantId::new();
        let now = Timestamp::now();

        let p1 = limiter.acquire(tenant, 2, &now).await.expect("slot 1");
        let p2 = limiter.acquire(tenant, 2, &now).await.expect("slot 2");
        assert_eq!(limiter.in_use(tenant, &now).await.expect("use"), 2);

        let err = limiter
            .acquire(tenant, 2, &now)
            .await
            .expect_err("saturation");
        assert_eq!(err.error_code(), "RATE_LIMITED");
        assert_eq!(
            limiter.in_use(tenant, &now).await.expect("use"),
            2,
            "rejection leaves no leak"
        );

        limiter.release(&p1, &now).await.expect("release");
        assert_eq!(limiter.in_use(tenant, &now).await.expect("use"), 1);
        let double = limiter
            .release(&p1, &now)
            .await
            .expect_err("double release");
        assert_eq!(double.error_code(), "CONFLICT");

        // Other tenant unaffected by tenant 1's remaining slot.
        let other = TenantId::new();
        limiter.acquire(other, 1, &now).await.expect("other tenant");
        limiter.release(&p2, &now).await.expect("release 2");
        assert_eq!(limiter.in_use(tenant, &now).await.expect("use"), 0);
    }

    #[tokio::test]
    async fn expired_permits_are_swept_and_unblock_acquisition() {
        let limiter = Arc::new(ConcurrencyLimiter::runs(InMemoryCounterStore::new()));
        let tenant = TenantId::new();
        let t0 = Timestamp::now();

        let p1 = limiter.acquire(tenant, 1, &t0).await.expect("acquire");
        let err = limiter.acquire(tenant, 1, &t0).await.expect_err("full");
        assert_eq!(err.error_code(), "RATE_LIMITED");

        // Travel past the TTL: the sweep inside the next acquire frees the
        // dead slot (owner crashed).
        let t1 = t0
            .checked_add(DEFAULT_PERMIT_TTL)
            .and_then(|t| t.checked_add(std::time::Duration::from_secs(1)))
            .expect("t1");
        let p2 = limiter
            .acquire(tenant, 1, &t1)
            .await
            .expect("acquire after sweep");
        assert_ne!(p1.id, p2.id);
        assert_eq!(limiter.in_use(tenant, &t1).await.expect("use"), 1);

        // Releasing the stale permit explicitly must be refused, or the
        // gauge would go negative via double-subtraction.
        let stale = limiter.release(&p1, &t1).await.expect_err("stale release");
        assert_eq!(stale.error_code(), "CONFLICT");
        limiter.release(&p2, &t1).await.expect("release live");
        assert_eq!(limiter.in_use(tenant, &t1).await.expect("use"), 0);
    }
}
