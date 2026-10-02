//! The quota engine: registry-driven consumption decisions and the bridge
//! into orchestration's `QuotaPort`.
//!
//! Pipeline for one consumption:
//! 1. Resolve the [`mas_domain::quota::Quota`] definition from the
//!    [`QuotaRegistryPort`] (limits/enforcement are configuration, not code).
//! 2. Apply consumption to the counter window matching the quota's period
//!    (fixed global tumbling windows; gauges (`Unbounded`) use bucket 0).
//! 3. Enforce: `Enforce` rejects over-limit consumption (`allowed = false`),
//!    `WarnOnly` allows + flags `over_limit`, `AllowWithOverage` allows +
//!    marks `overage_units` — the billing side consumes this breakdown.
//!
//! Rollback (`release`) compensates optimistic reservations: on periodic
//! dimensions it only pulls a live window *back below* some in-flight amount
//! (bounded releases — you cannot un-consume past requests); on gauges it
//! decrements (slots are returned).

use std::collections::BTreeMap;
use std::sync::Mutex;

use mas_common::error::AppError;
use mas_common::ids::TenantId;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::quota::{EnforcementMode, Quota, QuotaDimension, QuotaPeriod};

use crate::counter::{CounterKey, CounterStorePort};

/// Where quota definitions come from (Postgres in production; the
/// [`InMemoryQuotaRegistry`] in tests/dev).
#[async_trait::async_trait]
pub trait QuotaRegistryPort: Send + Sync + std::fmt::Debug {
    /// The quota definition for one (tenant, dimension), if configured.
    async fn find(&self, tenant: TenantId, dimension: QuotaDimension) -> Result<Option<Quota>>;

    /// Registers/replaces a quota definition (admin paths + test seeds).
    async fn upsert(&self, quota: &Quota) -> Result<()>;
}

/// In-memory quota registry.
#[derive(Debug, Default)]
pub struct InMemoryQuotaRegistry {
    quotas: Mutex<BTreeMap<(TenantId, QuotaDimension), Quota>>,
}

impl InMemoryQuotaRegistry {
    /// Empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Test seeding convenience.
    pub fn seed(&self, quota: Quota) {
        self.quotas
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert((quota.tenant_id, quota.dimension), quota);
    }

    /// Number of registered quotas.
    #[must_use]
    pub fn len(&self) -> usize {
        self.quotas.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Whether empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait::async_trait]
impl QuotaRegistryPort for InMemoryQuotaRegistry {
    async fn find(&self, tenant: TenantId, dimension: QuotaDimension) -> Result<Option<Quota>> {
        Ok(self
            .quotas
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(tenant, dimension))
            .cloned())
    }

    async fn upsert(&self, quota: &Quota) -> Result<()> {
        self.quotas
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert((quota.tenant_id, quota.dimension), quota.clone());
        Ok(())
    }
}

/// Outcome of one consumption check.
#[derive(Debug, Clone, PartialEq)]
pub struct ConsumptionDecision {
    /// Whether the consumption passed the enforcement rules.
    pub allowed: bool,
    /// The enforcement mode that was in effect.
    pub enforcement: EnforcementMode,
    /// Consumption counted for this window after this call.
    pub consumed: u64,
    /// Configured limit.
    pub limit: u64,
    /// Remaining allowance after this call (`0` when over).
    pub remaining: u64,
    /// Over-limit flag (always true when rejected; true for soft modes too).
    pub over_limit: bool,
    /// Units beyond the limit charged as overage (`AllowWithOverage` only).
    pub overage_units: u64,
    /// When the window ends (`None` for gauges).
    pub reset_at: Option<Timestamp>,
    /// Retry hint when rejected.
    pub retry_after: Option<std::time::Duration>,
}

/// The engine: a registry plus a counter store.
#[derive(Debug)]
pub struct QuotaEngine<S, R>
where
    S: CounterStorePort,
    R: QuotaRegistryPort,
{
    counters: S,
    registry: R,
}

impl<S: CounterStorePort, R: QuotaRegistryPort> QuotaEngine<S, R> {
    /// Builds an engine from its two ports.
    #[must_use]
    pub fn new(counters: S, registry: R) -> Self {
        Self { counters, registry }
    }

    /// Registers a quota definition.
    pub async fn register(&self, quota: &Quota) -> Result<()> {
        self.registry.upsert(quota).await
    }

    /// Checks and consumes `amount` for one tenant/dimension at `now`.
    ///
    /// Missing quota definition = **unlimited** (consumption is recorded for
    /// metering but never rejected); configuration drives enforcement, so a
    /// tenant without a definition is not silently blocked.
    pub async fn consume(
        &self,
        tenant: TenantId,
        dimension: QuotaDimension,
        amount: u64,
        now: &Timestamp,
    ) -> Result<ConsumptionDecision> {
        if amount == 0 {
            return Err(AppError::validation(
                "quota consumption amount must be positive",
            ));
        }
        let Some(quota) = self.registry.find(tenant, dimension).await? else {
            // No definition → record for metering, never reject.
            let key = CounterKey::for_time(tenant, dimension, Some(3_600), now);
            let consumed = self.counters.increment(key, amount).await?;
            return Ok(ConsumptionDecision {
                allowed: true,
                enforcement: EnforcementMode::WarnOnly,
                consumed,
                limit: u64::MAX,
                remaining: u64::MAX,
                over_limit: false,
                overage_units: 0,
                reset_at: None,
                retry_after: None,
            });
        };

        let window = window_seconds(quota.period);
        let key = CounterKey::for_time(tenant, dimension, window, now);
        let consumed = self.counters.increment(key, amount).await?;
        let over_limit = consumed > quota.limit;
        let overage_units = consumed.saturating_sub(quota.limit);
        let reset_at = window.and_then(|w| key.window_end(w));
        let retry_after = reset_at.and_then(|end| end.duration_since(now));

        match (quota.enforcement, over_limit) {
            (EnforcementMode::Enforce, true) => {
                // Roll the just-added amount back: Enforce-mode consumption
                // is atomic check-and-reserve, never "count and reject".
                let _ = self.counters.decrement(key, amount).await?;
                Ok(ConsumptionDecision {
                    allowed: false,
                    enforcement: quota.enforcement,
                    consumed: consumed - amount,
                    limit: quota.limit,
                    remaining: quota.limit.saturating_sub(consumed - amount),
                    over_limit: true,
                    overage_units: 0,
                    reset_at,
                    retry_after,
                })
            },
            (mode, over) => Ok(ConsumptionDecision {
                allowed: true,
                enforcement: mode,
                consumed,
                limit: quota.limit,
                remaining: quota.limit.saturating_sub(consumed),
                over_limit: over,
                overage_units: if mode == EnforcementMode::AllowWithOverage {
                    overage_units
                } else {
                    0
                },
                reset_at,
                retry_after: None,
            }),
        }
    }

    /// Current window usage (no consumption; gauges read the gauge).
    pub async fn usage(
        &self,
        tenant: TenantId,
        dimension: QuotaDimension,
        now: &Timestamp,
    ) -> Result<u64> {
        let quota = self.registry.find(tenant, dimension).await?;
        let window = quota
            .as_ref()
            .map(|q| window_seconds(q.period))
            .unwrap_or(Some(3_600));
        let key = CounterKey::for_time(tenant, dimension, window, now);
        self.counters.get(key).await
    }

    /// Compensates a previous reservation (`Terminal cleanup` in the engine
    /// lifecycle). Gauge dimensions give the slot back; periodic dimensions
    /// decrement (annotation: usage remains metered in the usage stream —
    /// releasing quota is *not* un-billing).
    pub async fn release(
        &self,
        tenant: TenantId,
        dimension: QuotaDimension,
        amount: u64,
        now: &Timestamp,
    ) -> Result<()> {
        if amount == 0 {
            return Ok(());
        }
        let quota = self.registry.find(tenant, dimension).await?;
        let window = quota
            .as_ref()
            .map(|q| window_seconds(q.period))
            .unwrap_or(Some(3_600));
        let key = CounterKey::for_time(tenant, dimension, window, now);
        let _ = self.counters.decrement(key, amount).await?;
        Ok(())
    }
}

/// Maps a quota period to its counter window (`None` = gauge bucket).
#[must_use]
pub fn window_seconds(period: QuotaPeriod) -> Option<u64> {
    match period {
        QuotaPeriod::PerMinute => Some(60),
        QuotaPeriod::Hourly => Some(3_600),
        QuotaPeriod::Daily => Some(86_400),
        QuotaPeriod::Monthly => Some(2_592_000),
        QuotaPeriod::Unbounded => None,
    }
}

/// The orchestration bridge: `QuotaPort` from the engine crate implemented
/// over the [`QuotaEngine`], mapping decisions to `RATE_LIMITED` errors.
#[derive(Debug)]
pub struct EngineQuotaBridge<S, R>
where
    S: CounterStorePort,
    R: QuotaRegistryPort,
{
    engine: QuotaEngine<S, R>,
}

impl<S: CounterStorePort, R: QuotaRegistryPort> EngineQuotaBridge<S, R> {
    /// Wraps an engine.
    #[must_use]
    pub fn new(engine: QuotaEngine<S, R>) -> Self {
        Self { engine }
    }

    /// The wrapped engine (registration & metering access).
    #[must_use]
    pub fn engine(&self) -> &QuotaEngine<S, R> {
        &self.engine
    }
}

#[async_trait::async_trait]
impl<S, R> mas_orchestration::engine::QuotaPort for EngineQuotaBridge<S, R>
where
    S: CounterStorePort,
    R: QuotaRegistryPort,
{
    async fn check_and_reserve(
        &self,
        tenant_id: TenantId,
        dimension: QuotaDimension,
        amount: u64,
    ) -> Result<()> {
        let now = Timestamp::now();
        let decision = self
            .engine
            .consume(tenant_id, dimension, amount, &now)
            .await?;
        if !decision.allowed {
            let mut error = AppError::rate_limited(format!(
                "{dimension} quota exceeded for tenant {tenant_id} ({}/{})",
                decision.consumed, decision.limit
            ));
            if let Some(retry) = decision.retry_after {
                error = error.with_context(format!("retry_after_seconds={}", retry.as_secs()));
            }
            return Err(error);
        }
        Ok(())
    }

    async fn release(
        &self,
        tenant_id: TenantId,
        dimension: QuotaDimension,
        amount: u64,
    ) -> Result<()> {
        self.engine
            .release(tenant_id, dimension, amount, &Timestamp::now())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::counter::InMemoryCounterStore;
    use mas_orchestration::engine::QuotaPort;

    fn engine_pair() -> (
        QuotaEngine<InMemoryCounterStore, InMemoryQuotaRegistry>,
        TenantId,
    ) {
        let engine = QuotaEngine::new(InMemoryCounterStore::new(), InMemoryQuotaRegistry::new());
        (engine, TenantId::new())
    }

    fn quota(tenant: TenantId, limit: u64, mode: EnforcementMode) -> Quota {
        Quota::new(
            tenant,
            QuotaDimension::Executions,
            QuotaPeriod::PerMinute,
            limit,
            mode,
        )
        .expect("quota")
    }

    #[tokio::test]
    async fn enforce_mode_rejects_atomically_and_retries_next_window() {
        let (engine, tenant) = engine_pair();
        engine
            .register(&quota(tenant, 2, EnforcementMode::Enforce))
            .await
            .expect("register");
        let t0 = Timestamp::from_unix_seconds(1_700_000_000).expect("t0");

        let d1 = engine
            .consume(tenant, QuotaDimension::Executions, 1, &t0)
            .await
            .expect("c1");
        assert!(d1.allowed && d1.remaining == 1);
        let d2 = engine
            .consume(tenant, QuotaDimension::Executions, 1, &t0)
            .await
            .expect("c2");
        assert!(d2.allowed && d2.remaining == 0);
        let d3 = engine
            .consume(tenant, QuotaDimension::Executions, 1, &t0)
            .await
            .expect("c3");
        assert!(!d3.allowed);
        assert_eq!(
            d3.consumed, 2,
            "rejection is rolled back — consumption stays honest"
        );
        assert!(d3.over_limit && d3.retry_after.is_some());

        // The rejection must NOT have overshot the counter.
        assert_eq!(
            engine
                .usage(tenant, QuotaDimension::Executions, &t0)
                .await
                .expect("usage"),
            2
        );

        // Next minute: fresh allowance.
        let t1 = Timestamp::from_unix_seconds(1_700_000_060).expect("t1");
        let d4 = engine
            .consume(tenant, QuotaDimension::Executions, 1, &t1)
            .await
            .expect("c4");
        assert!(d4.allowed);
    }

    #[tokio::test]
    async fn soft_modes_allow_and_flag() {
        let (engine, tenant) = engine_pair();
        engine
            .register(&quota(tenant, 1, EnforcementMode::WarnOnly))
            .await
            .expect("warn");
        let now = Timestamp::now();
        let first = engine
            .consume(tenant, QuotaDimension::Executions, 1, &now)
            .await
            .expect("first");
        assert!(first.allowed && !first.over_limit);
        let second = engine
            .consume(tenant, QuotaDimension::Executions, 1, &now)
            .await
            .expect("second");
        assert!(second.allowed && second.over_limit && second.overage_units == 0);

        let (engine2, tenant2) = engine_pair();
        engine2
            .register(&quota(tenant2, 1, EnforcementMode::AllowWithOverage))
            .await
            .expect("overage");
        engine2
            .consume(tenant2, QuotaDimension::Executions, 1, &now)
            .await
            .expect("within");
        let over = engine2
            .consume(tenant2, QuotaDimension::Executions, 2, &now)
            .await
            .expect("over");
        assert!(over.allowed && over.over_limit);
        assert_eq!(
            over.overage_units, 2,
            "3 of 1 allowed → 2 billed as overage"
        );
    }

    #[tokio::test]
    async fn unregistered_tenant_is_metered_but_unlimited() {
        let (engine, tenant) = engine_pair();
        let now = Timestamp::now();
        let d = engine
            .consume(tenant, QuotaDimension::Requests, 10_000, &now)
            .await
            .expect("no quota → unlimited");
        assert!(d.allowed && !d.over_limit);
        assert_eq!(
            engine
                .usage(tenant, QuotaDimension::Requests, &now)
                .await
                .expect("usage"),
            10_000,
            "traffic is still metered for later backfill"
        );
    }

    #[tokio::test]
    async fn bridge_maps_decisions_to_rate_limited_and_releases() {
        let (engine, tenant) = engine_pair();
        let registry_store = engine
            .registry
            .find(tenant, QuotaDimension::Executions)
            .await
            .expect("find");
        assert!(registry_store.is_none());
        let bridge = EngineQuotaBridge::new(engine);
        bridge
            .engine()
            .register(&quota(tenant, 1, EnforcementMode::Enforce))
            .await
            .expect("register");

        bridge
            .check_and_reserve(tenant, QuotaDimension::Executions, 1)
            .await
            .expect("reserve");
        let err = bridge
            .check_and_reserve(tenant, QuotaDimension::Executions, 1)
            .await
            .expect_err("second reserve over limit");
        assert_eq!(err.error_code(), "RATE_LIMITED");

        bridge
            .release(tenant, QuotaDimension::Executions, 1)
            .await
            .expect("release");
        let usage = bridge
            .engine()
            .usage(tenant, QuotaDimension::Executions, &Timestamp::now())
            .await
            .expect("usage");
        assert_eq!(usage, 0, "release returned the reserved unit to the window");

        // Reserving again now succeeds.
        bridge
            .check_and_reserve(tenant, QuotaDimension::Executions, 1)
            .await
            .expect("reserve after release");
    }
}
