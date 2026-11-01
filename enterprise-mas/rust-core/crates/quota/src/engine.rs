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

use crate::counter::{CounterKey, CounterStorePort, ReservationAttempt};

/// Where quota definitions come from (Postgres in production; the
/// [`InMemoryQuotaRegistry`] in tests/dev).
#[async_trait::async_trait]
pub trait QuotaRegistryPort: Send + Sync + std::fmt::Debug {
    /// The quota definition for one (tenant, dimension), if configured.
    async fn find(&self, tenant: TenantId, dimension: QuotaDimension) -> Result<Option<Quota>>;

    /// Registers/replaces a quota definition (admin paths + test seeds).
    async fn upsert(&self, quota: &Quota) -> Result<()>;
}

/// Forwards every operation through an `Arc` (same reason as the counter
/// store's forwarding impl: one shared store, `dyn`-friendly services).
#[async_trait::async_trait]
impl<T: QuotaRegistryPort + ?Sized> QuotaRegistryPort for std::sync::Arc<T> {
    async fn find(&self, tenant: TenantId, dimension: QuotaDimension) -> Result<Option<Quota>> {
        (**self).find(tenant, dimension).await
    }

    async fn upsert(&self, quota: &Quota) -> Result<()> {
        (**self).upsert(quota).await
    }
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

    /// The registry behind the engine.
    ///
    /// Exposed for one question: *is a ceiling configured for this
    /// dimension at all?* A caller that cannot ask it has to either hold a
    /// reservation for every tenant — rows and counter churn for a limit
    /// nobody set — or enforce a limit nobody asked for. Both are wrong, and
    /// neither is visible from the engine's other methods, which deliberately
    /// answer "allowed" when no definition exists.
    #[must_use]
    pub fn registry(&self) -> &R {
        &self.registry
    }

    /// Reads whether one consumption would fit without changing a counter.
    ///
    /// This is intentionally an *advisory* check: a distributed caller must
    /// still use [`Self::consume`] as its linearizable authorization point.
    /// It exists for in-memory composition, where the application holds its
    /// local submission gate across this check, durable creation, and the
    /// subsequent consume. That prevents a quota refusal from materialising a
    /// runnable test/dev execution while preserving the production database
    /// transaction as the authority across replicas.
    pub async fn preflight(
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
        let quota = self.registry.find(tenant, dimension).await?;
        let window = quota
            .as_ref()
            .map(|definition| window_seconds(definition.period))
            .unwrap_or(Some(3_600));
        let key = CounterKey::for_time(tenant, dimension, window, now);
        let consumed = self.counters.get(key).await?;
        let projected = consumed.saturating_add(amount);

        let Some(quota) = quota else {
            return Ok(ConsumptionDecision {
                allowed: true,
                enforcement: EnforcementMode::WarnOnly,
                consumed: projected,
                limit: u64::MAX,
                remaining: u64::MAX,
                over_limit: false,
                overage_units: 0,
                reset_at: None,
                retry_after: None,
            });
        };

        let over_limit = projected > quota.limit;
        let reset_at = window.and_then(|seconds| key.window_end(seconds));
        let retry_after = reset_at.and_then(|end| end.duration_since(now));
        Ok(ConsumptionDecision {
            allowed: quota.enforcement != EnforcementMode::Enforce || !over_limit,
            enforcement: quota.enforcement,
            consumed: projected,
            limit: quota.limit,
            remaining: quota.limit.saturating_sub(projected),
            over_limit,
            overage_units: if quota.enforcement == EnforcementMode::AllowWithOverage {
                projected.saturating_sub(quota.limit)
            } else {
                0
            },
            reset_at,
            retry_after: if over_limit && quota.enforcement == EnforcementMode::Enforce {
                retry_after
            } else {
                None
            },
        })
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
        let reset_at = window.and_then(|w| key.window_end(w));
        let retry_after = reset_at.and_then(|end| end.duration_since(now));

        // One round trip decides and consumes. The previous shape —
        // `increment`, compare, then `decrement` on rejection — was two round
        // trips with a read-modify-write between them, so two replicas could
        // each observe "still under the limit" and both commit. It also left
        // the counter inflated for the duration of the gap, and permanently
        // inflated if the compensating decrement never ran.
        let attempt: ReservationAttempt =
            self.counters.try_reserve(key, amount, quota.limit).await?;
        let over_limit = !attempt.allowed;

        match (quota.enforcement, over_limit) {
            (EnforcementMode::Enforce, true) => {
                // `Enforce` rejected the reservation, so the store wrote
                // nothing: `attempt.value` is the *projected* total, not the
                // stored one. Reporting what the counter actually holds keeps
                // the arithmetic a caller sees consistent with `usage()`.
                let held = attempt.value.saturating_sub(amount);
                Ok(ConsumptionDecision {
                    allowed: false,
                    enforcement: quota.enforcement,
                    consumed: held,
                    limit: quota.limit,
                    remaining: quota.limit.saturating_sub(held),
                    over_limit: true,
                    overage_units: 0,
                    reset_at,
                    retry_after,
                })
            },
            // `WarnOnly` and `AllowWithOverage` must consume even past the
            // ceiling — that is what distinguishes them from `Enforce` — so
            // they cannot use the guarded primitive. They take the plain
            // increment, and the overage they produce is deliberate.
            (EnforcementMode::WarnOnly, _) | (EnforcementMode::AllowWithOverage, _)
                if over_limit =>
            {
                let consumed = self.counters.increment(key, amount).await?;
                Ok(ConsumptionDecision {
                    allowed: true,
                    enforcement: quota.enforcement,
                    consumed,
                    limit: quota.limit,
                    remaining: 0,
                    over_limit: true,
                    overage_units: if quota.enforcement == EnforcementMode::AllowWithOverage {
                        consumed.saturating_sub(quota.limit)
                    } else {
                        0
                    },
                    reset_at,
                    retry_after: None,
                })
            },
            (mode, _) => Ok(ConsumptionDecision {
                allowed: true,
                enforcement: mode,
                consumed: attempt.value,
                limit: quota.limit,
                remaining: attempt.remaining(),
                over_limit: false,
                overage_units: 0,
                reset_at,
                retry_after: None,
            }),
        }
    }

    /// The counter window a dimension is metered over, when configured.
    ///
    /// Callers that name a bucket directly — the reservation path does, so a
    /// settlement can find the same window the charge landed in — need this.
    /// `None` means the gauge bucket (0), used by dimensions that never reset.
    pub async fn window_seconds_for(
        &self,
        tenant: TenantId,
        dimension: QuotaDimension,
    ) -> Option<u64> {
        match self.registry.find(tenant, dimension).await {
            Ok(Some(quota)) => window_seconds(quota.period),
            // No definition ⇒ the engine meters on an hourly window; callers
            // must use the same default or they will address a different
            // bucket than the one that was charged.
            _ => Some(3_600),
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
            // QUOTA_EXCEEDED, not RATE_LIMITED. They are different facts:
            // rate limiting is transient back-pressure, quota exhaustion is
            // an entitlement statement about the tenant's plan. Mapping both
            // to `RATE_LIMITED` made every quota denial retryable, so a worker
            // would spend its whole attempt budget re-submitting work that
            // could not succeed until the billing period rolled over.
            return Err(AppError::quota_exceeded(
                dimension.as_str(),
                decision.consumed,
                decision.limit,
                decision.retry_after,
                format!(
                    "{dimension} quota exceeded for tenant {tenant_id} ({}/{})",
                    decision.consumed, decision.limit
                ),
            ));
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
    async fn bridge_maps_denials_to_quota_exceeded_and_releases() {
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
        // The whole point of the dedicated variant: a quota denial is its own
        // class, is not retryable, and carries the arithmetic.
        assert_eq!(err.error_code(), "QUOTA_EXCEEDED");
        assert!(!err.is_retryable(), "a quota denial must not be retried");
        assert!(
            matches!(
                &err,
                AppError::QuotaExceeded {
                    dimension,
                    consumed: 1,
                    limit: 1,
                    ..
                } if dimension == "executions"
            ),
            "the denial names the dimension and the arithmetic, got {err:?}"
        );

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
