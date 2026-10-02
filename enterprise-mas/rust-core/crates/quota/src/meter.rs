//! Token-bucket metering: smooth burst control for metered consumption
//! (LLM tokens, tool invocations, api calls) that billing actually charges
//! for. Unlike the fixed-window rate limiter, a token bucket is *continuous*
//! — refill rate × elapsed time — so there is no 2× window-edge effect and
//! cost spikes track exactly.

use mas_common::error::AppError;
use mas_common::ids::TenantId;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::quota::QuotaDimension;

use crate::counter::{CounterKey, CounterStorePort};

/// Bucket configuration (validated).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenBucketConfig {
    /// Maximum tokens the bucket ever holds (the burst ceiling).
    pub capacity: u64,
    /// Tokens refilled per second (the sustainable rate).
    pub refill_per_second: u64,
}

impl TokenBucketConfig {
    /// Validates: positive capacity; refill may be 0 (fixed budget, never replenished).
    pub fn new(capacity: u64, refill_per_second: u64) -> Result<Self> {
        if capacity == 0 {
            return Err(AppError::invalid_field(
                "capacity",
                "out_of_range",
                "bucket capacity must be positive",
            ));
        }
        Ok(Self {
            capacity,
            refill_per_second,
        })
    }
}

/// Attempt result.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TokenDecision {
    /// The request passed (tokens were deducted).
    pub allowed: bool,
    /// Tokens remaining after this attempt.
    pub available: u64,
    /// Bucket capacity.
    pub capacity: u64,
    /// Seconds until enough tokens exist for the rejected amount (`None` when
    /// allowed, or when the bucket never refills).
    pub deficit_seconds: Option<u64>,
}

/// The token meter over a counter store.
///
/// State lives in the gauge bucket (bucket 0): the counter holds the
/// available-tokens value; the bucket's *key bucket number* double-duty as
/// the `last_refill` unix-second marker is impossible (keys are identity), so
/// the meter keeps refill timestamps in the store under a reserved parallel
/// key dimension? No — statefulness belongs to the meter: [`TokenMeter`]
/// tracks `last_refill` per (tenant, dimension) in-process and the store
/// holds only the token balances. For multi-process deployments the Redis
/// adapter implements [`crate::counter::CounterStorePort`] with Lua-side
/// compare-and-refill; semantics documented here stay identical.
#[derive(Debug)]
pub struct TokenMeter<S: CounterStorePort> {
    store: S,
    dimension: QuotaDimension,
    refill_marks:
        std::sync::Mutex<std::collections::BTreeMap<(TenantId, QuotaDimension), Timestamp>>,
}

impl<S: CounterStorePort> TokenMeter<S> {
    /// Binds a meter to one dimension.
    #[must_use]
    pub fn new(store: S, dimension: QuotaDimension) -> Self {
        Self {
            store,
            dimension,
            refill_marks: std::sync::Mutex::new(std::collections::BTreeMap::new()),
        }
    }

    fn balance_key(&self, tenant: TenantId, at: &Timestamp) -> CounterKey {
        CounterKey::for_time(tenant, self.dimension, None, at)
    }

    /// Tries to consume `amount` tokens at `now`. Refill is lazy: applied
    /// when the bucket is next touched, capped at capacity.
    pub async fn consume(
        &self,
        tenant: TenantId,
        config: &TokenBucketConfig,
        amount: u64,
        now: &Timestamp,
    ) -> Result<TokenDecision> {
        if amount == 0 {
            return Err(AppError::validation("token amount must be positive"));
        }
        let key = self.balance_key(tenant, now);

        // 1) Lazy refill. A touched bucket starts at its last stored level;
        //    an untouched bucket starts full (capacity), per token-bucket
        //    semantics. Refill accrues for time since the last touch.
        // Copy the touch-mark under a short-lived lock (never held across an
        // await); rewrite it after the store read.
        let last_touch: Option<Timestamp> = {
            let marks = self.refill_marks.lock().unwrap_or_else(|e| e.into_inner());
            marks.get(&(tenant, self.dimension)).copied()
        };
        let (level_before, last_touch) = match last_touch {
            Some(mark) => (self.store.get(key).await?, mark),
            // Untouched bucket starts at full capacity.
            None => (config.capacity, *now),
        };
        let new_level = {
            let elapsed = now
                .duration_since(&last_touch)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let refill = config.refill_per_second.saturating_mul(elapsed);
            let mut marks = self.refill_marks.lock().unwrap_or_else(|e| e.into_inner());
            marks.insert((tenant, self.dimension), *now);
            level_before.saturating_add(refill).min(config.capacity)
        };

        // 2) Spend if affordable; reject honestly (with a deficit hint)
        //    otherwise — no partial spends. Either way the recomputed level
        //    is persisted: the refill just accrued MUST survive, or updating
        //    the touch-mark without saving silently burns refill time.
        if new_level < amount {
            self.set_balance(key, new_level).await?;
            return Ok(TokenDecision {
                allowed: false,
                available: new_level,
                capacity: config.capacity,
                deficit_seconds: if config.refill_per_second == 0 {
                    None
                } else {
                    Some((amount - new_level).div_ceil(config.refill_per_second))
                },
            });
        }
        let remaining = new_level - amount;
        self.set_balance(key, remaining).await?;
        Ok(TokenDecision {
            allowed: true,
            available: remaining,
            capacity: config.capacity,
            deficit_seconds: None,
        })
    }

    /// Current balance with lazy refill applied (no consumption).
    pub async fn balance(
        &self,
        tenant: TenantId,
        config: &TokenBucketConfig,
        now: &Timestamp,
    ) -> Result<u64> {
        let decision = self.consume(tenant, config, 1, now).await;
        match decision {
            Ok(d) if d.allowed => {
                // give the probe token back (peek semantics)
                let key = self.balance_key(tenant, now);
                self.set_balance(key, d.available + 1).await?;
                Ok(d.available + 1)
            },
            _ => {
                let key = self.balance_key(tenant, now);
                self.store.get(key).await
            },
        }
    }

    async fn set_balance(&self, key: CounterKey, level: u64) -> Result<()> {
        // Counters are monotonic add-only; rebalance via delta.
        let current = self.store.get(key).await?;
        if level >= current {
            let _ = self.store.increment(key, level - current).await?;
        } else {
            let _ = self.store.decrement(key, current - level).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::counter::InMemoryCounterStore;

    fn at(secs: i64) -> Timestamp {
        Timestamp::from_unix_seconds(1_700_000_000 + secs).expect("ts")
    }

    #[tokio::test]
    async fn bucket_drains_refills_and_caps() {
        let meter = TokenMeter::new(InMemoryCounterStore::new(), QuotaDimension::Tokens);
        let tenant = TenantId::new();
        let config = TokenBucketConfig::new(100, 10).expect("config"); // 10 tok/s

        // Fresh bucket is full.
        let d = meter
            .consume(tenant, &config, 40, &at(0))
            .await
            .expect("consume");
        assert!(d.allowed && d.available == 60);

        let d = meter
            .consume(tenant, &config, 60, &at(5))
            .await
            .expect("consume");
        assert!(d.allowed, "60 + 50 refill = 110 → capped 100 funds 60");
        assert_eq!(d.available, 40);

        // Heavy ask beyond the current level is always rejected, with a
        // deficit hint (100 short ÷ 10 tok/s = 10 seconds).
        let d = meter
            .consume(tenant, &config, 150, &at(6))
            .await
            .expect("big ask");
        assert!(!d.allowed);
        assert_eq!(d.deficit_seconds, Some(10));

        // Level after the rejection, at t=6: 40 + 10 refill = 50 → 50 spends.
        let d = meter
            .consume(tenant, &config, 50, &at(6))
            .await
            .expect("spend");
        assert!(d.allowed);
        assert_eq!(d.available, 0);

        // Long idle → capped at capacity (never above); the spend peeks 90.
        let d = meter
            .consume(tenant, &config, 10, &at(10_000))
            .await
            .expect("consume late");
        assert!(d.allowed);
        assert_eq!(
            d.available, 90,
            "bucket was full at capacity 100 before spending 10"
        );

        // Zero-refill budget: drains and never recovers.
        let dry = TokenBucketConfig::new(5, 0).expect("dry");
        let other = TenantId::new();
        let d = meter.consume(other, &dry, 5, &at(0)).await.expect("drain");
        assert!(d.allowed && d.available == 0);
        let d = meter
            .consume(other, &dry, 1, &at(10_000))
            .await
            .expect("empty");
        assert!(!d.allowed);
        assert_eq!(d.deficit_seconds, None);
    }

    #[tokio::test]
    async fn peek_does_not_spend() {
        let meter = TokenMeter::new(InMemoryCounterStore::new(), QuotaDimension::Tokens);
        let tenant = TenantId::new();
        let config = TokenBucketConfig::new(10, 1).expect("config");
        let b1 = meter.balance(tenant, &config, &at(0)).await.expect("peek");
        let b2 = meter
            .balance(tenant, &config, &at(1))
            .await
            .expect("peek again");
        assert!(b1 >= b2 - 1, "peek must not burn the bucket ({b1} vs {b2})");
    }
}
