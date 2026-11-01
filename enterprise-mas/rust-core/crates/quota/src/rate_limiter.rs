//! Fixed-window rate limiter built on the counter store.
//!
//! Fixed window (not sliding-log): constant memory per (tenant, dimension),
//! atomic check+count in one increment, and a documented edge — up to 2× the
//! nominal rate may pass exactly at a window boundary. For the platform's
//! request/API-call throttles that's the right trade (precise neighbors of a
//! window aren't adversarial targets: billing-level precision lives in the
//! token meter).

use mas_common::error::AppError;
use mas_common::ids::TenantId;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::quota::QuotaDimension;

use crate::counter::{CounterKey, CounterStorePort};

/// Bounded rate-limit configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimit {
    /// Allowed events per window.
    pub limit: u64,
    /// Window length in seconds.
    pub window_seconds: u64,
}

impl RateLimit {
    /// Validates: window 1s..=86400s, limit > 0.
    pub fn new(limit: u64, window_seconds: u64) -> Result<Self> {
        if limit == 0 {
            return Err(AppError::invalid_field(
                "limit",
                "out_of_range",
                "rate limit must be positive",
            ));
        }
        if window_seconds == 0 || window_seconds > 86_400 {
            return Err(AppError::invalid_field(
                "window_seconds",
                "out_of_range",
                "window must be 1..=86400 seconds",
            ));
        }
        Ok(Self {
            limit,
            window_seconds,
        })
    }
}

/// Outcome of a rate-limit check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateDecision {
    /// Whether the event passes.
    pub allowed: bool,
    /// Events consumed in this window after this check.
    pub consumed: u64,
    /// Remaining allowance in this window (after this check).
    pub remaining: u64,
    /// When the current window ends (ms precision).
    pub reset_at: Timestamp,
    /// How long to wait before retrying when rejected.
    pub retry_after: Option<std::time::Duration>,
}

/// A fixed-window rate limiter for one dimension.
#[derive(Debug)]
pub struct RateLimiter<S: CounterStorePort> {
    store: S,
    dimension: QuotaDimension,
}

impl<S: CounterStorePort> RateLimiter<S> {
    /// Binds a limiter for one dimension over a counter store.
    #[must_use]
    pub fn new(store: S, dimension: QuotaDimension) -> Self {
        Self { store, dimension }
    }

    /// Consumes `amount` events for `tenant`, enforcing `limit`.
    ///
    /// Atomicity note: increment-first then compare (optimistic), which keeps
    /// the operation a single store call; under racing bursts the value may
    /// transiently overshoot by one racing amount — the response reports the
    /// true consumed number.
    pub async fn check(
        &self,
        tenant: TenantId,
        limit: &RateLimit,
        amount: u64,
        now: &Timestamp,
    ) -> Result<RateDecision> {
        if amount == 0 {
            return Err(AppError::validation("rate-limit amount must be positive"));
        }
        let key = CounterKey::for_time(tenant, self.dimension, Some(limit.window_seconds), now);
        let consumed = self.store.increment(key, amount).await?;
        let remaining = limit.limit.saturating_sub(consumed);
        let allowed = consumed <= limit.limit;
        let reset_at = key
            .window_end(limit.window_seconds)
            .ok_or_else(|| AppError::internal("rate limit window math overflow"))?;
        let retry_after = if allowed {
            None
        } else {
            reset_at.duration_since(now)
        };
        Ok(RateDecision {
            allowed,
            consumed,
            remaining,
            reset_at,
            retry_after,
        })
    }

    /// Current window usage without consuming anything.
    pub async fn usage(
        &self,
        tenant: TenantId,
        window_seconds: u64,
        now: &Timestamp,
    ) -> Result<u64> {
        let key = CounterKey::for_time(tenant, self.dimension, Some(window_seconds), now);
        self.store.get(key).await
    }

    /// Maps a decision to the platform error when rejected, including the
    /// structured retry hint (the API layer surfaces it as `Retry-After`).
    pub fn error_for(
        decision: &RateDecision,
        tenant: TenantId,
        dimension: QuotaDimension,
    ) -> AppError {
        let mut error = AppError::rate_limited(format!(
            "{dimension} rate limit exceeded for tenant {tenant} ({} events this window)",
            decision.consumed
        ));
        if let Some(retry) = decision.retry_after {
            error = error.with_context(format!("retry_after_seconds={}", retry.as_secs()));
        }
        error
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::counter::InMemoryCounterStore;

    #[tokio::test]
    async fn window_enforces_and_resets() {
        let store = InMemoryCounterStore::new();
        let limiter = RateLimiter::new(store, QuotaDimension::ApiCalls);
        let tenant = TenantId::new();
        let limit = RateLimit::new(3, 60).expect("limit");
        let t0 = Timestamp::from_unix_seconds(1_700_000_000).expect("t0");

        for expected_consumed in [1u64, 2, 3] {
            let d = limiter.check(tenant, &limit, 1, &t0).await.expect("check");
            assert!(d.allowed);
            assert_eq!(d.consumed, expected_consumed);
            assert_eq!(d.retry_after, None);
        }

        // 4th request rejected with a retry hint aligned to the window end.
        let d = limiter.check(tenant, &limit, 1, &t0).await.expect("check");
        assert!(!d.allowed);
        assert_eq!(d.remaining, 0);
        assert_eq!(d.consumed, 4, "rejection counted (reported back honestly)");
        let retry = d.retry_after.expect("hint");
        assert!(retry.as_secs() <= 60);
        let err =
            RateLimiter::<InMemoryCounterStore>::error_for(&d, tenant, QuotaDimension::ApiCalls);
        assert_eq!(err.error_code(), "RATE_LIMITED");

        // Other tenant unaffected.
        let d2 = limiter
            .check(TenantId::new(), &limit, 1, &t0)
            .await
            .expect("other tenant");
        assert!(d2.allowed);

        // Next window: allowance restored.
        let t1 = Timestamp::from_unix_seconds(1_700_000_000 + 60).expect("t1");
        let d3 = limiter
            .check(tenant, &limit, 2, &t1)
            .await
            .expect("new window");
        assert!(d3.allowed);
        assert_eq!(d3.consumed, 2);
        assert_eq!(limiter.usage(tenant, 60, &t1).await.expect("usage"), 2);
    }

    #[tokio::test]
    async fn oversized_amounts_rejected_atomically() {
        let limiter = RateLimiter::new(InMemoryCounterStore::new(), QuotaDimension::Requests);
        let limit = RateLimit::new(5, 10).expect("limit");
        let now = Timestamp::now();
        let d = limiter
            .check(TenantId::new(), &limit, 6, &now)
            .await
            .expect("check");
        assert!(!d.allowed);
        let d = limiter.check(TenantId::new(), &limit, 0, &now).await;
        assert!(d.is_err(), "zero amount is an API misuse");
    }
}
