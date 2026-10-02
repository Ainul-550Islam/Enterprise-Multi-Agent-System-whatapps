//! Retry pacing for `Nak` redeliveries: full-jitter exponential backoff.
//!
//! `delay = random(0 ..= min(cap, base * 2^(attempt-1)))` — the jitter avoids
//! thundering herds after broker/partition events, and saturating exponent
//! math makes the policy total for any `attempt` value.

use std::time::Duration;

/// Pure retry-delay calculator.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Delay after the first failed attempt (`attempt = 1`).
    pub base: Duration,
    /// Hard ceiling for any computed delay.
    pub cap: Duration,
    /// Maximum delivery count; above this a transient failure becomes `Term`
    /// regardless of anything else (dead-letter handoff lives in the
    /// consumer).
    pub max_deliver: u32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            base: Duration::from_millis(500),
            cap: Duration::from_secs(60),
            max_deliver: 10,
        }
    }
}

impl RetryPolicy {
    /// Validates the policy when built programmatically.
    pub fn validated(self) -> Result<Self, mas_common::error::AppError> {
        if self.base.is_zero() {
            return Err(mas_common::error::AppError::validation(
                "retry base must be positive",
            ));
        }
        if self.cap < self.base {
            return Err(mas_common::error::AppError::validation(
                "retry cap must be ≥ base",
            ));
        }
        if self.max_deliver == 0 {
            return Err(mas_common::error::AppError::validation(
                "max_deliver must be ≥ 1",
            ));
        }
        Ok(self)
    }

    /// Deterministic upper bound for `attempt` deliveries (saturating).
    #[must_use]
    pub fn bound_for(&self, attempt: u32) -> Duration {
        let exponent = attempt.saturating_sub(1).min(30);
        let multiplier = 1u128 << exponent;
        let nanos = self
            .base
            .as_nanos()
            .saturating_mul(multiplier)
            .min(self.cap.as_nanos());
        Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
    }

    /// Full-jitter delay (different on every call, in `[0, bound]`).
    #[must_use]
    pub fn delay_for(&self, attempt: u32) -> Duration {
        let bound = self.bound_for(attempt);
        if bound.is_zero() {
            return bound;
        }
        let nanos = rand_jitter_seed(bound.as_nanos());
        Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
    }

    /// Whether a `delivery`-th redelivery is still allowed.
    #[must_use]
    pub fn allows_redelivery(&self, delivery: u32) -> bool {
        delivery < self.max_deliver
    }
}

/// Deterministic-but-spread jitter seed without touching process-global
/// RNG state: time + a spinning local bump (tests see variation; production
/// sees entropy).
fn rand_jitter_seed(bound_nanos: u128) -> u128 {
    if bound_nanos == 0 {
        return 0;
    }
    let nanos_seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let spinning = SPIN.with(|cell| {
        let next = cell.get().wrapping_add(0x9E37_79B9_7F4A_7C15u64);
        cell.set(next);
        next
    });

    (u128::from(nanos_seed) ^ u128::from(spinning)) % bound_nanos
}

thread_local! {
    static SPIN: std::cell::Cell<u64> = const { std::cell::Cell::new(0xD1B5_4A32_D192_ED03) };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_grow_and_saturate() {
        let policy = RetryPolicy {
            base: Duration::from_millis(100),
            cap: Duration::from_secs(2),
            max_deliver: 5,
        };
        assert_eq!(policy.bound_for(1), Duration::from_millis(100));
        assert_eq!(policy.bound_for(2), Duration::from_millis(200));
        assert_eq!(policy.bound_for(3), Duration::from_millis(400));
        assert_eq!(policy.bound_for(20), Duration::from_secs(2), "cap applies");
        assert_eq!(
            policy.bound_for(u32::MAX),
            Duration::from_secs(2),
            "no overflow"
        );
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let policy = RetryPolicy::default();
        for attempt in 1..8 {
            let delay = policy.delay_for(attempt);
            assert!(delay <= policy.bound_for(attempt));
        }
        assert!(policy.validated().is_ok());
        assert!(policy.allows_redelivery(3));
        assert!(!policy.allows_redelivery(10));
        assert!(RetryPolicy {
            base: Duration::ZERO,
            cap: Duration::from_secs(1),
            max_deliver: 1
        }
        .validated()
        .is_err());
        assert!(RetryPolicy {
            base: Duration::from_secs(2),
            cap: Duration::from_secs(1),
            max_deliver: 1
        }
        .validated()
        .is_err());
    }
}
