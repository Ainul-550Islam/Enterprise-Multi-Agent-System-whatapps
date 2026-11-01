//! Retry policies: backoff strategies with jitter and retryable-error
//! classification.
//!
//! Classification rule: the *category* of an [`AppError`] decides whether a
//! retry is meaningful (`AppError::is_retryable`); this module decides *when*
//! and *how often*.

use mas_common::constants;
use mas_common::error::AppError;
use mas_common::result::Result;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// How the delay between attempts grows.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BackoffStrategy {
    /// Constant delay every time.
    Fixed { delay_ms: u64 },
    /// `base * factor^(attempt-1)`, capped at `max_delay_ms`.
    Exponential {
        base_ms: u64,
        factor: u32,
        max_delay_ms: u64,
    },
    /// Exponential, then multiplied by a uniform jitter in `[0.5, 1.5]` and
    /// re-capped. Default for production loads (prevents thundering herds).
    ExponentialWithJitter {
        base_ms: u64,
        factor: u32,
        max_delay_ms: u64,
    },
}

impl Default for BackoffStrategy {
    fn default() -> Self {
        Self::ExponentialWithJitter {
            base_ms: constants::DEFAULT_RETRY_BACKOFF_MS,
            factor: 2,
            max_delay_ms: constants::MAX_RETRY_BACKOFF_MS,
        }
    }
}

impl BackoffStrategy {
    /// Validates strategy parameters (factor ≥ 1, sane caps).
    pub fn validate(&self) -> Result<()> {
        let (base_ms, factor, max_delay_ms) = match *self {
            Self::Fixed { delay_ms } => (delay_ms, 1, delay_ms),
            Self::Exponential {
                base_ms,
                factor,
                max_delay_ms,
            }
            | Self::ExponentialWithJitter {
                base_ms,
                factor,
                max_delay_ms,
            } => (base_ms, factor, max_delay_ms),
        };
        if base_ms == 0 {
            return Err(AppError::invalid_field(
                "base_ms",
                "out_of_range",
                "backoff base must be positive",
            ));
        }
        if factor == 0 {
            return Err(AppError::invalid_field(
                "factor",
                "out_of_range",
                "backoff factor must be >= 1",
            ));
        }
        if max_delay_ms > 10 * 60_000 {
            return Err(AppError::invalid_field(
                "max_delay_ms",
                "out_of_range",
                "backoff cap may not exceed 10 minutes",
            ));
        }
        Ok(())
    }

    /// Raw (pre-jitter) delay for `attempt` (1-based).
    #[must_use]
    fn base_delay(&self, attempt: u32) -> Duration {
        match *self {
            Self::Fixed { delay_ms } => Duration::from_millis(delay_ms),
            Self::Exponential {
                base_ms,
                factor,
                max_delay_ms,
            }
            | Self::ExponentialWithJitter {
                base_ms,
                factor,
                max_delay_ms,
            } => {
                let exponent = attempt.saturating_sub(1).min(30);
                let multiplier = (factor as u64).checked_pow(exponent).unwrap_or(u64::MAX);
                let delay = base_ms.saturating_mul(multiplier).min(max_delay_ms);
                Duration::from_millis(delay)
            },
        }
    }

    /// Effective delay for `attempt` (1-based), jitter applied as configured.
    #[must_use]
    pub fn delay_for(&self, attempt: u32) -> Duration {
        let base = self.base_delay(attempt);
        match self {
            Self::ExponentialWithJitter { max_delay_ms, .. } => {
                // ±50% full jitter around the base delay.
                let base_ms = base.as_millis() as f64;
                let factor = rand::rng().random_range(0.5..=1.5_f64);
                let jittered = (base_ms * factor) as u64;
                Duration::from_millis(jittered.min(*max_delay_ms).max(1))
            },
            _ => base,
        }
    }
}

/// What to do after an attempt failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RetryDecision {
    /// Retry after the given delay.
    Retry { delay: Duration },
    /// Error class can never succeed on retry — fail permanently.
    DoNotRetry,
    /// Attempt budget exhausted — route to dead letter.
    Exhausted,
}

impl RetryDecision {
    #[must_use]
    pub const fn is_retry(&self) -> bool {
        matches!(self, Self::Retry { .. })
    }
}

/// Complete retry policy for a task/step.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Total attempts allowed including the first (≥ 1).
    pub max_attempts: u32,
    pub strategy: BackoffStrategy,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: constants::DEFAULT_MAX_RETRIES + 1,
            strategy: BackoffStrategy::default(),
        }
    }
}

impl RetryPolicy {
    pub fn new(max_attempts: u32, strategy: BackoffStrategy) -> Result<Self> {
        if max_attempts == 0 {
            return Err(AppError::invalid_field(
                "max_attempts",
                "out_of_range",
                "at least one attempt is required",
            ));
        }
        if max_attempts > constants::MAX_ALLOWED_RETRIES + 1 {
            return Err(AppError::invalid_field(
                "max_attempts",
                "out_of_range",
                format!(
                    "attempts may not exceed {}",
                    constants::MAX_ALLOWED_RETRIES + 1
                ),
            ));
        }
        strategy.validate()?;
        Ok(Self {
            max_attempts,
            strategy,
        })
    }

    /// Never retry: exactly one attempt.
    #[must_use]
    pub fn no_retries() -> Self {
        Self {
            max_attempts: 1,
            strategy: BackoffStrategy::Fixed { delay_ms: 0 },
        }
    }

    /// Decides the next step after attempt `failed_attempt` (1-based) failed
    /// with `error`.
    ///
    /// The error's own `retry_after_hint` (e.g. rate limits) raises the floor
    /// of the computed delay when higher.
    #[must_use]
    pub fn decision(&self, failed_attempt: u32, error: &AppError) -> RetryDecision {
        if failed_attempt == 0 {
            return RetryDecision::Retry {
                delay: self.strategy.delay_for(1),
            };
        }
        if !error.is_retryable() {
            return RetryDecision::DoNotRetry;
        }
        if failed_attempt >= self.max_attempts {
            return RetryDecision::Exhausted;
        }
        let mut delay = self.strategy.delay_for(failed_attempt + 1);
        if let Some(hint) = error.retry_after_hint() {
            if hint > delay {
                delay = hint;
            }
        }
        RetryDecision::Retry { delay }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_strategy_is_constant() {
        let strategy = BackoffStrategy::Fixed { delay_ms: 250 };
        assert_eq!(strategy.delay_for(1), Duration::from_millis(250));
        assert_eq!(strategy.delay_for(9), Duration::from_millis(250));
    }

    #[test]
    fn exponential_grows_and_caps() {
        let strategy = BackoffStrategy::Exponential {
            base_ms: 100,
            factor: 2,
            max_delay_ms: 1_000,
        };
        assert_eq!(strategy.delay_for(1), Duration::from_millis(100));
        assert_eq!(strategy.delay_for(2), Duration::from_millis(200));
        assert_eq!(strategy.delay_for(3), Duration::from_millis(400));
        assert_eq!(strategy.delay_for(4), Duration::from_millis(800));
        assert_eq!(strategy.delay_for(5), Duration::from_millis(1_000));
        assert_eq!(strategy.delay_for(40), Duration::from_millis(1_000));
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let strategy = BackoffStrategy::ExponentialWithJitter {
            base_ms: 1_000,
            factor: 2,
            max_delay_ms: 60_000,
        };
        for attempt in 1..6 {
            let delay = strategy.delay_for(attempt);
            let raw = 1_000_u64 * 2_u64.pow(attempt - 1);
            assert!(delay >= Duration::from_millis(raw / 2));
            assert!(delay <= Duration::from_millis((raw as f64 * 1.5) as u64));
        }
    }

    #[test]
    fn decision_matrix() {
        let policy = RetryPolicy::new(3, BackoffStrategy::Fixed { delay_ms: 100 }).unwrap();
        // Retryable error, attempts left → retry.
        let error = AppError::messaging("broker hiccup");
        assert!(policy.decision(1, &error).is_retry());
        // Non-retryable error → never retry, regardless of attempts.
        let error = AppError::validation("bad input");
        assert_eq!(policy.decision(1, &error), RetryDecision::DoNotRetry);
        // Retryable but exhausted.
        let error = AppError::timeout("slow");
        assert_eq!(policy.decision(3, &error), RetryDecision::Exhausted);
        // No-retry policy exhausts immediately after the first failure.
        assert_eq!(
            RetryPolicy::no_retries().decision(1, &AppError::messaging("x")),
            RetryDecision::Exhausted
        );
    }

    #[test]
    fn rate_limit_hint_raises_delay_floor() {
        let policy = RetryPolicy::new(5, BackoffStrategy::Fixed { delay_ms: 100 }).unwrap();
        let decision = policy.decision(1, &AppError::rate_limited("quota"));
        match decision {
            RetryDecision::Retry { delay } => assert!(delay >= Duration::from_secs(1)),
            other => panic!("expected retry, got {other:?}"),
        }
    }

    #[test]
    fn policy_validation() {
        assert!(RetryPolicy::new(0, BackoffStrategy::default()).is_err());
        assert!(RetryPolicy::new(99, BackoffStrategy::default()).is_err());
        assert!(BackoffStrategy::Fixed { delay_ms: 0 }.validate().is_err());
        assert!(BackoffStrategy::Exponential {
            base_ms: 1,
            factor: 0,
            max_delay_ms: 10
        }
        .validate()
        .is_err());
    }
}
