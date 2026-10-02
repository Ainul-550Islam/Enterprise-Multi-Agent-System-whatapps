//! Deadlines and timeout policies.
//!
//! [`Deadline`] is an absolute point in time (persistable); [`TimeoutPolicy`]
//! produces deadlines from configured durations, keeping everything under
//! platform caps.

use mas_common::constants;
use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::Timestamp;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::time::Duration;

/// Absolute expiry instant used throughout the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Deadline {
    expires_at: Timestamp,
}

impl Deadline {
    /// A deadline at a concrete UTC instant. Must be in the future.
    #[must_use]
    pub fn at(expires_at: Timestamp) -> Self {
        Self { expires_at }
    }

    /// A deadline `duration` from now.
    #[must_use]
    pub fn after(duration: Duration) -> Self {
        Self {
            expires_at: Timestamp::now().checked_add(duration).unwrap_or_else(|| {
                // Overflow is unreachable for realistic durations; clamp far out.
                Timestamp::from_unix_ms(i64::MAX / 2).unwrap_or_else(|_| Timestamp::now())
            }),
        }
    }

    /// The absolute expiry instant.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Time remaining; `None` when already expired.
    #[must_use]
    pub fn remaining(&self) -> Option<Duration> {
        self.expires_at.duration_since(&Timestamp::now())
    }

    /// Whether the deadline has passed.
    #[must_use]
    pub fn expired(&self) -> bool {
        self.remaining().is_none()
    }

    /// Fails with `Timeout` when expired. `action` names the guarded operation
    /// for diagnostics (e.g. `"tool.invoke http.get"`).
    pub fn check_deadline(&self, action: &str) -> Result<()> {
        if self.expired() {
            return Err(AppError::timeout(format!(
                "deadline expired while performing '{action}'"
            )));
        }
        Ok(())
    }

    /// Runs `future`, failing with `Timeout` at the deadline.
    pub async fn timeout_guard<F>(&self, action: &str, future: F) -> Result<F::Output>
    where
        F: Future,
    {
        match self.remaining() {
            None => Err(AppError::timeout(format!(
                "deadline expired before '{action}' could start"
            ))),
            Some(remaining) => tokio::time::timeout(remaining, future)
                .await
                .map_err(|_| AppError::timeout(format!("'{action}' exceeded its deadline"))),
        }
    }
}

/// Configured timeout behavior of the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeoutPolicy {
    /// Default operation timeout.
    pub default_timeout: Duration,
    /// Hard ceiling for any configured/requested timeout.
    pub max_timeout: Duration,
    /// Grace period granted for graceful cancellation/cleanup.
    pub cancellation_grace: Duration,
}

impl Default for TimeoutPolicy {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_millis(constants::DEFAULT_REQUEST_TIMEOUT_MS),
            max_timeout: Duration::from_millis(constants::MAX_EXECUTION_DURATION_MS),
            cancellation_grace: Duration::from_secs(5),
        }
    }
}

impl TimeoutPolicy {
    pub fn new(
        default_timeout: Duration,
        max_timeout: Duration,
        cancellation_grace: Duration,
    ) -> Result<Self> {
        if default_timeout.is_zero() || default_timeout > max_timeout {
            return Err(AppError::invalid_field(
                "default_timeout",
                "out_of_range",
                "default timeout must be > 0 and <= max timeout",
            ));
        }
        if max_timeout.as_millis() as u64 > constants::MAX_EXECUTION_DURATION_MS {
            return Err(AppError::invalid_field(
                "max_timeout",
                "out_of_range",
                format!(
                    "max timeout exceeds the platform cap of {} ms",
                    constants::MAX_EXECUTION_DURATION_MS
                ),
            ));
        }
        Ok(Self {
            default_timeout,
            max_timeout,
            cancellation_grace,
        })
    }

    /// Clamps `requested` to the policy ceiling and converts to a deadline.
    #[must_use]
    pub fn deadline_from(&self, requested: Option<Duration>) -> Deadline {
        let effective = requested
            .unwrap_or(self.default_timeout)
            .min(self.max_timeout);
        Deadline::after(effective)
    }

    /// Validates `requested` against the policy, erroring instead of clamping
    /// (used for user-supplied configuration where silent clamping is wrong).
    pub fn validate_request(&self, requested: Duration) -> Result<Duration> {
        if requested.is_zero() {
            return Err(AppError::invalid_field(
                "timeout",
                "out_of_range",
                "timeout must be positive",
            ));
        }
        if requested > self.max_timeout {
            return Err(AppError::invalid_field(
                "timeout",
                "out_of_range",
                format!(
                    "timeout exceeds the policy maximum of {:?}",
                    self.max_timeout
                ),
            ));
        }
        Ok(requested)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deadline_lifecycle() {
        let deadline = Deadline::after(Duration::from_secs(60));
        assert!(!deadline.expired());
        assert!(deadline.remaining().unwrap() <= Duration::from_secs(60));
        assert!(deadline.check_deadline("test").is_ok());

        let past = Deadline::at(
            Timestamp::now()
                .checked_sub(Duration::from_secs(1))
                .unwrap(),
        );
        assert!(past.expired());
        let err = past.check_deadline("test").unwrap_err();
        assert_eq!(err.error_code(), "TIMEOUT");
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn guard_completes_fast_futures() {
        let deadline = Deadline::after(Duration::from_secs(5));
        let value = deadline.timeout_guard("quick", async { 42 }).await.unwrap();
        assert_eq!(value, 42);
    }

    #[tokio::test]
    async fn guard_aborts_slow_futures() {
        let deadline = Deadline::after(Duration::from_millis(20));
        let err = deadline
            .timeout_guard("slow", async {
                tokio::time::sleep(Duration::from_secs(5)).await;
                1_u8
            })
            .await
            .unwrap_err();
        assert_eq!(err.error_code(), "TIMEOUT");
    }

    #[test]
    fn policy_clamps_and_validates() {
        let policy = TimeoutPolicy::new(
            Duration::from_secs(30),
            Duration::from_secs(300),
            Duration::from_secs(5),
        )
        .unwrap();
        let deadline = policy.deadline_from(Some(Duration::from_secs(600)));
        assert!(deadline.remaining().unwrap() <= Duration::from_secs(300));
        assert!(policy.validate_request(Duration::ZERO).is_err());
        assert!(policy.validate_request(Duration::from_secs(600)).is_err());
        assert_eq!(
            policy.validate_request(Duration::from_secs(60)).unwrap(),
            Duration::from_secs(60)
        );
        assert!(TimeoutPolicy::new(
            Duration::from_secs(500),
            Duration::from_secs(300),
            Duration::from_secs(1)
        )
        .is_err());
    }
}
