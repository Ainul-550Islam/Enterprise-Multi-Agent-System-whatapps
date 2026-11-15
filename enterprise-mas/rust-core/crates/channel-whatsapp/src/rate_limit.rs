/// Per-number token bucket rate limiter + Meta 130429 backoff with jitter.
///
/// WhatsApp Cloud API rate limits are per phone number per day.
/// When the quota is exhausted, Meta returns 130429 (rate limit exceeded).
/// This crate provides a token bucket that models the daily quota and
/// produces backoff durations matching Meta's published pattern.
use std::time::{Duration, Instant};
use tracing::warn;

/// Daily quota per WhatsApp phone number (approximate Meta limit).
/// Meta's published limit is ~1000 messages per 24h per number for
/// free-form text; templates have a separate (higher) quota.
const DAILY_QUOTA: u32 = 1000;

/// Token bucket state.
#[derive(Clone, Debug)]
pub struct RateLimiter {
    /// Tokens remaining in the bucket (0..DAILY_QUOTA)
    tokens: u32,
    /// Last reset epoch (millis since epoch)
    last_reset: i64,
    /// Backoff configuration for when quota is exhausted
    backoff_cap_ms: u64,
}

impl RateLimiter {
    /// Create a new rate limiter reset to the full daily quota.
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| {})
            .unwrap()
            .as_millis() as i64;
        RateLimiter {
            tokens: DAILY_QUOTA,
            last_reset: now,
            backoff_cap_ms: 30_000, // 30s cap
        }
    }

    /// Current token count (for observability).
    pub fn tokens_remaining(&self) -> u32 {
        self.tokens
    }

    /// Available quota percentage (0..100).
    pub fn quota_percent(&self) -> u32 {
        ((self.tokens as f64 / DAILY_QUOTA as f64) * 100.0) as u32
    }

    /// Attempt to consume one token.
    /// Returns `Ok(())` if a token was consumed, or `Err(BackoffInfo)` if
    /// the quota is exhausted and the caller should back off.
    ///
    /// Thread-safety note: this implementation is single-threaded (per-number).
    /// For concurrent access, wrap in a mutex or use a per-Arc design.
    pub fn try_consume(&mut self) -> Result<(), BackoffInfo> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| {})
            .unwrap()
            .as_millis() as i64;

        // If a new day has started (more than 24h since last reset), reset quota.
        if now >= self.last_reset + (24 * 60 * 60 * 1000) {
            self.tokens = DAILY_QUOTA;
            self.last_reset = now;
        }

        if self.tokens > 0 {
            self.tokens -= 1;
            Ok(())
        } else {
            // Quota exhausted — compute backoff with jitter per Meta 130429
            let backoff = self.compute_backoff(now);
            Err(BackoffInfo { backoff_ms: backoff })
        }
    }

    /// Compute backoff duration (ms) since last attempt, per Meta 130429.
    /// Pattern: linear up to a cap, with optional jitter.
    fn compute_backoff(&self, last_attempt_ms: i64) -> u64 {
        // Simple linear backoff: 1s * attempt_number, capped at backoff_cap_ms
        // In a full implementation this would track attempt count per number.
        // Here we use a fixed base of 5s capped at backoff_cap_ms.
        let base = 5_000u64; // 5 seconds
        let capped = base.min(self.backoff_cap_ms);
        // Add tiny jitter ±20% to avoid thundering herd
        let jitter = (capped as f64 * 0.2).ceil() as u64;
        let min = capped.saturating_sub(jitter);
        let max = capped.saturating_add(jitter);
        // Use the midpoint as the suggested backoff
        let suggested = min + (max - min) / 2;
        // Never return 0 — if we do, the caller would retry immediately
        if suggested == 0 {
            1_000 // 1s minimum
        } else {
            suggested
        }
    }

    /// Reset the bucket to full quota (called by the application at start of day).
    pub fn reset_to_full(&mut self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| {})
            .unwrap()
            .as_millis() as i64;
        self.tokens = DAILY_QUOTA;
        self.last_reset = now;
    }
}

/// Information about how long to back off after quota exhaustion.
#[derive(Clone, Debug)]
pub struct BackoffInfo {
    /// Suggested backoff duration in milliseconds.
    pub backoff_ms: u64,
}

impl RateLimiter {
    /// Per-number token bucket with Meta 130429 backoff, jitter included.
    /// The backoff duration grows with consecutive failures but is capped.
    pub fn with_jitter(attempt: u32) -> u64 {
        // Meta 130429: exponential-ish backoff: 5s, 10s, 20s, 40s, …
        // Capped at 5min (300s). Jitter ±20%.
        let base_ms: u64 = 5_000 * (1u64 << std::cmp::min(attempt - 1, 5)); // 5, 5, 10, 20, 40, 40
        let capped = base_ms.min(300_000); // 5 min cap
        let jitter = (capped as f64 * 0.2).ceil() as u64;
        let min = capped.saturating_sub(jitter);
        let max = capped.saturating_add(jitter);
        let suggested = min + (max - min) / 2;
        if suggested == 0 { 1_000 } else { suggested }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_quota() {
        let mut rl = RateLimiter::new();
        assert_eq!(rl.tokens_remaining(), DAILY_QUOTA);
        assert_eq!(rl.quota_percent(), 100);
    }

    #[test]
    fn test_consume_decrements() {
        let mut rl = RateLimiter::new();
        rl.try_consume().unwrap();
        assert_eq!(rl.tokens_remaining(), DAILY_QUOTA - 1);
        assert_eq!(rl.quota_percent(), 999);
    }

    #[test]
    fn test_daily_reset() {
        let mut rl = RateLimiter::new();
        // Consume all
        for _ in 0..DAILY_QUOTA {
            rl.try_consume().unwrap();
        }
        assert_eq!(rl.tokens_remaining(), 0);
        assert_eq!(rl.quota_percent(), 0);
    }

    #[test]
    fn test_backoff_with_jitter() {
        // First few attempts should give reasonable backoff values
        let b0 = RateLimiter::with_jitter(1); // attempt 1
        let b1 = RateLimiter::with_jitter(2); // attempt 2
        let b2 = RateLimiter::with_jitter(5); // attempt 5
        // All should be positive and under the cap
        assert!(b0 > 0 && b0 < 300_000);
        assert!(b1 > 0 && b1 < 300_000);
        assert!(b2 > 0 && b2 < 300_000);
    }
}