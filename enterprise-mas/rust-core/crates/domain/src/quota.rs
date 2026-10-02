//! Quota definitions and their *definition-time* state. Runtime atomic
//! counters live in the quota crate (Redis); this aggregate is the source of
//! truth for limits, periods and enforcement mode.

use mas_common::ids::{QuotaId, TenantId};
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use serde::{Deserialize, Serialize};

string_enum! {
    /// Metered resource dimension.
    QuotaDimension {
        Requests => "requests",
        Executions => "executions",
        ConcurrentRuns => "concurrent_runs",
        Tokens => "tokens",
        StorageBytes => "storage_bytes",
        ToolInvocations => "tool_invocations",
        ApiCalls => "api_calls",
    }
}

string_enum! {
    /// Reset window of a quota.
    QuotaPeriod {
        PerMinute => "per_minute",
        Hourly => "hourly",
        Daily => "daily",
        Monthly => "monthly",
        /// Never resets (hard ceilings like concurrent slots / storage).
        Unbounded => "unbounded",
    }
}

string_enum! {
    /// What happens when the limit is reached.
    EnforcementMode {
        /// Reject immediately.
        Enforce => "enforce",
        /// Allow but signal (rate-limiting hint, warnings, billing flags).
        WarnOnly => "warn_only",
        /// Allow and bill/flag as overage.
        AllowWithOverage => "allow_with_overage",
    }
}

/// A quota definition with its current usage snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quota {
    pub id: QuotaId,
    pub tenant_id: TenantId,
    pub dimension: QuotaDimension,
    pub period: QuotaPeriod,
    /// Maximum allowed consumption within the period.
    pub limit: u64,
    /// Usage in the current period (mirror of the atomic counter;
    /// authoritative increments happen in the quota crate).
    pub current_usage: u64,
    pub enforcement: EnforcementMode,
    /// Start of the current accounting window.
    pub period_started_at: Timestamp,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Quota {
    pub fn new(
        tenant_id: TenantId,
        dimension: QuotaDimension,
        period: QuotaPeriod,
        limit: u64,
        enforcement: EnforcementMode,
    ) -> Result<Self> {
        if limit == 0 {
            return Err(mas_common::error::AppError::invalid_field(
                "limit",
                "out_of_range",
                "quota limit must be positive",
            ));
        }
        // Dimensions that don't make sense as resetting counters.
        if matches!(
            dimension,
            QuotaDimension::ConcurrentRuns | QuotaDimension::StorageBytes
        ) && period != QuotaPeriod::Unbounded
        {
            return Err(mas_common::error::AppError::invalid_field(
                "period",
                "incoherent_period",
                "concurrent_runs and storage_bytes gauges use the 'unbounded' period",
            ));
        }
        let now = Timestamp::now();
        Ok(Self {
            id: QuotaId::new(),
            tenant_id,
            dimension,
            period,
            limit,
            current_usage: 0,
            enforcement,
            period_started_at: now,
            created_at: now,
            updated_at: now,
        })
    }

    /// Remaining allowance this period.
    #[must_use]
    pub const fn remaining(&self) -> u64 {
        self.limit.saturating_sub(self.current_usage)
    }

    /// Utilization ratio 0.0..=1.0+ (>1 when over limit under soft modes).
    #[must_use]
    pub fn utilization(&self) -> f64 {
        if self.limit == 0 {
            return 0.0;
        }
        self.current_usage as f64 / self.limit as f64
    }

    /// Whether a consumption of `amount` would fit under `Enforce` semantics.
    #[must_use]
    pub const fn would_fit(&self, amount: u64) -> bool {
        self.current_usage.saturating_add(amount) <= self.limit
    }

    /// Records consumption of `amount` honoring the enforcement mode.
    /// Returns `Err(RateLimited)` only in `Enforce` mode when the amount does
    /// not fit; soft modes consume and let the caller read `is_over_limit`.
    pub fn consume(&mut self, amount: u64) -> Result<()> {
        if self.enforcement == EnforcementMode::Enforce && !self.would_fit(amount) {
            return Err(mas_common::error::AppError::rate_limited(format!(
                "{} quota exhausted for tenant {} ({}/{} used)",
                self.dimension, self.tenant_id, self.current_usage, self.limit
            )));
        }
        self.current_usage = self.current_usage.saturating_add(amount);
        self.updated_at = Timestamp::now();
        Ok(())
    }

    /// Releases previously consumed units (compensation after failures).
    pub fn release(&mut self, amount: u64) {
        self.current_usage = self.current_usage.saturating_sub(amount);
        self.updated_at = Timestamp::now();
    }

    #[must_use]
    pub const fn is_over_limit(&self) -> bool {
        self.current_usage > self.limit
    }

    /// Length of the reset window, when periodic.
    #[must_use]
    pub const fn period_length(&self) -> Option<std::time::Duration> {
        match self.period {
            QuotaPeriod::PerMinute => Some(std::time::Duration::from_secs(60)),
            QuotaPeriod::Hourly => Some(std::time::Duration::from_secs(3_600)),
            QuotaPeriod::Daily => Some(std::time::Duration::from_secs(86_400)),
            QuotaPeriod::Monthly => Some(std::time::Duration::from_secs(2_592_000)), // 30d
            QuotaPeriod::Unbounded => None,
        }
    }

    /// Whether the accounting window has rolled and counters must reset.
    #[must_use]
    pub fn is_period_due(&self, now: &Timestamp) -> bool {
        let Some(length) = self.period_length() else {
            return false;
        };
        match self.period_started_at.checked_add(length) {
            Some(end) => !now.is_before(&end),
            None => true,
        }
    }

    /// Resets the window starting at `now`.
    pub fn reset_period(&mut self, now: Timestamp) {
        self.current_usage = 0;
        self.period_started_at = now;
        self.updated_at = now;
    }

    /// Adjusts the limit (contract change); keeps usage untouched.
    pub fn set_limit(&mut self, limit: u64) -> Result<()> {
        if limit == 0 {
            return Err(mas_common::error::AppError::invalid_field(
                "limit",
                "out_of_range",
                "quota limit must be positive",
            ));
        }
        self.limit = limit;
        self.updated_at = Timestamp::now();
        Ok(())
    }
}
