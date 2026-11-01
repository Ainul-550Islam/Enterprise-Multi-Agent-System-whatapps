//! Per-execution resource guard: converts [`ResourceLimits`] + token budgets
//! into enforceable runtime decisions.
//!
//! One guard per execution run; the step/tool runners consult it *before*
//! charging work, so a blown budget fails fast instead of overrunning.
//! Accounting is additive and monotonic; the guard never decrements.

use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation::ValidationBuilder;
use mas_domain::{ResourceLimits, TokenBudget};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Point-in-time accounting view (for diagnostics/checkpoints).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuardSnapshot {
    pub steps_used: u32,
    pub tool_calls_used: u32,
    pub tokens_consumed: u64,
    /// `None` when the budget is unmetered.
    pub tokens_remaining: Option<u64>,
    pub elapsed_ms: u64,
    pub remaining_time_ms: i64,
}

/// The guard itself.
#[derive(Debug, Clone)]
pub struct ResourceGuard {
    limits: ResourceLimits,
    started_at: Timestamp,
    steps_used: u32,
    tool_calls_used: u32,
    token_budget: TokenBudget,
}

impl ResourceGuard {
    /// Creates a guard; merges `execution_limits` with platform caps already
    /// encoded in `ResourceLimits::validate` and takes an optional token
    /// budget (unmetered when `None`).
    pub fn new(
        execution_limits: Option<ResourceLimits>,
        token_budget: Option<TokenBudget>,
    ) -> Result<Self> {
        let limits = execution_limits.unwrap_or_default();
        limits.validate()?;
        let token_budget = match token_budget {
            Some(budget) => budget,
            None => TokenBudget::unlimited(),
        };
        Ok(Self {
            limits,
            started_at: Timestamp::now(),
            steps_used: 0,
            tool_calls_used: 0,
            token_budget,
        })
    }

    #[must_use]
    pub const fn limits(&self) -> &ResourceLimits {
        &self.limits
    }

    /// Remaining wall-clock budget (`0` when exceeded).
    #[must_use]
    pub fn remaining_time(&self) -> Duration {
        let limit = Duration::from_millis(self.limits.timeout_ms);
        let elapsed = self.started_at.elapsed().unwrap_or(Duration::ZERO);
        limit.saturating_sub(elapsed)
    }

    /// Wall clock exhausted?
    #[must_use]
    pub fn is_timed_out(&self) -> bool {
        self.remaining_time().is_zero()
    }

    /// Fails when the wall-clock budget is gone.
    pub fn check_time(&self) -> Result<()> {
        if self.is_timed_out() {
            return Err(AppError::timeout(format!(
                "execution exceeded its wall-clock budget of {} ms",
                self.limits.timeout_ms
            )));
        }
        Ok(())
    }

    /// Charges one execution step; fails closed when the budget is spent.
    pub fn charge_step(&mut self) -> Result<()> {
        self.check_time()?;
        if self.steps_used >= self.limits.max_steps {
            return Err(AppError::rate_limited(format!(
                "execution used all {} allowed steps",
                self.limits.max_steps
            )));
        }
        self.steps_used += 1;
        Ok(())
    }

    /// Charges one tool invocation.
    pub fn charge_tool_call(&mut self) -> Result<()> {
        self.check_time()?;
        if self.tool_calls_used
            >= self
                .limits
                .max_tool_calls
                .min(mas_common::constants::MAX_TOOL_CALLS_PER_EXECUTION)
        {
            return Err(AppError::rate_limited(format!(
                "execution used all {} allowed tool calls",
                self.limits.max_tool_calls
            )));
        }
        self.tool_calls_used += 1;
        Ok(())
    }

    /// Charges consumed tokens (LLM usage as reported by bridges).
    pub fn charge_tokens(&mut self, tokens: u64) -> Result<()> {
        self.token_budget
            .consume(tokens)
            .map_err(|e| e.with_context("execution token budget exhausted"))?;
        Ok(())
    }

    /// Pre-flight payload check: anything exceeding the limit is rejected
    /// before transport/inclusion in step records.
    pub fn check_payload_bytes(&self, size: usize) -> Result<()> {
        let hard_cap = mas_common::constants::MAX_PAYLOAD_BYTES;
        let cap = self.limits.max_payload_bytes.min(hard_cap);
        if size > cap {
            return Err(AppError::invalid_field(
                "payload",
                "too_large",
                format!("payload is {size} bytes; execution limit is {cap}"),
            ));
        }
        Ok(())
    }

    /// Number of step attempts a step may still run (cap-conservative).
    #[must_use]
    pub fn remaining_step_attempts(&self) -> u32 {
        self.limits
            .max_retries
            .saturating_add(1)
            .min(mas_common::constants::MAX_ALLOWED_RETRIES + 1)
    }

    /// All budget checks at once; intended for run-boundary assertions.
    pub fn validate_state(&self) -> Result<()> {
        let mut issues = ValidationBuilder::new();
        if self.steps_used > self.limits.max_steps {
            issues.add("steps", "budget_exceeded", "step budget exceeded");
        }
        if self.tool_calls_used > self.limits.max_tool_calls {
            issues.add("tool_calls", "budget_exceeded", "tool-call budget exceeded");
        }
        if self.is_timed_out() {
            issues.add("time", "budget_exceeded", "wall-clock budget exceeded");
        }
        issues
            .finish()
            .map_err(|e| e.with_context("resource guard state invalid"))
    }

    #[must_use]
    pub fn snapshot(&self) -> GuardSnapshot {
        let tokens_remaining = if self.token_budget.max_tokens() == 0 {
            None
        } else {
            Some(
                self.token_budget
                    .max_tokens()
                    .saturating_sub(self.token_budget.consumed()),
            )
        };
        GuardSnapshot {
            steps_used: self.steps_used,
            tool_calls_used: self.tool_calls_used,
            tokens_consumed: self.token_budget.consumed(),
            tokens_remaining,
            elapsed_ms: self
                .started_at
                .elapsed()
                .map_or(0, |d| d.as_millis() as u64),
            remaining_time_ms: {
                let remaining = self.remaining_time().as_millis() as i64;
                if self.is_timed_out() {
                    0
                } else {
                    remaining
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_limits() -> ResourceLimits {
        ResourceLimits {
            timeout_ms: 60_000,
            max_steps: 2,
            max_parallel_tasks: 1,
            max_tool_calls: 1,
            max_payload_bytes: 128,
            max_retries: 1,
        }
    }

    #[test]
    fn step_and_tool_budgets_fail_closed() {
        let mut guard = ResourceGuard::new(Some(small_limits()), None).expect("guard");
        guard.charge_step().expect("step 1");
        guard.charge_step().expect("step 2");
        let error = guard.charge_step().unwrap_err();
        assert_eq!(error.error_code(), "RATE_LIMITED");

        guard.charge_tool_call().expect("tool call");
        assert!(guard.charge_tool_call().is_err());
        guard.validate_state().expect("within budgets stays valid");
    }

    #[test]
    fn payload_and_token_budgets_are_enforced() {
        let mut guard = ResourceGuard::new(
            Some(small_limits()),
            Some(TokenBudget::new(1_000).expect("budget")),
        )
        .expect("guard");
        guard.check_payload_bytes(128).expect("at the cap is fine");
        assert!(guard.check_payload_bytes(129).is_err());

        guard.charge_tokens(500).expect("half the budget");
        assert_eq!(guard.snapshot().tokens_remaining, Some(500));
        assert!(guard.charge_tokens(501).is_err());
        // Overspend did not consume: consume() fails closed.
        assert_eq!(guard.token_budget.consumed(), 500);
    }

    #[test]
    fn snapshots_reflect_consumption() {
        let mut guard = ResourceGuard::new(Some(small_limits()), None).expect("guard");
        guard.charge_step().expect("step");
        guard.charge_tool_call().expect("tool");
        let snapshot = guard.snapshot();
        assert_eq!(snapshot.steps_used, 1);
        assert_eq!(snapshot.tool_calls_used, 1);
        assert_eq!(snapshot.tokens_remaining, None); // unmetered
        assert!(snapshot.remaining_time_ms > 0);
    }
}
