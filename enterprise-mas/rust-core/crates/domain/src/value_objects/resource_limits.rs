//! Per-execution resource limits.

use mas_common::constants;
use mas_common::result::Result;
use mas_common::validation::ValidationBuilder;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Hard bounds applied to a single execution.
///
/// All values must stay below platform constants; misconfiguration fails
/// validation instead of silently weakening safety.
///
/// Time values are milliseconds to keep serde/schema handling trivial.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// Wall-clock timeout for the whole execution (ms).
    pub timeout_ms: u64,
    /// Maximum number of execution steps.
    pub max_steps: u32,
    /// Maximum concurrently running tasks spawned by this execution.
    pub max_parallel_tasks: u32,
    /// Maximum tool invocations.
    pub max_tool_calls: u32,
    /// Maximum input/output payload size in bytes.
    pub max_payload_bytes: usize,
    /// Maximum retries of any single step.
    pub max_retries: u32,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            timeout_ms: constants::DEFAULT_REQUEST_TIMEOUT_MS,
            max_steps: 100,
            max_parallel_tasks: 8,
            max_tool_calls: 50,
            max_payload_bytes: constants::MAX_PAYLOAD_BYTES,
            max_retries: constants::DEFAULT_MAX_RETRIES,
        }
    }
}

impl ResourceLimits {
    /// Returns a copy whose step cap is `max_steps × factor`
    /// (saturating). Used by the execution runtime to derive a
    /// conservative superset cap for internally charged bookkeeping.
    #[must_use]
    pub fn expand_steps_by(&self, factor: u64) -> Self {
        Self {
            max_steps: u64::from(self.max_steps)
                .saturating_mul(factor.max(1))
                .min(u64::from(u32::MAX)) as u32,
            ..*self
        }
    }

    /// Validates all values against hard platform caps.
    pub fn validate(&self) -> Result<()> {
        let mut issues = ValidationBuilder::new();
        if self.timeout_ms == 0 || self.timeout_ms > constants::MAX_EXECUTION_DURATION_MS {
            issues.add(
                "timeout_ms",
                "out_of_range",
                format!("must be 1..={} ms", constants::MAX_EXECUTION_DURATION_MS),
            );
        }
        if self.max_steps == 0 || self.max_steps > constants::MAX_STEPS_PER_EXECUTION {
            issues.add(
                "max_steps",
                "out_of_range",
                format!("must be 1..={}", constants::MAX_STEPS_PER_EXECUTION),
            );
        }
        if self.max_parallel_tasks == 0
            || self.max_parallel_tasks > constants::MAX_PARALLEL_TASKS_PER_EXECUTION
        {
            issues.add(
                "max_parallel_tasks",
                "out_of_range",
                format!(
                    "must be 1..={}",
                    constants::MAX_PARALLEL_TASKS_PER_EXECUTION
                ),
            );
        }
        if self.max_tool_calls > constants::MAX_TOOL_CALLS_PER_EXECUTION {
            issues.add(
                "max_tool_calls",
                "out_of_range",
                format!("must be 0..={}", constants::MAX_TOOL_CALLS_PER_EXECUTION),
            );
        }
        if self.max_payload_bytes == 0 || self.max_payload_bytes > constants::MAX_PAYLOAD_BYTES {
            issues.add(
                "max_payload_bytes",
                "out_of_range",
                format!("must be 1..={} bytes", constants::MAX_PAYLOAD_BYTES),
            );
        }
        if self.max_retries > constants::MAX_ALLOWED_RETRIES {
            issues.add(
                "max_retries",
                "out_of_range",
                format!("must be 0..={}", constants::MAX_ALLOWED_RETRIES),
            );
        }
        issues.finish()
    }

    /// Builder-style constructor with validation.
    pub fn try_new(
        timeout_ms: u64,
        max_steps: u32,
        max_parallel_tasks: u32,
        max_tool_calls: u32,
        max_payload_bytes: usize,
        max_retries: u32,
    ) -> Result<Self> {
        let limits = Self {
            timeout_ms,
            max_steps,
            max_parallel_tasks,
            max_tool_calls,
            max_payload_bytes,
            max_retries,
        };
        limits.validate()?;
        Ok(limits)
    }

    /// Whole-execution timeout as [`Duration`].
    #[must_use]
    pub const fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }
}
