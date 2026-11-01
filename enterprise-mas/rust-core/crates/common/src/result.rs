//! Result aliases and lightweight execution-outcome helpers.

use crate::error::AppError;
use serde::{Deserialize, Serialize};

/// The canonical result type of the whole workspace.
pub type Result<T> = std::result::Result<T, AppError>;

/// Wraps a successful value plus non-fatal warnings collected while producing
/// it (e.g. deprecations, clamped values, skipped optional steps).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationResult<T> {
    pub value: T,
    pub warnings: Vec<String>,
}

impl<T> OperationResult<T> {
    #[must_use]
    pub fn ok(value: T) -> Self {
        Self {
            value,
            warnings: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_warnings(value: T, warnings: Vec<String>) -> Self {
        Self { value, warnings }
    }

    #[must_use]
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }

    pub fn push_warning(&mut self, warning: impl Into<String>) {
        self.warnings.push(warning.into());
    }

    #[must_use]
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> OperationResult<U> {
        OperationResult {
            value: f(self.value),
            warnings: self.warnings,
        }
    }

    #[must_use]
    pub fn into_value(self) -> T {
        self.value
    }
}

impl<T> From<T> for OperationResult<T> {
    fn from(value: T) -> Self {
        Self::ok(value)
    }
}

/// Internal execution outcome used when an operation legitimately ends in
/// several terminal states that are not "errors" in the Rust sense but still
/// need conversion back into the [`Result`] world at boundaries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ExecutionOutcome<T> {
    /// Operation completed; carries the produced value.
    Completed(T),
    /// Operation could not run now and must be retried later (e.g. quota,
    /// backpressure, join condition not yet satisfied).
    Deferred {
        reason: String,
        retry_after_ms: Option<u64>,
    },
    /// Operation was cancelled before producing a result.
    Cancelled { reason: String },
}

impl<T> ExecutionOutcome<T> {
    #[must_use]
    pub fn completed(value: T) -> Self {
        Self::Completed(value)
    }

    #[must_use]
    pub fn deferred(reason: impl Into<String>, retry_after_ms: Option<u64>) -> Self {
        Self::Deferred {
            reason: reason.into(),
            retry_after_ms,
        }
    }

    #[must_use]
    pub fn cancelled(reason: impl Into<String>) -> Self {
        Self::Cancelled {
            reason: reason.into(),
        }
    }

    #[must_use]
    pub fn is_completed(&self) -> bool {
        matches!(self, Self::Completed(_))
    }

    #[must_use]
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> ExecutionOutcome<U> {
        match self {
            Self::Completed(v) => ExecutionOutcome::Completed(f(v)),
            Self::Deferred {
                reason,
                retry_after_ms,
            } => ExecutionOutcome::Deferred {
                reason,
                retry_after_ms,
            },
            Self::Cancelled { reason } => ExecutionOutcome::Cancelled { reason },
        }
    }

    /// Convert to [`Result`]: `Completed` → `Ok`; `Deferred` → `RateLimited`
    /// (retryable, carries the hint); `Cancelled` → `Cancelled`.
    pub fn into_result(self) -> Result<T> {
        match self {
            Self::Completed(v) => Ok(v),
            Self::Deferred { reason, .. } => Err(AppError::rate_limited(reason)),
            Self::Cancelled { reason } => Err(AppError::cancelled(reason)),
        }
    }

    /// Borrow the completed value, if any.
    #[must_use]
    pub fn value(&self) -> Option<&T> {
        match self {
            Self::Completed(v) => Some(v),
            _ => None,
        }
    }
}

/// Split a collection of results into successes and failures, preserving order.
pub fn partition_results<I, T>(iter: I) -> (Vec<T>, Vec<AppError>)
where
    I: IntoIterator<Item = Result<T>>,
{
    let mut ok = Vec::new();
    let mut err = Vec::new();
    for item in iter {
        match item {
            Ok(v) => ok.push(v),
            Err(e) => err.push(e),
        }
    }
    (ok, err)
}

/// `Ok` when `opt` is `Some`, otherwise a `NotFound` error for `resource`.
pub fn require_found<T>(
    opt: Option<T>,
    resource: &'static str,
    lookup: impl Into<String>,
) -> Result<T> {
    opt.ok_or_else(|| AppError::not_found(resource, lookup))
}
