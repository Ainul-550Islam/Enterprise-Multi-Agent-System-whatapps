//! PURPOSE:
//! `TaskAttempt`: one execution try of one task, durably recorded.
//!
//! Attempts are the platform's authoritative retry ledger. They exist because
//! "how many times did we actually try this?" must be answerable after a
//! restart, from durable state, and must never be inferred from broker
//! delivery metadata — a broker redelivers for reasons that have nothing to
//! do with whether the work ran.
//!
//! Layer / ownership
//! -----------------
//! `mas-domain` owns the shape and the invariants. `mas-persistence` owns the
//! rows; the worker owns the lifecycle (claim → start → finish).
//!
//! Production responsibility
//! -------------------------
//! * Model the full attempt lifecycle: created/claimed with an owning worker,
//!   started, finished with a classified outcome and a durable timestamp.
//! * Keep the outcome classification next to the outcome, so retry logic keys
//!   off `error_code` + `retryable` rather than re-parsing a message.
//! * Be terminal-once: a finished attempt is immutable. Re-finishing it is a
//!   conflict, which is what stops a late worker from rewriting history.
//!
//! What this module must NOT do
//! ----------------------------
//! * It must NOT fabricate attempt numbers or derive them from broker
//!   redelivery counts.
//! * It must NOT store raw outputs or secrets; `result_metadata` is explicitly
//!   non-sensitive metadata only.

use mas_common::error::AppError;
use mas_common::ids::{ExecutionId, TaskAttemptId, TaskId, TenantId};
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};
use std::time::Duration;

string_enum! {
    /// Outcome/lifecycle of a single attempt.
    AttemptStatus {
        Running => "running",
        Succeeded => "succeeded",
        Failed => "failed",
        /// The owning worker stopped renewing its lease before settlement.
        /// This is terminal for the abandoned attempt, while the task may be
        /// retried by a later durable claim when its attempt budget permits.
        Expired => "expired",
        Cancelled => "cancelled",
        TimedOut => "timed_out",
    }
}

impl AttemptStatus {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Expired | Self::Cancelled | Self::TimedOut
        )
    }

    /// Whether the attempt outcome permits another try of the task.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Failed | Self::Expired | Self::TimedOut)
    }
}

/// Serde default for the version column (versions are 1-based).
const fn default_version() -> u64 {
    1
}

/// One attempt of a task. Terminal states are immutable afterwards.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAttempt {
    pub id: TaskAttemptId,
    /// Owning tenant (RLS scope, and part of every durable query).
    pub tenant_id: TenantId,
    pub task_id: TaskId,
    /// Owning execution, when the task was spawned by one. Carried here so a
    /// recovery sweep can reconcile attempts with executions without a join
    /// through the task row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<ExecutionId>,
    /// 1-based attempt number.
    pub attempt_number: u32,
    pub status: AttemptStatus,
    /// Identity of the worker/process running this attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    /// When the worker's lease on this attempt expires.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_expires_at: Option<Timestamp>,
    /// Why this attempt was scheduled (e.g. `initial`, `retry:timeout`).
    pub retry_reason: Option<String>,
    pub started_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<Timestamp>,
    /// Safe error summary (redacted, truncated) when failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Stable machine-readable error code when failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    /// Whether the failure was transient (drives the retry decision).
    #[serde(default)]
    pub error_retryable: bool,
    /// Non-sensitive result metadata (never full payloads).
    #[serde(default)]
    pub result_metadata: serde_json::Map<String, serde_json::Value>,
    /// Monotonic version, bumped on every lifecycle change.
    #[serde(default = "default_version")]
    pub version: u64,
}

impl TaskAttempt {
    /// Starts a new running attempt owned by `worker`.
    ///
    /// The worker and its lease are recorded at creation: an attempt that
    /// exists without an owner cannot be fenced, and an unfenced attempt is
    /// exactly the situation that produces duplicate execution.
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        tenant_id: TenantId,
        task_id: TaskId,
        execution_id: Option<ExecutionId>,
        attempt_number: u32,
        worker_id: &str,
        lease_expires_at: Option<Timestamp>,
        retry_reason: Option<String>,
        now: &Timestamp,
    ) -> Result<Self> {
        if attempt_number == 0 {
            return Err(AppError::invalid_field(
                "attempt_number",
                "out_of_range",
                "attempts are 1-based",
            ));
        }
        validation::validate_non_empty("worker_id", worker_id)?;
        validation::validate_length("worker_id", worker_id, 1, 128)?;
        Ok(Self {
            id: TaskAttemptId::new(),
            tenant_id,
            task_id,
            execution_id,
            attempt_number,
            status: AttemptStatus::Running,
            worker_id: Some(worker_id.to_owned()),
            lease_expires_at,
            retry_reason,
            started_at: *now,
            finished_at: None,
            error: None,
            error_code: None,
            error_retryable: false,
            result_metadata: serde_json::Map::new(),
            version: 1,
        })
    }

    /// The monotonic version (1-based).
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Wall time so far (or final), when measurable.
    #[must_use]
    pub fn duration(&self) -> Option<Duration> {
        let end = self.finished_at.unwrap_or_else(Timestamp::now);
        end.duration_since(&self.started_at)
    }

    /// Whether another attempt of the task should be scheduled.
    ///
    /// Combines the outcome with its classification: a timeout is transient,
    /// an authentication denial is not — even though both end the attempt in a
    /// non-succeeded state. Retry logic must ask this, never the status alone.
    #[must_use]
    pub const fn should_retry(&self) -> bool {
        self.status.is_retryable() && self.error_retryable
    }

    /// Whether the attempt's lease has lapsed at `now`.
    #[must_use]
    pub fn is_lease_expired_at(&self, now: &Timestamp) -> bool {
        match self.lease_expires_at {
            None => false,
            Some(until) => !until.is_after(now),
        }
    }

    fn finalize(
        &mut self,
        status: AttemptStatus,
        error_code: Option<String>,
        error: Option<String>,
        retryable: bool,
        now: &Timestamp,
    ) -> Result<()> {
        if self.status.is_terminal() {
            return Err(AppError::conflict(format!(
                "attempt {} of task {} already finished with status '{}'; attempts are immutable once terminal",
                self.attempt_number, self.task_id, self.status
            )));
        }
        let mut error = error;
        if let Some(text) = error.as_mut() {
            text.truncate(2048);
        }
        self.status = status;
        self.error = error;
        self.error_code = error_code;
        self.error_retryable = retryable;
        self.finished_at = Some(*now);
        self.version = self.version.saturating_add(1);
        Ok(())
    }

    /// Running → Succeeded.
    pub fn succeed(&mut self, now: &Timestamp) -> Result<()> {
        self.finalize(AttemptStatus::Succeeded, None, None, false, now)
    }

    /// Running → Failed with a classified, possibly-retryable cause.
    pub fn fail(
        &mut self,
        error_code: impl Into<String>,
        error: impl Into<String>,
        retryable: bool,
        now: &Timestamp,
    ) -> Result<()> {
        self.finalize(
            AttemptStatus::Failed,
            Some(error_code.into()),
            Some(error.into()),
            retryable,
            now,
        )
    }

    /// Running → Expired after the owning worker's lease lapsed.
    ///
    /// An expired attempt is not a provider failure and did not necessarily
    /// produce a result. It is nevertheless terminal: a later worker must
    /// create a new attempt rather than reopen this one.
    pub fn expire(&mut self, now: &Timestamp) -> Result<()> {
        self.finalize(
            AttemptStatus::Expired,
            Some("lease_expired".to_owned()),
            Some("worker lease lapsed before settlement".to_owned()),
            true,
            now,
        )
    }

    /// Running → TimedOut.
    pub fn time_out(&mut self, detail: impl Into<String>, now: &Timestamp) -> Result<()> {
        self.finalize(
            AttemptStatus::TimedOut,
            Some("timeout".to_owned()),
            Some(detail.into()),
            true,
            now,
        )
    }

    /// Running → Cancelled.
    pub fn cancel(&mut self, now: &Timestamp) -> Result<()> {
        self.finalize(AttemptStatus::Cancelled, None, None, false, now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> Timestamp {
        Timestamp::from_unix_seconds(1_700_000_000).expect("timestamp")
    }

    fn attempt() -> TaskAttempt {
        TaskAttempt::start(
            TenantId::new(),
            TaskId::new(),
            Some(ExecutionId::new()),
            1,
            "w-1",
            None,
            None,
            &now(),
        )
        .expect("attempt")
    }

    #[test]
    fn attempts_are_one_based_and_owned() {
        assert!(TaskAttempt::start(
            TenantId::new(),
            TaskId::new(),
            None,
            0,
            "w-1",
            None,
            None,
            &now()
        )
        .is_err());
        assert!(
            TaskAttempt::start(
                TenantId::new(),
                TaskId::new(),
                None,
                1,
                "",
                None,
                None,
                &now()
            )
            .is_err(),
            "an attempt without an owning worker cannot be fenced"
        );
        let attempt = attempt();
        assert_eq!(attempt.attempt_number, 1);
        assert_eq!(attempt.status, AttemptStatus::Running);
        assert_eq!(attempt.worker_id.as_deref(), Some("w-1"));
        assert_eq!(attempt.version(), 1);
        assert!(attempt.finished_at.is_none());
    }

    #[test]
    fn terminal_attempts_are_immutable_exactly_once() {
        let later = now()
            .checked_add(Duration::from_secs(5))
            .expect("timestamp");
        let mut attempt = attempt();
        attempt.succeed(&later).expect("succeed");
        assert_eq!(attempt.status, AttemptStatus::Succeeded);
        assert_eq!(attempt.finished_at, Some(later));
        assert_eq!(attempt.version(), 2);

        let error = attempt
            .fail(
                "late",
                "a late worker tried to rewrite history",
                true,
                &later,
            )
            .expect_err("a finished attempt must not be rewritten");
        assert!(matches!(error, AppError::Conflict(_)));
        assert_eq!(attempt.status, AttemptStatus::Succeeded);
        assert_eq!(attempt.version(), 2, "a refused write changes nothing");
        assert!(attempt.succeed(&later).is_err());
    }

    #[test]
    fn failure_outcomes_carry_code_and_retry_class() {
        let later = now()
            .checked_add(Duration::from_secs(1))
            .expect("timestamp");
        let mut throttled = attempt();
        throttled
            .fail("rate_limited", "429 too many requests", true, &later)
            .expect("fail");
        assert_eq!(throttled.error_code.as_deref(), Some("rate_limited"));
        assert!(throttled.error_retryable);
        assert!(throttled.status.is_retryable());

        let mut auth_failure = attempt();
        auth_failure
            .fail("unauthorized", "bad token", false, &later)
            .expect("fail");
        assert!(!auth_failure.error_retryable);
        assert!(
            !auth_failure.should_retry(),
            "an authorization denial is terminal regardless of the outcome status"
        );
        assert!(
            throttled.should_retry(),
            "a rate-limit failure is worth another attempt"
        );
    }

    #[test]
    fn timeout_and_cancellation_have_distinct_outcomes() {
        let later = now()
            .checked_add(Duration::from_secs(2))
            .expect("timestamp");
        let mut timed = attempt();
        timed
            .time_out("deadline exceeded", &later)
            .expect("timeout");
        assert_eq!(timed.status, AttemptStatus::TimedOut);
        assert!(timed.error_retryable, "a timeout is transient");

        let mut cancelled = attempt();
        cancelled.cancel(&later).expect("cancel");
        assert_eq!(cancelled.status, AttemptStatus::Cancelled);
        assert!(!cancelled.error_retryable);
        assert!(cancelled.error_code.is_none());
    }

    #[test]
    fn expiry_is_a_terminal_retryable_ledger_outcome() {
        let later = now()
            .checked_add(Duration::from_secs(2))
            .expect("timestamp");
        let mut expired = attempt();
        expired.expire(&later).expect("expire");
        assert_eq!(expired.status, AttemptStatus::Expired);
        assert_eq!(expired.error_code.as_deref(), Some("lease_expired"));
        assert!(expired.error_retryable);
        assert_eq!(expired.finished_at, Some(later));
        assert_eq!(expired.version(), 2);
        assert!(
            expired.succeed(&later).is_err(),
            "a replacement must be a new attempt, never a reopened expired one"
        );
    }

    #[test]
    fn lease_expiry_is_evaluated_against_durable_time() {
        let expiry = now();
        let mut attempt = attempt();
        attempt.lease_expires_at = Some(expiry);
        assert!(
            attempt.is_lease_expired_at(&expiry),
            "a lease expiring exactly now has lapsed"
        );
        let before = now()
            .checked_sub(Duration::from_secs(1))
            .expect("timestamp");
        assert!(!attempt.is_lease_expired_at(&before));
    }

    #[test]
    fn error_messages_are_truncated() {
        let later = now()
            .checked_add(Duration::from_secs(1))
            .expect("timestamp");
        let mut attempt = attempt();
        attempt
            .fail("boom", "x".repeat(9_000), false, &later)
            .expect("fail");
        assert_eq!(attempt.error.as_ref().map(String::len), Some(2048));
    }

    #[test]
    fn status_strings_match_the_durable_vocabulary() {
        assert_eq!(AttemptStatus::Running.as_str(), "running");
        assert_eq!(AttemptStatus::Succeeded.as_str(), "succeeded");
        assert_eq!(AttemptStatus::Failed.as_str(), "failed");
        assert_eq!(AttemptStatus::Expired.as_str(), "expired");
        assert_eq!(AttemptStatus::Cancelled.as_str(), "cancelled");
        assert_eq!(AttemptStatus::TimedOut.as_str(), "timed_out");
        assert!(!AttemptStatus::Running.is_terminal());
        assert!(AttemptStatus::Expired.is_terminal());
        assert!(AttemptStatus::Expired.is_retryable());
        assert!(AttemptStatus::Succeeded.is_terminal());
    }
}
