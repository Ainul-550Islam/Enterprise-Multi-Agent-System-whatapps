//! PURPOSE:
//! The named observability surface of the canonical durable execution pipeline.
//!
//! Layer / ownership
//! -----------------
//! `mas-application`, because every layer that emits these numbers (persistence,
//! worker, events) already depends on it, and because naming them in one place
//! is the only way to stop three crates from inventing three spellings of the
//! same signal.
//!
//! Production responsibility
//! -------------------------
//! Expose exactly the metrics an operator needs to answer "is the pipeline
//! draining, and if not, where is it stuck?" — submissions, idempotency
//! outcomes, claims, attempts, retries, stale rejections, terminal outcomes,
//! latency, outbox backlog and dead letters.
//!
//! What this module must NOT do
//! ----------------------------
//! * It must NOT accept identifiers as labels. Labels are *dimensions*
//!   (`outcome`, `lane`), never identities (`tenant_id`, `execution_id`,
//!   `task_id`): a per-tenant or per-execution label is a cardinality bomb that
//!   will OOM the collector. Identities belong in logs, where they are safe.
//! * It must NOT let a metrics failure affect the pipeline. Every emit is
//!   best-effort: a saturated or misused registry is reported once and then
//!   swallowed, because failing a durable settlement over a counter would be
//!   strictly worse than losing the counter.

use std::sync::Arc;

use mas_observability::metrics::{LabelSet, MetricsRegistry};

// ---------------------------------------------------------------------------
// The sixteen named families
// ---------------------------------------------------------------------------

/// Execution submissions accepted (new work, not replays).
pub const EXECUTION_SUBMISSIONS: &str = "mas_execution_submissions_total";
/// Client retries that found their own idempotency key and were answered from
/// the original response.
pub const IDEMPOTENCY_DUPLICATES: &str = "mas_idempotency_duplicate_requests_total";
/// Same idempotency key, different request fingerprint — rejected as a
/// deterministic conflict.
pub const IDEMPOTENCY_CONFLICTS: &str = "mas_idempotency_conflicts_total";
/// Durable tasks created (one per accepted execution submission).
pub const TASK_CREATED: &str = "mas_task_created_total";
/// Time from "task is claimable" to "task is owned by a worker".
pub const TASK_CLAIM_LATENCY_MS: &str = "mas_task_claim_latency_ms";
/// Attempts currently owned and unfinished on this worker.
pub const ACTIVE_ATTEMPTS: &str = "mas_active_attempts";
/// Attempts put back on the queue with a backoff after a transient failure.
pub const TASK_RETRIES: &str = "mas_task_retries_total";
/// Settlements refused because this worker no longer owned the task.
pub const STALE_WORKER_REJECTIONS: &str = "mas_stale_worker_rejections_total";
/// Wall-clock of an execution from submission to a terminal state.
pub const EXECUTION_DURATION_MS: &str = "mas_execution_duration_ms";
/// Executions that reached `succeeded`.
pub const EXECUTIONS_SUCCEEDED: &str = "mas_execution_succeeded_total";
/// Executions that reached `failed` (including dead-lettered).
pub const EXECUTIONS_FAILED: &str = "mas_execution_failed_total";
/// Executions that reached `cancelled`.
pub const EXECUTIONS_CANCELLED: &str = "mas_execution_cancelled_total";
/// Outbox rows waiting to be published.
pub const OUTBOX_BACKLOG: &str = "mas_outbox_backlog";
/// Outbox publish attempts the broker refused.
pub const OUTBOX_PUBLISH_FAILURES: &str = "mas_outbox_publish_failures_total";
/// Broker deliveries that are redeliveries of a message already seen.
pub const BROKER_REDELIVERIES: &str = "mas_broker_redeliveries_total";
/// Work that exhausted its attempt budget and was dead-lettered.
pub const DEAD_LETTERS: &str = "mas_dead_lettered_total";

/// Every family this pipeline owns, in declaration order.
///
/// Exposed so a test can assert that the registry actually produces all
/// sixteen, and so an operator can diff the exposition against this list.
pub const ALL_METRICS: [&str; 16] = [
    EXECUTION_SUBMISSIONS,
    IDEMPOTENCY_DUPLICATES,
    IDEMPOTENCY_CONFLICTS,
    TASK_CREATED,
    TASK_CLAIM_LATENCY_MS,
    ACTIVE_ATTEMPTS,
    TASK_RETRIES,
    STALE_WORKER_REJECTIONS,
    EXECUTION_DURATION_MS,
    EXECUTIONS_SUCCEEDED,
    EXECUTIONS_FAILED,
    EXECUTIONS_CANCELLED,
    OUTBOX_BACKLOG,
    OUTBOX_PUBLISH_FAILURES,
    BROKER_REDELIVERIES,
    DEAD_LETTERS,
];

// ---------------------------------------------------------------------------
// Dimensions — small, closed vocabularies only
// ---------------------------------------------------------------------------

/// Label key for the outcome dimension.
pub const OUTCOME_LABEL: &str = "outcome";

/// Closed vocabulary for the `outcome` label.
///
/// Closed on purpose: a free-text outcome is how a metric family acquires a
/// thousand series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// New work accepted.
    Accepted,
    /// An idempotent replay of already-accepted work.
    Replay,
    /// Rejected: the same key was reused with a different payload.
    Conflict,
    /// Rejected: the request itself is invalid.
    Invalid,
    /// The run finished successfully.
    Succeeded,
    /// The run failed (transient or terminal).
    Failed,
    /// The run was cancelled deliberately.
    Cancelled,
    /// The attempt budget was spent.
    DeadLettered,
}

impl Outcome {
    /// Stable metric label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Replay => "replay",
            Self::Conflict => "conflict",
            Self::Invalid => "invalid",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::DeadLettered => "dead_lettered",
        }
    }
}

/// Builds a one-dimensional `outcome` label set.
fn outcome_label(outcome: Outcome) -> LabelSet {
    // Cannot fail: one short key, one short value from a closed set. A failure
    // here would be a bug in this module, and metrics must never be able to
    // fail a durable write anyway, so the error is discarded.
    LabelSet::build([(OUTCOME_LABEL, outcome.as_str())]).unwrap_or_else(|_| LabelSet::empty())
}

// ---------------------------------------------------------------------------
// The emitter
// ---------------------------------------------------------------------------

/// Typed facade over the shared [`MetricsRegistry`].
///
/// Every method is best-effort: the counters are the pipeline's nervous system,
/// not its spine. If the registry refuses (bad name, saturated family), the
/// failure is logged once per call and otherwise ignored.
#[derive(Debug, Clone)]
pub struct PipelineMetrics {
    registry: Arc<MetricsRegistry>,
}

impl Default for PipelineMetrics {
    fn default() -> Self {
        Self::new(Arc::new(MetricsRegistry::new()))
    }
}

impl PipelineMetrics {
    /// Binds to a registry.
    #[must_use]
    pub fn new(registry: Arc<MetricsRegistry>) -> Self {
        Self { registry }
    }

    /// The underlying registry, for exposition endpoints.
    #[must_use]
    pub fn registry(&self) -> &Arc<MetricsRegistry> {
        &self.registry
    }

    // -- submission --------------------------------------------------------

    /// One execution submission resolved: accepted, replayed, or rejected.
    pub fn submission(&self, outcome: Outcome) {
        self.count(
            EXECUTION_SUBMISSIONS,
            outcome,
            "execution submission outcome",
            |_| {},
        );
    }

    /// A client retry that was answered from the original response.
    pub fn idempotency_replay(&self) {
        self.bump(IDEMPOTENCY_DUPLICATES, "idempotent replay");
    }

    /// The same idempotency key with a different request fingerprint.
    pub fn idempotency_conflict(&self) {
        self.bump(IDEMPOTENCY_CONFLICTS, "idempotency conflict");
    }

    /// A durable task row was written.
    pub fn task_created(&self) {
        self.bump(TASK_CREATED, "task created");
    }

    // -- claims and attempts ----------------------------------------------

    /// Time a task waited between becoming claimable and being owned.
    pub fn claim_latency_ms(&self, millis: f64) {
        if let Err(error) =
            self.registry
                .observe_histogram(TASK_CLAIM_LATENCY_MS, &LabelSet::empty(), millis)
        {
            tracing::warn!(metric = TASK_CLAIM_LATENCY_MS, %error, "metric emit failed");
        }
    }

    /// Current in-flight attempt count on this worker.
    pub fn set_active_attempts(&self, active: i64) {
        if let Err(error) = self
            .registry
            .set_gauge(ACTIVE_ATTEMPTS, &LabelSet::empty(), active)
        {
            tracing::warn!(metric = ACTIVE_ATTEMPTS, %error, "metric emit failed");
        }
    }

    /// A transient failure put the task back on the queue.
    pub fn task_retry(&self) {
        self.bump(TASK_RETRIES, "task requeued after a transient failure");
    }

    /// A settlement was refused because ownership had moved.
    pub fn stale_worker_rejection(&self) {
        self.bump(STALE_WORKER_REJECTIONS, "stale ownership");
    }

    // -- terminal outcomes -------------------------------------------------

    /// Records the terminal outcome the worker is about to settle.
    ///
    /// Mirrors the durable classification rather than re-deriving it, so the
    /// counters and the retry decision can never disagree about what happened.
    ///
    /// Retry-vs-dead-letter is **not** decided here: that is the durable port's
    /// call, and it reports it on the settlement receipt.
    pub fn observe_outcome(&self, outcome: &crate::durable::TaskOutcome) {
        use crate::durable::TaskOutcome;
        match outcome {
            TaskOutcome::Succeeded { .. } => self.execution_terminal(Outcome::Succeeded, 0.0),
            TaskOutcome::Cancelled => self.execution_terminal(Outcome::Cancelled, 0.0),
            TaskOutcome::Failed { .. } => self.execution_terminal(Outcome::Failed, 0.0),
        }
    }

    /// An execution reached a terminal state, with its measured duration.
    pub fn execution_terminal(&self, outcome: Outcome, duration_ms: f64) {
        let family = match outcome {
            Outcome::Succeeded => EXECUTIONS_SUCCEEDED,
            Outcome::Cancelled => EXECUTIONS_CANCELLED,
            _ => EXECUTIONS_FAILED,
        };
        self.bump(family, "execution terminal outcome");
        if let Err(error) = self.registry.observe_histogram(
            EXECUTION_DURATION_MS,
            &outcome_label(outcome),
            duration_ms,
        ) {
            tracing::warn!(metric = EXECUTION_DURATION_MS, %error, "metric emit failed");
        }
    }

    /// Work exhausted its attempt budget.
    pub fn dead_letter(&self) {
        self.bump(DEAD_LETTERS, "dead lettered");
    }

    // -- outbox and broker -------------------------------------------------

    /// Current unpublished outbox depth.
    pub fn set_outbox_backlog(&self, pending: i64) {
        if let Err(error) = self
            .registry
            .set_gauge(OUTBOX_BACKLOG, &LabelSet::empty(), pending)
        {
            tracing::warn!(metric = OUTBOX_BACKLOG, %error, "metric emit failed");
        }
    }

    /// The broker refused an outbox publish.
    pub fn outbox_publish_failure(&self) {
        self.bump(OUTBOX_PUBLISH_FAILURES, "outbox publish failed");
    }

    /// A delivery the broker has already handed us before.
    pub fn broker_redelivery(&self) {
        self.bump(BROKER_REDELIVERIES, "broker redelivery");
    }

    // -- internals ---------------------------------------------------------

    fn bump(&self, family: &'static str, _what: &'static str) {
        if let Err(error) = self.registry.inc_counter(family, &LabelSet::empty(), 1) {
            tracing::warn!(metric = family, %error, "metric emit failed");
        }
    }

    fn count<F: FnOnce(&MetricsRegistry)>(
        &self,
        family: &'static str,
        outcome: Outcome,
        _what: &'static str,
        _extra: F,
    ) {
        if let Err(error) = self
            .registry
            .inc_counter(family, &outcome_label(outcome), 1)
        {
            tracing::warn!(metric = family, %error, "metric emit failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metrics() -> PipelineMetrics {
        PipelineMetrics::default()
    }

    #[test]
    fn all_sixteen_families_are_declared_and_valid() {
        for name in ALL_METRICS {
            mas_observability::metrics::validate_family(name)
                .unwrap_or_else(|e| panic!("{name} is a valid metric family name: {e}"));
        }
        assert_eq!(ALL_METRICS.len(), 16, "the spec names sixteen metrics");
    }

    #[test]
    fn every_family_actually_appears_once_the_pipeline_uses_it() {
        let metrics = metrics();
        metrics.submission(Outcome::Accepted);
        metrics.submission(Outcome::Replay);
        metrics.submission(Outcome::Conflict);
        metrics.idempotency_replay();
        metrics.idempotency_conflict();
        metrics.task_created();
        metrics.claim_latency_ms(12.5);
        metrics.set_active_attempts(3);
        metrics.task_retry();
        metrics.stale_worker_rejection();
        metrics.execution_terminal(Outcome::Succeeded, 250.0);
        metrics.execution_terminal(Outcome::Failed, 90.0);
        metrics.execution_terminal(Outcome::Cancelled, 10.0);
        metrics.set_outbox_backlog(7);
        metrics.outbox_publish_failure();
        metrics.broker_redelivery();
        metrics.dead_letter();

        let registry = metrics.registry();
        for name in ALL_METRICS {
            let present = registry
                .render_prometheus()
                .lines()
                .any(|line| line.starts_with(&format!("# TYPE {name} ")));
            assert!(present, "{name} is exposed after the pipeline emits it");
        }
    }

    #[test]
    fn submission_outcomes_are_separate_series_not_one_bucket() {
        let metrics = metrics();
        metrics.submission(Outcome::Accepted);
        metrics.submission(Outcome::Accepted);
        metrics.submission(Outcome::Replay);

        let registry = metrics.registry();
        let accepted = LabelSet::build([(OUTCOME_LABEL, "accepted")]).expect("label");
        let replay = LabelSet::build([(OUTCOME_LABEL, "replay")]).expect("label");
        assert_eq!(registry.counter_value(EXECUTION_SUBMISSIONS, &accepted), 2);
        assert_eq!(registry.counter_value(EXECUTION_SUBMISSIONS, &replay), 1);
    }

    #[test]
    fn terminal_outcomes_land_in_their_own_families() {
        let metrics = metrics();
        metrics.execution_terminal(Outcome::Succeeded, 1.0);
        metrics.execution_terminal(Outcome::Failed, 2.0);
        metrics.execution_terminal(Outcome::Cancelled, 3.0);
        metrics.execution_terminal(Outcome::DeadLettered, 4.0);

        let registry = metrics.registry();
        let empty = LabelSet::empty();
        assert_eq!(registry.counter_value(EXECUTIONS_SUCCEEDED, &empty), 1);
        assert_eq!(registry.counter_value(EXECUTIONS_CANCELLED, &empty), 1);
        // `failed` and `dead_lettered` both count as failures, which is the
        // point: a dead letter is a failure an operator must see.
        assert_eq!(registry.counter_value(EXECUTIONS_FAILED, &empty), 2);
    }

    #[test]
    fn gauges_hold_the_latest_value_rather_than_accumulating() {
        let metrics = metrics();
        metrics.set_active_attempts(5);
        metrics.set_active_attempts(2);
        metrics.set_outbox_backlog(100);
        metrics.set_outbox_backlog(4);

        let registry = metrics.registry();
        let empty = LabelSet::empty();
        assert_eq!(registry.gauge_value(ACTIVE_ATTEMPTS, &empty), 2);
        assert_eq!(registry.gauge_value(OUTBOX_BACKLOG, &empty), 4);
    }

    #[test]
    fn outcome_labels_are_a_closed_vocabulary() {
        // Every variant renders to a distinct, lowercase, snake-case token:
        // a free-text outcome would be a cardinality bomb.
        let all = [
            Outcome::Accepted,
            Outcome::Replay,
            Outcome::Conflict,
            Outcome::Invalid,
            Outcome::Succeeded,
            Outcome::Failed,
            Outcome::Cancelled,
            Outcome::DeadLettered,
        ];
        let mut rendered = std::collections::BTreeSet::new();
        for outcome in all {
            let text = outcome.as_str();
            assert!(
                text.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "{text} is a safe label value"
            );
            assert!(rendered.insert(text), "{text} is distinct");
        }
    }
}
