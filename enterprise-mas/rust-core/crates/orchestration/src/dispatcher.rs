//! Task dispatch: from a stored, queued [`Task`] to a transport hop.
//!
//! [`DispatchTransport`] is the outbound port (NATS/queue adapter implements
//! it outside this crate); [`TaskDispatcher`] is the engine-facing facade
//! enforcing dispatch invariants:
//!
//! * only `Queued` tasks are dispatched (anything else is a conflict),
//! * every envelope carries a deterministic `dedup_key`
//!   (`<task_id>:<attempt>`) so redelivery is safe for the consumer,
//! * payloads are size-capped before hitting the transport.
//!
//! [`InMemoryDispatchTransport`] ships in-crate for unit tests and local
//! development; it records envelopes and supports failure injection.

use mas_common::constants;
use mas_common::enums::TaskStatus;
use mas_common::error::AppError;
use mas_common::ids::TaskId;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::Task;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// The wire unit handed to the transport. Snapshot semantics: the task row as
/// it was when dispatched; consumers treat it as immutable and re-load from
/// the store when in doubt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DispatchEnvelope {
    pub envelope_id: uuid::Uuid,
    pub task: Task,
    /// Attempt this dispatch is for (1-based; equals `task.attempt_count`
    /// at dispatch once started… the consumer trusts `dedup_key`, not this).
    pub attempt: u32,
    /// Deterministic redelivery key: consumer idempotency over duplicates.
    pub dedup_key: String,
    /// End-to-end trace key (execution correlation when present).
    pub correlation_id: String,
    /// Delay hint for transports that support scheduled delivery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_before: Option<Timestamp>,
    pub dispatched_at: Timestamp,
}

impl DispatchEnvelope {
    /// Builds an envelope for a queued task.
    pub fn new(
        task: &Task,
        correlation_id: impl Into<String>,
        not_before: Option<Timestamp>,
    ) -> Result<Self> {
        let correlation_id = correlation_id.into();
        if correlation_id.is_empty() {
            return Err(AppError::invalid_field(
                "correlation_id",
                "required",
                "dispatch envelopes need a correlation id",
            ));
        }
        let attempt = task.attempt_count.max(1);
        Ok(Self {
            envelope_id: uuid::Uuid::now_v7(),
            task: task.clone(),
            attempt,
            dedup_key: format!("{}:{attempt}", task.id),
            correlation_id,
            not_before,
            dispatched_at: Timestamp::now(),
        })
    }

    /// Serialized size guard used before dispatch.
    pub fn serialized_len(&self) -> Result<usize> {
        serde_json::to_vec(self)
            .map(|bytes| bytes.len())
            .map_err(|e| AppError::serialization(format!("envelope not serializable: {e}")))
    }
}

/// Transport acknowledgement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DispatchReceipt {
    /// Transport-native reference (NATS ack sequence, message id…).
    pub transport_ref: String,
    pub accepted_at: Timestamp,
    /// Which transport produced the receipt (`DispatchTransport::name`).
    pub transport: String,
}

/// Outbound messaging port (implemented by messaging/NATS adapters).
#[async_trait::async_trait]
pub trait DispatchTransport: Send + Sync + fmt::Debug {
    /// Stable transport identifier (for receipts + logs).
    fn name(&self) -> &'static str;

    /// Hands the envelope to the transport. `Ok(receipt)` means *accepted by
    /// the transport*, not consumed by a worker; delivery guarantees beyond
    /// this are the transport's contract.
    async fn send(&self, envelope: DispatchEnvelope) -> Result<DispatchReceipt>;
}

/// Engine-facing dispatch facade.
#[async_trait::async_trait]
pub trait TaskDispatcher: Send + Sync + fmt::Debug {
    /// Dispatches one queued task. Conflicts on non-queued state.
    async fn dispatch(&self, task: &Task) -> Result<DispatchReceipt>;

    /// Dispatches with an earliest-delivery hint (retry backoff).
    async fn dispatch_delayed(&self, task: &Task, not_before: Timestamp)
        -> Result<DispatchReceipt>;

    /// Dispatches many tasks; per-task results without early abort.
    async fn dispatch_batch(&self, tasks: &[Task]) -> Vec<(TaskId, Result<DispatchReceipt>)>;
}

/// Default dispatcher over any [`DispatchTransport`].
#[derive(Clone)]
pub struct BasicTaskDispatcher {
    transport: Arc<dyn DispatchTransport>,
}

impl fmt::Debug for BasicTaskDispatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BasicTaskDispatcher")
            .field("transport", &self.transport.name())
            .finish()
    }
}

impl BasicTaskDispatcher {
    pub fn new(transport: Arc<dyn DispatchTransport>) -> Self {
        Self { transport }
    }

    #[must_use]
    pub fn transport_name(&self) -> &'static str {
        self.transport.name()
    }

    fn prepare(&self, task: &Task, not_before: Option<Timestamp>) -> Result<DispatchEnvelope> {
        if task.status != TaskStatus::Queued {
            return Err(AppError::conflict(format!(
                "task {} is in status '{}' and cannot be dispatched",
                task.id, task.status
            )));
        }
        if task.is_expired() {
            return Err(AppError::timeout(format!(
                "task {} deadline passed before dispatch",
                task.id
            )));
        }
        let correlation = task
            .execution_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| format!("task:{}", task.id));
        let envelope = DispatchEnvelope::new(task, correlation, not_before)?;
        let size = envelope.serialized_len()?;
        if size > constants::MAX_PAYLOAD_BYTES {
            return Err(AppError::invalid_field(
                "payload",
                "too_large",
                format!(
                    "dispatch envelope is {size} bytes (limit {})",
                    constants::MAX_PAYLOAD_BYTES
                ),
            ));
        }
        Ok(envelope)
    }
}

#[async_trait::async_trait]
impl TaskDispatcher for BasicTaskDispatcher {
    async fn dispatch(&self, task: &Task) -> Result<DispatchReceipt> {
        let envelope = self.prepare(task, None)?;
        self.transport.send(envelope).await
    }

    async fn dispatch_delayed(
        &self,
        task: &Task,
        not_before: Timestamp,
    ) -> Result<DispatchReceipt> {
        if not_before.is_before(&Timestamp::now()) {
            return self.dispatch(task).await;
        }
        let envelope = self.prepare(task, Some(not_before))?;
        self.transport.send(envelope).await
    }

    async fn dispatch_batch(&self, tasks: &[Task]) -> Vec<(TaskId, Result<DispatchReceipt>)> {
        let mut out = Vec::with_capacity(tasks.len());
        for task in tasks {
            out.push((task.id, self.dispatch(task).await));
        }
        out
    }
}

/// Failure-injection modes for the in-memory transport.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum Injection {
    #[default]
    None,
    /// Every send fails with a retryable messaging error.
    FailAll,
    /// The next N sends fail (counted down atomically).
    FailNext(usize),
}

/// In-memory transport for unit tests and local development. Envelope order
/// is preserved; receipts carry a monotonically increasing reference.
#[derive(Debug, Default)]
pub struct InMemoryDispatchTransport {
    sent: Mutex<Vec<DispatchEnvelope>>,
    injection: Mutex<Injection>,
    sequence: AtomicUsize,
}

impl InMemoryDispatchTransport {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every `send` fails (transient messaging failure).
    pub fn fail_all(&self) {
        *self.injection.lock().unwrap_or_else(|e| e.into_inner()) = Injection::FailAll;
    }

    /// The next `count` sends fail.
    pub fn fail_next(&self, count: usize) {
        *self.injection.lock().unwrap_or_else(|e| e.into_inner()) = Injection::FailNext(count);
    }

    /// Snapshot of everything sent so far.
    #[must_use]
    pub fn sent(&self) -> Vec<DispatchEnvelope> {
        self.sent.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    #[must_use]
    pub fn sent_count(&self) -> usize {
        self.sent.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Envelopes carrying a given task id (all attempts).
    #[must_use]
    pub fn sent_for_task(&self, task_id: TaskId) -> Vec<DispatchEnvelope> {
        self.sent()
            .into_iter()
            .filter(|envelope| envelope.task.id == task_id)
            .collect()
    }
}

#[async_trait::async_trait]
impl DispatchTransport for InMemoryDispatchTransport {
    fn name(&self) -> &'static str {
        "in-memory"
    }

    async fn send(&self, envelope: DispatchEnvelope) -> Result<DispatchReceipt> {
        {
            let mut injection = self.injection.lock().unwrap_or_else(|e| e.into_inner());
            match *injection {
                Injection::FailAll => {
                    return Err(AppError::messaging(
                        "in-memory transport failure (injected, fail-all)",
                    ));
                },
                Injection::FailNext(remaining) if remaining > 0 => {
                    *injection = Injection::FailNext(remaining - 1);
                    return Err(AppError::messaging(
                        "in-memory transport failure (injected)",
                    ));
                },
                _ => {},
            }
        }
        self.sent
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(envelope);
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        Ok(DispatchReceipt {
            transport_ref: format!("mem-{sequence}"),
            accepted_at: Timestamp::now(),
            transport: self.name().to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::enums::TaskPriority;
    use mas_common::ids::{AgentId, OrganizationId, ProjectId, TenantId};

    fn queued_task(key: &str) -> Task {
        let mut task = Task::new(
            TenantId::new(),
            OrganizationId::new(),
            ProjectId::new(),
            Some(AgentId::new()),
            None,
            "agent.run",
            serde_json::json!({"prompt": "hello"}),
            TaskPriority::Normal,
            key,
            "tester",
        )
        .expect("task");
        task.queue().expect("queue");
        task
    }

    #[tokio::test]
    async fn dispatch_requires_queued_tasks() {
        let transport = Arc::new(InMemoryDispatchTransport::new());
        let dispatcher = BasicTaskDispatcher::new(transport.clone());
        let task = queued_task("idem-1");
        let receipt = dispatcher.dispatch(&task).await.expect("dispatch");
        assert_eq!(receipt.transport, "in-memory");
        assert_eq!(transport.sent_count(), 1);
        let envelope = &transport.sent()[0];
        assert_eq!(envelope.dedup_key, format!("{}:1", task.id));

        // A running task cannot be dispatched.
        let mut running = queued_task("idem-2");
        running.start().expect("start");
        let error = dispatcher.dispatch(&running).await.unwrap_err();
        assert_eq!(error.error_code(), "CONFLICT");
    }

    #[tokio::test]
    async fn delayed_dispatch_and_failure_injection() {
        let transport = Arc::new(InMemoryDispatchTransport::new());
        let dispatcher = BasicTaskDispatcher::new(transport.clone());
        let task = queued_task("idem-3");

        transport.fail_next(1);
        let error = dispatcher.dispatch(&task).await.unwrap_err();
        assert!(error.is_retryable());
        assert_eq!(transport.sent_count(), 0);

        let future = Timestamp::now()
            .checked_add(std::time::Duration::from_secs(60))
            .expect("future");
        dispatcher
            .dispatch_delayed(&task, future)
            .await
            .expect("delayed dispatch");
        let envelope = &transport.sent()[0];
        assert_eq!(envelope.not_before, Some(future));
    }

    #[tokio::test]
    async fn batch_dispatch_isolates_failures() {
        let transport = Arc::new(InMemoryDispatchTransport::new());
        let dispatcher = BasicTaskDispatcher::new(transport.clone());
        let ok = queued_task("ok-1");
        let mut bad = queued_task("bad-1");
        bad.start().expect("start"); // not queued anymore

        let results = dispatcher.dispatch_batch(&[ok, bad]).await;
        assert_eq!(results.len(), 2);
        assert!(results[0].1.is_ok());
        assert!(results[1].1.is_err());
    }
}
