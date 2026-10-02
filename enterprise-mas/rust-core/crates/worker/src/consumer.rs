//! The bounded-concurrency task consumer. See `lib.rs` for the settlement
//! protocol this implements; the code below is written so every outcome
//! maps to exactly one broker instruction *and* one lifecycle record.

use std::sync::Arc;
use std::time::Duration;

use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_contracts::task::TaskQueueMessage;
use mas_messaging::broker::{
    AckInstruction, BrokerMessage, BrokerPort, ConsumerConfig as BrokerConsumerConfig,
};
use mas_messaging::codec::{CodecConfig, JsonEventCodec};
use mas_scheduling::lease::{Lease, LeaseStorePort};

use crate::backoff::RetryPolicy;
use crate::grace::ShutdownWatch;
use crate::handler::TaskHandlerPort;
use crate::lifecycle::TaskLifecyclePort;

/// Tuning knobs for one consumer instance.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Durable consumer name (snake_case) — replicas share it for
    /// competing-consumers semantics.
    pub consumer_name: String,
    /// Subjection filter (e.g. `mas.tasks.>`).
    pub filter: mas_messaging::subjects::SubjectFilter,
    /// Messages per broker fetch.
    pub batch_size: usize,
    /// Broker ack window; heartbeat runs at half of this.
    pub ack_wait: Duration,
    /// Idle sleep between empty fetches.
    pub poll_interval: Duration,
    /// Fenced-task lease window (renewed with the heartbeat).
    pub lease_ttl: Duration,
    /// Max simultaneous in-flight tasks on this instance.
    pub max_in_flight: usize,
    /// How long `run()` keeps draining after shutdown before giving the
    /// broker back the remaining messages (their leases expire naturally).
    pub drain_timeout: Duration,
    /// Worker identity recorded in leases/logs.
    pub worker_name: String,
    /// Retry pacing (base/cap/max_deliver).
    pub retry: RetryPolicy,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            consumer_name: "mas-worker".to_owned(),
            filter: mas_messaging::subjects::SubjectFilter::parse("mas.tasks.>")
                .expect("valid subject filter"),
            batch_size: 16,
            ack_wait: Duration::from_secs(30),
            poll_interval: Duration::from_millis(50),
            lease_ttl: Duration::from_secs(15),
            max_in_flight: 8,
            drain_timeout: Duration::from_secs(10),
            worker_name: "worker-0".to_owned(),
            retry: RetryPolicy::default(),
        }
    }
}

/// Final accounting when `run` returns.
#[derive(Debug, Clone, Default)]
pub struct ConsumerOutcome {
    /// Messages settled `Ack`.
    pub acked: u64,
    /// Messages settled `Nak` (all classes: transient retry, lease conflict).
    pub naks: u64,
    /// Messages settled `Term` (poison/expired/exhausted/undecodable).
    pub terms: u64,
    /// Handlers succeeded.
    pub completed: u64,
    /// Handlers failed (any class).
    pub failed: u64,
    /// Messages skipped as orphans (row gone) or undecodable.
    pub skipped: u64,
    /// Times a lease conflict deferred a message.
    pub lease_conflicts: u64,
    /// In-flight messages the consumer could not drain before
    /// `drain_timeout` (broker redelivers after `ack_wait`).
    pub abandoned: u64,
}

/// The consumer: broker + leases + lifecycle + handler.
#[derive(Debug)]
pub struct TaskConsumer {
    broker: Arc<dyn BrokerPort>,
    leases: Arc<dyn LeaseStorePort>,
    lifecycle: Arc<dyn TaskLifecyclePort>,
    handler: Arc<dyn TaskHandlerPort>,
    config: ConsumerConfig,
}

/// Outcome category of one mediated execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Settlement {
    Ack,
    Nak { delay: Duration, kind: NakKind },
    Term { kind: TermKind },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NakKind {
    Transient,
    LeaseConflict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TermKind {
    Poison,
    Expired,
    Exhausted,
    Undecodable,
}

impl TaskConsumer {
    /// Wires the consumer ports.
    pub fn new(
        broker: Arc<dyn BrokerPort>,
        leases: Arc<dyn LeaseStorePort>,
        lifecycle: Arc<dyn TaskLifecyclePort>,
        handler: Arc<dyn TaskHandlerPort>,
        config: ConsumerConfig,
    ) -> Self {
        Self {
            broker,
            leases,
            lifecycle,
            handler,
            config,
        }
    }

    /// Ensures the durable consumer exists (subscribe-time subject config),
    /// then runs the fetch loop until `shutdown` flips, drains in-flight,
    /// and returns the accounting snapshot.
    pub async fn run(&self, mut shutdown: ShutdownWatch) -> Result<ConsumerOutcome> {
        self.broker
            .ensure_consumer(BrokerConsumerConfig {
                name: self.config.consumer_name.clone(),
                filter: self.config.filter.clone(),
                ack_wait: self.config.ack_wait,
                max_deliver: self.config.retry.max_deliver,
            })
            .await?;

        let mut counters = ConsumerOutcome::default();
        let mut in_flight = tokio::task::JoinSet::<(u64 /*sequence*/, MessageOutcome)>::new();

        // ── Main loop: fetch while shutdown is unset. ────────────────
        while !shutdown.is_set() {
            // Reap whichever in-flight mediations finished.
            reap_results(&mut in_flight, &mut counters);

            if in_flight.len() < self.config.max_in_flight {
                match self
                    .broker
                    .fetch(&self.config.consumer_name, self.config.batch_size)
                    .await
                {
                    Ok(messages) => {
                        if messages.is_empty() {
                            tokio::select! {
                                biased;
                                () = shutdown.changed() => break,
                                () = tokio::time::sleep(self.config.poll_interval) => {},
                            }
                        }
                        for message in messages {
                            if in_flight.len() >= self.config.max_in_flight {
                                // Leave overflow un-fetched in the next cycle:
                                // already-fetched ones are mediated regardless.
                                tracing::warn!("in-flight saturation: mediation deferred");
                            }
                            let mediator = self.mediator();
                            let sequence = message.sequence;
                            in_flight
                                .spawn(async move { (sequence, mediator.mediate(message).await) });
                        }
                    },
                    Err(err) => {
                        tracing::error!(%err, "broker fetch failed; backing off");
                        tokio::select! {
                            biased;
                            () = shutdown.changed() => break,
                            () = tokio::time::sleep(self.config.poll_interval * 10) => {},
                        }
                    },
                }
            } else {
                reap_results(&mut in_flight, &mut counters);
                tokio::time::sleep(self.config.poll_interval).await;
            }
        }

        // ── Drain: stop fetching, let in-flight finish or abandon. ───
        let drain_deadline = tokio::time::Instant::now() + self.config.drain_timeout;
        while !in_flight.is_empty() {
            let now = tokio::time::Instant::now();
            if now >= drain_deadline {
                counters.abandoned += in_flight.len() as u64;
                in_flight.detach_all();
                tracing::warn!(
                    abandoned = counters.abandoned,
                    "drain timeout reached; broker redelivers after ack_wait"
                );
                break;
            }
            let remaining = drain_deadline.saturating_duration_since(now);
            match tokio::time::timeout(remaining, in_flight.join_next()).await {
                Ok(Some(joined)) => {
                    let (sequence, outcome) =
                        joined.map_err(|e| AppError::internal(format!("task panicked: {e}")))?;
                    trace_result(sequence, &outcome);
                    credits_to_counters(&outcome, &mut counters);
                },
                Ok(None) => break,
                Err(_) => {}, // timeout elapsed; loop re-checks
            }
        }

        tracing::info!(
            acked = counters.acked,
            naks = counters.naks,
            terms = counters.terms,
            abandoned = counters.abandoned,
            "consumer drained"
        );
        Ok(counters)
    }

    /// Owned snapshot of all ports for spawned mediation futures.
    fn mediator(&self) -> Mediator {
        Mediator {
            broker: Arc::clone(&self.broker),
            leases: Arc::clone(&self.leases),
            lifecycle: Arc::clone(&self.lifecycle),
            handler: Arc::clone(&self.handler),
            codec: JsonEventCodec::new(CodecConfig::default()),
            config: self.config.clone(),
        }
    }
}

/// An owned snapshot of the consumer's ports, so per-message mediation
/// futures can be `join_set.spawn`ed with `'static` lifetime.
#[derive(Debug, Clone)]
struct Mediator {
    broker: Arc<dyn BrokerPort>,
    leases: Arc<dyn LeaseStorePort>,
    lifecycle: Arc<dyn TaskLifecyclePort>,
    handler: Arc<dyn TaskHandlerPort>,
    codec: JsonEventCodec,
    config: ConsumerConfig,
}

impl Mediator {
    /// One full mediation for one broker message: pre-checks, lease,
    /// heartbeat-wrapped handler invocation, settlement.
    async fn mediate(&self, message: BrokerMessage) -> MessageOutcome {
        let sequence = message.sequence;
        let deliveries = message.deliveries;

        // ── 1. Decode. Undecodable frames never heal. ────────────────
        let decoded: std::result::Result<TaskQueueMessage, _> =
            self.codec.decode_value(&message.payload);
        let task_msg = match decoded {
            Ok(m) => m,
            Err(err) => {
                tracing::error!(sequence, %err, "undecodable task frame");
                let _ = self
                    .broker
                    .ack(&self.config.consumer_name, sequence, AckInstruction::Term)
                    .await;
                return MessageOutcome {
                    settlement: Settlement::Term {
                        kind: TermKind::Undecodable,
                    },
                    handler_failed: false,
                };
            },
        };
        let task_id = task_msg.task_id;

        // ── 2. Deadline: expired before start ⇒ settle Term, fail row. ─
        if let Some(deadline) = task_msg.deadline {
            if !deadline.is_after(&Timestamp::now()) {
                self.fail_row(task_id, "task deadline passed before start")
                    .await;
                let _ = self
                    .broker
                    .ack(&self.config.consumer_name, sequence, AckInstruction::Term)
                    .await;
                return MessageOutcome {
                    settlement: Settlement::Term {
                        kind: TermKind::Expired,
                    },
                    handler_failed: true,
                };
            }
        }

        // ── 3. Lifecycle row (orphans are acknowledged away). ─────────
        let mut task = match self.lifecycle.load(task_id).await {
            Ok(Some(task)) => task,
            Ok(None) => {
                tracing::warn!(%task_id, sequence, "orphan task message — acking");
                let _ = self
                    .broker
                    .ack(&self.config.consumer_name, sequence, AckInstruction::Ack)
                    .await;
                return MessageOutcome {
                    settlement: Settlement::Ack,
                    handler_failed: false,
                };
            },
            Err(err) => {
                tracing::error!(%err, "lifecycle load failed");
                // Storage hiccup: retry via broker.
                let delay = self.config.retry.delay_for(deliveries);
                let _ = self
                    .broker
                    .ack(
                        &self.config.consumer_name,
                        sequence,
                        AckInstruction::Nak { delay: Some(delay) },
                    )
                    .await;
                return MessageOutcome {
                    settlement: Settlement::Nak {
                        delay,
                        kind: NakKind::Transient,
                    },
                    handler_failed: true,
                };
            },
        };

        // ── 4. Fenced lease. Conflict ⇒ defer, do not touch the row. ──
        let resource = format!("task:{task_id}");
        let lease = match self
            .leases
            .try_acquire(
                &resource,
                &self.config.worker_name,
                self.config.lease_ttl,
                &Timestamp::now(),
            )
            .await
        {
            Ok(lease) => Some(lease),
            Err(AppError::Conflict(_)) => {
                let delay = self.config.retry.bound_for(1);
                let _ = self
                    .broker
                    .ack(
                        &self.config.consumer_name,
                        sequence,
                        AckInstruction::Nak { delay: Some(delay) },
                    )
                    .await;
                return MessageOutcome {
                    settlement: Settlement::Nak {
                        delay,
                        kind: NakKind::LeaseConflict,
                    },
                    handler_failed: false,
                };
            },
            Err(err) => {
                tracing::error!(%err, "lease store failed");
                let delay = self.config.retry.delay_for(deliveries);
                let _ = self
                    .broker
                    .ack(
                        &self.config.consumer_name,
                        sequence,
                        AckInstruction::Nak { delay: Some(delay) },
                    )
                    .await;
                return MessageOutcome {
                    settlement: Settlement::Nak {
                        delay,
                        kind: NakKind::Transient,
                    },
                    handler_failed: true,
                };
            },
        };

        // ── 5. Row into Running (first live delivery only). ──────────
        let row_note: std::result::Result<(), AppError> = async {
            use mas_common::enums::TaskStatus;
            match task.status {
                TaskStatus::Queued => task.start()?,
                TaskStatus::Running => {}, // re-drive after crash: fine, row already counts it
                _ => {
                    // Already terminal (cancelled by api while queued, etc.):
                    // nothing to do; settle Ack below.
                    return Err(AppError::cancelled("task already terminal"));
                },
            }
            self.lifecycle.save(&task).await
        }
        .await;
        if let Err(note) = &row_note {
            if matches!(note, AppError::Cancelled(_)) {
                self.release_lease(lease.as_ref()).await;
                let _ = self
                    .broker
                    .ack(&self.config.consumer_name, sequence, AckInstruction::Ack)
                    .await;
                return MessageOutcome {
                    settlement: Settlement::Ack,
                    handler_failed: false,
                };
            }
            if matches!(note, AppError::Timeout(_) | AppError::Conflict(_)) {
                let term_kind = if matches!(note, AppError::Timeout(_)) {
                    TermKind::Expired
                } else {
                    TermKind::Exhausted
                };
                self.release_lease(lease.as_ref()).await;
                let _ = self
                    .broker
                    .ack(&self.config.consumer_name, sequence, AckInstruction::Term)
                    .await;
                return MessageOutcome {
                    settlement: Settlement::Term { kind: term_kind },
                    handler_failed: true,
                };
            }
            // Persistence hiccup: defer.
            let delay = self.config.retry.delay_for(deliveries);
            self.release_lease(lease.as_ref()).await;
            let _ = self
                .broker
                .ack(
                    &self.config.consumer_name,
                    sequence,
                    AckInstruction::Nak { delay: Some(delay) },
                )
                .await;
            return MessageOutcome {
                settlement: Settlement::Nak {
                    delay,
                    kind: NakKind::Transient,
                },
                handler_failed: true,
            };
        }

        // ── 6. Run the handler, heartbeating at half ack_wait. ───────
        let heartbeat_interval = self.config.ack_wait / 2;
        let broker = self.broker.clone();
        let leases = self.leases.clone();
        let consumer_name = self.config.consumer_name.clone();
        let lease_for_heartbeat = lease.clone();
        let lease_ttl = self.config.lease_ttl;
        let heartbeat = tokio::spawn(async move {
            let mut interval = tokio::time::interval(heartbeat_interval);
            // First `tick()` returns immediately — heartbeat right away so
            // freshly-started work extends its ack window without waiting.
            loop {
                interval.tick().await;
                let now = Timestamp::now();
                if let Some(lease) = &lease_for_heartbeat {
                    if let Err(err) = leases.renew(lease, lease_ttl, &now).await {
                        tracing::warn!(%err, "lease renew failed during heartbeat");
                    }
                }
                if let Err(err) = broker
                    .ack(&consumer_name, sequence, AckInstruction::InProgress)
                    .await
                {
                    tracing::warn!(%err, "in-progress ack failed during heartbeat");
                }
            }
        });

        let outcome: std::result::Result<serde_json::Value, AppError> =
            self.handler.execute(&task_msg).await;
        heartbeat.abort();
        let _ = heartbeat.await;

        let settlement = self.settle(task, &outcome, deliveries, sequence).await;
        self.release_lease(lease.as_ref()).await;
        MessageOutcome {
            settlement,
            handler_failed: outcome.is_err(),
        }
    }

    /// Broker instruction + row finalization for a handler outcome.
    async fn settle(
        &self,
        mut task: mas_domain::Task,
        outcome: &std::result::Result<serde_json::Value, AppError>,
        deliveries: u32,
        sequence: u64,
    ) -> Settlement {
        match outcome {
            Ok(_) => {
                if let Err(err) = task.complete() {
                    tracing::error!(%err, "task complete transition failed");
                }
                self.save_row_best_effort(&task).await;
                let _ = self
                    .broker
                    .ack(&self.config.consumer_name, sequence, AckInstruction::Ack)
                    .await;
                Settlement::Ack
            },
            Err(err) => {
                let class = classify(err);
                match class {
                    Failure::Cancelled => {
                        if let Err(e) = task.cancel() {
                            tracing::warn!(%e, "cancel transition failed");
                        }
                        self.save_row_best_effort(&task).await;
                        let _ = self
                            .broker
                            .ack(&self.config.consumer_name, sequence, AckInstruction::Ack)
                            .await;
                        Settlement::Ack
                    },
                    Failure::Poison | Failure::Underspecified => {
                        let settlement_kind = if matches!(class, Failure::Poison) {
                            TermKind::Poison
                        } else {
                            TermKind::Undecodable
                        };
                        self.fail_row(task.id, &err.public_message()).await;
                        let _ = self
                            .broker
                            .ack(&self.config.consumer_name, sequence, AckInstruction::Term)
                            .await;
                        Settlement::Term {
                            kind: settlement_kind,
                        }
                    },
                    Failure::Retryable => {
                        if !self.config.retry.allows_redelivery(deliveries) {
                            self.fail_row(
                                task.id,
                                &format!("attempts exhausted after {deliveries} deliveries"),
                            )
                            .await;
                            let _ = self
                                .broker
                                .ack(&self.config.consumer_name, sequence, AckInstruction::Term)
                                .await;
                            return Settlement::Term {
                                kind: TermKind::Exhausted,
                            };
                        }
                        // Row: Failed → Queued (attempts remain — domain enforces).
                        if let Err(e) = task.fail(err.public_message()) {
                            tracing::warn!(%e, "fail transition failed");
                        }
                        if let Err(e) = task.retry() {
                            tracing::warn!(%e, "retry transition failed");
                        }
                        self.save_row_best_effort(&task).await;
                        let delay = self.config.retry.delay_for(deliveries);
                        let _ = self
                            .broker
                            .ack(
                                &self.config.consumer_name,
                                sequence,
                                AckInstruction::Nak { delay: Some(delay) },
                            )
                            .await;
                        Settlement::Nak {
                            delay,
                            kind: NakKind::Transient,
                        }
                    },
                }
            },
        }
    }

    async fn fail_row(&self, task_id: mas_common::ids::TaskId, message: &str) {
        if let Ok(Some(mut task)) = self.lifecycle.load(task_id).await {
            if let Err(err) = task.fail(truncate(message, 1_024)) {
                tracing::warn!(%err, "fail transition failed");
            }
            self.save_row_best_effort(&task).await;
        }
    }

    async fn save_row_best_effort(&self, task: &mas_domain::Task) {
        if let Err(err) = self.lifecycle.save(task).await {
            tracing::error!(%err, "task row save failed (heals on redelivery)");
        }
    }

    async fn release_lease(&self, lease: Option<&Lease>) {
        if let Some(lease) = lease {
            if let Err(err) = self.leases.release(lease, &Timestamp::now()).await {
                tracing::warn!(%err, "lease release failed (expires naturally)");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Failure classification (mirrors mas-execution's categorize(): poison =
// never-heals; retryable = transient upstream/platform; cancelled = caller
// cut it off).
// ---------------------------------------------------------------------------
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Failure {
    Retryable,
    Poison,
    Cancelled,
    Underspecified,
}

fn classify(err: &AppError) -> Failure {
    match err {
        AppError::Cancelled(_) => Failure::Cancelled,
        AppError::Timeout(_) | AppError::RateLimited(_) => Failure::Retryable,
        AppError::ExternalService { .. } | AppError::Messaging(_) => Failure::Retryable,
        AppError::Database(_) | AppError::Internal(_) | AppError::NotFound { .. } => {
            Failure::Retryable
        },
        AppError::Validation { .. } | AppError::Conflict(_) => Failure::Poison,
        AppError::Forbidden(_) | AppError::Unauthorized(_) => Failure::Poison,
        AppError::Serialization(_) => Failure::Underspecified,
    }
}

fn truncate(text: &str, max: usize) -> String {
    text.chars().take(max).collect()
}

/// Internal per-message report.
#[derive(Debug)]
struct MessageOutcome {
    settlement: Settlement,
    handler_failed: bool,
}

impl MessageOutcome {}

fn reap_results(
    in_flight: &mut tokio::task::JoinSet<(u64, MessageOutcome)>,
    counters: &mut ConsumerOutcome,
) {
    while let Some(joined) = in_flight.try_join_next() {
        match joined {
            Ok((sequence, outcome)) => {
                trace_result(sequence, &outcome);
                credits_to_counters(&outcome, counters);
            },
            Err(err) => {
                tracing::error!(%err, "mediation panicked");
                counters.failed += 1;
            },
        }
    }
}

fn credits_to_counters(outcome: &MessageOutcome, counters: &mut ConsumerOutcome) {
    match outcome.settlement {
        Settlement::Ack => counters.acked += 1,
        Settlement::Nak {
            kind: NakKind::LeaseConflict,
            ..
        } => {
            counters.naks += 1;
            counters.lease_conflicts += 1;
        },
        Settlement::Nak { .. } => counters.naks += 1,
        Settlement::Term {
            kind: TermKind::Undecodable,
        } => {
            counters.terms += 1;
            counters.skipped += 1;
        },
        Settlement::Term { .. } => counters.terms += 1,
    }
    if outcome.handler_failed {
        counters.failed += 1;
    } else {
        counters.completed += 1;
    }
}

fn trace_result(sequence: u64, outcome: &MessageOutcome) {
    match outcome.settlement {
        Settlement::Ack => tracing::debug!(sequence, "message acked"),
        Settlement::Nak { delay, kind } => {
            tracing::debug!(sequence, ?delay, ?kind, "message requeued");
        },
        Settlement::Term { kind } => tracing::debug!(sequence, ?kind, "message terminated"),
    }
}

// ---------------------------------------------------------------------------
// Tests (InMemoryBroker + InMemoryLeaseStore + InMemoryTaskStore)
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::{canned::CannedHandler, OperationDispatcher};
    use crate::lifecycle::InMemoryTaskStore;
    use mas_common::enums::TaskPriority;
    use mas_common::error::AppError;
    use mas_common::ids::{OrganizationId, ProjectId, TenantId, WorkflowId};
    use mas_domain::Task;
    use mas_messaging::broker::{InMemoryBroker, PublishRequest};
    use mas_messaging::headers::HeaderSet;
    use mas_messaging::subjects::Subject;
    use mas_scheduling::lease::InMemoryLeaseStore;
    use std::time::Duration;

    struct Rig {
        broker: Arc<InMemoryBroker>,
        leases: Arc<InMemoryLeaseStore>,
        lifecycle: Arc<InMemoryTaskStore>,
        handler: Arc<CannedHandler>,
    }

    fn rig() -> Rig {
        let broker = InMemoryBroker::new();
        broker.set_available(true);
        Rig {
            broker: Arc::new(broker),
            leases: Arc::new(InMemoryLeaseStore::new()),
            lifecycle: Arc::new(InMemoryTaskStore::new()),
            handler: Arc::new(CannedHandler::default()),
        }
    }

    fn consumer(rig: &Rig, dispatcher: OperationDispatcher) -> TaskConsumer {
        TaskConsumer::new(
            rig.broker.clone(),
            rig.leases.clone(),
            rig.lifecycle.clone(),
            Arc::new(dispatcher),
            ConsumerConfig {
                consumer_name: "c-test".to_owned(),
                filter: mas_messaging::subjects::SubjectFilter::parse("mas.tasks.>").expect("f"),
                batch_size: 4,
                ack_wait: Duration::from_millis(200),
                poll_interval: Duration::from_millis(5),
                lease_ttl: Duration::from_millis(150),
                max_in_flight: 4,
                drain_timeout: Duration::from_secs(3),
                worker_name: "w-test".to_owned(),
                retry: RetryPolicy {
                    base: Duration::from_millis(5),
                    cap: Duration::from_millis(40),
                    max_deliver: 10,
                },
            },
        )
    }

    fn queued_task() -> Task {
        let mut task = Task::new(
            TenantId::new(),
            OrganizationId::new(),
            ProjectId::new(),
            None,
            Some(WorkflowId::new()),
            "execution.run",
            serde_json::json!({"job": "x"}),
            TaskPriority::High,
            "idem-test-1",
            "svc-test",
        )
        .expect("task");
        task.max_attempts = 5;
        task.queue().expect("queue");
        task
    }

    fn queue_message(task: &Task) -> TaskQueueMessage {
        TaskQueueMessage::staged(
            task.id,
            task.tenant_id,
            task.organization_id,
            task.project_id,
            task.operation.clone(),
            task.input.clone(),
            task.execution_id,
            task.agent_id,
            task.workflow_id,
            task.idempotency_key.clone(),
            task.priority,
            1,
            task.max_attempts,
            task.deadline,
            "corr-worker-test",
        )
        .expect("message")
    }

    async fn publish(rig: &Rig, payload: &TaskQueueMessage) -> u64 {
        let codec = JsonEventCodec::new(CodecConfig::default());
        rig.broker
            .publish(PublishRequest {
                subject: Subject::parse("mas.tasks.execution.run").expect("subject"),
                headers: HeaderSet::default(),
                payload: codec.encode_value(payload).expect("frame"),
                msg_id: Some(payload.idempotency_key.clone()),
            })
            .await
            .expect("publish")
    }

    fn dispatcher(handler: Arc<CannedHandler>) -> OperationDispatcher {
        OperationDispatcher::new().with_handler("execution.run", handler)
    }

    async fn run_until<P>(
        rig: &Rig,
        dispatcher: OperationDispatcher,
        sentinel: P,
    ) -> ConsumerOutcome
    where
        P: Fn(&CannedHandler) -> bool,
    {
        let consumer = consumer(rig, dispatcher);
        let (handle, watch) = crate::grace::channel();
        let handler_hold = rig.handler.clone();
        let run = tokio::spawn(async move { consumer.run(watch).await });
        // Drive until the sentinel confirms the interesting behavior, then stop.
        let timeout = tokio::time::sleep(Duration::from_secs(5));
        tokio::pin!(timeout);
        loop {
            if sentinel(&handler_hold) {
                break;
            }
            if tokio::time::timeout(Duration::from_millis(1), &mut timeout)
                .await
                .is_ok()
            {
                panic!("sentinel never fired");
            }
            tokio::task::yield_now().await;
        }
        handle.trigger();
        run.await.expect("join").expect("consumer run")
    }

    #[tokio::test]
    async fn happy_path_acks_and_completes_the_row() {
        let rig = rig();
        let task = queued_task();
        rig.lifecycle.seed(task.clone());
        publish(&rig, &queue_message(&task)).await;

        let outcome = run_until(&rig, dispatcher(rig.handler.clone()), |h| {
            h.invocations() >= 1
        })
        .await;
        assert_eq!(outcome.acked, 1);
        assert_eq!(outcome.completed, 1);
        assert_eq!(outcome.failed, 0);

        let row = TaskLifecyclePort::load(rig.lifecycle.as_ref(), task.id)
            .await
            .expect("load")
            .expect("row");
        assert!(matches!(
            row.status,
            mas_common::enums::TaskStatus::Completed
        ));
        assert!(row.finished_at.is_some());

        let stats = rig.broker.consumer_stats("c-test").await.expect("stats");
        assert_eq!(stats.acked, 1);
        assert_eq!(stats.terminated, 0);
    }

    #[tokio::test]
    async fn transient_failure_retries_with_backoff_then_succeeds() {
        let rig = rig();
        rig.handler
            .push_failure(AppError::internal("upstream hiccup #1"));
        let task = queued_task();
        rig.lifecycle.seed(task.clone());
        publish(&rig, &queue_message(&task)).await;

        let outcome = run_until(&rig, dispatcher(rig.handler.clone()), |h| {
            h.invocations() >= 2
        })
        .await;
        let row = TaskLifecyclePort::load(rig.lifecycle.as_ref(), task.id)
            .await
            .expect("load")
            .expect("row");
        assert!(matches!(
            row.status,
            mas_common::enums::TaskStatus::Completed
        ));
        assert!(
            row.attempt_count >= 2,
            "both attempts counted, got {}",
            row.attempt_count
        );
        assert_eq!(outcome.acked, 1, "second delivery acked");
        assert!(outcome.naks >= 1, "first delivery requeued");
    }

    #[tokio::test]
    async fn poison_messages_dead_letter_without_retry() {
        let rig = rig();
        rig.handler
            .push_failure(AppError::validation("payload cannot ever parse"));
        let task = queued_task();
        rig.lifecycle.seed(task.clone());
        publish(&rig, &queue_message(&task)).await;

        let outcome = run_until(&rig, dispatcher(rig.handler.clone()), |h| {
            h.invocations() >= 1
        })
        .await;
        assert_eq!(outcome.terms, 1, "validation ⇒ Term");
        assert_eq!(outcome.naks, 0, "poison is never retried");
        assert_eq!(rig.handler.invocations(), 1, "exactly one invocation");

        let row = TaskLifecyclePort::load(rig.lifecycle.as_ref(), task.id)
            .await
            .expect("load")
            .expect("row");
        assert!(matches!(row.status, mas_common::enums::TaskStatus::Failed));
        assert!(row.last_error.is_some());
        let stats = rig.broker.consumer_stats("c-test").await.expect("stats");
        assert_eq!(stats.terminated, 1, "poison settled via Term");
        let dead = rig.broker.dead_letters().await.expect("dlq");
        assert!(
            dead.is_empty(),
            "DLQ is capped-deliveries only, not first-try poison"
        );
    }

    #[tokio::test]
    async fn unknown_operation_is_poison_at_the_dispatcher() {
        let rig = rig();
        let mut task = queued_task();
        task.operation = "no.such.operation".to_owned();
        rig.lifecycle.seed(task.clone());
        publish(&rig, &queue_message(&task)).await;

        let consumer = consumer(&rig, dispatcher(rig.handler.clone()));
        let (handle, watch) = crate::grace::channel();
        let run = tokio::spawn(async move { consumer.run(watch).await });
        tokio::time::sleep(Duration::from_millis(120)).await;
        handle.trigger();
        let outcome = run.await.expect("join").expect("run");
        assert_eq!(outcome.terms, 1, "unknown operation ⇒ Term");
        assert_eq!(
            rig.handler.invocations(),
            0,
            "registered handler never sees it"
        );
        let row = TaskLifecyclePort::load(rig.lifecycle.as_ref(), task.id)
            .await
            .expect("load")
            .expect("row");
        assert!(matches!(row.status, mas_common::enums::TaskStatus::Failed));
    }

    #[tokio::test]
    async fn undecodable_frames_terminate_immediately() {
        let rig = rig();
        rig.broker
            .publish(PublishRequest {
                subject: Subject::parse("mas.tasks.spoofed").expect("subject"),
                headers: HeaderSet::default(),
                payload: b"not-a-task-frame".to_vec(),
                msg_id: None,
            })
            .await
            .expect("publish");
        let consumer = consumer(&rig, dispatcher(rig.handler.clone()));
        let (handle, watch) = crate::grace::channel();
        let run = tokio::spawn(async move { consumer.run(watch).await });
        tokio::time::sleep(Duration::from_millis(80)).await;
        handle.trigger();
        let outcome = run.await.expect("join").expect("run");
        assert_eq!(outcome.terms, 1);
        assert_eq!(outcome.skipped, 1);
    }

    #[tokio::test]
    async fn lease_conflicts_defer_without_touching_the_row_or_handler() {
        let rig = rig();
        let task = queued_task();
        rig.lifecycle.seed(task.clone());
        publish(&rig, &queue_message(&task)).await;

        // A competitor holds the lease for the task's resource key.
        let mut obstacle = rig
            .leases
            .try_acquire(
                &format!("task:{}", task.id),
                "competitor",
                Duration::from_secs(30),
                &Timestamp::now(),
            )
            .await
            .expect("competitor lease");
        obstacle.holder = "competitor".to_owned();

        let consumer = consumer(&rig, dispatcher(rig.handler.clone()));
        let (handle, watch) = crate::grace::channel();
        let run = tokio::spawn(async move { consumer.run(watch).await });
        tokio::time::sleep(Duration::from_millis(120)).await;
        handle.trigger();
        let outcome = run.await.expect("join").expect("run");

        assert!(outcome.lease_conflicts >= 1, "conflicts defer: {outcome:?}");
        assert_eq!(
            rig.handler.invocations(),
            0,
            "work never leaked past the fence"
        );
        let row = TaskLifecyclePort::load(rig.lifecycle.as_ref(), task.id)
            .await
            .expect("load")
            .expect("row");
        assert!(matches!(row.status, mas_common::enums::TaskStatus::Queued));
    }

    #[tokio::test]
    async fn graceful_shutdown_drains_in_flight_work() {
        let rig = rig();
        let slow = Arc::new(SlowHandler {
            delay: Duration::from_millis(120),
            completed: std::sync::atomic::AtomicU64::new(0),
        });
        let consumer = consumer(
            &rig,
            OperationDispatcher::new().with_handler("execution.run", slow.clone()),
        );
        let task = queued_task();
        rig.lifecycle.seed(task.clone());
        publish(&rig, &queue_message(&task)).await;

        let (handle, watch) = crate::grace::channel();
        let run = tokio::spawn(async move { consumer.run(watch).await });
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        // Shutdown mid-task: the drain must wait for the slow handler.
        handle.trigger();
        let outcome = run.await.expect("join").expect("run");
        assert_eq!(outcome.abandoned, 0, "in-flight drained within the timeout");
        assert_eq!(outcome.acked, 1);
        assert_eq!(slow.completed.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[derive(Debug)]
    struct SlowHandler {
        delay: Duration,
        completed: std::sync::atomic::AtomicU64,
    }

    #[async_trait::async_trait]
    impl TaskHandlerPort for SlowHandler {
        async fn execute(&self, _msg: &TaskQueueMessage) -> Result<serde_json::Value> {
            tokio::time::sleep(self.delay).await;
            self.completed
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(serde_json::json!({"slow": true}))
        }
    }
}
