//! PURPOSE: The bounded-concurrency task consumer: the plane between "a task
//! message is on the subject" and "the work ran, the row reflects it, the
//! broker settled the delivery".
//!
//! Owning layer: `mas-worker`.
//!
//! Production responsibility: every delivery maps to exactly one broker
//! instruction and one durable record. The broker message is only a *hint*
//! that work exists — a durable claim is the permission to run, so a duplicate
//! delivery, an orphaned row or a lost lease all resolve the same way.
//!
//! It must NOT:
//!   • run work without a durable claim;
//!   • derive attempt counts from broker delivery metadata;
//!   • keep a second lifecycle in the worker — settlement goes through
//!     `AttemptCoordinator` and the durable port.

//! The bounded-concurrency task consumer. See `lib.rs` for the settlement
//! protocol this implements; the code below is written so every outcome
//! maps to exactly one broker instruction *and* one lifecycle record.

use std::sync::Arc;
use std::time::Duration;

use mas_application::durable::ExecutionUsage;
use mas_common::enums::TaskStatus;
use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_contracts::task::TaskQueueMessage;
use mas_messaging::broker::{
    AckInstruction, BrokerMessage, BrokerPort, ConsumerConfig as BrokerConsumerConfig,
};
use mas_messaging::codec::{CodecConfig, JsonEventCodec};

use crate::attempts::{AttemptCoordinator, ClaimOneOutcome};
use crate::backoff::RetryPolicy;
use crate::grace::ShutdownWatch;
use crate::handler::TaskHandlerPort;
use crate::run_slot::RunSlot;

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
    /// Settlements rejected because this worker no longer owned the task —
    /// the durable ownership fence doing its job (spec: stale-worker
    /// protection). A rising value means leases are too short or work is
    /// outliving them, and is an operator signal, not a silent retry.
    pub stale_ownership: u64,
    /// In-flight messages the consumer could not drain before
    /// `drain_timeout` (broker redelivers after `ack_wait`).
    pub abandoned: u64,
}

/// The consumer: broker + leases + lifecycle + handler.
#[derive(Debug)]
pub struct TaskConsumer {
    broker: Arc<dyn BrokerPort>,
    attempts: Arc<AttemptCoordinator>,
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
    /// The work may be retried; durable state still allows another attempt.
    Transient,
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
    ///
    /// The coordinator is mandatory: ownership, attempt counts and capacity
    /// all live in durable state, so a consumer without one has no authority
    /// to run anything.
    pub fn new(
        broker: Arc<dyn BrokerPort>,
        attempts: Arc<AttemptCoordinator>,
        handler: Arc<dyn TaskHandlerPort>,
        config: ConsumerConfig,
    ) -> Self {
        Self {
            broker,
            attempts,
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

            // Two independent ceilings apply, and both must:
            //
            //   * `max_in_flight - in_flight.len()` — the futures this process
            //     is currently holding;
            //   * `remaining_capacity()` — durable, unfinished attempts owned
            //     by this worker. This is the one that survives a restart: a
            //     fresh process that inherits five open attempts must not
            //     believe it is idle.
            let local_room = self.config.max_in_flight.saturating_sub(in_flight.len());
            let durable_room = match self.attempts.remaining_capacity(&Timestamp::now()).await {
                Ok(room) => room,
                Err(err) => {
                    tracing::error!(%err, "capacity read failed; not fetching this cycle");
                    tokio::select! {
                        biased;
                        () = shutdown.changed() => break,
                        () = tokio::time::sleep(self.config.poll_interval) => {},
                    }
                    continue;
                },
            };
            // Never fetch more than the worker can actually run: an over-fetch
            // would leave messages unacked until their ack window expired.
            let batch = self.config.batch_size.min(local_room).min(durable_room);

            if batch == 0 {
                tracing::debug!(
                    in_flight = in_flight.len(),
                    durable_room,
                    local_room,
                    "at capacity; not fetching"
                );
                reap_results(&mut in_flight, &mut counters);
                tokio::select! {
                    biased;
                    () = shutdown.changed() => break,
                    () = tokio::time::sleep(self.config.poll_interval) => {},
                }
                continue;
            }

            match self.broker.fetch(&self.config.consumer_name, batch).await {
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
                        in_flight.spawn(async move { (sequence, mediator.mediate(message).await) });
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
            attempts: Arc::clone(&self.attempts),
            handler: Arc::clone(&self.handler),
            codec: JsonEventCodec::new(CodecConfig::default()),
            config: self.config.clone(),
        }
    }
}

/// An owned snapshot of the consumer's ports, so per-message mediation
/// futures can be `join_set.spawn`ed with `'static` lifetime.
///
/// Deliberately holds **no** task store and **no** lease store. Durable
/// ownership and durable attempts are the only authority, and both live
/// behind [`AttemptCoordinator`]. A process-local lease would let a worker
/// keep believing it owns work it has already lost.
#[derive(Debug, Clone)]
struct Mediator {
    broker: Arc<dyn BrokerPort>,
    attempts: Arc<AttemptCoordinator>,
    handler: Arc<dyn TaskHandlerPort>,
    codec: JsonEventCodec,
    config: ConsumerConfig,
}

impl Mediator {
    /// One full mediation for one broker message.
    ///
    /// The ordering is the contract:
    ///
    /// ```text
    ///   decode → durable claim (attempt row + ownership fence) → run the
    ///   handler → durable settle (attempt + task + execution + usage +
    ///   outbox, committed together) → broker ack
    /// ```
    ///
    /// The broker message is only a *hint* that work exists. It is never the
    /// permission to run: only a durable claim is. Consequently the attempt
    /// count comes from `task_attempts`, never from broker redeliveries.
    async fn mediate(&self, message: BrokerMessage) -> MessageOutcome {
        let sequence = message.sequence;
        let deliveries = message.deliveries;

        // A redelivery is a signal about the pipeline, not about the task: it
        // means someone already failed to finish this message. Counted here,
        // before any durable work, so a broker that is looping is visible even
        // when every individual task eventually succeeds.
        if deliveries > 1 {
            self.attempts.metrics().broker_redelivery();
        }

        // ── 1. Decode. Undecodable frames never heal. ────────────────
        let decoded: std::result::Result<TaskQueueMessage, _> =
            self.codec.decode_value(&message.payload);
        let task_msg = match decoded {
            Ok(msg) => msg,
            Err(err) => {
                tracing::error!(sequence, %err, "undecodable task frame");
                self.ack(sequence, AckInstruction::Term).await;
                return MessageOutcome {
                    settlement: Settlement::Term {
                        kind: TermKind::Undecodable,
                    },
                    handler_failed: false,
                    stale_ownership: false,
                };
            },
        };
        let task_id = task_msg.task_id;
        let tenant_id = task_msg.tenant_id;

        // ── 2. Concurrency slot — taken BEFORE the durable claim. ─────
        //
        // Ordering is the whole point. If the slot were taken after the
        // claim, a refusal would leave a durable attempt behind for work
        // that never started, and attempts are finite: under sustained
        // pressure a tenant's tasks would be dead-lettered for the crime
        // of arriving while the tenant was busy. Taken first, a refusal
        // costs nothing — no attempt, no ownership, no state.
        let slot = match self
            .attempts
            .acquire_run_slot(
                tenant_id,
                task_msg.organization_id,
                task_msg.execution_id,
                task_id,
                &Timestamp::now(),
            )
            .await
        {
            Ok(slot) => slot,
            Err(err) => {
                // Either the tenant is at its concurrency ceiling, or the
                // gate could not answer. Both are deferrals: a run must not
                // start when the platform cannot say it may.
                tracing::info!(
                    %task_id, sequence, %err,
                    "no concurrency headroom; deferring delivery"
                );
                let settlement = self.nak_transient(sequence, deliveries).await;
                return MessageOutcome {
                    settlement,
                    handler_failed: false,
                    stale_ownership: false,
                };
            },
        };

        // A slot that belongs to another live attempt is not ours to use and
        // not ours to return. Deferring here — before the durable claim, so no
        // attempt row is created — is the whole reason the slot is taken
        // first: the alternative is to claim, discover the conflict, and leave
        // an attempt behind for work that never started.
        if slot == RunSlot::HeldElsewhere {
            tracing::info!(
                %task_id, sequence,
                "another attempt holds this run's concurrency slot; deferring"
            );
            let settlement = self.nak_transient(sequence, deliveries).await;
            return MessageOutcome {
                settlement,
                handler_failed: false,
                stale_ownership: false,
            };
        }

        // ── 3. Everything else, with the slot held. ──────────────────
        let outcome = self
            .mediate_with_slot(sequence, deliveries, &task_msg, slot)
            .await;

        // ── 4. Return the slot, whatever happened. ───────────────────
        //
        // This runs on every path — settled, failed, deferred, or a claim
        // that produced no work at all. A slot released only on success is
        // a slot that leaks on every failure, and the tenant's ceiling
        // would ratchet down until it could not run anything.
        //
        // A release that itself fails is logged, not fatal: the lease is
        // what ultimately reclaims a leaked slot, so the failure mode of a
        // failed release is a delay, not a permanent loss.
        if slot.must_be_released() {
            if let Err(err) = self
                .attempts
                .release_run_slot(
                    tenant_id,
                    task_msg.organization_id,
                    task_msg.execution_id,
                    task_id,
                    &Timestamp::now(),
                )
                .await
            {
                tracing::warn!(
                    %task_id, %err,
                    "concurrency slot release failed; the lease will reclaim it"
                );
            }
        }
        outcome
    }

    /// One mediation from the durable claim onwards, with the run's
    /// concurrency slot already held.
    ///
    /// Split out of [`Mediator::mediate`] for one reason: the slot is
    /// released by the caller on every exit path from this method, so no
    /// `return` inside it can forget to.
    #[allow(clippy::too_many_lines)]
    async fn mediate_with_slot(
        &self,
        sequence: u64,
        deliveries: u32,
        task_msg: &TaskQueueMessage,
        slot: RunSlot,
    ) -> MessageOutcome {
        let task_id = task_msg.task_id;
        let tenant_id = task_msg.tenant_id;
        tracing::debug!(%task_id, ?slot, "mediating with the run's concurrency slot held");
        // ── 3a. Durable claim: attempt row + ownership fence, one unit. ──
        let claim = match self
            .attempts
            .claim_one(task_id, tenant_id, &Timestamp::now())
            .await
        {
            Ok(ClaimOneOutcome::Claimed(claim)) => *claim,
            Ok(ClaimOneOutcome::NotClaimable { defer: false, .. }) => {
                // Owned by another worker, or already settled. Either way this
                // delivery has nothing to do; acking drops the duplicate.
                tracing::info!(%task_id, sequence, "task not claimable; dropping delivery");
                self.ack(sequence, AckInstruction::Ack).await;
                return MessageOutcome {
                    settlement: Settlement::Ack,
                    handler_failed: false,
                    stale_ownership: false,
                };
            },
            Ok(ClaimOneOutcome::NotClaimable {
                defer: true,
                available_at,
            }) => {
                // The task is real and unfinished but not available yet — a
                // retry waiting out its backoff. Dropping the wake-up would
                // lose the retry, and deferring for less than the backoff
                // would burn the broker's redelivery budget without ever
                // reaching a claimable state. So the wait is the larger of
                // the two.
                let settlement = self.nak_after(sequence, deliveries, available_at).await;
                return MessageOutcome {
                    settlement,
                    handler_failed: false,
                    stale_ownership: false,
                };
            },
            Ok(ClaimOneOutcome::AtCapacity) => {
                tracing::debug!(
                    %task_id, sequence,
                    "worker at durable capacity; deferring delivery"
                );
                let settlement = self.nak_transient(sequence, deliveries).await;
                return MessageOutcome {
                    settlement,
                    handler_failed: false,
                    stale_ownership: false,
                };
            },
            Err(err) => {
                tracing::error!(%task_id, %err, "durable claim failed; deferring delivery");
                let settlement = self.nak_transient(sequence, deliveries).await;
                return MessageOutcome {
                    settlement,
                    handler_failed: false,
                    stale_ownership: false,
                };
            },
        };
        let attempt_number = claim.attempt.attempt_number;

        // ── 3b. Deadline that lapsed before work could start. ─────────
        if let Some(deadline) = task_msg.deadline {
            if !deadline.is_after(&Timestamp::now()) {
                let failure = crate::classify::ClassifiedFailure {
                    class: crate::classify::FailureClass::Terminal,
                    code: "deadline_exceeded".to_owned(),
                    message: "the task deadline passed before work started".to_owned(),
                };
                match self
                    .attempts
                    .fail(&claim, &failure, true, &Timestamp::now())
                    .await
                {
                    Ok(_) => {
                        self.ack(sequence, AckInstruction::Term).await;
                        return MessageOutcome {
                            settlement: Settlement::Term {
                                kind: TermKind::Expired,
                            },
                            handler_failed: true,
                            stale_ownership: false,
                        };
                    },
                    Err(err) => {
                        tracing::error!(%task_id, %err, "deadline settlement failed; deferring");
                        let settlement = self.nak_transient(sequence, deliveries).await;
                        return MessageOutcome {
                            settlement,
                            handler_failed: true,
                            stale_ownership: false,
                        };
                    },
                }
            }
        }

        // ── 3c. Run the handler, heartbeating at half ack_wait. ───────
        let outcome = self.execute(&claim, task_msg, sequence).await;

        // ── 3d. Settle durably, THEN ack. Terminal state is committed
        //       before the broker is told the message is handled, so a
        //       crash between the two can only ever duplicate an ack,
        //       never lose a result.
        let (settlement, stale_ownership) =
            self.settle(&claim, &outcome, sequence, deliveries).await;

        tracing::info!(
            %task_id, attempt = attempt_number, sequence,
            outcome = if outcome.is_ok() { "succeeded" } else { "failed" },
            ?settlement, "attempt settled"
        );

        MessageOutcome {
            settlement,
            handler_failed: outcome.is_err(),
            stale_ownership,
        }
    }

    /// Heartbeat-wrapped handler invocation.
    ///
    /// The heartbeat renews the **durable** lease as well as the broker ack
    /// window. If the durable renewal reports ownership lost, the settlement
    /// below will be rejected by the ownership fence — which is the correct
    /// outcome, and is deliberately not hidden here.
    async fn execute(
        &self,
        claim: &mas_application::durable::TaskClaim,
        task_msg: &TaskQueueMessage,
        sequence: u64,
    ) -> std::result::Result<serde_json::Value, AppError> {
        let heartbeat_interval = self.config.ack_wait / 2;
        let broker = Arc::clone(&self.broker);
        let attempts = Arc::clone(&self.attempts);
        let consumer_name = self.config.consumer_name.clone();
        let task_id = claim.task.id;
        let slot_tenant = claim.task.tenant_id;
        let slot_organization = claim.task.organization_id;
        let slot_execution = claim.task.execution_id;
        let heartbeat = tokio::spawn(async move {
            let mut interval = tokio::time::interval(heartbeat_interval);
            // First `tick()` returns immediately — heartbeat right away so
            // freshly-started work extends its ack window without waiting.
            loop {
                interval.tick().await;
                match attempts.renew(task_id, &Timestamp::now()).await {
                    Ok(true) => {},
                    Ok(false) => tracing::warn!(
                        %task_id, "durable lease lost during execution"
                    ),
                    Err(err) => tracing::warn!(%task_id, %err, "durable lease renew failed"),
                }
                // The slot's lease rides the same heartbeat as the task's.
                // A run that is still going must not have its slot swept, and
                // a run that died stops heartbeating — so the slot lapses with
                // it. One clock, one decision.
                if let Err(err) = attempts
                    .renew_run_slot(
                        slot_tenant,
                        slot_organization,
                        slot_execution,
                        task_id,
                        &Timestamp::now(),
                    )
                    .await
                {
                    tracing::warn!(%task_id, %err, "concurrency slot lease renew failed");
                }
                if let Err(err) = broker
                    .ack(&consumer_name, sequence, AckInstruction::InProgress)
                    .await
                {
                    tracing::warn!(%err, "in-progress ack failed during heartbeat");
                }
            }
        });

        let outcome = self.handler.execute(task_msg).await;
        heartbeat.abort();
        let _ = heartbeat.await;
        outcome
    }

    /// Durable settlement first, broker instruction second.
    ///
    /// The retry decision comes from the durable receipt (`requeued`), not
    /// from the broker's delivery counter: the attempt budget is a domain
    /// fact held in `task_attempts`.
    async fn settle(
        &self,
        claim: &mas_application::durable::TaskClaim,
        outcome: &std::result::Result<serde_json::Value, AppError>,
        sequence: u64,
        deliveries: u32,
    ) -> (Settlement, bool) {
        let now = Timestamp::now();
        // A task that carries an execution id is that execution's unit of
        // work, so the execution is finalized in the same committed unit.
        let finalize_execution = claim.task.execution_id.is_some();

        let settled = match outcome {
            Ok(value) => {
                self.attempts
                    .succeed(claim, usage_from(value), finalize_execution, &now)
                    .await
            },
            Err(err) => {
                let classified = crate::classify::classify(err);
                tracing::warn!(
                    task_id = %claim.task.id,
                    attempt = claim.attempt.attempt_number,
                    code = %classified.code,
                    class = classified.class.as_str(),
                    "attempt failed"
                );
                self.attempts
                    .fail(claim, &classified, finalize_execution, &now)
                    .await
            },
        };

        let receipt = match settled {
            Ok(receipt) => receipt,
            Err(err) => {
                if matches!(err, AppError::Conflict(_)) {
                    // Stale write: another worker owns this task, or the
                    // attempt was already settled. Retrying the settlement
                    // cannot help, and must not overwrite newer state, so the
                    // delivery is dropped rather than requeued.
                    tracing::warn!(
                        task_id = %claim.task.id, sequence, %err,
                        "stale ownership rejected the settlement; dropping delivery"
                    );
                    self.ack(sequence, AckInstruction::Ack).await;
                    return (Settlement::Ack, true);
                }
                // The durable write did not commit, so the task is still
                // owned and still running here. Redelivery re-attempts the
                // settlement rather than reporting a false success.
                tracing::error!(
                    task_id = %claim.task.id, sequence, %err,
                    "durable settlement failed; deferring"
                );
                return (self.nak_transient(sequence, deliveries).await, false);
            },
        };

        if receipt.dead_lettered {
            // Distinguish "spent the whole attempt budget" from "can never
            // succeed": they need different operator responses even though
            // both end the message.
            let kind = if receipt.task.attempt_count >= receipt.task.max_attempts {
                TermKind::Exhausted
            } else {
                TermKind::Poison
            };
            self.ack(sequence, AckInstruction::Term).await;
            return (Settlement::Term { kind }, false);
        }
        if receipt.task.status == TaskStatus::Cancelled {
            // A cancellation is a deliberate stop: it is not a failure and it
            // is never requeued.
            self.ack(sequence, AckInstruction::Ack).await;
            return (Settlement::Ack, false);
        }
        if receipt.requeued {
            // Durable state says another attempt is allowed. The pacing is the
            // broker's business; the *decision* is the domain's.
            // The domain decided another attempt is allowed and stamped when
            // that attempt may start; the broker must not be asked to
            // redeliver sooner than durable state permits.
            let delay = self.retry_delay(deliveries, Some(receipt.task.available_at()));
            self.ack(sequence, AckInstruction::Nak { delay: Some(delay) })
                .await;
            return (
                Settlement::Nak {
                    delay,
                    kind: NakKind::Transient,
                },
                false,
            );
        }
        if matches!(
            receipt.task.status,
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
        ) {
            self.ack(sequence, AckInstruction::Ack).await;
            return (Settlement::Ack, false);
        }
        (self.nak_transient(sequence, deliveries).await, false)
    }

    /// Best-effort broker acknowledgement. A failed ack is logged, never
    /// treated as a task failure: the broker redelivers on its own schedule.
    async fn ack(&self, sequence: u64, instruction: AckInstruction) {
        if let Err(err) = self
            .broker
            .ack(&self.config.consumer_name, sequence, instruction)
            .await
        {
            tracing::error!(sequence, %err, "broker ack failed");
        }
    }

    async fn nak_transient(&self, sequence: u64, deliveries: u32) -> Settlement {
        let settlement = self.nak_after(sequence, deliveries, None).await;
        settlement
    }

    /// Defers a delivery until at least `available_at`.
    ///
    /// The broker's backoff and the durable backoff are different clocks. When
    /// durable state says the work cannot start for another 250ms, redelivering
    /// after 5ms achieves nothing except spending a redelivery — so the wait
    /// is the larger of the two.
    async fn nak_after(
        &self,
        sequence: u64,
        deliveries: u32,
        available_at: Option<Timestamp>,
    ) -> Settlement {
        let delay = self.retry_delay(deliveries, available_at);
        self.ack(sequence, AckInstruction::Nak { delay: Some(delay) })
            .await;
        Settlement::Nak {
            delay,
            kind: NakKind::Transient,
        }
    }

    /// The delay to request, never shorter than durable state requires.
    fn retry_delay(&self, deliveries: u32, available_at: Option<Timestamp>) -> Duration {
        let backoff = self.config.retry.delay_for(deliveries);
        let Some(available_at) = available_at else {
            return backoff;
        };
        let now = Timestamp::now();
        if !available_at.is_after(&now) {
            return backoff;
        }
        let wait = available_at.duration_since(&now).unwrap_or(Duration::ZERO);
        backoff.max(wait)
    }
}

/// Reads the meter from a handler result.
///
/// Only a successful run meters anything, and a run that reports no units
/// produces no usage rows: usage is a measurement, never a synthetic counter
/// incremented because "an execution happened".
fn usage_from(value: &serde_json::Value) -> ExecutionUsage {
    let Some(usage) = value.get("usage") else {
        return ExecutionUsage::default();
    };
    let str_field = |key: &str| {
        usage
            .get(key)
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
    };
    let num_field = |key: &str| {
        usage
            .get(key)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    };
    ExecutionUsage {
        provider: str_field("provider"),
        model: str_field("model"),
        tokens_input: num_field("tokens_input"),
        tokens_output: num_field("tokens_output"),
        tool_calls: num_field("tool_calls"),
        steps: num_field("steps"),
        cost_micros: usage.get("cost_micros").and_then(serde_json::Value::as_u64),
    }
}

/// Internal per-message report.
#[derive(Debug)]
struct MessageOutcome {
    settlement: Settlement,
    handler_failed: bool,
    /// The durable ownership fence rejected this worker's settlement.
    stale_ownership: bool,
}

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
        Settlement::Nak { .. } => counters.naks += 1,
        Settlement::Term {
            kind: TermKind::Undecodable,
        } => {
            counters.terms += 1;
            counters.skipped += 1;
        },
        Settlement::Term { .. } => counters.terms += 1,
    }
    if outcome.stale_ownership {
        counters.stale_ownership += 1;
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
    use crate::run_slot::RunSlotPort;
    use mas_application::durable::{DurableExecutionPort, SubmissionIntent};
    use mas_application::durable_memory::InMemoryDurableExecution;
    use mas_application::fingerprint::{FingerprintInput, RequestFingerprint};
    use mas_common::enums::TaskPriority;
    use mas_common::error::AppError;
    use mas_common::ids::{OrganizationId, ProjectId, TenantId, WorkflowId};
    use mas_domain::TransitionActor;
    use mas_messaging::broker::{InMemoryBroker, PublishRequest};
    use mas_messaging::headers::HeaderSet;
    use mas_messaging::subjects::Subject;
    use std::time::Duration;

    /// One test rig: a broker, one durable store, one handler.
    ///
    /// There is deliberately **no** task store and **no** lease store here.
    /// The durable port is the only authority the consumer is allowed to use,
    /// so a test that would have passed against a process-local lease simply
    /// cannot be written any more.
    struct Rig {
        broker: Arc<InMemoryBroker>,
        durable: Arc<InMemoryDurableExecution>,
        handler: Arc<CannedHandler>,
    }

    fn rig() -> Rig {
        let broker = InMemoryBroker::new();
        broker.set_available(true);
        Rig {
            broker: Arc::new(broker),
            durable: Arc::new(InMemoryDurableExecution::new()),
            handler: Arc::new(CannedHandler::default()),
        }
    }

    fn consumer(rig: &Rig, dispatcher: OperationDispatcher, worker: &str) -> TaskConsumer {
        let attempts = Arc::new(
            AttemptCoordinator::new(rig.durable.clone(), worker, 4, Duration::from_secs(30))
                .expect("coordinator"),
        );
        TaskConsumer::new(
            rig.broker.clone(),
            attempts,
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
                worker_name: worker.to_owned(),
                retry: RetryPolicy {
                    base: Duration::from_millis(5),
                    cap: Duration::from_millis(40),
                    max_deliver: 10,
                },
            },
        )
    }

    /// Submits one execution through the canonical port and returns its task.
    ///
    /// Work is seeded the way production seeds it — by submitting — so the
    /// tests exercise the real claim path rather than a hand-built row.
    async fn seed(rig: &Rig, key: &str) -> mas_domain::Task {
        let tenant_id = TenantId::from_uuid(uuid::Uuid::from_u128(0xF00D));
        let organization_id = OrganizationId::from_uuid(uuid::Uuid::from_u128(1));
        let project_id = ProjectId::from_uuid(uuid::Uuid::from_u128(2));
        let workflow_id = WorkflowId::from_uuid(uuid::Uuid::from_u128(3));
        let input = serde_json::json!({"job": key});
        let fingerprint = RequestFingerprint::submission(&FingerprintInput {
            tenant_id,
            organization_id,
            project_id,
            workflow_id: Some(workflow_id),
            agent_id: None,
            operation: "execution.run",
            input: &input,
            priority: TaskPriority::High,
            deadline: None,
        })
        .expect("fingerprint");
        let receipt = rig
            .durable
            .submit(SubmissionIntent {
                tenant_id,
                organization_id,
                project_id,
                workflow_id: Some(workflow_id),
                agent_id: None,
                input,
                idempotency_key: Some(key.to_owned()),
                fingerprint,
                operation: "execution.run".to_owned(),
                priority: TaskPriority::High,
                max_attempts: 5,
                deadline: None,
                correlation_id: "corr-worker-test".to_owned(),
                created_by: "svc-test".to_owned(),
                actor: TransitionActor::system("test").expect("actor"),
                create_initial_attempt: false,
                now: Timestamp::now(),
            })
            .await
            .expect("submit");
        receipt.task
    }

    fn queue_message(task: &mas_domain::Task) -> TaskQueueMessage {
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

    /// A mediator built over a caller-supplied concurrency gate.
    ///
    /// Built directly rather than through [`TaskConsumer::run`] so the test can
    /// mediate exactly one message: the ownership rules below are about one
    /// decision on one delivery, and a fetch loop would retry the message
    /// several times before the assertion ran.
    fn mediator(rig: &Rig, worker: &str, slots: Arc<dyn RunSlotPort>) -> Mediator {
        let attempts = Arc::new(
            AttemptCoordinator::new(rig.durable.clone(), worker, 4, Duration::from_secs(30))
                .expect("coordinator")
                .with_run_slots(slots),
        );
        Mediator {
            broker: rig.broker.clone(),
            attempts,
            handler: rig.handler.clone(),
            codec: JsonEventCodec::new(CodecConfig::default()),
            config: ConsumerConfig {
                consumer_name: "c-test".to_owned(),
                filter: mas_messaging::subjects::SubjectFilter::parse("mas.tasks.>").expect("f"),
                batch_size: 4,
                ack_wait: Duration::from_millis(200),
                poll_interval: Duration::from_millis(5),
                lease_ttl: Duration::from_millis(150),
                max_in_flight: 4,
                drain_timeout: Duration::from_secs(3),
                worker_name: worker.to_owned(),
                retry: RetryPolicy {
                    base: Duration::from_millis(5),
                    cap: Duration::from_millis(40),
                    max_deliver: 10,
                },
            },
        }
    }

    /// One message for one task, encoded the way the broker would hand it over.
    fn delivery(task: &mas_domain::Task) -> BrokerMessage {
        let codec = JsonEventCodec::new(CodecConfig::default());
        BrokerMessage {
            sequence: 1,
            subject: mas_messaging::subjects::Subject::parse("mas.tasks.execution.run")
                .expect("subject"),
            headers: mas_messaging::headers::HeaderSet::default(),
            payload: codec.encode_value(&queue_message(task)).expect("frame"),
            published_at: Timestamp::now(),
            deliveries: 1,
        }
    }

    /// A gate with one answer and a full record of what it was asked.
    #[derive(Debug)]
    struct ScriptedRunSlots {
        decision: RunSlot,
        acquired: std::sync::atomic::AtomicUsize,
        released: std::sync::atomic::AtomicUsize,
    }

    impl ScriptedRunSlots {
        fn new(decision: RunSlot) -> Self {
            Self {
                decision,
                acquired: std::sync::atomic::AtomicUsize::new(0),
                released: std::sync::atomic::AtomicUsize::new(0),
            }
        }
        fn acquired(&self) -> usize {
            self.acquired.load(std::sync::atomic::Ordering::SeqCst)
        }
        fn released(&self) -> usize {
            self.released.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl RunSlotPort for ScriptedRunSlots {
        async fn acquire(
            &self,
            _tenant: TenantId,
            _organization: OrganizationId,
            _execution: Option<mas_common::ids::ExecutionId>,
            _task: mas_common::ids::TaskId,
            _at: &Timestamp,
        ) -> Result<RunSlot> {
            self.acquired
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(self.decision)
        }
        async fn renew(
            &self,
            _tenant: TenantId,
            _organization: OrganizationId,
            _execution: Option<mas_common::ids::ExecutionId>,
            _task: mas_common::ids::TaskId,
            _until: &Timestamp,
            _at: &Timestamp,
        ) -> Result<()> {
            Ok(())
        }
        async fn release(
            &self,
            _tenant: TenantId,
            _organization: OrganizationId,
            _execution: Option<mas_common::ids::ExecutionId>,
            _task: mas_common::ids::TaskId,
            _at: &Timestamp,
        ) -> Result<()> {
            self.released
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    /// The ownership rule, at the point where it is enforced.
    ///
    /// The control plane reports who holds a slot; the consumer is what refuses
    /// to use or return one that is not its own. A redelivery that arrives
    /// while the previous attempt still holds its lease is ordinary broker
    /// behaviour and must not run the work a second time — and above all must
    /// not release the hold, because that would hand the tenant's headroom to
    /// the next caller while the original attempt is still executing.
    #[tokio::test]
    async fn a_slot_held_elsewhere_defers_the_run_and_is_never_released() {
        let rig = rig();
        let slots = Arc::new(ScriptedRunSlots::new(RunSlot::HeldElsewhere));
        let task = seed(&rig, "idem-held-elsewhere").await;
        let mediator = mediator(&rig, "w-guest", slots.clone());

        let outcome = mediator.mediate(delivery(&task)).await;

        assert!(
            matches!(outcome.settlement, Settlement::Nak { .. }),
            "the delivery is deferred, not acked or terminated: another attempt \
             owns this run and will finish it"
        );
        assert_eq!(
            rig.handler.invocations(),
            0,
            "the handler did not run — the work is already running elsewhere"
        );
        assert_eq!(slots.acquired(), 1, "the gate was asked once");
        assert_eq!(
            slots.released(),
            0,
            "a slot this attempt does not own was not released. Releasing it \
             would free the tenant's headroom for the next caller while the \
             attempt that owns the slot is still executing"
        );
    }

    /// The mirror image: a slot this attempt did take is returned.
    ///
    /// Asserted on the failure path rather than the success path, because a
    /// release that only happens on success leaks a slot on every failure — and
    /// the tenant's ceiling would ratchet down until it could not run at all.
    #[tokio::test]
    async fn a_slot_this_attempt_took_is_released_even_when_the_run_fails() {
        let rig = rig();
        let slots = Arc::new(ScriptedRunSlots::new(RunSlot::Acquired));
        let failing = Arc::new(CannedHandler::scripted(vec![AppError::internal(
            "the provider is down",
        )]));
        rig.handler
            .push_failure(AppError::internal("the provider is down"));
        let task = seed(&rig, "idem-slot-failure").await;

        let mut mediator = mediator(&rig, "w-owner", slots.clone());
        mediator.handler = failing;
        let outcome = mediator.mediate(delivery(&task)).await;

        assert!(
            outcome.handler_failed,
            "the run failed, which is the path under test"
        );
        assert_eq!(
            slots.released(),
            1,
            "the slot was returned on the failure path too"
        );
    }

    /// An unmetered tenant runs, and releasing costs nothing.
    #[tokio::test]
    async fn an_unmetered_run_is_executed_and_never_deferred() {
        let rig = rig();
        let slots = Arc::new(ScriptedRunSlots::new(RunSlot::NotMetered));
        let task = seed(&rig, "idem-unmetered").await;
        let mediator = mediator(&rig, "w-unmetered", slots.clone());

        let outcome = mediator.mediate(delivery(&task)).await;

        assert!(
            matches!(outcome.settlement, Settlement::Ack),
            "no ceiling configured is not a reason to refuse work"
        );
        assert_eq!(rig.handler.invocations(), 1, "the run executed");
        assert_eq!(
            slots.released(),
            0,
            "there was no slot, so there was nothing to release and no query \
             spent discovering that"
        );
    }

    /// A gate that cannot answer defers the run rather than admitting it.
    #[tokio::test]
    async fn a_gate_that_cannot_answer_defers_the_run() {
        let rig = rig();
        let slots = Arc::new(BrokenRunSlots);
        let task = seed(&rig, "idem-broken-gate").await;
        let mediator = mediator(&rig, "w-broken", slots);

        let outcome = mediator.mediate(delivery(&task)).await;

        assert!(
            matches!(outcome.settlement, Settlement::Nak { .. }),
            "an unreachable gate refuses: a run that starts because the gate was \
             silent is exactly the over-admission the gate exists to prevent"
        );
        assert_eq!(
            rig.handler.invocations(),
            0,
            "the handler did not run behind a gate that could not answer"
        );
    }

    /// A gate that is down.
    #[derive(Debug)]
    struct BrokenRunSlots;

    #[async_trait::async_trait]
    impl RunSlotPort for BrokenRunSlots {
        async fn acquire(
            &self,
            _tenant: TenantId,
            _organization: OrganizationId,
            _execution: Option<mas_common::ids::ExecutionId>,
            _task: mas_common::ids::TaskId,
            _at: &Timestamp,
        ) -> Result<RunSlot> {
            Err(AppError::internal("the quota store is unreachable"))
        }
        async fn renew(
            &self,
            _tenant: TenantId,
            _organization: OrganizationId,
            _execution: Option<mas_common::ids::ExecutionId>,
            _task: mas_common::ids::TaskId,
            _until: &Timestamp,
            _at: &Timestamp,
        ) -> Result<()> {
            Ok(())
        }
        async fn release(
            &self,
            _tenant: TenantId,
            _organization: OrganizationId,
            _execution: Option<mas_common::ids::ExecutionId>,
            _task: mas_common::ids::TaskId,
            _at: &Timestamp,
        ) -> Result<()> {
            Ok(())
        }
    }

    async fn run_until<P>(
        rig: &Rig,
        dispatcher: OperationDispatcher,
        sentinel: P,
    ) -> ConsumerOutcome
    where
        P: Fn(&CannedHandler) -> bool,
    {
        let consumer = consumer(rig, dispatcher, "w-test");
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
    async fn happy_path_persists_terminal_state_before_acking() {
        let rig = rig();
        let task = seed(&rig, "idem-happy").await;
        publish(&rig, &queue_message(&task)).await;

        let outcome = run_until(&rig, dispatcher(rig.handler.clone()), |h| {
            h.invocations() >= 1
        })
        .await;
        assert_eq!(outcome.acked, 1);
        assert_eq!(outcome.completed, 1);
        assert_eq!(outcome.failed, 0);

        // Terminal state is durable BEFORE the broker was told: the row is
        // completed even if we inspect it before reading broker stats.
        let (row, execution) = rig
            .durable
            .load_task(task.tenant_id, task.id)
            .await
            .expect("load")
            .expect("row");
        assert_eq!(row.status, mas_common::enums::TaskStatus::Completed);
        assert!(row.finished_at.is_some());
        assert_eq!(
            execution.map(|e| e.status),
            Some(mas_common::enums::ExecutionStatus::Completed),
            "the execution finalized in the same unit as the task"
        );

        let stats = rig.broker.consumer_stats("c-test").await.expect("stats");
        assert_eq!(stats.acked, 1);
        assert_eq!(stats.terminated, 0);
    }

    #[tokio::test]
    async fn transient_failure_retries_from_the_durable_attempt_budget() {
        let rig = rig();
        rig.handler
            .push_failure(AppError::internal("upstream hiccup #1"));
        let task = seed(&rig, "idem-retry").await;
        publish(&rig, &queue_message(&task)).await;

        let outcome = run_until(&rig, dispatcher(rig.handler.clone()), |h| {
            h.invocations() >= 2
        })
        .await;

        let (row, _) = rig
            .durable
            .load_task(task.tenant_id, task.id)
            .await
            .expect("load")
            .expect("row");
        assert_eq!(row.status, mas_common::enums::TaskStatus::Completed);
        assert!(
            row.attempt_count >= 2,
            "attempts are counted in durable state, got {}",
            row.attempt_count
        );
        assert_eq!(outcome.acked, 1, "second delivery acked");
        assert!(outcome.naks >= 1, "first delivery requeued");
    }

    #[tokio::test]
    async fn terminal_failures_dead_letter_without_retry() {
        let rig = rig();
        rig.handler
            .push_failure(AppError::validation("payload cannot ever parse"));
        let task = seed(&rig, "idem-poison").await;
        publish(&rig, &queue_message(&task)).await;

        let outcome = run_until(&rig, dispatcher(rig.handler.clone()), |h| {
            h.invocations() >= 1
        })
        .await;
        assert_eq!(outcome.terms, 1, "validation ⇒ Term");
        assert_eq!(outcome.naks, 0, "terminal failures are never retried");
        assert_eq!(rig.handler.invocations(), 1, "exactly one invocation");

        let (row, _) = rig
            .durable
            .load_task(task.tenant_id, task.id)
            .await
            .expect("load")
            .expect("row");
        assert_eq!(row.status, mas_common::enums::TaskStatus::DeadLettered);
        assert_eq!(row.last_error_code.as_deref(), Some("VALIDATION_FAILED"));
        let stats = rig.broker.consumer_stats("c-test").await.expect("stats");
        assert_eq!(stats.terminated, 1);
    }

    #[tokio::test]
    async fn unknown_operation_is_poison_at_the_dispatcher() {
        let rig = rig();
        let task = seed(&rig, "idem-unknown").await;
        publish(&rig, &queue_message(&task)).await;

        let consumer = consumer(&rig, OperationDispatcher::new(), "w-test");
        let (handle, watch) = crate::grace::channel();
        let run = tokio::spawn(async move { consumer.run(watch).await });
        tokio::time::sleep(Duration::from_millis(150)).await;
        handle.trigger();
        let outcome = run.await.expect("join").expect("run");
        assert_eq!(outcome.terms, 1, "unknown operation ⇒ Term");
        let (row, _) = rig
            .durable
            .load_task(task.tenant_id, task.id)
            .await
            .expect("load")
            .expect("row");
        assert_eq!(row.status, mas_common::enums::TaskStatus::DeadLettered);
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
        let consumer = consumer(&rig, dispatcher(rig.handler.clone()), "w-test");
        let (handle, watch) = crate::grace::channel();
        let run = tokio::spawn(async move { consumer.run(watch).await });
        tokio::time::sleep(Duration::from_millis(120)).await;
        handle.trigger();
        let outcome = run.await.expect("join").expect("run");
        assert_eq!(outcome.terms, 1);
        assert_eq!(outcome.skipped, 1);
    }

    #[tokio::test]
    async fn a_second_worker_never_runs_a_task_the_first_one_owns() {
        // The duplicate-parallel-machine guarantee: worker A owns the task, so
        // worker B's claim must return "not claimable" and B must leave the
        // work alone rather than run it a second time.
        let rig = rig();
        let task = seed(&rig, "idem-owned").await;

        let owner =
            AttemptCoordinator::new(rig.durable.clone(), "w-owner", 4, Duration::from_secs(30))
                .expect("coordinator");
        let claim = owner
            .claim_one(task.id, task.tenant_id, &Timestamp::now())
            .await
            .expect("claim");
        assert!(
            matches!(claim, ClaimOneOutcome::Claimed(_)),
            "the owner took the task"
        );

        // Worker B sees the same message.
        publish(&rig, &queue_message(&task)).await;
        let competitor_handler = Arc::new(CannedHandler::default());
        let consumer = consumer(&rig, dispatcher(competitor_handler.clone()), "w-other");
        let (handle, watch) = crate::grace::channel();
        let run = tokio::spawn(async move { consumer.run(watch).await });
        tokio::time::sleep(Duration::from_millis(150)).await;
        handle.trigger();
        let outcome = run.await.expect("join").expect("run");

        assert_eq!(
            competitor_handler.invocations(),
            0,
            "work never leaked past the durable ownership fence"
        );
        assert_eq!(outcome.acked, 1, "the duplicate delivery is dropped");

        let (row, _) = rig
            .durable
            .load_task(task.tenant_id, task.id)
            .await
            .expect("load")
            .expect("row");
        assert_eq!(row.status, mas_common::enums::TaskStatus::Running);
        assert_eq!(row.locked_by.as_deref(), Some("w-owner"));
    }

    #[tokio::test]
    async fn capacity_zero_claims_nothing_and_runs_nothing() {
        // A worker configured to run nothing must run nothing — not "run it
        // anyway because the broker delivered it".
        let rig = rig();
        let task = seed(&rig, "idem-capacity").await;
        publish(&rig, &queue_message(&task)).await;

        let attempts = Arc::new(
            AttemptCoordinator::new(rig.durable.clone(), "w-test", 0, Duration::from_secs(30))
                .expect("coordinator"),
        );
        let consumer = TaskConsumer::new(
            rig.broker.clone(),
            attempts,
            Arc::new(dispatcher(rig.handler.clone())),
            ConsumerConfig {
                consumer_name: "c-test".to_owned(),
                filter: mas_messaging::subjects::SubjectFilter::parse("mas.tasks.>").expect("f"),
                batch_size: 4,
                ack_wait: Duration::from_millis(200),
                poll_interval: Duration::from_millis(5),
                lease_ttl: Duration::from_millis(150),
                max_in_flight: 0,
                drain_timeout: Duration::from_secs(3),
                worker_name: "w-test".to_owned(),
                retry: RetryPolicy {
                    base: Duration::from_millis(5),
                    cap: Duration::from_millis(40),
                    max_deliver: 10,
                },
            },
        );
        let (handle, watch) = crate::grace::channel();
        let run = tokio::spawn(async move { consumer.run(watch).await });
        tokio::time::sleep(Duration::from_millis(150)).await;
        handle.trigger();
        let outcome = run.await.expect("join").expect("run");

        assert_eq!(rig.handler.invocations(), 0, "at capacity, nothing runs");
        assert_eq!(outcome.completed, 0);
        let (row, _) = rig
            .durable
            .load_task(task.tenant_id, task.id)
            .await
            .expect("load")
            .expect("row");
        assert_eq!(
            row.status,
            mas_common::enums::TaskStatus::Queued,
            "the task stays claimable"
        );
    }

    #[tokio::test]
    async fn a_stale_worker_settlement_is_rejected_not_applied() {
        // Worker A claims, then loses the lease to B, then finishes late. Its
        // settlement must be refused and counted — never silently applied over
        // B's newer state, and never silently dropped either.
        let rig = rig();
        let task = seed(&rig, "idem-stale").await;
        publish(&rig, &queue_message(&task)).await;

        let first = AttemptCoordinator::new(rig.durable.clone(), "w-1", 4, Duration::from_secs(30))
            .expect("coordinator");
        let ClaimOneOutcome::Claimed(claim) = first
            .claim_one(task.id, task.tenant_id, &Timestamp::now())
            .await
            .expect("claim")
        else {
            panic!("expected the first worker to take the task");
        };

        // A's lease lapses and B takes over.
        let second =
            AttemptCoordinator::new(rig.durable.clone(), "w-2", 4, Duration::from_secs(30))
                .expect("coordinator");
        let later = Timestamp::now()
            .checked_add(Duration::from_secs(120))
            .expect("later");
        let stolen = second.claim(4, &later).await.expect("reclaim");
        assert_eq!(stolen.len(), 1, "the lapsed lease was reclaimed");

        // A finishes late.
        let error = first
            .succeed(&claim, Default::default(), true, &later)
            .await
            .expect_err("a stale worker must not overwrite newer state");
        assert!(matches!(error, AppError::Conflict(_)), "{error:?}");

        assert!(
            first
                .succeed(&claim, Default::default(), true, &later)
                .await
                .is_err(),
            "the rejection is stable, not a one-off"
        );
    }

    #[tokio::test]
    async fn graceful_shutdown_drains_in_flight_work() {
        let rig = rig();
        let slow = Arc::new(SlowHandler {
            delay: Duration::from_millis(120),
            completed: std::sync::atomic::AtomicU64::new(0),
        });
        let task = seed(&rig, "idem-drain").await;
        publish(&rig, &queue_message(&task)).await;

        let consumer = consumer(
            &rig,
            OperationDispatcher::new().with_handler("execution.run", slow.clone()),
            "w-test",
        );
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
