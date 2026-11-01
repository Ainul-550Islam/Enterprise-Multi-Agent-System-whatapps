//! JetStream-shaped broker port with an in-memory reference implementation.
//!
//! The [`BrokerPort`] mirrors the slice of JetStream the platform depends on:
//! sequenced append into subjects, durable pull consumers with an explicit ack
//! protocol (`Ack` / `Nak{delay}` / `Term` / `InProgress`), bounded
//! redelivery (`max_deliver`) and a dead-letter sink. [`InMemoryBroker`]
//! implements the full semantics — including ack-window expiry, idempotent
//! publish via message ids, and dead-lettering — so unit tests and local
//! development exercise exactly the contract the production NATS adapter
//! must honor.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Mutex;
use std::time::Duration;

use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_events::envelope::EventEnvelope;

use crate::codec::EventFrameCodec;
use crate::headers::{headers_for_envelope, HeaderSet, IDEMPOTENCY_KEY};
use crate::subjects::conventions;
use crate::subjects::{Subject, SubjectFilter};

/// Default ceiling for one frame accepted by the broker (1 MiB).
pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;

/// Default retention of the in-memory stream (bounded ring).
pub const DEFAULT_MAX_MESSAGES: usize = 100_000;

/// Something to publish.
#[derive(Debug, Clone)]
pub struct PublishRequest {
    /// Concrete subject to append to.
    pub subject: Subject,
    /// Propagation + producer headers (validated).
    pub headers: HeaderSet,
    /// Encoded payload, already framed by a codec.
    pub payload: Vec<u8>,
    /// Optional idempotency key: a second publish with the same key returns
    /// the original sequence instead of appending.
    pub msg_id: Option<String>,
}

impl PublishRequest {
    /// New request with empty headers.
    #[must_use]
    pub fn new(subject: Subject, payload: Vec<u8>) -> Self {
        Self {
            subject,
            headers: HeaderSet::new(),
            payload,
            msg_id: None,
        }
    }

    /// Attaches validated headers.
    #[must_use]
    pub fn with_headers(mut self, headers: HeaderSet) -> Self {
        self.headers = headers;
        self
    }

    /// Attaches an idempotency key (also mirrored into the headers).
    #[must_use]
    pub fn with_msg_id(mut self, msg_id: impl Into<String>) -> Self {
        let msg_id = msg_id.into();
        // Mirror into headers; the header may already exist — treat explicit
        // argument as authoritative.
        let _ = self.headers.insert(IDEMPOTENCY_KEY, msg_id.clone());
        self.msg_id = Some(msg_id);
        self
    }

    /// Effective idempotency key (explicit field wins, header fallback).
    #[must_use]
    pub fn dedup_key(&self) -> Option<&str> {
        self.msg_id
            .as_deref()
            .or_else(|| self.headers.get(IDEMPOTENCY_KEY))
    }
}

/// A sequenced message delivered to a consumer.
#[derive(Debug, Clone)]
pub struct BrokerMessage {
    /// Stream sequence (globally monotonically increasing).
    pub sequence: u64,
    /// Subject it was published to.
    pub subject: Subject,
    /// Header set as published.
    pub headers: HeaderSet,
    /// Framed payload.
    pub payload: Vec<u8>,
    /// When the broker accepted it.
    pub published_at: Timestamp,
    /// How many times this delivery happened (1 = first delivery).
    pub deliveries: u32,
}

impl BrokerMessage {
    /// Whether this is a redelivery of an earlier attempt.
    #[must_use]
    pub fn is_redelivery(&self) -> bool {
        self.deliveries > 1
    }
}

/// The consumer's decision about an in-flight message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckInstruction {
    /// Done: remove from pending.
    Ack,
    /// Retry: redeliver, optionally after an explicit delay (default: the
    /// consumer's `ack_wait`).
    Nak {
        /// Optional explicit redelivery delay.
        delay: Option<Duration>,
    },
    /// Poison: never redeliver to this consumer; counts toward dead-letter
    /// when configured.
    Term,
    /// Extend the in-flight window by one `ack_wait`.
    InProgress,
}

impl AckInstruction {
    /// Convenience for a NAK with default delay.
    #[must_use]
    pub fn nak() -> Self {
        Self::Nak { delay: None }
    }
}

/// Durable pull-consumer configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerConfig {
    /// Durable name (`snake_case`, no wildcards).
    pub name: String,
    /// Which subjects feed this consumer.
    pub filter: SubjectFilter,
    /// How long a delivery may stay un-acked before redelivery.
    pub ack_wait: Duration,
    /// Max deliveries of one message before it is dead-lettered.
    pub max_deliver: u32,
}

impl ConsumerConfig {
    /// Reasonable defaults: 30s ack window, 5 attempts.
    pub fn new(name: impl Into<String>, filter: SubjectFilter) -> Result<Self> {
        let config = Self {
            name: name.into(),
            filter,
            ack_wait: Duration::from_secs(30),
            max_deliver: 5,
        };
        config.validated()
    }

    /// Overrides the ack window.
    #[must_use]
    pub fn with_ack_wait(mut self, ack_wait: Duration) -> Self {
        self.ack_wait = ack_wait;
        self
    }

    /// Overrides the delivery ceiling.
    #[must_use]
    pub fn with_max_deliver(mut self, max_deliver: u32) -> Self {
        self.max_deliver = max_deliver;
        self
    }

    /// Validates all fields; called by constructors, re-checked by brokers so
    /// hand-assembled configs cannot smuggle invalid values in.
    pub fn validated(self) -> Result<Self> {
        if self.name.is_empty() || self.name.len() > 128 {
            return Err(AppError::validation(
                "consumer name must be 1..=128 characters",
            ));
        }
        if !self
            .name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
        {
            return Err(AppError::validation(
                "consumer name may contain alphanumerics, '-', '_' only",
            ));
        }
        if self.ack_wait < Duration::from_millis(10) || self.ack_wait > Duration::from_secs(300) {
            return Err(AppError::validation("ack_wait must be in 10ms..=300s"));
        }
        if !(1..=64).contains(&self.max_deliver) {
            return Err(AppError::validation("max_deliver must be in 1..=64"));
        }
        if self.filter.as_str().ends_with(".dlq") {
            return Err(AppError::validation(
                "consumers may not subscribe to dead-letter subjects directly",
            ));
        }
        Ok(self)
    }
}

/// Per-consumer counters, mirrored from JetStream's consumer info.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConsumerStats {
    /// Total deliveries (first-time + redeliveries).
    pub delivered: u64,
    /// Messages positively acknowledged.
    pub acked: u64,
    /// Messages negatively acknowledged (scheduled for redelivery).
    pub nacked: u64,
    /// Messages terminated by the consumer.
    pub terminated: u64,
    /// Messages that exhausted `max_deliver` and were dead-lettered.
    pub dead_lettered: u64,
    /// Currently in-flight (delivered, awaiting ack).
    pub pending: usize,
}

/// The messaging port every transport adapter implements.
#[async_trait::async_trait]
pub trait BrokerPort: Send + Sync + fmt::Debug {
    /// Appends to a subject; returns the assigned sequence. Idempotent on
    /// `msg_id` — a duplicate returns the original sequence.
    async fn publish(&self, request: PublishRequest) -> Result<u64>;

    /// Creates (or validates idempotently) a durable consumer.
    async fn ensure_consumer(&self, config: ConsumerConfig) -> Result<()>;

    /// Pulls up to `max` messages for a consumer, honoring the ack window
    /// (expired pending deliveries are redelivered first).
    async fn fetch(&self, consumer: &str, max: usize) -> Result<Vec<BrokerMessage>>;

    /// Settles one in-flight message.
    async fn ack(&self, consumer: &str, sequence: u64, instruction: AckInstruction) -> Result<()>;

    /// Introspection for one consumer.
    async fn consumer_stats(&self, consumer: &str) -> Result<ConsumerStats>;

    /// Snapshot of dead-lettered messages (for inspection/replay tooling).
    async fn dead_letters(&self) -> Result<Vec<BrokerMessage>>;

    /// Cheap liveness probe used by [`crate::health`].
    async fn ping(&self) -> Result<()>;
}

/// Publishes one domain event through the codec + headers machinery.
///
/// Subject is `mas.events.<aggregate>.<event.type>`; the envelope id becomes
/// the idempotency key, so re-publishing an envelope is naturally at-most-once.
pub async fn publish_event(
    broker: &dyn BrokerPort,
    codec: &dyn EventFrameCodec,
    envelope: &EventEnvelope,
) -> Result<u64> {
    let payload = codec.encode(envelope).map_err(AppError::from)?;
    let request = PublishRequest::new(
        conventions::event_subject(&envelope.aggregate_type, &envelope.event_type)?,
        payload,
    )
    .with_headers(headers_for_envelope(envelope)?)
    .with_msg_id(envelope.id.to_string());
    broker.publish(request).await
}

#[derive(Debug)]
struct PendingDelivery {
    not_before: Timestamp,
    deliveries: u32,
}

#[derive(Debug, Default)]
struct ConsumerRuntime {
    config: Option<ConsumerConfig>,
    /// Next sequence to consider for first-time delivery (deliver-all policy).
    cursor: u64,
    pending: BTreeMap<u64, PendingDelivery>,
    terminated: BTreeSet<u64>,
    dead: Vec<BrokerMessage>,
    stats: ConsumerStats,
}

#[derive(Debug, Default)]
struct BrokerState {
    next_sequence: u64,
    log: BTreeMap<u64, BrokerMessage>,
    by_msg_id: BTreeMap<String, u64>,
    consumers: BTreeMap<String, ConsumerRuntime>,
    available: bool,
}

/// Full JetStream-semantics in-memory broker (tests, local development).
#[derive(Debug)]
pub struct InMemoryBroker {
    state: Mutex<BrokerState>,
    max_payload: usize,
    max_messages: usize,
}

impl Default for InMemoryBroker {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryBroker {
    /// Empty broker with default limits.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new(BrokerState::default()),
            max_payload: DEFAULT_MAX_PAYLOAD_BYTES,
            max_messages: DEFAULT_MAX_MESSAGES,
        }
    }

    /// Overrides limits (bounded so tests can't request unbounded memory by
    /// accident).
    pub fn with_limits(mut self, max_payload: usize, max_messages: usize) -> Result<Self> {
        if max_payload == 0 || max_payload > 64 * 1024 * 1024 {
            return Err(AppError::validation("max_payload must be 1..=64MiB"));
        }
        if max_messages == 0 || max_messages > 1_000_000 {
            return Err(AppError::validation("max_messages must be 1..=1_000_000"));
        }
        self.max_payload = max_payload;
        self.max_messages = max_messages;
        Ok(self)
    }

    /// Chaos knob for tests and health-probe drills: when unavailable,
    /// `publish`/`ping` fail with `external_service`.
    pub fn set_available(&self, available: bool) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .available = available;
    }

    fn with_state<R>(&self, f: impl FnOnce(&mut BrokerState) -> Result<R>) -> Result<R> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut state)
    }

    fn require_available(state: &BrokerState) -> Result<()> {
        if !state.available {
            return Err(AppError::external_service(
                "message-broker",
                "broker is currently unavailable",
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl BrokerPort for InMemoryBroker {
    async fn publish(&self, request: PublishRequest) -> Result<u64> {
        if request.payload.len() > self.max_payload {
            return Err(AppError::validation(format!(
                "payload of {} bytes exceeds broker ceiling of {} bytes",
                request.payload.len(),
                self.max_payload
            )));
        }
        self.with_state(|state| {
            Self::require_available(state)?;
            let dedup_key = request.dedup_key().map(str::to_owned);
            if let Some(key) = &dedup_key {
                if let Some(existing) = state.by_msg_id.get(key) {
                    return Ok(*existing);
                }
            }
            let sequence = state.next_sequence.saturating_add(1);
            state.next_sequence = sequence;
            let message = BrokerMessage {
                sequence,
                subject: request.subject,
                headers: request.headers,
                payload: request.payload,
                published_at: Timestamp::now(),
                deliveries: 0,
            };
            if let Some(key) = dedup_key {
                state.by_msg_id.insert(key, sequence);
            }
            state.log.insert(sequence, message);
            // Bounded retention: drop the oldest messages (plus their
            // dedup entries) when the ring is full.
            while state.log.len() > self.max_messages {
                if let Some((oldest, _)) = state.log.iter().next().map(|(k, v)| (*k, v.clone())) {
                    state.log.remove(&oldest);
                    state.by_msg_id.retain(|_, v| *v != oldest);
                }
            }
            Ok(sequence)
        })
    }

    async fn ensure_consumer(&self, config: ConsumerConfig) -> Result<()> {
        let name = config.name.clone();
        self.with_state(|state| {
            let runtime = state.consumers.entry(name).or_default();
            match &runtime.config {
                None => {
                    runtime.config = Some(config.validated()?);
                    runtime.cursor = 1; // deliver-all policy
                    Ok(())
                },
                Some(existing) if *existing == config.clone().validated()? => Ok(()),
                Some(existing) => Err(AppError::conflict(format!(
                    "consumer '{}' already exists with a different config \
                     (filter {:?} vs {:?})",
                    existing.name,
                    existing.filter.as_str(),
                    config.filter.as_str()
                ))),
            }
        })
    }

    async fn fetch(&self, consumer: &str, max: usize) -> Result<Vec<BrokerMessage>> {
        if max == 0 {
            return Ok(Vec::new());
        }
        self.with_state(|state| {
            Self::require_available(state)?;
            let name = consumer.to_owned();
            let runtime = state
                .consumers
                .get_mut(&name)
                .ok_or_else(|| AppError::not_found("broker consumer", consumer))?;
            let config = runtime
                .config
                .clone()
                .ok_or_else(|| AppError::not_found("broker consumer", consumer))?;
            let now = Timestamp::now();
            let mut out: Vec<BrokerMessage> = Vec::new();

            // 1) Redeliveries first: expired pending entries, honoring
            //    max_deliver → dead-letter.
            let expired: Vec<u64> = runtime
                .pending
                .iter()
                .filter(|(_, p)| !p.not_before.is_future())
                .map(|(seq, _)| *seq)
                .collect();
            for seq in expired {
                if out.len() >= max {
                    break;
                }
                let Some(pending) = runtime.pending.get(&seq) else {
                    continue;
                };
                let deliveries = pending.deliveries;
                if deliveries >= config.max_deliver {
                    if let Some(original) = state.log.get(&seq).cloned() {
                        let mut dead = original;
                        dead.deliveries = deliveries;
                        runtime.dead.push(dead);
                        runtime.stats.dead_lettered += 1;
                    }
                    runtime.pending.remove(&seq);
                    continue;
                }
                if let Some(original) = state.log.get(&seq).cloned() {
                    let mut redelivered = original;
                    redelivered.deliveries = deliveries + 1;
                    let not_before = now.checked_add(config.ack_wait).unwrap_or(now);
                    runtime.pending.insert(
                        seq,
                        PendingDelivery {
                            not_before,
                            deliveries: deliveries + 1,
                        },
                    );
                    runtime.stats.delivered += 1;
                    out.push(redelivered);
                } else {
                    // Message aged out of retention: nothing to deliver.
                    runtime.pending.remove(&seq);
                }
            }

            // 2) Fresh messages from the cursor.
            let mut cursor = runtime.cursor;
            for (seq, message) in state.log.range(cursor..) {
                if out.len() >= max {
                    break;
                }
                cursor = seq + 1;
                if runtime.terminated.contains(seq) {
                    continue;
                }
                if !config.filter.matches(&message.subject) {
                    continue;
                }
                let mut delivered = message.clone();
                delivered.deliveries = 1;
                let not_before = now.checked_add(config.ack_wait).unwrap_or(now);
                runtime.pending.insert(
                    *seq,
                    PendingDelivery {
                        not_before,
                        deliveries: 1,
                    },
                );
                runtime.stats.delivered += 1;
                out.push(delivered);
            }
            runtime.cursor = cursor;
            runtime.stats.pending = runtime.pending.len();
            Ok(out)
        })
    }

    async fn ack(&self, consumer: &str, sequence: u64, instruction: AckInstruction) -> Result<()> {
        self.with_state(|state| {
            let runtime = state
                .consumers
                .get_mut(consumer)
                .ok_or_else(|| AppError::not_found("broker consumer", consumer))?;
            let config = runtime
                .config
                .clone()
                .ok_or_else(|| AppError::not_found("broker consumer", consumer))?;
            let Some(pending) = runtime.pending.remove(&sequence) else {
                return Err(AppError::not_found(
                    "in-flight delivery",
                    format!("{consumer}#{sequence}"),
                ));
            };
            let now = Timestamp::now();
            match instruction {
                AckInstruction::Ack => {
                    runtime.stats.acked += 1;
                },
                AckInstruction::Nak { delay } => {
                    runtime.stats.nacked += 1;
                    let wait = delay.unwrap_or(config.ack_wait);
                    let not_before = now.checked_add(wait).unwrap_or(now);
                    runtime.pending.insert(
                        sequence,
                        PendingDelivery {
                            not_before,
                            deliveries: pending.deliveries,
                        },
                    );
                },
                AckInstruction::Term => {
                    runtime.stats.terminated += 1;
                    runtime.terminated.insert(sequence);
                    if config.max_deliver <= pending.deliveries {
                        // TERM at the delivery ceiling goes straight to DLQ.
                        if let Some(original) = state.log.get(&sequence).cloned() {
                            runtime.dead.push(original);
                            runtime.stats.dead_lettered += 1;
                        }
                    }
                },
                AckInstruction::InProgress => {
                    let not_before = now.checked_add(config.ack_wait).unwrap_or(now);
                    runtime.pending.insert(
                        sequence,
                        PendingDelivery {
                            not_before,
                            deliveries: pending.deliveries,
                        },
                    );
                },
            }
            runtime.stats.pending = runtime.pending.len();
            Ok(())
        })
    }

    async fn consumer_stats(&self, consumer: &str) -> Result<ConsumerStats> {
        self.with_state(|state| {
            let runtime = state
                .consumers
                .get(consumer)
                .ok_or_else(|| AppError::not_found("broker consumer", consumer))?;
            let mut stats = runtime.stats;
            stats.pending = runtime.pending.len();
            Ok(stats)
        })
    }

    async fn dead_letters(&self) -> Result<Vec<BrokerMessage>> {
        self.with_state(|state| {
            Ok(state
                .consumers
                .values()
                .flat_map(|c| c.dead.clone())
                .collect())
        })
    }

    async fn ping(&self) -> Result<()> {
        self.with_state(|state| Self::require_available(state))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::JsonEventCodec;
    use mas_common::ids::TenantId;

    fn filter(raw: &str) -> SubjectFilter {
        SubjectFilter::parse(raw).expect("filter")
    }

    async fn make_broker() -> InMemoryBroker {
        let broker = InMemoryBroker::new();
        broker.set_available(true);
        broker
    }

    #[tokio::test]
    async fn publish_fetch_ack_happy_path_and_idempotency() {
        let broker = make_broker().await;
        broker
            .ensure_consumer(ConsumerConfig::new("workers", filter("tasks.>")).expect("cfg"))
            .await
            .expect("consumer");

        let subject = Subject::parse("tasks.critical").expect("subject");
        let seq1 = broker
            .publish(PublishRequest::new(subject.clone(), b"one".to_vec()).with_msg_id("m-1"))
            .await
            .expect("publish");
        // Idempotent re-publish: same key, same sequence, no duplicate.
        let seq2 = broker
            .publish(PublishRequest::new(subject.clone(), b"one-dup".to_vec()).with_msg_id("m-1"))
            .await
            .expect("publish dup");
        assert_eq!(seq1, seq2);
        broker
            .publish(PublishRequest::new(
                Subject::parse("other.topic").expect("subject"),
                b"noise".to_vec(),
            ))
            .await
            .expect("publish other");

        let batch = broker.fetch("workers", 10).await.expect("fetch");
        assert_eq!(batch.len(), 1, "filter excludes other.topic");
        assert_eq!(batch[0].payload, b"one");
        assert!(!batch[0].is_redelivery());
        broker
            .ack("workers", batch[0].sequence, AckInstruction::Ack)
            .await
            .expect("ack");

        let stats = broker.consumer_stats("workers").await.expect("stats");
        assert_eq!(stats.delivered, 1);
        assert_eq!(stats.acked, 1);
        assert_eq!(stats.pending, 0);
        assert!(broker.fetch("workers", 10).await.expect("empty").is_empty());
    }

    #[tokio::test]
    async fn nak_redelivers_term_drops_and_max_deliver_dead_letters() {
        let broker = make_broker().await;
        broker
            .ensure_consumer(
                ConsumerConfig::new("retrying", filter("jobs.>"))
                    .expect("cfg")
                    .with_ack_wait(Duration::from_millis(50))
                    .with_max_deliver(2),
            )
            .await
            .expect("consumer");

        broker
            .publish(PublishRequest::new(
                Subject::parse("jobs.a").expect("subject"),
                b"a".to_vec(),
            ))
            .await
            .expect("publish a");
        broker
            .publish(PublishRequest::new(
                Subject::parse("jobs.b").expect("subject"),
                b"b".to_vec(),
            ))
            .await
            .expect("publish b");

        let batch = broker.fetch("retrying", 10).await.expect("fetch");
        assert_eq!(batch.len(), 2);

        // NAK the first: not redelivered until the delay passes.
        broker
            .ack("retrying", batch[0].sequence, AckInstruction::nak())
            .await
            .expect("nak");
        // TERM the second: never seen again, not dead-lettered (deliveries=1 < max).
        broker
            .ack("retrying", batch[1].sequence, AckInstruction::Term)
            .await
            .expect("term");

        assert!(broker
            .fetch("retrying", 10)
            .await
            .expect("too early")
            .is_empty());
        tokio::time::sleep(Duration::from_millis(80)).await;
        let redelivery = broker.fetch("retrying", 10).await.expect("redeliver");
        assert_eq!(redelivery.len(), 1);
        assert!(redelivery[0].is_redelivery());
        assert_eq!(redelivery[0].payload, b"a");

        // That redelivery was delivery #2 == max_deliver: leaving it unacked
        // past the window dead-letters it instead of redelivering again.
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(
            broker
                .fetch("retrying", 10)
                .await
                .expect("exhausted")
                .is_empty(),
            "message is dead-lettered instead of redelivered"
        );

        let dead = broker.dead_letters().await.expect("dlq");
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].payload, b"a");
        let stats = broker.consumer_stats("retrying").await.expect("stats");
        assert_eq!(stats.nacked, 1);
        assert_eq!(stats.terminated, 1);
        assert_eq!(stats.dead_lettered, 1);
    }

    #[tokio::test]
    async fn in_progress_extends_the_ack_window() {
        let broker = make_broker().await;
        broker
            .ensure_consumer(
                ConsumerConfig::new("longwork", filter("w.>"))
                    .expect("cfg")
                    .with_ack_wait(Duration::from_millis(50)),
            )
            .await
            .expect("consumer");
        broker
            .publish(PublishRequest::new(
                Subject::parse("w.one").expect("subject"),
                b"x".to_vec(),
            ))
            .await
            .expect("publish");
        let batch = broker.fetch("longwork", 1).await.expect("fetch");
        broker
            .ack("longwork", batch[0].sequence, AckInstruction::InProgress)
            .await
            .expect("progress");
        assert!(
            broker.fetch("longwork", 1).await.expect("fetch").is_empty(),
            "window was extended, nothing to redeliver yet"
        );
    }

    #[tokio::test]
    async fn publish_event_end_to_end_and_limits() {
        let broker = InMemoryBroker::new()
            .with_limits(64 * 1024, 1000)
            .expect("limits");
        broker.set_available(true);
        broker
            .ensure_consumer(ConsumerConfig::new("auditors", filter("mas.events.>")).expect("cfg"))
            .await
            .expect("consumer");

        let codec = JsonEventCodec::default();
        let envelope =
            EventEnvelope::builder("agent.registered", 1, "agent", "agent-7", TenantId::new())
                .expect("builder")
                .with_correlation("corr-77")
                .expect("correlation")
                .with_payload(serde_json::json!({"name": "researcher"}))
                .build()
                .expect("envelope");

        let seq = publish_event(&broker, &codec, &envelope)
            .await
            .expect("publish");
        // Idempotent: re-publishing the same envelope id is a no-op.
        let again = publish_event(&broker, &codec, &envelope)
            .await
            .expect("publish");
        assert_eq!(seq, again);

        let batch = broker.fetch("auditors", 5).await.expect("fetch");
        assert_eq!(batch.len(), 1);
        assert_eq!(
            batch[0].subject.as_str(),
            "mas.events.agent.agent.registered"
        );
        let decoded = codec.decode(&batch[0].payload).expect("decode");
        assert_eq!(decoded.id, envelope.id);
        assert_eq!(decoded.correlation_id, "corr-77");
        assert_eq!(
            batch[0].headers.get(crate::headers::TENANT_ID_KEY),
            Some(
                batch[0]
                    .headers
                    .get(crate::headers::TENANT_ID_KEY)
                    .unwrap_or_default()
            )
        );

        // Tenant header actually equals the envelope tenant.
        let tenant_header = batch[0]
            .headers
            .get(crate::headers::TENANT_ID_KEY)
            .expect("tenant header");
        assert_eq!(tenant_header, decoded.tenant_id.to_string());

        // Oversize publish is refused before touching the log.
        let big = PublishRequest::new(
            Subject::parse("mas.events.blob.stored").expect("subject"),
            vec![0u8; 64 * 1024 + 1],
        );
        assert!(broker.publish(big).await.is_err());
    }

    #[tokio::test]
    async fn availability_gates_publish_and_ping() {
        let broker = InMemoryBroker::new(); // starts unavailable
        assert!(broker.ping().await.is_err());
        broker.set_available(true);
        broker.ping().await.expect("available now");
        broker.set_available(false);
        assert!(broker.ping().await.is_err());
        assert!(broker
            .publish(PublishRequest::new(
                Subject::parse("x.y").expect("subject"),
                b"z".to_vec()
            ))
            .await
            .is_err());
    }
}
