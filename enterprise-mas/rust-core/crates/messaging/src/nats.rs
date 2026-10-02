//! Real NATS JetStream adapter for [`crate::broker::BrokerPort`]
//! (feature `nats`).
//!
//! Shape per `docs/nats-wiring.md`:
//!
//! * Stream `MAS_TASKS` (overridable) covers `mas.tasks.>`; explicit-ack
//!   pull consumers; `Nak{delay}` maps to `nack_with_delay` (JetStream
//!   `async_nats::jetstream::AckKind::Nak`) and `Term` to the server-side terminate advice.
//! * Publish dedup rides the `Nats-Msg-Id` header (JetStream's duplicate
//!   window), mirroring the in-memory and pg lanes.
//! * **Settling is sequence-based at the port, handle-based at JetStream**:
//!   the adapter keeps a bounded in-process flight ledger
//!   (`(consumer, stream_sequence) → message handle`) — redeliveries refresh
//!   the entry, and the server's `AckWait` remains the real timeout arbiter.
//!   A sequence not found in the ledger is an error, never a silent ack.
//! * JetStream has no native dead-letter queue: when a fetched message's
//!   delivery count crosses the consumer's `max_deliver`, the adapter
//!   republishes it to `mas.tasks.dlq` (same stream) and terminates the
//!   original. `dead_letters()` snapshots that lane through an ephemeral
//!   read-only consumer (no acks — repeated snapshots stay cumulative, like
//!   the in-memory lane's dead queue).
//! * Connection resilience is delegated to `async-nats`'s built-in reconnect;
//!   the platform's [`crate::connection`] controller remains the policy
//!   reference for surfaces the client library does not own.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use async_nats::jetstream::consumer::{self, PullConsumer};
use async_nats::jetstream::{self, stream};
use async_nats::HeaderMap as NatsHeaderMap;
use futures_util::StreamExt;

use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;

use crate::broker::{
    AckInstruction, BrokerMessage, BrokerPort, ConsumerConfig, ConsumerStats, PublishRequest,
};
use crate::headers::HeaderSet;
use crate::subjects::Subject;

/// Dead-letter subject suffix inside the task subject space.
pub const DLQ_SUBJECT: &str = "mas.tasks.dlq";
/// Header JetStream consults for publish de-duplication.
pub const NATS_MSG_ID: &str = "Nats-Msg-Id";
/// JetStream duplicate window provisioned for the task stream.
const DUPLICATE_WINDOW: Duration = Duration::from_secs(120);
/// Server-side ack expiry = client ack window + this margin (the server
/// must never strictly expire a message while a client believes it may
/// still settle).
const ACK_WAIT_MARGIN: Duration = Duration::from_secs(5);
/// Maximum in-flight handles retained per broker instance (per-consumer
/// sequences are deduped by key; eviction drops the oldest sequences).
const FLIGHT_LEDGER_CAPACITY: usize = 8192;
/// Batch cap for DLQ snapshots.
const DLQ_SNAPSHOT_LIMIT: usize = 64;
/// How long pull batch requests wait for the server to fill them (worker
/// loops poll on their own cadence, so short waits are correct).
const BATCH_EXPIRY: Duration = Duration::from_millis(500);

fn nats_err(context: &str, error: impl std::fmt::Display) -> AppError {
    AppError::external_service("nats", format!("{context}: {error}"))
}

/// Connection + stream configuration for the adapter.
#[derive(Debug, Clone)]
pub struct NatsBrokerConfig {
    /// Connect URL (`nats://…`, `tls://…`, comma-separated cluster peers).
    pub url: String,
    /// JetStream stream name (covers `mas.tasks.>` once provisioned).
    pub stream: String,
    /// Consumer-ack window default used when a consumer arrives without an
    /// explicit override.
    pub ack_wait: Duration,
}

impl NatsBrokerConfig {
    /// Sensible defaults: stream `MAS_TASKS`, 30s ack window.
    #[must_use]
    pub fn for_url(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            stream: "MAS_TASKS".to_owned(),
            ack_wait: Duration::from_secs(30),
        }
    }

    /// Overrides the stream name (JetStream stream names are ASCII-safe
    /// identifiers; anything else is refused at provisioning).
    #[must_use]
    pub fn with_stream(mut self, stream: impl Into<String>) -> Self {
        let candidate = stream.into();
        if !candidate.is_empty()
            && candidate.len() <= 64
            && candidate
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            self.stream = candidate;
        }
        self
    }

    /// Overrides the consumer ack window (one-second floor).
    #[must_use]
    pub fn with_ack_wait(mut self, ack_wait: Duration) -> Self {
        if ack_wait >= Duration::from_secs(1) {
            self.ack_wait = ack_wait;
        }
        self
    }

    /// Validates the URL shape without touching the network.
    pub fn validated(&self) -> Result<&Self> {
        let trimmed = self.url.trim();
        if trimmed.is_empty() {
            return Err(AppError::validation("broker.nats_url must not be empty"));
        }
        let ok = trimmed
            .split(',')
            .all(|peer| peer.starts_with("nats://") || peer.starts_with("tls://"));
        if !ok {
            return Err(AppError::validation(
                "broker.nats_url expects nats:// or tls:// scheme (comma-separated peers allowed)",
            ));
        }
        Ok(self)
    }
}

/// LRU-ish keyed ledger of in-flight message handles.
#[derive(Debug, Default)]
struct FlightLedger {
    inner: BTreeMap<(String, u64), jetstream::Message>,
}

impl FlightLedger {
    fn insert(&mut self, consumer: &str, sequence: u64, message: jetstream::Message) {
        self.inner.insert((consumer.to_owned(), sequence), message);
        if self.inner.len() > FLIGHT_LEDGER_CAPACITY {
            // Oldest (smallest sequence) first — a stale evicted handle is
            // harmless: redelivery refreshes it, and the un-acked original
            // is reclaimed by the server's AckWait either way.
            let victim = self.inner.keys().next().cloned();
            if let Some(key) = victim {
                self.inner.remove(&key);
            }
        }
    }

    fn take(&mut self, consumer: &str, sequence: u64) -> Option<jetstream::Message> {
        self.inner.remove(&(consumer.to_owned(), sequence))
    }
}

/// The production JetStream broker adapter.
#[derive(Debug)]
pub struct NatsBroker {
    context: jetstream::Context,
    client: async_nats::Client,
    config: NatsBrokerConfig,
    flight: Mutex<FlightLedger>,
}

impl NatsBroker {
    /// Connects and provisions the task stream (idempotent; fails loudly on
    /// shape drift instead of silently working around it).
    pub async fn connect(config: &NatsBrokerConfig) -> Result<Self> {
        config.validated()?;
        let client = async_nats::connect(&config.url)
            .await
            .map_err(|e| nats_err("connect", e))?;
        let context = jetstream::new(client.clone());
        let broker = Self {
            context,
            client,
            config: config.clone(),
            flight: Mutex::new(FlightLedger::default()),
        };
        broker.provision().await?;
        Ok(broker)
    }

    /// Creates (or verifies) the task stream: subjects exactly
    /// `mas.tasks.>`, limits retention, `duplicate_window`. Existing streams
    /// that do not cover the task space are drift, not a convenience.
    pub async fn provision(&self) -> Result<()> {
        let config = stream::Config {
            name: self.config.stream.clone(),
            subjects: vec!["mas.tasks.>".to_owned()],
            retention: stream::RetentionPolicy::Limits,
            duplicate_window: DUPLICATE_WINDOW,
            ..Default::default()
        };
        let mut created = self
            .context
            .get_or_create_stream(config)
            .await
            .map_err(|e| nats_err("create stream", e))?;
        let info = created
            .info()
            .await
            .map_err(|e| nats_err("stream info", e))?;
        if !info
            .config
            .subjects
            .iter()
            .any(|subject| subject == "mas.tasks.>")
        {
            return Err(AppError::external_service(
                "nats",
                format!(
                    "stream {} exists but does not cover mas.tasks.> (subjects={:?}) — reprovision or point broker.stream elsewhere",
                    info.config.name, info.config.subjects
                ),
            ));
        }
        Ok(())
    }

    /// Looks up the durable pull consumer (creates nothing).
    async fn consumer(&self, name: &str) -> Result<PullConsumer> {
        let stream: stream::Stream = self
            .context
            .get_stream(&self.config.stream.clone())
            .await
            .map_err(|e| nats_err("get stream", e))?;
        stream
            .get_consumer(name)
            .await
            .map_err(|e| nats_err("get consumer", e))
    }

    fn to_nats_headers(headers: &HeaderSet, msg_id: Option<&str>) -> NatsHeaderMap {
        let mut map = NatsHeaderMap::new();
        for (key, value) in headers.iter() {
            map.insert(key, value);
        }
        if let Some(id) = msg_id {
            map.insert(NATS_MSG_ID, id);
        }
        map
    }

    fn headers_from_nats(nats_headers: Option<&NatsHeaderMap>) -> HeaderSet {
        let mut headers = HeaderSet::new();
        if let Some(map) = nats_headers {
            for (key, values) in map.iter() {
                for value in values {
                    let _ = headers.insert(AsRef::<str>::as_ref(key), value.to_string());
                }
            }
        }
        headers
    }
}

#[async_trait::async_trait]
impl BrokerPort for NatsBroker {
    async fn publish(&self, request: PublishRequest) -> Result<u64> {
        let headers = Self::to_nats_headers(&request.headers, request.msg_id.as_deref());
        let acked = self
            .context
            .publish_with_headers(request.subject.to_string(), headers, request.payload.into())
            .await
            .map_err(|e| nats_err("publish", e))?;
        let ack = acked.await.map_err(|e| nats_err("publish ack", e))?;
        Ok(ack.sequence)
    }

    async fn ensure_consumer(&self, config: ConsumerConfig) -> Result<()> {
        let stream: stream::Stream = self
            .context
            .get_stream(self.config.stream.clone())
            .await
            .map_err(|e| nats_err("get stream", e))?;
        let pull_config = consumer::pull::Config {
            durable_name: Some(config.name.clone()),
            deliver_policy: consumer::DeliverPolicy::All,
            ack_policy: consumer::AckPolicy::Explicit,
            ack_wait: config.ack_wait + ACK_WAIT_MARGIN,
            max_deliver: i64::from(config.max_deliver),
            filter_subject: config.filter.to_string(),
            replay_policy: consumer::ReplayPolicy::Instant,
            ..Default::default()
        };
        // Idempotent ensure: read first, create on miss. A consumer that
        // already exists with different delivery semantics is config drift
        // the operator must reconcile (documented): we surface it as an
        // error instead of silently reconfiguring a live lane.
        match stream
            .get_consumer::<consumer::pull::Config>(&config.name)
            .await
        {
            Ok(mut existing) => {
                let info = existing
                    .info()
                    .await
                    .map_err(|e| nats_err("consumer info", e))?;
                if info.config.max_deliver != i64::from(config.max_deliver)
                    || info.config.filter_subject != config.filter.to_string()
                {
                    return Err(AppError::external_service(
                        "nats",
                        format!(
                            "consumer {} exists with drifted config (max_deliver={}, filter={}) — delete or reconcile via 'nats consumer edit'",
                            config.name, info.config.max_deliver, info.config.filter_subject
                        ),
                    ));
                }
                Ok(())
            },
            Err(_) => stream
                .create_consumer(pull_config)
                .await
                .map(|_| ())
                .map_err(|e| nats_err("ensure consumer (create)", e)),
        }
    }

    async fn fetch(&self, consumer_name: &str, max: usize) -> Result<Vec<BrokerMessage>> {
        let mut puller = self.consumer(consumer_name).await?;
        let max_deliver = {
            let info = puller
                .info()
                .await
                .map_err(|e| nats_err("consumer info", e))?;
            i64::max(info.config.max_deliver, 1).min(i64::from(u32::MAX)) as u32
        };
        let request = if max == usize::MAX {
            DLQ_SNAPSHOT_LIMIT
        } else {
            max
        };
        let mut batch = puller
            .batch()
            .max_messages(request.max(1))
            .expires(BATCH_EXPIRY)
            .messages()
            .await
            .map_err(|e| nats_err("fetch batch", e))?;

        let mut messages = Vec::with_capacity(request.max(1));
        while let Some(item) = batch.next().await {
            let message = item.map_err(|e| nats_err("fetch message", e))?;
            let info = message.info().map_err(|e| nats_err("message info", e))?;
            let (sequence, delivered) = (info.stream_sequence, info.delivered as u64);
            let subject = Subject::parse(message.subject.as_str())
                .map_err(|e| AppError::validation(format!("delivered subject invalid: {e}")))?;
            let payload = message.payload.to_vec();
            let headers = Self::headers_from_nats(message.headers.as_ref());
            let broker_message = BrokerMessage {
                sequence,
                subject: subject.clone(),
                headers,
                payload: payload.clone(),
                published_at: Timestamp::now(),
                deliveries: u32::try_from(delivered).unwrap_or(u32::MAX),
            };
            if delivered > u64::from(max_deliver) {
                // Platform dead-letter wrapper: copy to the DLQ lane, then
                // terminate so the consumer stops redelivering.
                let dedup = format!("dlq-{consumer_name}-{sequence}-{delivered}");
                let mut dlq_headers = NatsHeaderMap::new();
                dlq_headers.insert(NATS_MSG_ID, dedup);
                self.context
                    .publish_with_headers(DLQ_SUBJECT.to_owned(), dlq_headers, payload.into())
                    .await
                    .map_err(|e| nats_err("dlq republish", e))?
                    .await
                    .map_err(|e| nats_err("dlq publish ack", e))?;
                message
                    .ack_with(async_nats::jetstream::AckKind::Term)
                    .await
                    .map_err(|e| nats_err("dlq terminate", e))?;
                continue;
            }
            self.flight
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(consumer_name, sequence, message);
            messages.push(broker_message);
        }
        Ok(messages)
    }

    async fn ack(&self, consumer: &str, sequence: u64, instruction: AckInstruction) -> Result<()> {
        let message = self
            .flight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take(consumer, sequence)
            .ok_or_else(|| {
                AppError::not_found(
                    "flight message",
                    format!("{consumer}/{sequence} (unknown or already settled)"),
                )
            })?;
        match instruction {
            AckInstruction::Ack => message
                .ack_with(async_nats::jetstream::AckKind::Ack)
                .await
                .map_err(|e| nats_err("ack", e)),
            AckInstruction::Nak { delay } => message
                .ack_with(async_nats::jetstream::AckKind::Nak(delay))
                .await
                .map_err(|e| nats_err("nak", e)),
            AckInstruction::InProgress => message
                .ack_with(async_nats::jetstream::AckKind::Progress)
                .await
                .map_err(|e| nats_err("progress heartbeat", e)),
            AckInstruction::Term => {
                // Poison: copy into the DLQ lane first (inspection/replay
                // surface), then server-side terminate.
                let mut headers = NatsHeaderMap::new();
                headers.insert(NATS_MSG_ID, format!("dlq-{consumer}-{sequence}-term"));
                self.context
                    .publish_with_headers(DLQ_SUBJECT.to_owned(), headers, message.payload.clone())
                    .await
                    .map_err(|e| nats_err("term republish", e))?
                    .await
                    .map_err(|e| nats_err("term publish ack", e))?;
                message
                    .ack_with(async_nats::jetstream::AckKind::Term)
                    .await
                    .map_err(|e| nats_err("term ack", e))
            },
        }
    }

    async fn consumer_stats(&self, consumer_name: &str) -> Result<ConsumerStats> {
        let mut puller = self.consumer(consumer_name).await?;
        let info = puller
            .info()
            .await
            .map_err(|e| nats_err("consumer info", e))?;
        // JetStream tracks total deliveries + ack floor + in-flight counts
        // per consumer; nak/term/dead-letter counters are not part of its
        // consumer-info model (Term counters exist only as advisories), so
        // the honest mapping is: nacked ≈ redelivered-so-far (a redelivery
        // implies a prior non-acknowledgement), terminated/dead_lettered
        // tracked only at the platform level (outbox audit, DLQ lane).
        Ok(ConsumerStats {
            delivered: info.delivered.consumer_sequence,
            acked: info.ack_floor.consumer_sequence,
            nacked: u64::try_from(info.num_redelivered).unwrap_or(u64::MAX),
            terminated: 0,
            dead_lettered: 0,
            pending: info.num_ack_pending,
        })
    }

    async fn dead_letters(&self) -> Result<Vec<BrokerMessage>> {
        // Ephemeral read-only inspection consumer over the DLQ lane; no
        // acks, so repeated snapshots stay cumulative (in-memory-lane
        // parity).
        let stream: stream::Stream = self
            .context
            .get_stream(self.config.stream.clone())
            .await
            .map_err(|e| nats_err("get stream", e))?;
        let mut reader = stream
            .create_consumer(consumer::pull::Config {
                filter_subject: DLQ_SUBJECT.to_owned(),
                ack_policy: consumer::AckPolicy::Explicit,
                ack_wait: Duration::from_secs(30),
                max_deliver: -1,
                ..Default::default()
            })
            .await
            .map_err(|e| nats_err("dlq consumer", e))?;
        let ephemeral_name = reader
            .info()
            .await
            .map_err(|e| nats_err("dlq consumer info", e))?
            .name
            .clone();
        let mut batch = reader
            .batch()
            .max_messages(DLQ_SNAPSHOT_LIMIT)
            .expires(BATCH_EXPIRY)
            .messages()
            .await
            .map_err(|e| nats_err("dlq fetch", e))?;
        let mut out = Vec::new();
        while let Some(item) = batch.next().await {
            let message = item.map_err(|e| nats_err("dlq message", e))?;
            let info = message
                .info()
                .map_err(|e| nats_err("dlq message info", e))?;
            let subject = Subject::parse(message.subject.as_str())
                .map_err(|e| AppError::validation(format!("dlq subject invalid: {e}")))?;
            out.push(BrokerMessage {
                sequence: info.stream_sequence,
                subject,
                headers: Self::headers_from_nats(message.headers.as_ref()),
                payload: message.payload.to_vec(),
                published_at: Timestamp::now(),
                deliveries: u32::try_from(info.delivered).unwrap_or(u32::MAX),
            });
        }
        // No acks above: the lane stays intact for the next snapshot.
        let _ = stream.delete_consumer(&ephemeral_name).await;
        Ok(out)
    }

    async fn ping(&self) -> Result<()> {
        self.client.flush().await.map_err(|e| nats_err("ping", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_and_overrides_are_sanitized() {
        let base = NatsBrokerConfig::for_url("nats://127.0.0.1:4222");
        assert_eq!(base.stream, "MAS_TASKS");
        assert_eq!(base.ack_wait, Duration::from_secs(30));
        base.validated().expect("nats:// scheme valid");

        let customized = base
            .clone()
            .with_stream("TASKS_PROD_7")
            .with_ack_wait(Duration::from_secs(10));
        assert_eq!(customized.stream, "TASKS_PROD_7");
        assert_eq!(customized.ack_wait, Duration::from_secs(10));
        // Illegal stream names/sub-second windows are ignored (keep old).
        let untouched = customized
            .with_stream("bad name !")
            .with_ack_wait(Duration::ZERO);
        assert_eq!(untouched.stream, "TASKS_PROD_7");
        assert_eq!(untouched.ack_wait, Duration::from_secs(10));
    }

    #[test]
    fn url_validation_expects_nats_family_schemes() {
        assert!(NatsBrokerConfig::for_url("").validated().is_err());
        assert!(NatsBrokerConfig::for_url("http://nats:4222")
            .validated()
            .is_err());
        assert!(NatsBrokerConfig::for_url("nats://a:4222,tls://b:4222")
            .validated()
            .is_ok());
        assert!(NatsBrokerConfig::for_url(" nats://a:4222 ")
            .validated()
            .is_ok());
    }

    #[test]
    fn dlq_subject_is_a_valid_platform_subject() {
        Subject::parse(DLQ_SUBJECT).expect("DLQ subject parses");
    }

    #[test]
    fn header_projection_round_trips_headers_and_msg_id() {
        let mut set = HeaderSet::new();
        set.insert("x-trace-id", "t-1").expect("insert");
        let nats_headers = NatsBroker::to_nats_headers(&set, Some("msg-1"));
        let back = NatsBroker::headers_from_nats(Some(&nats_headers));
        assert_eq!(back.get("x-trace-id"), Some("t-1"));
        assert_eq!(back.get(NATS_MSG_ID), Some("msg-1"));
        assert!(NatsBroker::headers_from_nats(None).len() == 0);
    }
}
