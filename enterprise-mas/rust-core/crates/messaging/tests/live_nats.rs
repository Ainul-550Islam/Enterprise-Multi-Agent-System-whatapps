//! Live NATS/JetStream integration suite for the `nats` feature's
//! [`mas_messaging::nats::NatsBroker`]: publish dedup, durable-consumer
//! ensure + idempotency, fetch/ack round trip, nak-with-delay redelivery,
//! poison `Term` → DLQ lane snapshot, consumer stats, and liveness probe.
//!
//! Requires `MAS_TEST_NATS_URL` (falls back to `MAS_NATS_URL`), pointing to
//! an isolated JetStream test server. Missing or broken prerequisites fail.
//! Bring up a server with JetStream (`nats-server -js -p 4222` or
//! `docker run --rm -p 4222:4222 nats -js`) and run:
//!
//! ```text
//! MAS_TEST_NATS_URL=nats://127.0.0.1:4222 \
//!   cargo test -p mas-messaging --features nats --test live_nats
//! ```
//!
//! Each test uses a unique stream name so runs never fight a stale stream.

#![cfg(feature = "nats")]

use std::sync::OnceLock;
use std::time::Duration;

use tokio::sync::{Mutex, MutexGuard};

use mas_common::timestamps::Timestamp;
use mas_messaging::broker::{AckInstruction, BrokerPort, ConsumerConfig, PublishRequest};
use mas_messaging::headers::HeaderSet;
use mas_messaging::nats::{NatsBroker, NatsBrokerConfig, DLQ_SUBJECT, NATS_MSG_ID};
use mas_messaging::subjects::{Subject, SubjectFilter};

/// Environment gate; `MAS_NATS_URL` honoured as the composition key too.
const ENV_VAR: &str = "MAS_TEST_NATS_URL";

fn live_url() -> Option<String> {
    std::env::var(ENV_VAR)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .or_else(|| {
            std::env::var("MAS_NATS_URL")
                .ok()
                .filter(|v| !v.trim().is_empty())
        })
}

/// Serializes tests because JetStream forbids overlapping stream subjects.
static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// Creates a unique stream on a dedicated test server; setup errors are failures.
async fn live_broker(tag: &str) -> (NatsBroker, String, MutexGuard<'static, ()>) {
    let guard = TEST_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    let url = live_url().expect("set MAS_TEST_NATS_URL to a dedicated JetStream test server");
    let stream = format!(
        "MAS_TASKS_TEST_{}_{}",
        tag.replace('-', "_").to_uppercase(),
        Timestamp::now().to_unix_ms() % 10_000_000
    );
    let config = NatsBrokerConfig::for_url(url).with_stream(stream.clone());
    let broker = NatsBroker::connect(&config)
        .await
        .expect("connect and provision isolated test stream");
    (broker, stream, guard)
}

/// Remove only the run-unique stream created by this test before unlocking.
async fn cleanup_stream(stream: &str) {
    assert!(stream.starts_with("MAS_TASKS_TEST_"));
    let client = async_nats::connect(live_url().expect("test URL"))
        .await
        .expect("connect for cleanup");
    async_nats::jetstream::new(client)
        .delete_stream(stream)
        .await
        .expect("delete test stream");
}

fn request(subject: &str, payload: &str, msg_id: Option<&str>) -> PublishRequest {
    PublishRequest {
        subject: Subject::parse(subject).expect("subject"),
        headers: HeaderSet::new(),
        payload: payload.as_bytes().to_vec(),
        msg_id: msg_id.map(str::to_owned),
    }
}

#[tokio::test]
async fn publish_dedups_via_msg_id_and_ack_round_trip() {
    let (broker, stream, _guard) = live_broker("publish-ack").await;
    broker
        .ensure_consumer(
            ConsumerConfig::new(
                "w-publish",
                SubjectFilter::parse("mas.tasks.execute.*").expect("filter"),
            )
            .expect("consumer config"),
        )
        .await
        .expect("ensure consumer");

    let first = broker
        .publish(request("mas.tasks.execute.run1", "p1", Some("idem-1")))
        .await
        .expect("publish 1");
    let duplicate = broker
        .publish(request("mas.tasks.execute.run1", "p1", Some("idem-1")))
        .await
        .expect("publish duplicate");
    assert_eq!(
        first, duplicate,
        "duplicate msg-id must not acquire a new stream sequence"
    );

    let fetched = broker.fetch("w-publish", 4).await.expect("fetch");
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].payload, b"p1");
    assert_eq!(fetched[0].sequence, first);

    broker
        .ack("w-publish", first, AckInstruction::Ack)
        .await
        .expect("ack");
    assert!(broker
        .ack("w-publish", first, AckInstruction::Ack)
        .await
        .is_err());
    cleanup_stream(&stream).await;
}

#[tokio::test]
async fn nak_with_delay_redelivers_and_term_dead_letters() {
    let (broker, stream, _guard) = live_broker("nak-term").await;
    broker
        .ensure_consumer(
            ConsumerConfig::new(
                "w-nak",
                SubjectFilter::parse("mas.tasks.execute.nt").expect("filter"),
            )
            .expect("consumer config"),
        )
        .await
        .expect("ensure consumer");
    let sequence = broker
        .publish(request("mas.tasks.execute.nt", "x1", Some("nt-1")))
        .await
        .expect("publish");

    let first = broker.fetch("w-nak", 1).await.expect("fetch 1");
    broker
        .ack("w-nak", sequence, AckInstruction::Nak { delay: None })
        .await
        .expect("nak");
    tokio::time::sleep(Duration::from_millis(400)).await;
    let redelivered = broker.fetch("w-nak", 1).await.expect("fetch 2");
    assert_eq!(redelivered.len(), 1);
    assert_eq!(redelivered[0].sequence, first[0].sequence);
    assert!(redelivered[0].deliveries >= 2, "redelivery count must grow");

    broker
        .ack("w-nak", sequence, AckInstruction::Term)
        .await
        .expect("term");
    let dlq = broker.dead_letters().await.expect("dlq snapshot");
    assert!(dlq
        .iter()
        .any(|m| m.subject.to_string() == DLQ_SUBJECT && m.payload == b"x1"));
    // Snapshot is cumulative (no acks on the read-only inspector).
    let again = broker.dead_letters().await.expect("second snapshot");
    assert!(again.len() >= dlq.len(), "snapshot must not drain the lane");
    assert!(broker
        .ack("w-nak", sequence, AckInstruction::Ack)
        .await
        .is_err());
    cleanup_stream(&stream).await;
}

#[tokio::test]
async fn ensure_consumer_is_idempotent_and_stats_flow() {
    let (broker, stream, _guard) = live_broker("ensure").await;
    let config = ConsumerConfig::new(
        "w-stats",
        SubjectFilter::parse("mas.tasks.execute.s").expect("filter"),
    )
    .expect("consumer config");
    broker
        .ensure_consumer(config.clone())
        .await
        .expect("ensure 1");
    broker
        .ensure_consumer(config)
        .await
        .expect("ensure 2 (idempotent)");

    broker
        .publish(request("mas.tasks.execute.s", "s1", Some("s-1")))
        .await
        .expect("publish");
    let pending = broker.fetch("w-stats", 1).await.expect("fetch");
    broker
        .ack("w-stats", pending[0].sequence, AckInstruction::Ack)
        .await
        .expect("ack");

    let stats = broker
        .consumer_stats("w-stats")
        .await
        .expect("consumer stats");
    assert!(stats.delivered >= 1);
    assert!(stats.acked >= 1);
    assert_eq!(stats.pending, 0);
    cleanup_stream(&stream).await;
}

#[tokio::test]
async fn publish_carries_headers_and_supplies_msg_id_header() {
    let (broker, stream, _guard) = live_broker("headers").await;
    broker
        .ensure_consumer(
            ConsumerConfig::new(
                "w-headers",
                SubjectFilter::parse("mas.tasks.execute.h").expect("filter"),
            )
            .expect("consumer config"),
        )
        .await
        .expect("ensure consumer");
    let mut headers = HeaderSet::new();
    headers
        .insert("x-trace-id", "trace-9")
        .expect("header insert");
    let sequence = broker
        .publish(PublishRequest {
            subject: Subject::parse("mas.tasks.execute.h").expect("subject"),
            headers,
            payload: b"hb".to_vec(),
            msg_id: Some("h-1".into()),
        })
        .await
        .expect("publish");
    let fetched = broker.fetch("w-headers", 1).await.expect("fetch");
    assert_eq!(fetched[0].headers.get("x-trace-id"), Some("trace-9"));
    assert_eq!(fetched[0].headers.get(NATS_MSG_ID), Some("h-1"));
    broker
        .ack("w-headers", sequence, AckInstruction::Ack)
        .await
        .expect("ack");
    cleanup_stream(&stream).await;
}

#[tokio::test]
async fn ping_reports_liveness() {
    let (broker, stream, _guard) = live_broker("ping").await;
    broker.ping().await.expect("ping healthy");
    // Provisioning is idempotent: connecting a second broker to the same
    // stream must succeed, not error on "stream already exists".
    let second = NatsBroker::connect(
        &NatsBrokerConfig::for_url(live_url().expect("url")).with_stream(stream.clone()),
    )
    .await
    .expect("default-stream connect is idempotent");
    second.ping().await.expect("second ping");
    cleanup_stream(&stream).await;
}
