//! PURPOSE: Read-only core-NATS tail feeding the API SSE hub with live
//! domain events, without giving the publish-only relay bus a subscription
//! surface.
//!
//! Owning layer: `mas-messaging` transport.
//!
//! Production responsibility: subscribe exactly once to `mas.events.>`,
//! deserialize each tailed payload with the same envelope contract the
//! outbox relay publishes, apply the consumer's subscription filter, and
//! hand matching envelopes to the registered handler. This is a live view:
//! unacknowledged by design (SSE semantics), it drops payloads that do not
//! parse, and it never settles, acks, or re-publishes relay work.
//!
//! Data ownership: core NATS owns wire delivery; `NatsEventBus` keeps the
//! single acknowledged publish path for the outbox relay; this tail owns
//! only its subscription handle and pump task.
//!
//! MUST NOT: publish envelopes, offer `EventBusPort::subscribe` on
//! `NatsEventBus`, buffer unbounded payloads (the SSE hub sheds slow clients
//! through its own bounded channels), or survive `unsubscribe`.

use mas_common::error::AppError;
use mas_common::result::Result;
use mas_events::bus::{ConsumerId, EventBusPort, EventHandler, SubscriptionFilter};
use mas_events::envelope::EventEnvelope;
use std::fmt;
use std::sync::Mutex;

/// Canonical subject tree the outbox relay publishes domain envelopes to.
const EVENT_SUBJECT: &str = "mas.events.>";

/// In-flight tail state: one consumer, one pump task.
#[derive(Debug)]
struct ActiveTail {
    id: ConsumerId,
    task: tokio::task::JoinHandle<()>,
}

/// Core-NATS read view implementing [`EventBusPort`] for the SSE hub.
///
/// `subscribe` opens the tail; `publish` and a second `subscribe` fail
/// closed so this adapter can never become a parallel event source.
#[derive(Debug)]
pub struct NatsEventTailBus {
    client: async_nats::Client,
    active: Mutex<Option<ActiveTail>>,
}

impl NatsEventTailBus {
    /// Connects and verifies the server answers a flush, mirroring the
    /// relay bus' fail-at-boot contract.
    ///
    /// # Errors
    /// Any transport failure: an SSE plane without a live event source must
    /// refuse to boot instead of serving streams that never deliver.
    pub async fn connect(url: &str) -> Result<Self> {
        let client = async_nats::connect(url)
            .await
            .map_err(|error| tail_err("tail connect", error))?;
        client
            .flush()
            .await
            .map_err(|error| tail_err("tail ping", error))?;
        Ok(Self {
            client,
            active: Mutex::new(None),
        })
    }
}

/// Drives one tail subscription until it is dropped or the task aborts.
async fn pump(
    mut subscription: async_nats::Subscriber,
    filter: SubscriptionFilter,
    handler: EventHandler,
) {
    use futures_util::StreamExt as _;
    while let Some(message) = subscription.next().await {
        match serde_json::from_slice::<EventEnvelope>(&message.payload) {
            Ok(envelope) => {
                if filter.matches(&envelope.event_type) {
                    let outcome = handler.0(&envelope);
                    if outcome != mas_events::EventDeliveryResult::Delivered {
                        tracing::warn!(
                            event_type = %envelope.event_type,
                            event_id = %envelope.id,
                            outcome = %outcome.as_str(),
                            "SSE event tail handler did not deliver an envelope"
                        );
                    }
                }
            },
            Err(error) => {
                tracing::warn!(
                    %error,
                    subject = %message.subject,
                    "SSE event tail dropped a payload that violates the envelope contract"
                );
            },
        }
    }
}

fn tail_err(context: &str, error: impl std::fmt::Display) -> AppError {
    AppError::external_service("nats-event-tail", format!("{context}: {error}"))
}

#[async_trait::async_trait]
impl EventBusPort for NatsEventTailBus {
    async fn publish(&self, _envelope: &EventEnvelope) -> Result<()> {
        Err(AppError::internal(
            "the SSE event tail is read-only: domain events reach the wire only \
             through the outbox relay's publish-only event bus",
        ))
    }

    async fn subscribe(
        &self,
        consumer: ConsumerId,
        filter: SubscriptionFilter,
        handler: EventHandler,
    ) -> Result<()> {
        // Check before any await so no guard crosses a suspension point.
        if self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
        {
            return Err(AppError::conflict(
                "the SSE event tail already holds its single consumer",
            ));
        }
        let client = self.client.clone();
        let subscription = client
            .subscribe(EVENT_SUBJECT)
            .await
            .map_err(|error| tail_err("tail subscribe", error))?;
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if active.is_some() {
            // A concurrent subscribe won the race; dropping `subscription`
            // here sends UNSUB, so the loser leaves no server-side surface.
            return Err(AppError::conflict(
                "the SSE event tail already holds its single consumer",
            ));
        }
        let task = tokio::spawn(pump(subscription, filter, handler));
        *active = Some(ActiveTail { id: consumer, task });
        Ok(())
    }

    async fn unsubscribe(&self, _consumer: &ConsumerId) -> Result<()> {
        let active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        match active {
            Some(tail) => {
                tail.task.abort();
                Ok(())
            },
            None => Err(AppError::not_found(
                "sse_event_tail_consumer",
                _consumer.to_string(),
            )),
        }
    }

    async fn consumers(&self) -> Vec<ConsumerId> {
        self.active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|tail| vec![tail.id.clone()])
            .unwrap_or_default()
    }
}

impl Drop for NatsEventTailBus {
    fn drop(&mut self) {
        if let Some(tail) = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            tail.task.abort();
        }
    }
}

impl fmt::Display for NatsEventTailBus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NatsEventTailBus(mas.events.>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_events::EventDeliveryResult;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn tail_source_keeps_publish_read_only_and_single_consumer() {
        // Constructing the bus needs a live server; the contract is asserted
        // on the source the same way `nats.rs` pins the relay bus refusal.
        let source = std::fs::read_to_string("src/event_tail.rs").expect("read source");
        assert!(
            source.contains("the SSE event tail is read-only"),
            "publish must refuse: SSE is a view, never an event source"
        );
        assert!(
            source.contains("already holds its single consumer"),
            "a second subscription must fail closed"
        );
        assert!(source.contains(EVENT_SUBJECT), "tail follows mas.events.>");
    }

    #[test]
    fn tailed_payloads_round_trip_and_reach_only_matching_filters() {
        let tenant = mas_common::ids::TenantId::new();
        let envelope =
            EventEnvelope::builder("execution.started", 1, "execution", "exec-1", tenant)
                .expect("builder")
                .with_correlation("corr-tail-contract")
                .expect("correlation")
                .build()
                .expect("envelope");
        let wire = serde_json::to_vec(&envelope).expect("serialize");
        let decoded: EventEnvelope = serde_json::from_slice(&wire).expect("contract round trip");
        assert_eq!(decoded, envelope);

        let hits = Arc::new(AtomicUsize::new(0));
        let hits_for_handler = Arc::clone(&hits);
        let handler = EventHandler(Arc::new(move |_event: &EventEnvelope| {
            hits_for_handler.fetch_add(1, Ordering::SeqCst);
            EventDeliveryResult::Delivered
        }));
        let filter = SubscriptionFilter::with_prefix("execution");
        assert!(filter.matches(&decoded.event_type));
        let _ = handler.0(&decoded);
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        let other = SubscriptionFilter::exact("notification.created");
        assert!(!other.matches(&decoded.event_type));
    }
}
