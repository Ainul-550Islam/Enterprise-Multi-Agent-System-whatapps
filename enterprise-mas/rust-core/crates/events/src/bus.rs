//! In-process pub/sub event bus with pattern-matched delivery.
//!
//! Production deployments swap this for a real transport (the messaging
//! crate) — the port stays identical, so engine/tooling code does not know
//! the difference. Delivery here is synchronous, at-least-once, and
//! best-effort-ordered per publish order.

use crate::envelope::EventEnvelope;
use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::string_enum;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

/// Unique consumer identity (subscription owner).
pub type ConsumerId = String;

string_enum! {
    /// Terminal class of a delivery attempt.
    EventDeliveryResult {
        Delivered => "delivered",
        /// Transient handler failure — caller may retry.
        TransientFailure => "transient_failure",
        /// Permanent handler failure — do not retry (dead-letter path).
        PermanentFailure => "permanent_failure",
    }
}

/// Event-type filter: exact match, `prefix.*` tail-star, or `*`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubscriptionFilter(String);

impl SubscriptionFilter {
    pub fn any() -> Self {
        Self("*".to_owned())
    }

    pub fn exact(event_type: &str) -> Self {
        Self(event_type.to_owned())
    }

    /// Matches `prefix` and anything under it (`tasks.*` ↔ `task.submitted`).
    pub fn with_prefix(prefix: &str) -> Self {
        Self(format!("{prefix}.*"))
    }

    /// Whether `event_type` matches this filter.
    #[must_use]
    pub fn matches(&self, event_type: &str) -> bool {
        if self.0 == "*" {
            return true;
        }
        if let Some(prefix) = self.0.strip_suffix(".*") {
            return event_type == prefix || event_type.starts_with(&format!("{prefix}."));
        }
        self.0 == event_type
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A consumer handler. Errors map to `TransientFailure`; panics are not
/// possible in the handler contract (handlers must never panic).
/// Ergonomic handler wrapper that remains `Debug` for registry diagnostics.
#[derive(Clone)]
pub struct EventHandler(pub Arc<dyn Fn(&EventEnvelope) -> EventDeliveryResult + Send + Sync>);

impl fmt::Debug for EventHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EventHandler(<fn>)")
    }
}

/// A live consumer record; cheap to clone (Arc handler).
#[derive(Debug, Clone)]
struct Consumer {
    id: ConsumerId,
    filter: SubscriptionFilter,
    handler: EventHandler,
}

/// Delivery stats per consumer (audit + health).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerStats {
    pub delivered: u64,
    pub transient_failures: u64,
    pub permanent_failures: u64,
}

/// The pub/sub port.
#[async_trait::async_trait]
pub trait EventBusPort: Send + Sync + fmt::Debug {
    /// Publishes one envelope: all matching consumers get it (synchronously;
    /// failures recorded, delivery still proceeds).
    async fn publish(&self, envelope: &EventEnvelope) -> Result<()>;

    async fn subscribe(
        &self,
        consumer: ConsumerId,
        filter: SubscriptionFilter,
        handler: EventHandler,
    ) -> Result<()>;

    async fn unsubscribe(&self, consumer: &ConsumerId) -> Result<()>;

    /// Snapshot of registered consumers.
    async fn consumers(&self) -> Vec<ConsumerId>;
}

/// In-memory bus.
#[derive(Debug, Default)]
pub struct InMemoryEventBus {
    consumers: Mutex<BTreeMap<ConsumerId, Consumer>>,
    stats: Mutex<BTreeMap<ConsumerId, ConsumerStats>>,
    published: Mutex<Vec<EventEnvelope>>, // bounded only by published_to limit
    published_to_limit: usize,
}

impl InMemoryEventBus {
    pub fn new() -> Self {
        Self {
            consumers: Mutex::new(BTreeMap::new()),
            stats: Mutex::new(BTreeMap::new()),
            published: Mutex::new(Vec::new()),
            published_to_limit: 10_000,
        }
    }

    /// Bounded ring of remembered envelopes (test introspection, replay).
    pub fn remembered(&self) -> Vec<EventEnvelope> {
        self.published
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn stats_for(&self, consumer: &str) -> ConsumerStats {
        self.stats
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(consumer)
            .copied()
            .unwrap_or_default()
    }
}

#[async_trait::async_trait]
impl EventBusPort for InMemoryEventBus {
    async fn publish(&self, envelope: &EventEnvelope) -> Result<()> {
        // Snapshot consumers first: a handler may subscribe/unsubscribe
        // while publishing (no lock held through handler calls).
        let targets: Vec<Consumer> = {
            let consumers = self.consumers.lock().unwrap_or_else(|e| e.into_inner());
            consumers
                .values()
                .filter(|consumer| consumer.filter.matches(&envelope.event_type))
                .cloned()
                .collect()
        };
        for consumer in targets {
            let result = (consumer.handler.0)(envelope);
            let mut stats = self.stats.lock().unwrap_or_else(|e| e.into_inner());
            let entry = stats.entry(consumer.id.clone()).or_default();
            match result {
                EventDeliveryResult::Delivered => entry.delivered += 1,
                EventDeliveryResult::TransientFailure => entry.transient_failures += 1,
                EventDeliveryResult::PermanentFailure => entry.permanent_failures += 1,
            }
            if result != EventDeliveryResult::Delivered {
                tracing::warn!(consumer = %consumer.id, event = %envelope.id, ?result, "event delivery failed");
            }
        }
        // Remember for replay/tests (bounded).
        let mut published = self.published.lock().unwrap_or_else(|e| e.into_inner());
        if published.len() >= self.published_to_limit {
            published.remove(0); // drop oldest (ring behaviour)
        }
        published.push(envelope.clone());
        Ok(())
    }

    async fn subscribe(
        &self,
        consumer: ConsumerId,
        filter: SubscriptionFilter,
        handler: EventHandler,
    ) -> Result<()> {
        mas_common::validation::validate_non_empty("consumer_id", &consumer)?;
        let mut consumers = self.consumers.lock().unwrap_or_else(|e| e.into_inner());
        consumers.insert(
            consumer.clone(),
            Consumer {
                id: consumer,
                filter,
                handler,
            },
        );
        Ok(())
    }

    async fn unsubscribe(&self, consumer: &ConsumerId) -> Result<()> {
        let mut consumers = self.consumers.lock().unwrap_or_else(|e| e.into_inner());
        match consumers.remove(consumer) {
            Some(_) => Ok(()),
            None => Err(AppError::not_found("event subscription", consumer.clone())),
        }
    }

    async fn consumers(&self) -> Vec<ConsumerId> {
        self.consumers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::ids::TenantId;

    fn envelope(event_type: &str) -> EventEnvelope {
        EventEnvelope::builder(event_type, 1, "execution", "exec-1", TenantId::new())
            .expect("builder")
            .with_correlation("corr")
            .expect("correlation")
            .build()
            .expect("envelope")
    }

    #[tokio::test]
    async fn filters_and_delivery_stats_work() {
        let bus = InMemoryEventBus::new();
        let hits = Arc::new(Mutex::new(Vec::<String>::new()));
        let hits2 = Arc::clone(&hits);
        bus.subscribe(
            "exec-listener".to_owned(),
            SubscriptionFilter::exact("execution.started"),
            EventHandler(Arc::new(move |env: &EventEnvelope| {
                hits2.lock().expect("lock").push(env.event_type.clone());
                EventDeliveryResult::Delivered
            })),
        )
        .await
        .expect("subscribe");
        bus.subscribe(
            "task-listener".to_owned(),
            SubscriptionFilter::with_prefix("task"),
            EventHandler(Arc::new(|_| EventDeliveryResult::TransientFailure)),
        )
        .await
        .expect("subscribe");
        bus.subscribe(
            "audit-any".to_owned(),
            SubscriptionFilter::any(),
            EventHandler(Arc::new(|_| EventDeliveryResult::Delivered)),
        )
        .await
        .expect("subscribe");

        bus.publish(&envelope("execution.started"))
            .await
            .expect("publish");
        bus.publish(&envelope("task.submitted"))
            .await
            .expect("publish");
        bus.publish(&envelope("tool.invoked"))
            .await
            .expect("publish");

        assert_eq!(
            hits.lock().expect("lock").as_slice(),
            ["execution.started".to_owned()]
        );
        let task_stats = bus.stats_for("task-listener");
        assert_eq!(
            task_stats.transient_failures, 1,
            "task-listener only sees task.*"
        );
        let any = bus.stats_for("audit-any");
        assert_eq!(any.delivered, 3);
        assert_eq!(bus.remembered().len(), 3);
    }

    #[tokio::test]
    async fn subscribe_unsubscribe_consumers() {
        let bus = InMemoryEventBus::new();
        bus.subscribe(
            "a".to_owned(),
            SubscriptionFilter::any(),
            EventHandler(Arc::new(|_| EventDeliveryResult::Delivered)),
        )
        .await
        .expect("subscribe");
        assert!(bus
            .subscribe(
                "".to_owned(),
                SubscriptionFilter::any(),
                EventHandler(Arc::new(|_| EventDeliveryResult::Delivered))
            )
            .await
            .is_err());
        bus.unsubscribe(&"a".to_owned()).await.expect("unsub");
        assert!(bus.unsubscribe(&"a".to_owned()).await.is_err());
        assert!(bus.consumers().await.is_empty());
    }

    #[test]
    fn filter_matching_is_precise() {
        assert!(SubscriptionFilter::any().matches("anything.here"));
        assert!(SubscriptionFilter::exact("execution.started").matches("execution.started"));
        assert!(!SubscriptionFilter::exact("execution.started").matches("task.x"));
        assert!(SubscriptionFilter::with_prefix("task").matches("task"));
        assert!(SubscriptionFilter::with_prefix("task").matches("task.submitted"));
        assert!(!SubscriptionFilter::with_prefix("task").matches("tasks.foo"));
        assert!(!SubscriptionFilter::with_prefix("task").matches("execution.started"));
    }
}
