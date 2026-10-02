//! Adapter: orchestration's `EventPublisher` trait onto the event bus.
//!
//! The engine emits `(event_type, aggregate_id, tenant, payload)` — the
//! adapter wraps that into a full [`EventEnvelope`]: correlation rides from
//! the payload when present (default: the event id itself).

use crate::{bus::EventBusPort, envelope::EventEnvelope};

use mas_common::ids::TenantId;
use mas_common::result::Result;
use mas_orchestration::engine::EventPublisher;
use serde_json::Value;
use std::fmt;
use std::sync::Arc;

/// EventPublisher over any bus (in-memory in dev/tests, queue-backed in
/// production via the messaging crate).
#[derive(Debug)]
pub struct BusEventPublisher {
    bus: Arc<dyn EventBusPort>,
}

impl fmt::Display for BusEventPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BusEventPublisher").finish()
    }
}

impl BusEventPublisher {
    pub fn new(bus: Arc<dyn EventBusPort>) -> Self {
        Self { bus }
    }
}

#[async_trait::async_trait]
impl EventPublisher for BusEventPublisher {
    async fn publish(
        &self,
        event_type: &str,
        aggregate_id: &str,
        tenant_id: TenantId,
        payload: Value,
    ) -> Result<()> {
        // Aggregate kind sits inside engine event types' first segment
        // ("execution.started" → aggregate "execution").
        let aggregate_type = event_type.split('.').next().unwrap_or("unknown").to_owned();
        let correlation = payload
            .get("correlation_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut builder =
            EventEnvelope::builder(event_type, 1, aggregate_type, aggregate_id, tenant_id)?
                .with_payload(payload);
        // Set correlation: prefer payload-provided, else the event id itself
        // (uuid — a valid correlation beacon).
        let fallback_correlation = builder.event_id().to_string();
        builder =
            builder.with_correlation(correlation.as_deref().unwrap_or(&fallback_correlation))?;
        let envelope = builder.build()?;
        self.bus.publish(&envelope).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::InMemoryEventBus;
    use serde_json::json;

    #[tokio::test]
    async fn engine_calls_become_bus_events() {
        let bus = Arc::new(InMemoryEventBus::new());
        let publisher = BusEventPublisher::new(bus.clone());
        let tenant = TenantId::new();
        publisher
            .publish(
                "execution.started",
                "exec-1",
                tenant,
                json!({"correlation_id": "corr-42", "status": "running"}),
            )
            .await
            .expect("publish");
        publisher
            .publish("task.submitted", "task-1", tenant, json!({}))
            .await
            .expect("publish");
        let seen = bus.remembered();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].event_type, "execution.started");
        assert_eq!(seen[0].correlation_id, "corr-42");
        assert_eq!(seen[0].aggregate_type, "execution");
        // fallback correlation = the event id itself
        assert_eq!(seen[1].correlation_id, seen[1].id.to_string());
    }
}
