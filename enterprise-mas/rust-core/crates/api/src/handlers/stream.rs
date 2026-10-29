//! PURPOSE: Tenant-isolated Server-Sent Events fan-out with bounded per-client
//! queues, principal connection caps, and sanitized event projections.
//!
//! Owning layer: `mas-api` transport process.
//!
//! Production responsibility: subscribe once to the existing event bus, route
//! only events tagged with the authenticated tenant, shed slow consumers, and
//! close streams when the client or server task is dropped.
//!
//! Data ownership: the event bus owns source envelopes; this hub owns only
//! ephemeral bounded subscriber channels and connection accounting.
//!
//! MUST NOT: serialize event payloads, actor/correlation metadata, secret
//! references, notification bodies, or events from another tenant.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use futures_util::stream;
use mas_common::config::StreamConfig;
use mas_common::ids::TenantId;
use mas_events::bus::EventHandler;
use mas_events::{EventBusPort, EventDeliveryResult, EventEnvelope, SubscriptionFilter};
use tokio::sync::{broadcast, watch};

use crate::context::RequestContext;
use crate::handlers::Ctx;
use crate::response::ApiError;
use crate::state::AppState;

const BUS_CONSUMER: &str = "api-sse-hub";

/// Tenant-scoped in-process stream router.
#[derive(Debug)]
pub struct StreamHub {
    inner: Arc<HubInner>,
    config: StreamConfig,
    bus: Mutex<Option<Arc<dyn EventBusPort>>>,
}

#[derive(Debug)]
struct TenantChannel {
    sender: broadcast::Sender<EventEnvelope>,
}

impl TenantChannel {
    fn new(capacity: usize) -> Self {
        let (sender, _receiver) = broadcast::channel(capacity);
        Self { sender }
    }
}

#[derive(Debug)]
struct HubInner {
    // A channel exists only while at least one tenant stream is active; this
    // map therefore cannot grow with the lifetime tenant population.
    channels: Mutex<HashMap<TenantId, TenantChannel>>,
    connections: Mutex<HashMap<String, usize>>,
    tenant_connections: Mutex<HashMap<TenantId, usize>>,
    shutdown: watch::Sender<bool>,
}

impl StreamHub {
    /// Constructs a hub after validating runtime-provided bounds.
    pub fn new(config: StreamConfig) -> mas_common::result::Result<Self> {
        config.validate()?;
        let (shutdown, _receiver) = watch::channel(false);
        Ok(Self {
            inner: Arc::new(HubInner {
                channels: Mutex::new(HashMap::new()),
                connections: Mutex::new(HashMap::new()),
                tenant_connections: Mutex::new(HashMap::new()),
                shutdown,
            }),
            config,
            bus: Mutex::new(None),
        })
    }

    /// Installs exactly one event-bus subscriber. EventBusPort has no replay
    /// contract; `Last-Event-ID` resume is therefore deliberately unsupported
    /// by this adapter rather than pretending to replay retained envelopes.
    pub async fn attach(&self, bus: Arc<dyn EventBusPort>) -> mas_common::result::Result<()> {
        let inner = Arc::clone(&self.inner);
        bus.subscribe(
            BUS_CONSUMER.to_owned(),
            SubscriptionFilter::any(),
            EventHandler(Arc::new(move |event| {
                let channels = inner
                    .channels
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(channel) = channels.get(&event.tenant_id) {
                    let _ = channel.sender.send(event.clone());
                }
                EventDeliveryResult::Delivered
            })),
        )
        .await?;
        *self
            .bus
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(bus);
        Ok(())
    }

    /// Emits an SSE response for one resolved tenant and authenticated actor.
    pub fn response(
        &self,
        ctx: &RequestContext,
        tenant: TenantId,
    ) -> mas_common::result::Result<impl IntoResponse> {
        let principal = ctx
            .principal
            .as_ref()
            .ok_or_else(|| mas_common::error::AppError::unauthorized("unauthenticated"))?;
        let principal_key = format!("{}:{}", tenant, principal.actor_id);
        let lease = ConnectionLease::acquire(
            Arc::clone(&self.inner),
            principal_key,
            tenant,
            self.config.max_connections_per_principal,
        )?;
        let receiver = {
            let mut channels = self
                .inner
                .channels
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            channels
                .entry(tenant)
                .or_insert_with(|| TenantChannel::new(self.config.channel_buffer))
                .sender
                .subscribe()
        };
        let shutdown = self.inner.shutdown.subscribe();
        let events = stream::unfold(
            (receiver, shutdown, lease),
            |(mut receiver, mut shutdown, lease)| async move {
                loop {
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() { return None; }
                        },
                        received = receiver.recv() => match received {
                            Ok(envelope) => {
                                if let Some(frame) = safe_frame(&envelope) {
                                    return Some((Ok::<Event, Infallible>(frame), (receiver, shutdown, lease)));
                                }
                            },
                            // A lagging client loses its connection rather than
                            // silently skipping an unbounded backlog.
                            Err(broadcast::error::RecvError::Lagged(_)) | Err(broadcast::error::RecvError::Closed) => return None,
                        },
                    }
                }
            },
        );
        let sse = Sse::new(events)
            .keep_alive(KeepAlive::new().interval(Duration::from_secs(self.config.heartbeat_secs)));
        Ok(sse)
    }

    #[cfg(test)]
    fn active_for(&self, principal: &str) -> usize {
        self.inner
            .connections
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(principal)
            .copied()
            .unwrap_or_default()
    }
}

impl Drop for StreamHub {
    fn drop(&mut self) {
        self.inner.shutdown.send_replace(true);
    }
}

struct ConnectionLease {
    inner: Arc<HubInner>,
    principal: String,
    tenant: TenantId,
}

impl std::fmt::Debug for ConnectionLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectionLease")
            .field("principal", &"<redacted>")
            .finish()
    }
}

impl ConnectionLease {
    fn acquire(
        inner: Arc<HubInner>,
        principal: String,
        tenant: TenantId,
        cap: usize,
    ) -> mas_common::result::Result<Self> {
        let mut connections = inner
            .connections
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let active = connections.entry(principal.clone()).or_default();
        if *active >= cap {
            return Err(mas_common::error::AppError::conflict(
                "maximum concurrent event streams reached for this principal",
            ));
        }
        *active += 1;
        *inner
            .tenant_connections
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(tenant)
            .or_default() += 1;
        drop(connections);
        Ok(Self {
            inner,
            principal,
            tenant,
        })
    }
}

impl Drop for ConnectionLease {
    fn drop(&mut self) {
        let mut connections = self
            .inner
            .connections
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(active) = connections.get_mut(&self.principal) {
            *active = active.saturating_sub(1);
            if *active == 0 {
                connections.remove(&self.principal);
            }
        }
        drop(connections);
        let mut tenants = self
            .inner
            .tenant_connections
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(active) = tenants.get_mut(&self.tenant) {
            *active = active.saturating_sub(1);
            if *active == 0 {
                tenants.remove(&self.tenant);
                self.inner
                    .channels
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&self.tenant);
            }
        }
    }
}

fn safe_frame(envelope: &EventEnvelope) -> Option<Event> {
    let resource_id = match (
        envelope.event_type.as_str(),
        envelope.aggregate_type.as_str(),
    ) {
        (
            "execution.started"
            | "execution.completed"
            | "execution.failed"
            | "execution.paused"
            | "execution.resumed",
            "execution",
        ) => Some(envelope.aggregate_id.as_str()),
        ("agent.created" | "agent.updated" | "agent.published" | "agent.deployed", "agent") => {
            Some(envelope.aggregate_id.as_str())
        },
        ("notification.created", "notification") => None,
        _ => return None,
    };
    let kind = envelope.event_type.as_str();
    let data = serde_json::json!({
        "event_id": envelope.id,
        "type": kind,
        "occurred_at": envelope.occurred_at,
        "resource_id": resource_id,
    });
    Event::default()
        .id(envelope.id.to_string())
        .event(kind)
        .json_data(data)
        .ok()
}

/// `GET /v1/stream` — connects only after the normal API authentication and
/// resolved-scope middleware has completed.
pub async fn connect(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    headers: axum::http::HeaderMap,
) -> std::result::Result<impl IntoResponse, ApiError> {
    let service_context = crate::handlers::authorized_service_context(
        &ctx,
        mas_security::ResourceKind::Tenant,
        mas_security::Action::Read,
        None,
    )
    .map_err(|error| ApiError::new(error, &ctx))?;
    let tenant = service_context
        .require_tenant()
        .map_err(|error| ApiError::new(error, &ctx))?;
    // The bus interface has no replay cursor. Explicitly consume, but do not
    // misrepresent, a client cursor as replay support.
    let _last_event_id = headers.get("last-event-id");
    let hub = state.stream_hub_service(&ctx)?;
    hub.response(&ctx, tenant)
        .map_err(|error| ApiError::new(error, &ctx))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::ids::{EventId, OrganizationId};
    use mas_common::timestamps::Timestamp;
    use mas_events::EventMetadata;

    fn envelope(tenant: TenantId, event_type: &str) -> EventEnvelope {
        EventEnvelope {
            id: EventId::new(),
            event_type: event_type.to_owned(),
            event_version: 1,
            aggregate_type: "execution".to_owned(),
            aggregate_id: "execution-id".to_owned(),
            tenant_id: tenant,
            organization_id: Some(OrganizationId::new()),
            occurred_at: Timestamp::now(),
            correlation_id: "sensitive-correlation".to_owned(),
            causation_id: Some("sensitive-causation".to_owned()),
            actor: Some("user@example.invalid".to_owned()),
            metadata: EventMetadata::default(),
            payload: serde_json::json!({"secret_reference":"never-send", "message":"private"}),
        }
    }

    #[test]
    fn frames_allow_only_known_event_types_and_strip_payload_and_identity_metadata() {
        let event = envelope(TenantId::new(), "execution.completed");
        let frame = safe_frame(&event).expect("safe event");
        let rendered = format!("{frame:?}");
        assert!(!rendered.contains("never-send"));
        assert!(!rendered.contains("user@example.invalid"));
        assert!(!rendered.contains("sensitive-correlation"));
        assert!(safe_frame(&envelope(TenantId::new(), "execution.output")).is_none());
    }

    #[tokio::test]
    async fn bus_events_are_confined_to_their_tenant_channel() {
        let hub = StreamHub::new(StreamConfig::default()).expect("hub");
        let bus = Arc::new(mas_events::InMemoryEventBus::new());
        hub.attach(bus.clone()).await.expect("attach");
        let tenant_a = TenantId::new();
        let tenant_b = TenantId::new();
        let mut rx_a = {
            let mut channels = hub.inner.channels.lock().expect("channel mutex");
            channels
                .entry(tenant_a)
                .or_insert_with(|| TenantChannel::new(8))
                .sender
                .subscribe()
        };
        bus.publish(&envelope(tenant_a, "execution.completed"))
            .await
            .expect("publish tenant A");
        bus.publish(&envelope(tenant_b, "execution.completed"))
            .await
            .expect("publish tenant B");
        assert_eq!(
            rx_a.recv().await.expect("tenant A event").tenant_id,
            tenant_a
        );
        assert!(matches!(
            rx_a.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn lagged_slow_client_is_detected_by_the_bounded_channel() {
        let channel = TenantChannel::new(1);
        let mut receiver = channel.sender.subscribe();
        let tenant = TenantId::new();
        channel
            .sender
            .send(envelope(tenant, "execution.started"))
            .expect("first");
        channel
            .sender
            .send(envelope(tenant, "execution.completed"))
            .expect("second");
        assert!(matches!(
            receiver.recv().await,
            Err(broadcast::error::RecvError::Lagged(_))
        ));
    }

    #[test]
    fn per_principal_connection_cap_is_enforced_and_released() {
        let config = StreamConfig {
            max_connections_per_principal: 1,
            ..StreamConfig::default()
        };
        let hub = StreamHub::new(config).expect("hub");
        let inner = Arc::clone(&hub.inner);
        let tenant = TenantId::new();
        let lease = ConnectionLease::acquire(inner.clone(), "tenant:actor".to_owned(), tenant, 1)
            .expect("first");
        assert_eq!(hub.active_for("tenant:actor"), 1);
        assert!(ConnectionLease::acquire(inner, "tenant:actor".to_owned(), tenant, 1).is_err());
        drop(lease);
        assert_eq!(hub.active_for("tenant:actor"), 0);
    }
}
