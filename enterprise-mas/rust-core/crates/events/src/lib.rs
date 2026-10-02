//! Events crate: the platform's domain-event layer.
//!
//! # Pieces
//!
//! * [`EventEnvelope`] — the immutable record of *something that happened*:
//!   ids, type + schema version, aggregate, tenant, causation/correlation,
//!   and a payload carrying identifiers (never mutable body state).
//! * [`bus`] — an in-process pub/sub port ([`EventBusPort`]) with
//!   pattern-matched delivery; production adapters (queueing, bridge to the
//!   outbox dispatcher) implement the same port.
//! * [`outbox`] — the transactional outbox: records written in the same
//!   transaction as state changes, then dispatched with bounded retries and
//!   dead-lettering. `OutboxDispatcher` bridges outbox → any
//!   [`EventBusPort`].
//! * [`publisher`] — glue implementing the orchestration `EventPublisher`
//!   trait on top of the bus so engines emit once through the same pipeline.
//!
//! Delivery discipline: at-least-once, per-tenant ordering (per aggregate
//! sequence key), bounded retries, then dead-letter — consumers must be
//! idempotent.

pub mod bus;
pub mod envelope;
pub mod outbox;
pub mod publisher;

pub use bus::{
    ConsumerId, EventBusPort, EventDeliveryResult, InMemoryEventBus, SubscriptionFilter,
};
pub use envelope::{EventEnvelope, EventEnvelopeBuilder, EventMetadata, EventTypeError};
pub use outbox::{InMemoryOutbox, OutboxDispatcher, OutboxRecord, OutboxStorePort, RetryPolicy};
pub use publisher::BusEventPublisher;
