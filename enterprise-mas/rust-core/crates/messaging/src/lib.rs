//! # mas-messaging — message transport layer
//!
//! This crate owns everything between the domain event plane ([`mas_events`])
//! and the wire:
//!
//! * [`codec`] — framed payload encoding with hard size caps. Malformed or
//!   oversize frames are rejected as **non-retriable**; consumers must NAK
//!   (never blindly retry) malformed payloads.
//! * [`headers`] — validated header sets plus correlation/causation/tenant
//!   propagation between [`mas_events::EventEnvelope`]s and transport frames
//!   and gRPC metadata.
//! * [`subjects`] — NATS-shaped subject names with safe construction and
//!   subscription wildcard (`*` / `>`) matching.
//! * [`connection`] — broker connection lifecycle state machine with bounded
//!   exponential-backoff reconnection.
//! * [`broker`] — the JetStream-shaped [`broker::BrokerPort`] (publish, pull,
//!   ack/nak/term, dead-letter) and a complete in-memory reference broker used
//!   by tests and local development.
//! * [`grpc`] — dependency-free gRPC helpers: canonical status codes and their
//!   [`mas_common::AppError`] mapping, metadata validation, `grpc-timeout`
//!   header encoding/decoding, and W3C traceparent handling.
//! * [`health`] — broker health probes producing
//!   [`mas_contracts::health::DependencyHealth`] without leaking connection
//!   strings or credentials.
//!
//! Real NATS/gRPC adapters (behind features in the runtime services) implement
//! the ports defined here; nothing in this crate opens a socket.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod broker;
pub mod codec;
pub mod connection;
pub mod grpc;
pub mod headers;
pub mod health;
#[cfg(feature = "nats")]
pub mod nats;
pub mod subjects;
