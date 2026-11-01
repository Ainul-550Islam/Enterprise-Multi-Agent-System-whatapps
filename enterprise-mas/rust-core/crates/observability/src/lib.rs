//! # mas-observability
//!
//! The telemetry plane of rust-core. Every piece is transport-agnostic and
//! deterministic: actual log drains, OTLP collectors and probe schedulers
//! bind behind ports owned by the `application`/process crates.
//!
//! * [`logging`] — structured JSON log events with a shared
//!   [`mas_common::redaction::SecretRedactor`] applied to every field,
//!   level filtering, static enrichment fields, and AppError-safe error
//!   enrichment (public message + stable code only).
//! * [`metrics`] — a cardinality-guarded metrics registry (counter / gauge
//!   / histogram) rendering a Prometheus-compatible text exposition.
//! * [`tracing_otel`] — W3C `traceparent`-compatible trace/span ids, span
//!   records with bounded attributes, a deterministic tail sampler, and a
//!   batching [`tracing_otel::SpanProcessor`] over
//!   [`tracing_otel::SpanExporterPort`] (OTLP-shaped; real exporters bind
//!   to the port).
//! * [`health`] — liveness/readiness component registry aggregating probe
//!   outcomes into a platform status with sanitized details.
//! * [`audit_telemetry`] — bridges append-only `domain::AuditEvent`s into
//!   the log/metrics planes so compliance output rides the same telemetry
//!   without ever leaking secret material.

pub mod audit_telemetry;
pub mod health;
pub mod logging;
pub mod metrics;
pub mod tracing_otel;
