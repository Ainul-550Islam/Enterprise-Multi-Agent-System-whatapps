//! # mas-worker
//!
//! The **task consumer** plane: everything between "a task message sits on
//! the task subject" and "the work ran, the row reflects it, the broker
//! settled the delivery".
//!
//! * [`consumer::TaskConsumer`] — the bounded-concurrency pull loop:
//!   `ensure_consumer` → `fetch` (bounded batch) → per-message mediated
//!   execution (decode → deadline check → lifecycle row → fenced lease →
//!   handler with heartbeats → settle). Runs until the shutdown watch flips,
//!   then drains in-flight work before returning (never abandons an
//!   in-progress lease silently).
//! * [`handler::TaskHandlerPort`] + [`handler::OperationDispatcher`] — the
//!   dispatch seam: operation string → handler. Process binaries register
//!   real runtimes here (e.g. `execution.run` → the `mas-execution`
//!   `Executor`); the consumer itself stays runtime-agnostic.
//! * [`lifecycle::TaskLifecyclePort`] — mirrors broker settlement onto
//!   domain `Task` rows (start/complete/fail/retry/cancel in the legal
//!   order). Update failures never block broker settlement but are counted
//!   and traced — the row heals on redelivery.
//! * [`backoff::RetryPolicy`] — deterministic exponential backoff with
//!   full jitter and saturating caps (attempts are managed by the broker's
//!   redelivery count, which is authoritative).
//! * [`grace::ShutdownWatch`] — SIGINT/SIGTERM-aware watch channel driving
//!   the drain semantics.
//!
//! ## Settlement protocol (the heart of this crate)
//!
//! outcome | broker | task row
//! ---|---|---
//! handler success | `Ack` | `complete`
//! `Cancelled` | `Ack` | `cancel`
//! transient (`Timeout` / `ResourceExhausted` / `Upstream` / `Platform`), attempts left | `Nak(delay)` | `fail` + `retry`
//! transient, attempts exhausted | `Term` | `fail`
//! poison (`NonRetryable` / `PolicyDenied` / `PayloadRejected`) | `Term` | `fail`
//! deadline passed before start | `Term` | `fail`
//! task row missing (orphan) | `Ack` | —
//! frame undecodable | `Term` | —
//! lease held by another worker | `Nak(short delay)` | untouched
//!
//! Heartbeats while a task runs: `LeaseStorePort::renew` +
//! `AckInstruction::InProgress` at half of `ack_wait`.

pub mod backoff;
pub mod consumer;
pub mod grace;
pub mod handler;
pub mod lifecycle;

pub use consumer::{ConsumerConfig, ConsumerOutcome, TaskConsumer};
pub use handler::{OperationDispatcher, TaskHandlerPort};
pub use lifecycle::{InMemoryTaskStore, TaskLifecyclePort};
