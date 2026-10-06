//! PURPOSE: The task-consumer plane: the consumer, the attempt coordinator,
//! the capacity gate, the failure classifier and the failure/recovery matrix.
//!
//! Owning layer: `mas-worker`.
//!
//! Production responsibility: own everything between the broker and the
//! canonical runtime — claiming, leasing, capacity, retry classification and
//! settlement — while implementing no execution state machine of its own.
//!
//! It must NOT:
//!   • contain a handler that fakes work (no `EchoHandler`, no in-memory
//!     agent bridge on the production path);
//!   • reach the agent runtime except through the canonical executor;
//!   • claim more work than its remaining capacity.

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

pub mod attempts;
pub mod backoff;
pub mod capacity;
pub mod classify;
pub mod consumer;
#[cfg(test)]
mod failure_matrix;
pub mod grace;
pub mod handler;
pub mod lifecycle;
pub mod run_slot;

pub use attempts::{AttemptCoordinator, ClaimOneOutcome};
pub use capacity::CapacityGate;
pub use classify::{classify, ClassifiedFailure, FailureClass};
pub use consumer::{ConsumerConfig, ConsumerOutcome, TaskConsumer};
pub use handler::{OperationDispatcher, TaskHandlerPort};
pub use lifecycle::{InMemoryTaskStore, TaskLifecyclePort};
pub use run_slot::{NoRunSlots, RunSlot, RunSlotPort};
