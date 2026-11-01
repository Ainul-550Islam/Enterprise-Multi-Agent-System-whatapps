//! # mas-scheduling — the scheduler service core
//!
//! Turns [`mas_domain::schedule::Schedule`] definitions into dispatched runs,
//! safely and fairly, on many replicas:
//!
//! * [`due`] — the due scanner: takes candidate schedules and produces
//!   ordered [`due::DueWork`] items with per-tenant fair interleave so one
//!   noisy tenant cannot starve the batch.
//! * [`policies`] — the two judgment calls a scheduler makes on every tick:
//!   **catch-up** (what to do about runs missed while paused/down) and
//!   **overlap** (what to do when a run is due while the previous one is
//!   still executing). Both are pure, total functions.
//! * [`lease`] — fencing-token lease store ([`lease::LeaseStorePort`]) so
//!   concurrent scheduler replicas never double-fire a schedule; plus the
//!   in-memory reference store.
//! * [`queue`] — the delayed queue: `(planned_at, sequence)`-ordered,
//!   deduplicated per `(schedule, planned tick)`, capacity-bounded,
//!   round-robin fair across tenants on pop.
//! * [`store`] — the [`store::ScheduleStorePort`] the runtime reads/writes
//!   schedules through (Postgres-backed adapters live in the application
//!   composition root; the in-memory store backs tests and local dev).
//! * [`runner`] — the deterministic [`runner::SchedulerRuntime::tick`] loop
//!   plus startup [`runner::SchedulerRuntime::recover`]. Time is injected,
//!   so the whole lifecycle runs in unit tests without sleeping.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod due;
pub mod lease;
pub mod policies;
pub mod queue;
pub mod runner;
pub mod store;
