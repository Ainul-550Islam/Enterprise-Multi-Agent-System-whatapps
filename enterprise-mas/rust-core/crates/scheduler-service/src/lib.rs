//! `mas-scheduler-service` — the production wall-clock around the
//! deterministic scheduler runtime ([`mas_scheduling`]).
//!
//! `mas-scheduling` is time-agnostic: `recover(now)` and `tick(now)` take an
//! explicit timestamp so every branch is unit-testable. This crate is the
//! *only* place a wall clock enters: [`driver::TickDriver`] owns a
//! `tokio::time::interval`, calls `tick(Timestamp::now())` each iteration,
//! folds every [`mas_scheduling::runner::TickReport`] into rolling
//! [`driver::DriverStats`], and drains cleanly on shutdown (mid-tick ticks
//! always complete first — a half-dispatched run would otherwise wedge the
//! delayed queue).
//!
//! ## Dispatch adapters ([`dispatch`])
//!
//! The runtime speaks [`mas_scheduling::runner::DispatchPort`]; this crate
//! provides:
//!
//! * [`dispatch::HttpDispatcher`] — POSTs each due run to the API service's
//!   internal schedule-run endpoint. The in-flight overlap ledger is owned
//!   locally: `2xx` with an execution id inserts the schedule,
//!   `409`/`429` → `Deferred` (capacity; requeued next tick), other 4xx/5xx
//!   → `Err` (requeued and counted). Completion is signalled *back* through
//!   the completion listener served by [`server::completion_router`], which
//!   removes the schedule from the ledger — this is what makes
//!   `is_running` honest across replicas sharing a downstream API.
//! * [`dispatch::LoopbackDispatcher`] — dev-only adapter that pretends a
//!   run starts and finishes instantly (ledger is never left holding).
//!
//! ## Process lifecycle ([`driver`])
//!
//! ```text
//! recover(now)      reap dead leases, re-queue anything already due
//! loop {            on interval tick (first tick fires immediately):
//!   tick(now)         scan → lease → overlap/catch-up → enqueue → dispatch
//!   fold report       into DriverStats (error ⇒ consecutive-errors backoff)
//!   watch.changed()   finish the in-progress tick, then exit with stats
//! }
//! ```
//!
//! The binary (`mas-scheduler`) composes this over in-memory ports for
//! `--dev-inmemory` and refuses to start otherwise until the Postgres
//! schedule/lease adapters land in `mas-persistence`.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod dispatch;
pub mod driver;
pub mod durable;
pub mod server;

pub use dispatch::{HttpDispatcher, InFlightLedger, LoopbackDispatcher};
pub use driver::{DriverConfig, DriverStats, TickDriver};
pub use server::{completion_router, health_router};

/// Squashes a raw sqlx error into the platform taxonomy (DB errors at this
/// boundary never leak connection strings or SQL text upward).
pub(crate) fn squash_db(error: sqlx::Error) -> mas_common::error::AppError {
    use mas_common::error::AppError;
    match &error {
        sqlx::Error::PoolTimedOut => AppError::database("timed out waiting for a free connection"),
        sqlx::Error::RowNotFound => AppError::not_found("row", "missing"),
        sqlx::Error::Database(db) if db.is_unique_violation() => {
            AppError::conflict("unique constraint violated")
        },
        sqlx::Error::Database(db) if db.is_foreign_key_violation() => {
            AppError::validation("foreign key constraint violated")
        },
        _ => AppError::database("database error in scheduler dispatcher"),
    }
}
