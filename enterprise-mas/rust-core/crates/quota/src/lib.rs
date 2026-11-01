//! # mas-quota — the quota enforcement plane
//!
//! Runtime counterparts of [`mas_domain::quota::Quota`] definitions (limits,
//! periods, enforcement modes). Everything quota-adjacent that must be atomic
//! and fast lives here:
//!
//! * [`counter`] — the [`counter::CounterStorePort`] abstraction (the shape a
//!   Redis `INCR` implementation drops into) plus the in-memory reference
//!   store with windowed-bucket garbage collection.
//! * [`rate_limiter`] — fixed-window request/token rate limiting with
//!   `retry_after` computation.
//! * [`concurrency`] — concurrency-slot leases ([`concurrency::Permit`]) with
//!   optimistic check-and-incr semantics and double-release protection.
//! * [`meter`] — token-bucket metering (smooth burst control for LLM tokens
//!   and API calls).
//! * [`engine`] — the [`engine::QuotaEngine']s decision pipeline: registry of
//!   [`mas_domain::quota::Quota`] definitions → counter windows →
//!   enforcement modes (`Enforce` rejects, `WarnOnly` allows+flags,
//!   `AllowWithOverage` allows+marks overage), and the bridge implementing
//!   `mas_orchestration::engine::QuotaPort` so the engine consumes quota
//!   without knowing any of this.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod concurrency;
pub mod counter;
pub mod engine;
pub mod meter;
pub mod rate_limiter;
