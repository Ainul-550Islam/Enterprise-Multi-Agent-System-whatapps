// PURPOSE:
// The library half of the worker host crate: the production adapters that the
// binary's composition root installs into `mas-worker`.
//
// Owning layer: worker service (composition root), bridging `mas-worker` and
// the Postgres-backed control plane.
//
// Production role. `mas-worker` declares the ports it needs — it says *when*
// a concurrency slot must be taken, renewed and returned — and this crate
// supplies the only implementation, bound at boot to the runtime pool and the
// control plane. Splitting the crate in two is what makes that seam visible:
// the binary decides configuration, the library decides wiring, and neither
// can quietly grow a second path to the reservation table.
//
// Data ownership. This crate owns no tables. Every row it touches belongs to
// `mas-persistence`, reached through the control plane, inside the tenant's
// own row-level-security scope.
//
// It must NOT:
//   • contain business logic. Anything that decides an outcome belongs in
//     `mas-worker` (when) or `mas-quota` through the control plane (whether);
//   • be depended on by `mas-worker`. The edge runs this way on purpose: the
//     durable consumer must remain compilable and testable with no database
//     in sight, and reversing the edge would let it grow one.

//! The production adapters the `mas-worker` binary installs at boot.
//!
//! See the crate source header for the boundary this crate enforces. The only
//! module today is [`run_slots`], the concurrency gate; any future adapter that
//! needs both the worker's port and the runtime pool belongs here too.

pub mod agent_runtime_bridge;
pub mod run_slots;

pub use agent_runtime_bridge::{PostgresAgentRuntimeSnapshotResolver, ResolvedAgentBridge};
