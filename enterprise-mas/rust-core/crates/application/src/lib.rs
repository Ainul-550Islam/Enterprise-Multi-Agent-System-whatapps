//! # mas-application
//!
//! The use-case layer. `api`, `worker` and `scheduler-service` are thin
//! processes around these services; nothing in here binds HTTP, gRPC,
//! databases or brokers — every dependency arrives as a port (see
//! [`stores`]) with in-memory reference implementations for composition
//! and testing.
//!
//! Uniform rules shared by every service:
//!
//! * **Scoping** — a [`context::ServiceContext`] carries the acting principal,
//!   tenant/organization scope and correlation id. Tenant-scoped reads for
//!   foreign ids fail as `NotFound` (existence is hidden), never `Forbidden`.
//! * **Domain-first validation** — all business invariants live in the
//!   `domain` aggregates; services sequence calls, enforce
//!   scope/uniqueness/idempotency, and persist via ports.
//! * **Audit, via ports** — every mutating use-case records an
//!   [`AuditEvent`](mas_domain::AuditEvent) through
//!   [`audit::AuditSinkPort`]; a failed audit record fails the use-case
//!   (compliance before convenience).
//! * **DTO mapping** — domain `respond` functions in [`dto`] produce the
//!   versioned `contracts` wire shapes; the API never serializes domain
//!   aggregates directly.

pub mod agent_service;
pub mod audit;
pub mod context;
pub mod dto;
pub mod execution_service;
pub mod schedule_service;
pub mod stores;
pub mod tenancy_service;
pub mod workflow_service;
