//! # mas-persistence — PostgreSQL persistence layer
//!
//! Owns every byte that crosses the SQL boundary:
//!
//! * [`pool`] — `sqlx` pool construction, validated configuration, redacted
//!   database URLs (connection strings are credentials and never logged),
//!   pool statistics, healthy/warm probes.
//! * [`transaction`] — the [`transaction::UnitOfWork`] wrapper: one
//!   transaction per unit of work, named savepoints, explicit commit/rollback
//!   (no implicit drops that silently commit).
//! * [`migrations`] — the [`migrations::MigrationRunner`] plus the embedded
//!   migration registry (`migrations/NNNN_name/up.sql`, 12 migrations). The
//!   runner verifies applied versions and checksums, refuses incompatible
//!   states (DB-ahead, gaps, checksum drift), and applies pending migrations
//!   transactionally in strict order. RLS policies for every tenant-owned
//!   table live in the SQL migration set, **not** in app code.
//! * [`rls`] — the [`rls::RlsContext`] applied per request/transaction
//!   (`app.current_tenant`, `app.current_org`, `app.current_user` GUCs). The
//!   context fails closed: without a scope, tenant-owned tables are invisible.
//! * [`rows`] — row models for the relational tables and the hydrate/dehydrate
//!   bridge between SQL columns and serde-shaped domain aggregates.
//! * [`repositories`] — concrete PostgreSQL repositories implementing the
//!   ports defined by upstream crates (starting with the transactional
//!   outbox and the append-only audit log).
//!
//! Connection strings never enter domain objects; raw secrets never persist
//! (only [`mas_domain::secret_reference::SecretReference`] payloads are
//! stored, e.g. in `credentials.secret_ref`).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod error;
pub mod migrations;
pub mod pool;
pub mod repositories;
pub mod rls;
pub mod rows;
pub mod transaction;
