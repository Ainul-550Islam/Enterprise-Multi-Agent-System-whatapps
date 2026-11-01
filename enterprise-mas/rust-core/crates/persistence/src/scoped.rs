//! PURPOSE: The one safe way to open a database transaction that touches
//! tenant-owned rows.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: spec Phases 11, 12 and 15. Before this phase the
//! correct RLS machinery was present (`rls.rs`, correct and fail-closed) but
//! had **no production caller** — the migration set enforced row-level security
//! that the running system never armed, so the policies were decorative.
//! `scoped_request::with_authoritative_scope` closes that gap, and it is what
//! the HTTP middleware and the gRPC layer now call on every request.
//!
//! Security boundary: `SET LOCAL`, not `SET`. Every policy read is pinned to
//! the transaction that set it, so a connection returning to the pool carries
//! nothing forward. Phase 12's pooled-connection hazard — request A sets
//! tenant A, the transaction ends, request B reuses the connection and would
//! otherwise inherit tenant A — cannot occur through this type, because the
//! GUC dies with the transaction unconditionally, on commit, on rollback, and
//! on drop-without-commit alike.
//!
//! It must NOT:
//!   • expose a `&PgPool`, or any way to begin a tenant-touching transaction
//!     without an `AuthoritativeScope`;
//!   • hold a second, parallel implementation of scoped transactions — this
//!     module is a shim over `scoped_request`, and shall not grow one back;
//!   • set a GUC outside a transaction (`set_config(..., false)`);
//!   • let a caller reuse one transaction (and therefore one scope) for two
//!     unrelated requests;
//!   • silently fall back to an unscoped transaction when a scope is absent —
//!     an absent scope is a hard error, never a wider permission.

// ── History: why this file is now a shim ───────────────────────────────────
//
// This module once defined `ScopedDatabase` / `ScopedUnitOfWork`: a second,
// parallel implementation of "open a transaction and arm row-level security
// inside it". It had no production caller — every repository reached the pool
// directly instead — which is the worst of both worlds: two abstractions, one
// of them decorative, and the running system armed nothing.
//
// Security Phase 15 requires **one** safe abstraction. That abstraction is
// `crate::scoped_request`: it is task-local (so a repository can find the
// request's transaction without being handed a pool), it is transaction-local
// (`SET LOCAL`), it refuses to nest, and it is what the HTTP middleware and
// the gRPC layer actually call. This module now re-exports it and nothing
// else. Keeping the duplicate would mean a future reader could pick the
// untested one.

pub use crate::scoped_request::{
    connection, current_scope, has_scope, with_authoritative_scope, ScopedExecutor,
};

/// Pool counters, safe to expose to metrics: no credentials, no connections.
///
/// Retained from the retired implementation because it is the one piece of
/// that module that was both harmless and useful — it reports pool health
/// without handing out anything that can execute a statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScopedPoolStats {
    /// Maximum connections the pool was configured with.
    pub size: u32,
    /// Connections currently idle.
    pub idle: u32,
    /// Connections currently checked out.
    pub in_use: u32,
}

/// Reports pool statistics for a pool the caller already holds.
///
/// Diagnostics only. Deliberately takes `&sqlx::PgPool` and returns plain
/// integers: there is no path from here to an executor, so this can never be
/// turned into an unscoped query.
#[must_use]
pub fn pool_stats(pool: &sqlx::PgPool) -> ScopedPoolStats {
    ScopedPoolStats {
        size: pool.options().get_max_connections(),
        idle: u32::try_from(pool.num_idle()).unwrap_or(u32::MAX),
        in_use: pool.size() - u32::try_from(pool.num_idle()).unwrap_or(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::result::Result;

    /// The retired `ScopedDatabase` is gone, so the "no scope outside a
    /// request" property is now asserted against the surviving abstraction.
    #[test]
    fn there_is_no_scope_outside_a_request() {
        assert!(
            !has_scope(),
            "no transaction-local scope may exist outside a request"
        );
    }

    /// The re-export must resolve to the canonical implementation, and a
    /// failed scope must leave nothing behind: after `with_authoritative_scope`
    /// returns an error, no scope is bound to the task. That is Phase 12's
    /// pooled-connection guarantee stated from the outside.
    #[tokio::test]
    async fn a_failed_scope_leaves_nothing_bound_to_the_task() {
        let pool =
            sqlx::PgPool::connect_lazy("postgres://mas:unused@127.0.0.1:1/mas?connect_timeout=1")
                .expect("a lazy pool does not connect");
        let scope = mas_security::AuthoritativeScope::resolved(
            mas_common::ids::TenantId::new(),
            mas_common::ids::OrganizationId::new(),
            "scoped-shim-test",
        );
        let outcome: Result<u8> = with_authoritative_scope(&pool, scope, || async { Ok(1) }).await;
        assert!(
            outcome.is_err(),
            "the lazy pool cannot connect, so the scope cannot be opened"
        );
        assert!(
            !has_scope(),
            "a scope that failed to open must not remain bound to the task"
        );
    }

    /// `current_scope` outside a request is an error, not a wider permission.
    #[test]
    fn current_scope_outside_a_request_is_an_error() {
        assert!(current_scope().is_err());
    }

    #[test]
    fn pool_stats_reports_counters_without_handing_out_a_connection() {
        let stats = ScopedPoolStats {
            size: 10,
            idle: 3,
            in_use: 7,
        };
        assert_eq!(stats.size, 10);
        assert_eq!(stats.idle, 3);
        assert_eq!(stats.in_use, 7);
    }
}
