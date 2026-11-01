//! PURPOSE: Chooses the connection a control-plane statement runs on: the
//! request's own transaction when there is one, a pooled connection when there
//! is not.
//!
//! Owning layer: `mas-persistence` (control-plane repositories).
//!
//! Production responsibility: every tenant-owned control-plane table —
//! subscriptions, entitlements, overrides, quota counters, reservations, usage,
//! policies — is `ENABLE` **and** `FORCE ROW LEVEL SECURITY`, and every policy
//! reads `app.current_tenant`. Those GUCs are `SET LOCAL` inside the one
//! transaction [`crate::scoped_request::with_authoritative_scope`] opens per
//! request. A repository that ran its own statement straight off the pool
//! therefore ran with no GUC at all: under `FORCE RLS` that is not "a query
//! that sees everything" but "a query that sees nothing", and an insert is
//! refused outright. Worse, the read that saw nothing came back as an empty
//! result, which is indistinguishable from a tenant that consumed nothing.
//!
//! This module is the seam that closes that gap. [`executor`] hands back the
//! request's transaction whenever a scope is bound to the current task, so the
//! statement inherits both the row-level-security scope and the request's
//! atomicity. Outside a request there is no scope to inherit, and the
//! connection comes from the pool exactly as it always did: `FORCE RLS` still
//! fails closed, so the fallback is safe, but it is only ever reached by
//! callers that were not running inside a request to begin with.
//!
//! Data ownership: none. This module owns no rows and decides no policy; it
//! only decides which connection a statement is executed on.
//!
//! It must NOT:
//!   • set a GUC itself — the scope is applied once, transaction-locally, by
//!     `scoped_request`, and a second place that sets it is a second place
//!     that can get it wrong;
//!   • open a transaction of its own when a request scope is already in
//!     effect; nesting is refused by `scoped_request`, and a nested unit of
//!     work would break the atomicity the request transaction exists to give;
//!   • hold the borrowed connection across another repository call. The
//!     handle is a temporary that lives for exactly one statement, which is
//!     what keeps two control-plane repository calls in one task from
//!     deadlocking on the request's connection mutex.

use sqlx::pool::PoolConnection;
use sqlx::{PgConnection, PgPool, Postgres};

use mas_common::result::Result;

use crate::error::map_sqlx;
use crate::scoped_request;

/// The connection one control-plane statement will run on.
pub(crate) enum ControlPlaneExecutor {
    /// The request's transaction, with row-level security armed inside it.
    ///
    /// Holding this holds the request's connection mutex. It is a temporary
    /// that lives for one statement, never longer.
    Scoped(scoped_request::ScopedExecutor),
    /// A connection taken straight from the pool, for a caller that is not
    /// running inside a request.
    Pooled(PoolConnection<Postgres>),
}

impl ControlPlaneExecutor {
    /// The connection, ready to be used as a `sqlx` executor.
    ///
    /// Both arms deref to the same thing — a `&mut PgConnection` — so a
    /// repository does not branch on which one it got; it only ever sees the
    /// connection it asked for.
    pub(crate) fn connection(&mut self) -> &mut PgConnection {
        match self {
            Self::Scoped(scoped) => scoped.connection(),
            Self::Pooled(pooled) => pooled,
        }
    }
}

/// The executor for one control-plane statement.
///
/// Call it at the point of use and let the value die with the statement:
///
/// ```ignore
/// sqlx::query("SELECT 1")
///     .fetch_optional(executor(&self.pool).await?.connection())
///     .await
/// ```
///
/// # Errors
/// * when a request scope is bound but its transaction cannot be borrowed —
///   reported as an internal error rather than degraded into an unscoped
///   query;
/// * when no scope is bound and a connection cannot be acquired from the pool.
pub(crate) async fn executor(pool: &PgPool) -> Result<ControlPlaneExecutor> {
    if scoped_request::has_scope() {
        return Ok(ControlPlaneExecutor::Scoped(
            scoped_request::connection().await?,
        ));
    }
    let connection = pool.acquire().await.map_err(map_sqlx)?;
    Ok(ControlPlaneExecutor::Pooled(connection))
}
