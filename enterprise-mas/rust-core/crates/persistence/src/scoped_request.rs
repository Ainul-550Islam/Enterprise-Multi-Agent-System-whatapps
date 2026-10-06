//! PURPOSE: One transaction per request, with row-level security armed inside
//! it.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: spec Phases 11, 12 and 15. Every tenant-owned
//! table is `ENABLE` + `FORCE ROW LEVEL SECURITY`, and every policy reads
//! `app.current_tenant` / `app.current_org`. Those GUCs were never set in the
//! production path: `PgServices` ran every statement straight off the pool, so
//! the policies were armed but the platform never armed them — which under
//! `FORCE RLS` means every query returned nothing.
//!
//! This module is the fix. [`with_authoritative_scope`] opens one transaction
//! for one request, applies the GUCs **inside** it, and keeps that single
//! transaction available to every repository call the request makes. Handlers
//! therefore get real per-request atomicity as well as real isolation, which
//! is what the name of the abstraction promises.
//!
//! Security boundary: the GUCs are `SET LOCAL`. They are pinned to the
//! transaction that set them, so they die on commit, on rollback, on drop, and
//! on a panic mid-request. Phase 12's pooled-connection hazard — request A
//! sets tenant A, the transaction ends, request B reuses the connection and
//! inherits tenant A — cannot happen here because the connection carries no
//! GUC state once the transaction is over.
//!
//! It must NOT:
//!   • offer a way to run a tenant-touching query outside a scope:
//!     [`current_scope`] fails closed, so a repository call made with no scope
//!     in effect is an error rather than an unscoped read;
//!   • set a GUC outside a transaction;
//!   • allow a nested scope to widen an outer one. Nesting is refused
//!     outright — one request, one transaction, one tenant.

use std::future::Future;
use std::sync::Arc;

use mas_common::error::AppError;
use mas_common::result::Result;
use mas_security::AuthoritativeScope;
use sqlx::pool::PoolConnection;
use sqlx::{PgPool, Postgres};
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::error::map_sqlx;
use crate::rls::RlsContext;

/// The connection and scope bound to the task currently running a request.
///
/// Held behind a mutex so a repository call can borrow it for the duration of
/// one statement and release it immediately: a guard held across another
/// repository call would deadlock.
struct ScopedConnection {
    connection: PoolConnection<Postgres>,
    scope: AuthoritativeScope,
}

tokio::task_local! {
    static CONNECTION: Arc<Mutex<ScopedConnection>>;
    static SCOPE: AuthoritativeScope;
}

/// Whether a scope is in effect for the current task.
#[must_use]
pub fn has_scope() -> bool {
    SCOPE.try_with(|_| ()).is_ok()
}

/// The scope in effect for the current task.
///
/// Fails closed. A repository call that reaches this without a scope is a
/// wiring defect, and the correct response to a wiring defect in a
/// tenant-touching path is to do nothing at all.
///
/// # Errors
/// `AppError` when no scope is bound to the current task.
pub fn current_scope() -> Result<AuthoritativeScope> {
    SCOPE
        .try_with(|scope| scope.clone())
        .map_err(|_| no_scope())
}

fn no_scope() -> AppError {
    AppError::internal(
        "no tenant scope is bound to this task; tenant-owned data cannot be \
         reached without one",
    )
}

/// A borrowed handle on the request's transaction.
///
/// Obtained from [`connection`]. Holding one is what lets a repository run a
/// statement inside the request's transaction — and therefore inside the
/// request's row-level security scope.
pub struct ScopedExecutor {
    guard: OwnedMutexGuard<ScopedConnection>,
}

impl ScopedExecutor {
    /// The connection, ready to be used as a `sqlx` executor.
    pub fn connection(&mut self) -> &mut PoolConnection<Postgres> {
        &mut self.guard.connection
    }

    /// The scope this transaction runs under.
    #[must_use]
    pub fn scope(&self) -> &AuthoritativeScope {
        &self.guard.scope
    }
}

/// Borrows the current task's scoped transaction.
///
/// # Errors
/// `AppError` when no transaction is bound to the current task — that is, when
/// the caller is not inside [`with_authoritative_scope`].
pub async fn connection() -> Result<ScopedExecutor> {
    let handle = CONNECTION
        .try_with(|handle| handle.clone())
        .map_err(|_| no_scope())?;
    // An owned guard rather than a borrowed one: it keeps the `Arc` alive for
    // as long as the repository call needs the connection, without borrowing
    // from a temporary.
    let guard = handle.lock_owned().await;
    Ok(ScopedExecutor { guard })
}

/// True while a nested scope is already in effect.
fn already_scoped() -> bool {
    CONNECTION.try_with(|_| ()).is_ok()
}

/// Runs `work` inside one transaction carrying `scope`.
///
/// The transaction is opened, the RLS GUCs are applied inside it, and `work`
/// runs with both bound to the current task. On success the transaction
/// commits; on error, on panic, or if the task is cancelled, it rolls back and
/// the connection returns to the pool carrying no GUC state.
///
/// Nesting is refused. One request gets one transaction and one tenant; a
/// second scope inside the first would mean two tenants in one request, which
/// is not a thing this platform lets you do.
///
/// # Errors
/// * when a scope is already bound to this task (nesting);
/// * when the connection cannot be acquired or the GUCs cannot be applied —
///   in which case nothing is committed and the caller sees the error;
/// * whatever `work` returns.
pub async fn with_authoritative_scope<F, Fut, T>(
    pool: &PgPool,
    scope: AuthoritativeScope,
    work: F,
) -> Result<T>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    if already_scoped() {
        return Err(AppError::internal(
            "a tenant scope is already bound to this task; scopes do not nest",
        ));
    }

    let mut connection = pool.acquire().await.map_err(map_sqlx)?;

    // BEGIN, then arm RLS inside the transaction. There is deliberately no
    // window between the two: the first statement after BEGIN is the GUC.
    sqlx::query("BEGIN")
        .execute(connection.as_mut())
        .await
        .map_err(map_sqlx)?;
    apply(&mut connection, &scope).await?;

    let handle = Arc::new(Mutex::new(ScopedConnection {
        connection,
        scope: scope.clone(),
    }));

    let outcome = CONNECTION
        .scope(handle.clone(), SCOPE.scope(scope, work()))
        .await;

    let mut guard = handle.lock().await;
    match &outcome {
        Ok(_) => {
            sqlx::query("COMMIT")
                .execute(guard.connection.as_mut())
                .await
                .map_err(map_sqlx)?;
        },
        Err(_) => {
            // Best-effort: the original error is what the caller needs, and
            // dropping the connection rolls back regardless.
            let _ = sqlx::query("ROLLBACK")
                .execute(guard.connection.as_mut())
                .await;
        },
    }
    drop(guard);
    outcome
}

/// Applies the RLS GUCs, transaction-locally.
async fn apply(
    connection: &mut PoolConnection<Postgres>,
    scope: &AuthoritativeScope,
) -> Result<()> {
    let context =
        RlsContext::for_organization_with_tenant(scope.organization_id(), scope.tenant_id());
    if context.is_unscoped() {
        return Err(AppError::internal(
            "refusing to open a tenant transaction without a scope",
        ));
    }
    for statement in context.statements() {
        sqlx::query(statement.sql)
            .bind(statement.value)
            .execute(connection.as_mut())
            .await
            .map_err(map_sqlx)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Without a scope in effect, a repository must not be able to read
    /// anything. This is the assertion that makes "fail closed" a property
    /// rather than an intention.
    #[tokio::test]
    async fn no_scope_means_no_data() {
        assert!(!has_scope());
        assert!(current_scope().is_err());
        assert!(connection().await.is_err());
    }

    /// The error a missing scope produces must be explicit about why, so an
    /// operator reading a log sees a wiring bug and not a database problem.
    #[test]
    fn the_missing_scope_error_names_itself() {
        let message = no_scope().to_string();
        assert!(message.contains("no tenant scope"));
    }

    /// A scope bound in one task must not leak into a sibling task: the whole
    /// point of a task-local is that concurrent requests cannot see each
    /// other's tenant.
    #[tokio::test]
    async fn a_scope_does_not_leak_into_a_sibling_task() {
        let scope = AuthoritativeScope::resolved(
            mas_common::ids::TenantId::new(),
            mas_common::ids::OrganizationId::new(),
            "actor",
        );
        let scoped = tokio::spawn(SCOPE.scope(scope.clone(), async { current_scope().is_ok() }));
        let unscoped = tokio::spawn(async { current_scope().is_ok() });
        assert!(scoped.await.expect("scoped task"));
        assert!(!unscoped.await.expect("unscoped task"));
    }
}
