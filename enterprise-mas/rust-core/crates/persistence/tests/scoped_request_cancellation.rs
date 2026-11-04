//! Live PostgreSQL regression for cancelled/panicking scoped requests.
//! Uses one pooled session so transaction and RLS state leaks are observable.

use std::time::Duration;

use mas_common::error::AppError;
use mas_common::ids::{OrganizationId, TenantId};
use mas_persistence::scoped_request::{connection, with_authoritative_scope};
use mas_security::AuthoritativeScope;
use sqlx::{postgres::PgPoolOptions, PgPool};
use tokio::sync::oneshot;

fn scope() -> AuthoritativeScope {
    AuthoritativeScope::resolved(TenantId::new(), OrganizationId::new(), "test")
}

async fn assert_clean(pool: &PgPool, pid: i32, expected_rows: i64) {
    let (actual_pid, rows, tenant): (i32, i64, Option<String>) = tokio::time::timeout(
        Duration::from_secs(5),
        sqlx::query_as("SELECT pg_backend_pid(), (SELECT count(*) FROM scope_cancel_probe), current_setting('app.current_tenant', true)")
            .fetch_one(pool),
    )
    .await
    .expect("pool returned a connection")
    .expect("read probe");
    assert_eq!(
        actual_pid, pid,
        "test must reuse the same PostgreSQL session"
    );
    assert_eq!(rows, expected_rows, "only committed writes must remain");
    assert!(
        tenant.as_deref().is_none_or(str::is_empty),
        "scope leaked: {tenant:?}"
    );
}

#[tokio::test]
async fn scoped_transaction_does_not_leak_after_cancel_panic_error_or_escaped_guard() {
    let app_url =
        std::env::var("MAS_TEST_DATABASE_APP_URL").expect("MAS_TEST_DATABASE_APP_URL required");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&app_url)
        .await
        .expect("app pool");
    sqlx::query("CREATE TEMP TABLE scope_cancel_probe (value integer)")
        .execute(&pool)
        .await
        .expect("create temp table");
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .expect("pid");

    let (ready_tx, ready_rx) = oneshot::channel();
    let task_pool = pool.clone();
    let task = tokio::spawn(async move {
        with_authoritative_scope(&task_pool, scope(), || async move {
            let mut guard = connection().await?;
            sqlx::query("INSERT INTO scope_cancel_probe VALUES (1)")
                .execute(guard.connection().as_mut())
                .await
                .expect("insert");
            drop(guard);
            ready_tx.send(()).expect("notify caller");
            std::future::pending::<()>().await;
            Ok::<_, AppError>(())
        })
        .await
    });
    ready_rx.await.expect("request reached cancellation point");
    task.abort();
    assert!(task.await.expect_err("task cancelled").is_cancelled());
    assert_clean(&pool, pid, 0).await;

    let task_pool = pool.clone();
    let panic_task = tokio::spawn(async move {
        with_authoritative_scope(&task_pool, scope(), || async {
            let mut guard = connection().await?;
            sqlx::query("INSERT INTO scope_cancel_probe VALUES (2)")
                .execute(guard.connection().as_mut())
                .await
                .expect("insert");
            panic!("simulated handler panic");
            #[allow(unreachable_code)]
            Ok::<_, AppError>(())
        })
        .await
    });
    assert!(panic_task.await.expect_err("task panicked").is_panic());
    assert_clean(&pool, pid, 0).await;

    let error = with_authoritative_scope(&pool, scope(), || async {
        let mut guard = connection().await?;
        sqlx::query("INSERT INTO scope_cancel_probe VALUES (3)")
            .execute(guard.connection().as_mut())
            .await
            .expect("insert");
        Err::<(), _>(AppError::internal("simulated failure"))
    })
    .await;
    assert!(error.is_err());
    assert_clean(&pool, pid, 0).await;

    let escaped = tokio::time::timeout(
        Duration::from_secs(5),
        with_authoritative_scope(&pool, scope(), || async { connection().await }),
    )
    .await
    .expect("escaped guard must fail rather than deadlock");
    assert!(escaped.is_err());
    assert_clean(&pool, pid, 0).await;

    with_authoritative_scope(&pool, scope(), || async {
        let mut guard = connection().await?;
        sqlx::query("INSERT INTO scope_cancel_probe VALUES (4)")
            .execute(guard.connection().as_mut())
            .await
            .expect("insert");
        Ok(())
    })
    .await
    .expect("commit");
    assert_clean(&pool, pid, 1).await;
}
