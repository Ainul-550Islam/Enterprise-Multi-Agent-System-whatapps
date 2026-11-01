//! PURPOSE: The privilege contract between the database and the three plane
//! roles — a regression guard for missing grants.
//!
//! Owning layer: `mas-persistence` (tests).
//!
//! Production responsibility: every defect this file guards was found in
//! production code by running it, not by reading it, and all three of them
//! failed the same way: a statement that worked perfectly under the migrator
//! (a superuser, which bypasses every policy and every missing grant) and
//! failed with SQLSTATE 42501 under the role that actually runs it.
//!
//!   1. `mas_app` had no `USAGE` on any sequence, so every subscription
//!      transition — including the one a billing webhook performs — failed
//!      (fixed by migration 0021).
//!   2. `mas_worker` had no `INSERT` on `usage_records`, so every settlement
//!      that carried usage — that is, every successful run — failed and
//!      rolled back (fixed by migration 0022).
//!
//! Both are invisible to a suite that only ever connects as the owner. The
//! assertions here run `has_*_privilege` as the owner and read the answer for
//! the plane role, which is the only way to ask the question without holding
//! every plane credential.
//!
//! Security boundary: the assertions are two-sided on purpose. Each role must
//! have what it needs **and must not have** what it does not. A grant that is
//! widened for convenience is a grant a compromised plane can use, and the
//! negative assertions are what make that widening fail the build.
//!
//! It must NOT:
//!   • run without `MAS_TEST_DATABASE_URL` (it skips loudly instead);
//!   • assert a privilege by connecting as the plane role — these roles are
//!     `NOLOGIN` by design, and a test that needed them to log in would be
//!     arguing for the very thing 0017 exists to prevent;
//!   • be pointed at a production database.

//! Live PostgreSQL suite for the plane-role privilege contract.
//!
//! Gated by `MAS_TEST_DATABASE_URL` (the owner/migrator role, needed to run
//! migrations and to read `has_*_privilege` for the other roles).

mod support;

use sqlx::PgPool;

/// Reads `has_table_privilege(role, table, privilege)` as the current
/// (owner) role.
async fn table_privilege(pool: &PgPool, role: &str, table: &str, privilege: &str) -> bool {
    sqlx::query_scalar::<_, bool>("SELECT has_table_privilege($1, $2, $3)")
        .bind(role)
        .bind(table)
        .bind(privilege)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|error| panic!("has_table_privilege({role},{table},{privilege}): {error}"))
}

/// Reads `has_sequence_privilege(role, sequence, privilege)` for every
/// sequence in the schema, and answers whether *all* of them grant it.
///
/// "All" rather than "the one I know about" is deliberate: a single new
/// `bigserial` reintroduces defect 1, and a test that named one sequence
/// would have passed while it did.
async fn every_sequence_grants(pool: &PgPool, role: &str, privilege: &str) -> (usize, bool) {
    let rows: Vec<(String, bool)> = sqlx::query_as(
        "SELECT sequencename,
                has_sequence_privilege($1, quote_ident(schemaname) || '.' || quote_ident(sequencename), $2)
           FROM pg_sequences
          WHERE schemaname = 'public'",
    )
    .bind(role)
    .bind(privilege)
    .fetch_all(pool)
    .await
    .expect("sequence privileges");
    let total = rows.len();
    (total, rows.iter().all(|(_, granted)| *granted))
}

/// The runtime role can use every sequence in the schema.
///
/// Migration 0001 excluded sequences from `ALTER DEFAULT PRIVILEGES` on
/// purpose ("ids come from pgcrypto"), and `subscription_events.seq` — the
/// monotonic counter that orders two transitions sharing a timestamp — is a
/// sequence. Without this, a subscription transition is refused for a role
/// that is otherwise allowed to make it.
#[tokio::test]
async fn the_runtime_role_may_use_every_sequence() {
    let Some(pool) = support::live_pool("grants-sequences").await else {
        return;
    };
    if std::env::var("MAS_TEST_DATABASE_APP_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .is_none()
    {
        eprintln!(
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\
             plane_grants: skipping (set MAS_TEST_DATABASE_APP_URL — this\n\
             assertion is about the runtime role, and there is nothing to\n\
             check without it)\n\
             ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        );
        return;
    }
    let (total, all) = every_sequence_grants(&pool, "mas_app", "USAGE").await;
    assert!(
        total > 0,
        "the schema has sequences; a suite that found none is querying the wrong database"
    );
    assert!(
        all,
        "mas_app must hold USAGE on every sequence ({total} of them): a bigserial \\
         column it cannot advance is a production failure, not a permission detail"
    );
}

/// The worker plane may append the usage it observed.
///
/// Settlement writes usage inside the transaction that makes the attempt
/// terminal, and settlement runs as `mas_worker`. Without `INSERT` on
/// `usage_records`, every successful run fails its settlement — and because
/// the durable unit rolls back, it fails again on every retry.
#[tokio::test]
async fn the_worker_plane_may_append_usage() {
    let Some(pool) = support::live_pool("grants-worker-usage").await else {
        return;
    };
    assert!(
        table_privilege(&pool, "mas_worker", "usage_records", "INSERT").await,
        "mas_worker must be able to append usage: settlement writes it in the same \\
         transaction that makes the attempt terminal, and without this grant every \\
         successful run fails and is retried"
    );
}

/// Metering is append-only: the role that writes a meter cannot read or
/// rewrite one.
///
/// `INSERT` without `SELECT`/`UPDATE`/`DELETE` is the narrowest grant that
/// lets the worker do its job. Widening it to `UPDATE` would let a compromised
/// worker rewrite what a tenant consumed, which is billing fraud rather than
/// an outage.
#[tokio::test]
async fn the_worker_plane_cannot_read_or_rewrite_usage() {
    let Some(pool) = support::live_pool("grants-worker-usage-negative").await else {
        return;
    };
    for privilege in ["SELECT", "UPDATE", "DELETE"] {
        assert!(
            !table_privilege(&pool, "mas_worker", "usage_records", privilege).await,
            "mas_worker must not hold {privilege} on usage_records: metering is \\
             append-only, and a role that can rewrite a meter can rewrite a bill"
        );
    }
}

/// A compromised worker must not reach tenant secrets.
///
/// This is migration 0017's stated invariant, re-asserted here because a later
/// migration that granted the worker "just one more table" would otherwise
/// pass silently.
#[tokio::test]
async fn the_worker_plane_cannot_reach_credentials_or_keys() {
    let Some(pool) = support::live_pool("grants-worker-secrets").await else {
        return;
    };
    for table in ["credentials", "api_keys", "sessions", "connectors"] {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename=$1)",
        )
        .bind(table)
        .fetch_one(&pool)
        .await
        .expect("table lookup");
        if !exists {
            continue;
        }
        for privilege in ["SELECT", "INSERT", "UPDATE", "DELETE"] {
            assert!(
                !table_privilege(&pool, "mas_worker", table, privilege).await,
                "mas_worker must not hold {privilege} on {table}: 0017 confines the \\
                 worker plane to the durable execution tables"
            );
        }
    }
}

/// The worker plane keeps the durable-execution grants it was created with.
///
/// The negative assertions above are only meaningful if the positive ones
/// still hold: a role stripped of `UPDATE` on `tasks` would pass every "must
/// not" here and be completely unable to work.
#[tokio::test]
async fn the_worker_plane_keeps_its_durable_execution_grants() {
    let Some(pool) = support::live_pool("grants-worker-durable").await else {
        return;
    };
    for table in ["tasks", "task_attempts", "executions"] {
        assert!(
            table_privilege(&pool, "mas_worker", table, "UPDATE").await,
            "mas_worker must hold UPDATE on {table}: it fences and settles there"
        );
    }
}

/// The runtime role may not bypass row-level security.
///
/// Every isolation assertion in the platform is void if the role serving
/// requests can skip the policies. Asserted here as well as in the control
/// plane suite because a grant migration is exactly the place someone might
/// "fix" a permission problem by reaching for BYPASSRLS.
#[tokio::test]
async fn the_runtime_role_cannot_bypass_row_level_security() {
    let Some(pool) = support::live_pool("grants-rls").await else {
        return;
    };
    let bypass: bool =
        sqlx::query_scalar("SELECT rolbypassrls FROM pg_roles WHERE rolname = 'mas_app'")
            .fetch_one(&pool)
            .await
            .expect("role lookup");
    assert!(
        !bypass,
        "mas_app must stay subject to row-level security: every tenant-isolation \\
         guarantee in this platform is enforced by a policy"
    );
}
