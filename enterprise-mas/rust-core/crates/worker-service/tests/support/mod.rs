//! PURPOSE: Shared harness for the worker service's live-PostgreSQL suite: the
//! environment gate and the two pool bootstraps the concurrency-gate tests
//! need (migrator and runtime role).
//!
//! Owning layer: `mas-worker-service` (tests).
//!
//! Production responsibility: hand each test a pool whose *identity* matches
//! what the test asserts. Seeding and sweeping run on the migrator; the gate
//! itself runs on `mas_app`, the runtime role the production worker actually
//! uses. A gate tested only on the migrator would prove nothing, because every
//! defect this suite exists to catch is a statement that succeeds as a
//! superuser and fails as the plane role.
//!
//! Security boundary: [`live_app_pool`] refuses to hand back a pool whose role
//! holds BYPASSRLS and asserts that before any test uses it. Under a bypassing
//! role the isolation assertion would pass on a policy that is switched off.
//!
//! It must NOT:
//!   • fall back to the migrator URL when the runtime URL is missing — the
//!     gate tests skip loudly instead, because a vacuous green is worse than a
//!     visible skip;
//!   • be pointed at a production database;
//!   • starve the concurrency test: the runtime pool is sized above the number
//!     of racing acquires, or the test would measure the pool rather than the
//!     ceiling.

use mas_persistence::migrations::MigrationRunner;
use mas_persistence::pool::{Database, DatabaseUrl, PoolConfig};
use sqlx::PgPool;

/// Environment variable consulted first, then `DATABASE_URL`.
pub const ENV_VAR: &str = "MAS_TEST_DATABASE_URL";

/// Environment variable for the **runtime** database URL (`mas_app`).
pub const APP_ENV_VAR: &str = "MAS_TEST_DATABASE_APP_URL";

/// Reads the migrator URL from the environment, when provided.
pub fn live_url() -> Option<String> {
    std::env::var(ENV_VAR)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::env::var("DATABASE_URL")
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
}

/// Builds a migrated pool against the live URL, or `None` when the suite is
/// not enabled.
pub async fn live_pool(owner: &'static str) -> Option<PgPool> {
    let raw = live_url().unwrap_or_else(|| {
        eprintln!(
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\
             run_slots_pg: skipping (set {ENV_VAR})\n\
             ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        );
        String::new()
    });
    if raw.is_empty() {
        return None;
    }
    let url = DatabaseUrl::new(raw).expect("MAS_TEST_DATABASE_URL parses");
    let mut config = PoolConfig::for_url(url, owner).expect("pool config");
    config.max_connections = 8;
    let database = Database::connect(&config)
        .await
        .expect("live database reachable");
    let pool = database.pool().clone();
    let runner = MigrationRunner::new(pool.clone());
    runner.ensure_table().await.expect("ledger table");
    runner.migrate().await.expect("migrations applied");
    Some(pool)
}

/// Builds a pool connected as the runtime role (`mas_app`).
///
/// Returns `None` when [`APP_ENV_VAR`] is unset so the suite still runs — and
/// still skips — in an environment that has only provisioned the migrator
/// credential.
pub async fn live_app_pool(owner: &'static str) -> Option<PgPool> {
    let raw = std::env::var(APP_ENV_VAR)
        .ok()
        .filter(|value| !value.trim().is_empty())?;
    let url = DatabaseUrl::new(raw).expect("MAS_TEST_DATABASE_APP_URL parses");
    let mut config = PoolConfig::for_url(url, owner).expect("pool config");
    // Sized for the concurrency test: eight workers must be able to ask for a
    // slot at the same instant, or the test would be measuring the connection
    // pool's ceiling instead of the tenant's.
    config.max_connections = 8;
    config.min_connections = 1;
    let database = Database::connect(&config)
        .await
        .expect("runtime-role database reachable");
    let pool = database.pool().clone();

    let bypass: bool =
        sqlx::query_scalar("SELECT rolbypassrls FROM pg_roles WHERE rolname = current_user")
            .fetch_one(&pool)
            .await
            .expect("role lookup");
    assert!(
        !bypass,
        "{APP_ENV_VAR} must connect as a role that is subject to row-level \
         security (mas_app); current_user bypasses it, so every isolation \
         assertion in this suite would pass without testing anything"
    );
    Some(pool)
}
