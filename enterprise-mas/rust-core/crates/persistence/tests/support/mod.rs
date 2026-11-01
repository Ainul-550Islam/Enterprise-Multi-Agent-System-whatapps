//! PURPOSE: Shared harness for the live-PostgreSQL suite: the environment
//! gate, two pool bootstraps (migrator and runtime role), and tiny hash
//! helpers mirroring the verifier's on-disk formulas.
//!
//! Owning layer: `mas-persistence` (tests).
//!
//! Production responsibility: hand each test a pool whose *identity* matches
//! what the test is actually asserting. Lifecycle and round-trip tests get the
//! migrator; isolation tests get `mas_app`. Conflating the two is the single
//! easiest way to ship an isolation suite that cannot fail.
//!
//! Security boundary: [`live_app_pool`] refuses to hand back a pool whose role
//! holds BYPASSRLS, and asserts that fact before any test uses it. A pool that
//! bypasses row-level security would let every isolation assertion in the
//! suite pass on a policy that is switched off.
//!
//! It must NOT:
//!   • fall back to the migrator URL when the runtime URL is missing — the
//!     isolation tests skip loudly instead, because a vacuous green is worse
//!     than a visible skip;
//!   • be pointed at a production database;
//!   • grant the runtime pool more than one connection: the pooled-connection
//!     leakage test needs the second `acquire()` to be the same socket.

use mas_persistence::migrations::MigrationRunner;
use mas_persistence::pool::{Database, DatabaseUrl, PoolConfig};
use sqlx::PgPool;

/// Environment variable consulted first, then `DATABASE_URL`.
pub const ENV_VAR: &str = "MAS_TEST_DATABASE_URL";

/// Reads the live database URL from the environment, when provided.
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

/// Builds a migrated pool against the live URL (or `None` when the suite is
/// not enabled). Every caller ends the returned pool's test with normal
/// teardown (tables are shared deliberately — fixtures carry fresh ids).
pub async fn live_pool(owner: &'static str) -> Option<PgPool> {
    let raw = live_url().unwrap_or_else(|| {
        eprintln!(
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\
            live_pg: skipping (set {ENV_VAR} — see docs/runbooks/postgres-live-tests.md)\n\
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        );
        String::new()
    });
    if raw.is_empty() {
        return None;
    }
    let url = DatabaseUrl::new(raw).expect("MAS_TEST_DATABASE_URL parses");
    let mut config = PoolConfig::for_url(url, owner).expect("pool config");
    config.max_connections = 4;
    let database = Database::connect(&config)
        .await
        .expect("live database reachable");
    let pool = database.pool().clone();
    let runner = MigrationRunner::new(pool.clone());
    runner.ensure_table().await.expect("ledger table");
    runner.migrate().await.expect("migrations applied");
    Some(pool)
}

/// Environment variable for the **runtime** database URL.
///
/// This is the pool the application itself uses: `mas_app`, a role with no
/// BYPASSRLS. Tests that assert row-level security is *doing* something have
/// to run here. Under the migrator URL every one of them would pass vacuously,
/// because a superuser bypasses the policies the test is trying to exercise.
// Shared by several test binaries in this crate; the privilege suite does not
// need a runtime-role pool, so from one binary's point of view this is unused.
// A helper in a shared harness is not dead code.
#[allow(dead_code)]
pub const APP_ENV_VAR: &str = "MAS_TEST_DATABASE_APP_URL";

/// Builds a pool connected as the runtime role (`mas_app`).
///
/// Returns `None` when [`APP_ENV_VAR`] is unset, so the suite still runs —
/// and still skips — in an environment that has only provisioned the migrator
/// credential. The RLS tests print a notice in that case rather than passing
/// quietly, because a vacuous isolation test is worse than no isolation test.
///
/// Migrations are deliberately **not** applied from this pool: `mas_app` has
/// no DDL rights on the schema, and a test that could not tell the difference
/// between "migrations applied" and "migrations already applied" would not be
/// testing anything. Call [`live_pool`] first to bring the schema up.
// Same reason as [`APP_ENV_VAR`] above: shared harness, per-binary unused.
#[allow(dead_code)]
pub async fn live_app_pool(owner: &'static str) -> Option<PgPool> {
    let raw = std::env::var(APP_ENV_VAR)
        .ok()
        .filter(|v| !v.trim().is_empty())?;
    let url = DatabaseUrl::new(raw).expect("MAS_TEST_DATABASE_APP_URL parses");
    let mut config = PoolConfig::for_url(url, owner).expect("pool config");
    // One connection on purpose. The pooled-connection test needs to be able
    // to take the single connection in the pool, use it for one "request",
    // hand it back, and take it again — that is the reuse it is asserting is
    // safe. A pool of twenty would let each request have a fresh connection
    // and the test would prove nothing.
    config.max_connections = 1;
    // ...and hold exactly one open, so the socket is warm and waiting. A pool
    // that lazily opened a connection per acquire would defeat the reuse this
    // exercise is about.
    config.min_connections = 1;
    let database = Database::connect(&config)
        .await
        .expect("runtime-role database reachable");
    let pool = database.pool().clone();

    // Fail loudly and immediately if this is not actually the runtime role.
    // Every assertion below is void if the connection can bypass RLS.
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

/// SHA-256 hex digest (the on-disk `secret_hash` formula).
#[allow(dead_code)] // shared by several test binaries; not all use every helper
pub fn sha256_hex(input: &[u8]) -> String {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(input);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// The public lookup prefix the verifier persists (`mas_` + first 8).
#[allow(dead_code)] // shared by several test binaries; not all use every helper
pub fn visible_prefix(raw: &str) -> String {
    raw.chars().take(4 + 8).collect()
}
