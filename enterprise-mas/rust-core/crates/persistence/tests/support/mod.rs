//! Shared harness for the live-PostgreSQL suite: environment gate, pool
//! bootstrap with migration apply, and tiny hash helpers mirroring the
//! verifier's on-disk formulas.

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

/// SHA-256 hex digest (the on-disk `secret_hash` formula).
pub fn sha256_hex(input: &[u8]) -> String {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(input);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// The public lookup prefix the verifier persists (`mas_` + first 8).
pub fn visible_prefix(raw: &str) -> String {
    raw.chars().take(4 + 8).collect()
}
