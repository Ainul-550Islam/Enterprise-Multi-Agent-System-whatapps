//! `mas_migrate` — thin operator front over `mas_persistence`'s migration
//! planner + verified runner. Kept deliberately tiny: all doctrine
//! (deterministic FNV-1a checksums, strict 1..N contiguity, advisory-lock
//! apply guard) lives in the library; this file only renders + dispatches.
//!
//! Commands:
//!   mas_migrate plan    — prints the registry manifest (no DB contact) —
//!                         the export production/DBA workflows rely on
//!   mas_migrate status  — read-only reconciliation against the ledger
//!   mas_migrate up      — applies pending migrations in stable order
//!
//! Env: DATABASE_URL (postgres://…), required for status/up. For CI
//! tooling this binary is wired via `scripts/migrate.sh`.

use mas_persistence::migrations::{plan_pending, validate_registry, MigrationRunner, MIGRATIONS};
use mas_persistence::pool::{Database, DatabaseUrl, PoolConfig};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let command = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "status".to_owned());
    if let Err(err) = run(&command).await {
        eprintln!("mas_migrate {command}: {err}");
        std::process::exit(1);
    }
}

async fn run(command: &str) -> mas_common::result::Result<()> {
    match command {
        "plan" => {
            validate_registry()
                .map_err(|e| mas_common::error::AppError::validation(e.to_string()))?;
            println!("version name            checksum");
            for migration in MIGRATIONS {
                println!(
                    "{:07} {:-14} {:#018x}",
                    migration.version, migration.name, migration.checksum
                );
            }
        },
        "status" => {
            let runner = connect().await?;
            runner.ensure_table().await?;
            runner.verify().await?;
            let applied = runner.applied().await?;
            let pending = plan_pending(&applied)
                .map_err(|e| mas_common::error::AppError::validation(e.to_string()))?;
            println!("applied: {}", applied.len());
            for migration in &applied {
                println!(
                    "  ✓ {:07} {:-14} {:#018x}",
                    migration.version, migration.name, migration.checksum
                );
            }
            println!("pending: {}", pending.len());
            for migration in pending {
                println!("  … {:07} {}", migration.version, migration.name);
            }
        },
        "up" => {
            let runner = connect().await?;
            runner.ensure_table().await?;
            let applied_now = runner.migrate().await?;
            if applied_now.is_empty() {
                println!("nothing to apply — registry is already satisfied");
            } else {
                for version in applied_now {
                    println!("applied {version:07}");
                }
            }
        },
        other => {
            return Err(mas_common::error::AppError::validation(format!(
                "unknown command '{other}' — plan | status | up"
            )));
        },
    }
    Ok(())
}

async fn connect() -> mas_common::result::Result<MigrationRunner> {
    let url = std::env::var("DATABASE_URL")
        .map_err(|_| mas_common::error::AppError::validation("DATABASE_URL is required"))?;
    let config = PoolConfig::for_url(DatabaseUrl::new(url)?, "mas_migrate")?.validated()?;
    let database = Database::connect(&config).await?;
    Ok(MigrationRunner::new(database.pool().clone()))
}
