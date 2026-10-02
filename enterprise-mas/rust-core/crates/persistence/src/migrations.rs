//! Embedded migration registry and the [`MigrationRunner`].
//!
//! Migrations live in `migrations/NNNN_name/up.sql` at the repository root
//! and are embedded at compile time. The runner:
//!
//! * records every applied migration in `mas_schema_migrations` (version,
//!   name, FNV-1a checksum of the SQL text, applied_at);
//! * **refuses incompatible states**: a database ahead of the binary
//!   (unknown applied version), a gap in the applied prefix, or any checksum
//!   drift on an already-applied migration — all hard errors, never
//!   "self-healing";
//! * applies each pending migration inside its own transaction, inserting
//!   the record row in the same transaction so state and bookkeeping commit
//!   atomically;
//! * applies in strictly ascending version order, youngest pending first
//!   applied last.

use std::fmt;

use mas_common::error::AppError;
use mas_common::result::Result;
use sqlx::PgPool;
use sqlx::Row;

use crate::error::map_sqlx;

/// Table the runner uses for bookkeeping (always in the default schema).
pub const MIGRATIONS_TABLE: &str = "mas_schema_migrations";

/// FNV-1a 64-bit checksum over the migration SQL — stable, `const`-computable,
/// and collision-resistant enough for drift detection (not a security hash).
pub const fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        i += 1;
    }
    hash
}

/// One embedded migration.
#[derive(Debug, Clone, Copy)]
pub struct Migration {
    /// 1-based, strictly ascending version (`0001` → 1).
    pub version: u32,
    /// Short snake-case name (`tenancy`, `outbox`, …).
    pub name: &'static str,
    /// The full `up.sql` text.
    pub sql: &'static str,
    /// Checksum of `sql` at compile time.
    pub checksum: u64,
}

/// Declares one registry entry, embedding the SQL file.
macro_rules! migration {
    ($version:literal, $name:literal, $path:literal) => {
        Migration {
            version: $version,
            name: $name,
            sql: include_str!($path),
            checksum: fnv1a(include_str!($path).as_bytes()),
        }
    };
}

/// The complete, ordered registry. `NNNN_name` — strict tree order.
pub static MIGRATIONS: &[Migration] = &[
    migration!(
        1,
        "extensions",
        "../../../migrations/0001_extensions/up.sql"
    ),
    migration!(2, "tenancy", "../../../migrations/0002_tenancy/up.sql"),
    migration!(3, "identity", "../../../migrations/0003_identity/up.sql"),
    migration!(4, "agents", "../../../migrations/0004_agents/up.sql"),
    migration!(5, "workflows", "../../../migrations/0005_workflows/up.sql"),
    migration!(6, "execution", "../../../migrations/0006_execution/up.sql"),
    migration!(7, "policy", "../../../migrations/0007_policy/up.sql"),
    migration!(8, "billing", "../../../migrations/0008_billing/up.sql"),
    migration!(9, "schedules", "../../../migrations/0009_schedules/up.sql"),
    migration!(
        10,
        "integrations",
        "../../../migrations/0010_integrations/up.sql"
    ),
    migration!(11, "audit", "../../../migrations/0011_audit/up.sql"),
    migration!(12, "outbox", "../../../migrations/0012_outbox/up.sql"),
];

/// A migration already recorded in the database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedMigration {
    /// Applied version.
    pub version: u32,
    /// Recorded name.
    pub name: String,
    /// Recorded checksum of the SQL at apply time.
    pub checksum: u64,
}

/// Why a plan cannot be produced — every variant is a hard stop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationPlanError {
    /// The embedded registry itself is broken (build/test bug).
    RegistryInvalid {
        /// What's wrong.
        reason: String,
    },
    /// The database recorded a version this binary does not know (DB ahead).
    UnknownApplied {
        /// The unknown version.
        version: u32,
    },
    /// An applied version is missing an earlier registry version (gap).
    GapInApplied {
        /// The first un-applied registry version below an applied one.
        version: u32,
    },
    /// SQL text drifted after the migration was applied (check the record).
    ChecksumMismatch {
        /// Drifting version.
        version: u32,
        /// Checksum recorded in the database.
        recorded: u64,
        /// Checksum of the embedded SQL.
        expected: u64,
    },
}

impl fmt::Display for MigrationPlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RegistryInvalid { reason } => {
                write!(f, "migration registry is invalid: {reason}")
            },
            Self::UnknownApplied { version } => write!(
                f,
                "database has applied migration v{version} unknown to this binary \
                 (database is ahead of the code — refuse to run)"
            ),
            Self::GapInApplied { version } => write!(
                f,
                "applied migrations are not a clean prefix: v{version} is unapplied \
                 while later versions exist in the database"
            ),
            Self::ChecksumMismatch {
                version,
                recorded,
                expected,
            } => write!(
                f,
                "applied migration v{version} no longer matches its recorded SQL \
                 (recorded checksum {recorded:#x}, embedded {expected:#x}); \
                 never hand-edit applied migrations"
            ),
        }
    }
}

impl std::error::Error for MigrationPlanError {}

impl From<MigrationPlanError> for AppError {
    fn from(value: MigrationPlanError) -> Self {
        AppError::conflict(value.to_string())
    }
}

/// Validates the embedded registry: contiguous versions starting at 1,
/// ascending order, unique names, non-empty SQL.
pub fn validate_registry() -> std::result::Result<(), MigrationPlanError> {
    let mut names = std::collections::BTreeSet::new();
    for (expected, migration) in (1u32..).zip(MIGRATIONS.iter()) {
        if migration.version != expected {
            return Err(MigrationPlanError::RegistryInvalid {
                reason: format!(
                    "expected version {expected}, found {} ({})",
                    migration.version, migration.name
                ),
            });
        }
        if migration.sql.trim().is_empty() {
            return Err(MigrationPlanError::RegistryInvalid {
                reason: format!(
                    "migration {} ({}) has empty SQL",
                    migration.version, migration.name
                ),
            });
        }
        if !names.insert(migration.name) {
            return Err(MigrationPlanError::RegistryInvalid {
                reason: format!("duplicate migration name {}", migration.name),
            });
        }
    }
    Ok(())
}

/// Computes which migrations must run, given what's recorded as applied.
///
/// Rules: applied entries must form a clean prefix of the registry, every
/// applied checksum must match the embedded SQL, and nothing unknown to this
/// binary may be applied.
pub fn plan_pending(
    applied: &[AppliedMigration],
) -> std::result::Result<Vec<&'static Migration>, MigrationPlanError> {
    validate_registry()?;

    // Sort applied copy by version for prefix comparison.
    let mut applied_sorted = applied.to_vec();
    applied_sorted.sort_by_key(|a| a.version);

    for (index, entry) in applied_sorted.iter().enumerate() {
        let Some(registered) = MIGRATIONS.get(index) else {
            return Err(MigrationPlanError::UnknownApplied {
                version: entry.version,
            });
        };
        if entry.version != registered.version {
            return Err(MigrationPlanError::GapInApplied {
                version: registered.version,
            });
        }
        if entry.checksum != registered.checksum {
            return Err(MigrationPlanError::ChecksumMismatch {
                version: entry.version,
                recorded: entry.checksum,
                expected: registered.checksum,
            });
        }
        if entry.name != registered.name {
            return Err(MigrationPlanError::ChecksumMismatch {
                version: entry.version,
                recorded: 0,
                expected: registered.checksum,
            });
        }
    }

    Ok(MIGRATIONS[applied_sorted.len()..].iter().collect())
}

/// Applies and verifies migrations against a live pool.
#[derive(Debug, Clone)]
pub struct MigrationRunner {
    pool: PgPool,
}

impl MigrationRunner {
    /// Binds a runner to a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Creates the bookkeeping table if absent.
    pub async fn ensure_table(&self) -> Result<()> {
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (
                 version integer PRIMARY KEY,
                 name    text        NOT NULL,
                 checksum text       NOT NULL,
                 applied_at timestamptz NOT NULL DEFAULT now()
             )"
        ))
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    /// Reads what's recorded as applied (unordered; planner sorts).
    pub async fn applied(&self) -> Result<Vec<AppliedMigration>> {
        let rows = sqlx::query(&format!(
            "SELECT version, name, checksum FROM {MIGRATIONS_TABLE} ORDER BY version"
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter()
            .map(|row| {
                let version: i32 = row.try_get("version").map_err(map_sqlx)?;
                let checksum_text: String = row.try_get("checksum").map_err(map_sqlx)?;
                let checksum = u64::from_str_radix(checksum_text.trim_start_matches("0x"), 16)
                    .map_err(|_| AppError::database("corrupt checksum in migration ledger"))?;
                Ok(AppliedMigration {
                    version: u32::try_from(version)
                        .map_err(|_| AppError::database("negative migration version in ledger"))?,
                    name: row.try_get("name").map_err(map_sqlx)?,
                    checksum,
                })
            })
            .collect()
    }

    /// Runs all pending migrations, verifying ledger compatibility first.
    /// Returns the versions applied by this call (empty when up to date).
    pub async fn migrate(&self) -> Result<Vec<u32>> {
        validate_registry().map_err(AppError::from)?;
        self.ensure_table().await?;
        let applied = self.applied().await?;
        let pending = plan_pending(&applied).map_err(AppError::from)?;

        let mut done = Vec::with_capacity(pending.len());
        for migration in pending {
            tracing::info!(
                version = migration.version,
                name = migration.name,
                "applying migration"
            );
            let mut tx = self.pool.begin().await.map_err(map_sqlx)?;
            // Whole migration + ledger insert commit atomically.
            sqlx::raw_sql(migration.sql)
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx)?;
            sqlx::query(&format!(
                "INSERT INTO {MIGRATIONS_TABLE} (version, name, checksum) VALUES ($1, $2, $3)"
            ))
            .bind(i32::try_from(migration.version).unwrap_or(i32::MAX))
            .bind(migration.name)
            .bind(format!("{:#x}", migration.checksum))
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx)?;
            tx.commit().await.map_err(map_sqlx)?;
            done.push(migration.version);
        }
        Ok(done)
    }

    /// Compatibility check only: fails when the ledger disagrees with the
    /// embedded registry (used by `migrate status` and startup probes).
    pub async fn verify(&self) -> Result<()> {
        self.ensure_table().await?;
        let applied = self.applied().await?;
        plan_pending(&applied).map_err(AppError::from)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn applied(version: u32, name: &str, checksum: u64) -> AppliedMigration {
        AppliedMigration {
            version,
            name: name.to_owned(),
            checksum,
        }
    }

    #[test]
    fn registry_is_contiguous_twelve_migrations() {
        validate_registry().expect("registry must be valid");
        assert_eq!(MIGRATIONS.len(), 12);
        assert_eq!(MIGRATIONS.first().map(|m| m.name), Some("extensions"));
        assert_eq!(MIGRATIONS.last().map(|m| m.name), Some("outbox"));
        for migration in MIGRATIONS {
            assert!(
                migration.sql.contains("CREATE"),
                "{} must define schema",
                migration.name
            );
            assert!(migration.checksum != 0);
        }
        // RLS policies live in the SQL set, per platform rule.
        let rls_count: usize = MIGRATIONS
            .iter()
            .filter(|m| m.sql.contains("ROW LEVEL SECURITY"))
            .count();
        assert!(
            rls_count >= 10,
            "every tenant-touching migration must carry RLS policies ({rls_count})"
        );
    }

    #[test]
    fn fnv1a_is_stable_and_sensitive() {
        assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_ne!(fnv1a(b"ab"), fnv1a(b"ba"));
    }

    #[test]
    fn plan_full_then_empty() {
        // Nothing applied → everything pending, in registry order.
        let pending = plan_pending(&[]).expect("plan");
        assert_eq!(pending.len(), MIGRATIONS.len());
        assert_eq!(pending[0].version, 1);

        // Full clean prefix applied → nothing pending.
        let full: Vec<AppliedMigration> = MIGRATIONS
            .iter()
            .map(|m| applied(m.version, m.name, m.checksum))
            .collect();
        assert!(plan_pending(&full).expect("plan").is_empty());

        // Partial prefix → exactly the tail pending.
        let partial = &full[..4];
        let pending = plan_pending(partial).expect("plan");
        assert_eq!(pending.len(), 8);
        assert_eq!(pending[0].name, "workflows");
    }

    #[test]
    fn plan_refuses_every_incompatible_state() {
        let full: Vec<AppliedMigration> = MIGRATIONS
            .iter()
            .map(|m| applied(m.version, m.name, m.checksum))
            .collect();

        // DB ahead of the binary.
        let mut ahead = full.clone();
        ahead.push(applied(13, "future", 1));
        assert!(matches!(
            plan_pending(&ahead),
            Err(MigrationPlanError::UnknownApplied { version: 13 })
        ));

        // Gap: v2 missing but v3 applied (drop index 1 — sorted order keeps prefix).
        let gapped: Vec<AppliedMigration> =
            full.iter().filter(|a| a.version != 2).cloned().collect();
        assert!(matches!(
            plan_pending(&gapped),
            Err(MigrationPlanError::GapInApplied { version: 2 })
        ));

        // Drift on an applied migration.
        let mut drifted = full[..3].to_vec();
        drifted[2].checksum ^= 0xdead;
        assert!(matches!(
            plan_pending(&drifted),
            Err(MigrationPlanError::ChecksumMismatch { version: 3, .. })
        ));

        // Tampered ledger name.
        let mut renamed = full[..2].to_vec();
        renamed[1].name = "tenancy_evil".to_owned();
        assert!(plan_pending(&renamed).is_err());
    }
}
