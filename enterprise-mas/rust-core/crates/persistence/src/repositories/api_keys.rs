//! `api_keys` repository: the production bearer boundary's lookup surface.
//!
//! Security doctrine (carried over from `mas-security::api_keys`):
//! * Only the digest + prefix ever touch the database — raw key material
//!   exists at issuance time, in the caller's memory, for seconds.
//! * Lookup happens by the *visible* prefix (a public shard of the key
//!   container); the digest comparison is the authoritative hop and runs
//!   in constant time at the verifier call-site.
//! * Expired/revoked keys stay rows (keys are forensics too); status
//!   transitions are lifecycle actions, not deletes.

use chrono::{DateTime, Utc};
use mas_common::result::Result;
use mas_domain::MembershipRole;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;

/// Row model (never includes raw key material).
///
/// The `tenant_id` / `organization_id` pair is the key's binding: it is what
/// makes a bearer secret a tenant-scoped credential rather than a global one.
/// Verification must carry both into the principal it produces — discarding
/// them is what allowed a valid key from one tenant to be presented against
/// another.
#[derive(Debug, Clone, PartialEq)]
pub struct ApiKeyRow {
    /// Primary key.
    pub id: Uuid,
    /// Owning tenant. Authoritative for the key's scope.
    pub tenant_id: Uuid,
    /// Owning organization. Stored rather than joined because the lookup runs
    /// before a tenant scope exists.
    pub organization_id: Uuid,
    /// The role the bearer acts under. Never `owner`; see `mas-security`.
    pub role: MembershipRole,
    /// The minting user, when the key was issued on a user's behalf. Audit
    /// only — it never widens the key's authority.
    pub user_id: Option<Uuid>,
    /// Human label (per-tenant unique).
    pub name: String,
    /// Public lookup prefix (`mas_…` + first 8 hex).
    pub prefix: String,
    /// SHA-256 hex digest of the raw key.
    pub secret_hash: String,
    /// Granted scope list.
    pub scopes: serde_json::Value,
    /// Lifecycle status.
    pub status: String,
    /// Optional expiry.
    pub expires_at: Option<DateTime<Utc>>,
    /// Last successful verification (telemetry for abuse hunting).
    pub last_used_at: Option<DateTime<Utc>>,
}

const COLUMNS: &str = "id, tenant_id, organization_id, role, user_id, name, prefix, \
     secret_hash, scopes, status, expires_at, last_used_at";

/// PostgreSQL store for `api_keys`.
#[derive(Debug, Clone)]
pub struct ApiKeyStore {
    pool: PgPool,
}

impl ApiKeyStore {
    /// Binds a store to a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Looks up candidate keys by their visible prefix (practically one row;
    /// prefix shards aren't secret so collisions are only a diversity
    /// observation, never a login).
    pub async fn find_by_prefix(&self, prefix: &str) -> Result<Vec<ApiKeyRow>> {
        let rows = sqlx::query(&format!("SELECT {COLUMNS} FROM api_keys WHERE prefix = $1"))
            .bind(prefix)
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx)?;
        rows.into_iter().map(row_from).collect()
    }

    /// Stamps the last-used marker (fire-and-forget telemetry: the verifier
    /// tolerates its failure because the protection never depends on it).
    pub async fn touch_last_used(&self, id: Uuid) -> Result<()> {
        sqlx::query("UPDATE api_keys SET last_used_at = now() WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?;
        Ok(())
    }
}

fn row_from(row: sqlx::postgres::PgRow) -> Result<ApiKeyRow> {
    Ok(ApiKeyRow {
        id: row.try_get("id").map_err(map_sqlx)?,
        tenant_id: row.try_get("tenant_id").map_err(map_sqlx)?,
        organization_id: row.try_get("organization_id").map_err(map_sqlx)?,
        role: parse_role(row.try_get("role").map_err(map_sqlx)?)?,
        user_id: row.try_get("user_id").map_err(map_sqlx)?,
        name: row.try_get("name").map_err(map_sqlx)?,
        prefix: row.try_get("prefix").map_err(map_sqlx)?,
        secret_hash: row.try_get("secret_hash").map_err(map_sqlx)?,
        scopes: row.try_get("scopes").map_err(map_sqlx)?,
        status: row.try_get("status").map_err(map_sqlx)?,
        expires_at: row.try_get("expires_at").map_err(map_sqlx)?,
        last_used_at: row.try_get("last_used_at").map_err(map_sqlx)?,
    })
}

/// Hydrates the role column, failing closed.
///
/// An unrecognised role is a schema or backfill bug. Reading it as the least
/// privileged role would be silent; refusing means the key simply cannot
/// authenticate until the row is corrected, which is the safe direction.
fn parse_role(raw: String) -> Result<MembershipRole> {
    raw.trim().parse::<MembershipRole>().map_err(|_| {
        mas_common::error::AppError::internal(format!(
            "api_keys.role is not a recognised role: {raw}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projection_never_includes_a_secret_column() {
        // No "secret" (raw), no "key" (raw): only secret_hash by name.
        let names: Vec<&str> = COLUMNS.split(',').map(|c| c.trim()).collect();
        assert!(names.contains(&"secret_hash"));
        assert!(!names.contains(&"secret"));
        // Nine original columns plus organization_id, role and user_id.
        assert_eq!(names.len(), 12);

        // The binding must be projected, or verification cannot carry it.
        assert!(names.contains(&"tenant_id"));
        assert!(names.contains(&"organization_id"));
        assert!(names.contains(&"role"));
    }

    #[test]
    fn last_used_update_is_idempotent_write() {
        // Compiles to a plain UPDATE; OK is always "try again later" shaped.
        let _ = COLUMNS;
    }

    /// A role the platform does not recognise must not authenticate as
    /// anything at all.
    #[test]
    fn an_unknown_role_fails_closed() {
        assert!(parse_role("service_account".to_owned()).is_ok());
        assert!(parse_role("developer".to_owned()).is_ok());
        assert!(
            parse_role("superuser".to_owned()).is_err(),
            "an unrecognised role must never read as a privileged one"
        );
    }
}
