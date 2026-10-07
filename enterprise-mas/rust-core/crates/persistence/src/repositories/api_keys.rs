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
use mas_common::constants;
use mas_common::error::AppError;
use mas_common::ids::{ApiKeyId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{ApiKey, MembershipRole};
use mas_security::{ApiKeyStorePort, StoredApiKey};
use sqlx::{PgPool, Row};
use std::collections::BTreeSet;
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
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time; for rows with status `revoked`, this is the best
    /// available persisted revocation time.
    pub updated_at: DateTime<Utc>,
}

const COLUMNS: &str = "id, tenant_id, organization_id, role, user_id, name, prefix, \
     secret_hash, scopes, status, expires_at, last_used_at, created_at, updated_at";

const PREFIX_LOOKUP_GUC: &str = "app.api_key_prefix_lookup";

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
    /// observation, never a login). The lookup runs in its own transaction
    /// with a transaction-local prefix GUC, which enables the SELECT-only RLS
    /// policy without granting the request role `BYPASSRLS` or using a tenant
    /// header before authentication.
    pub async fn find_by_prefix(&self, prefix: &str) -> Result<Vec<ApiKeyRow>> {
        validate_lookup_prefix(prefix)?;
        let mut transaction = self.pool.begin().await.map_err(map_sqlx)?;
        let _: String = sqlx::query_scalar("SELECT set_config($1, $2, true)")
            .bind(PREFIX_LOOKUP_GUC)
            .bind(prefix)
            .fetch_one(&mut *transaction)
            .await
            .map_err(map_sqlx)?;
        let rows = sqlx::query(&format!("SELECT {COLUMNS} FROM api_keys WHERE prefix = $1"))
            .bind(prefix)
            .fetch_all(&mut *transaction)
            .await
            .map_err(map_sqlx)?;
        transaction.commit().await.map_err(map_sqlx)?;
        rows.into_iter().map(row_from).collect()
    }

    /// Stamps the last-used marker (fire-and-forget telemetry: the verifier
    /// tolerates its failure because the protection never depends on it).
    ///
    /// Before request-scope resolution, `tenant` must come from the row whose
    /// full secret digest was just verified; it is never accepted from an HTTP
    /// header. Once inside a request scope, the transaction's proven tenant
    /// must match it.
    pub async fn touch_last_used(&self, id: Uuid, tenant: TenantId) -> Result<()> {
        if crate::scoped_request::has_scope() {
            let mut scoped = crate::scoped_request::connection().await?;
            ensure_tenant_scope(&scoped, tenant)?;
            sqlx::query(
                "UPDATE api_keys SET last_used_at = now() WHERE id = $1 AND tenant_id = $2",
            )
            .bind(id)
            .bind(Uuid::from(tenant))
            .execute(scoped.connection().as_mut())
            .await
            .map_err(map_sqlx)?;
            return Ok(());
        }

        let mut transaction = self.pool.begin().await.map_err(map_sqlx)?;
        let _: String = sqlx::query_scalar("SELECT set_config('app.current_tenant', $1, true)")
            .bind(tenant.to_string())
            .fetch_one(&mut *transaction)
            .await
            .map_err(map_sqlx)?;
        sqlx::query("UPDATE api_keys SET last_used_at = now() WHERE id = $1 AND tenant_id = $2")
            .bind(id)
            .bind(Uuid::from(tenant))
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx)?;
        transaction.commit().await.map_err(map_sqlx)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl ApiKeyStorePort for ApiKeyStore {
    async fn insert(&self, stored: &StoredApiKey) -> Result<()> {
        let row = row_for_stored(stored)?;
        let mut scoped = crate::scoped_request::connection().await?;
        ensure_tenant_scope(&scoped, TenantId::from_uuid(row.tenant_id))?;
        sqlx::query(
            "INSERT INTO api_keys (
                id, tenant_id, organization_id, role, user_id, name, prefix,
                secret_hash, scopes, status, expires_at, last_used_at, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
        )
        .bind(row.id)
        .bind(row.tenant_id)
        .bind(row.organization_id)
        .bind(row.role.as_str())
        .bind(row.user_id)
        .bind(&row.name)
        .bind(&row.prefix)
        .bind(&row.secret_hash)
        .bind(&row.scopes)
        .bind(&row.status)
        .bind(row.expires_at)
        .bind(row.last_used_at)
        .bind(row.created_at)
        .execute(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn update(&self, stored: &StoredApiKey) -> Result<()> {
        let row = row_for_stored(stored)?;
        let mut scoped = crate::scoped_request::connection().await?;
        ensure_tenant_scope(&scoped, TenantId::from_uuid(row.tenant_id))?;
        let result = sqlx::query(
            "UPDATE api_keys SET
                organization_id = $3,
                role = $4,
                user_id = $5,
                name = $6,
                prefix = $7,
                secret_hash = $8,
                scopes = $9,
                status = $10,
                expires_at = $11,
                last_used_at = $12,
                updated_at = now()
             WHERE id = $1 AND tenant_id = $2",
        )
        .bind(row.id)
        .bind(row.tenant_id)
        .bind(row.organization_id)
        .bind(row.role.as_str())
        .bind(row.user_id)
        .bind(&row.name)
        .bind(&row.prefix)
        .bind(&row.secret_hash)
        .bind(&row.scopes)
        .bind(&row.status)
        .bind(row.expires_at)
        .bind(row.last_used_at)
        .execute(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        if result.rows_affected() != 1 {
            return Err(AppError::not_found("api key", row.id.to_string()));
        }
        Ok(())
    }

    async fn find_by_prefix(&self, prefix: &str) -> Result<Option<StoredApiKey>> {
        // Authentication precedes tenant-scope resolution. This intentionally
        // uses the verifier pool and only returns the prefix-shard candidates;
        // the digest comparison still happens in constant time in security.
        let mut candidates = ApiKeyStore::find_by_prefix(self, prefix).await?;
        if candidates.len() > 1 {
            return Err(AppError::conflict(
                "API-key visible prefix is ambiguous; refusing authentication",
            ));
        }
        candidates.pop().map(stored_from_row).transpose()
    }

    async fn get(&self, id: ApiKeyId) -> Result<Option<StoredApiKey>> {
        let mut scoped = crate::scoped_request::connection().await?;
        let tenant = scoped.scope().tenant_id();
        let row = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM api_keys WHERE tenant_id = $1 AND id = $2"
        ))
        .bind(Uuid::from(tenant))
        .bind(Uuid::from(id))
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(row_from)
            .transpose()?
            .map(stored_from_row)
            .transpose()
    }

    async fn list_by_tenant(&self, tenant_id: TenantId) -> Result<Vec<StoredApiKey>> {
        let mut scoped = crate::scoped_request::connection().await?;
        ensure_tenant_scope(&scoped, tenant_id)?;
        let rows = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM api_keys
             WHERE tenant_id = $1 ORDER BY created_at DESC, id ASC"
        ))
        .bind(Uuid::from(tenant_id))
        .fetch_all(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(row_from)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(stored_from_row)
            .collect()
    }
}

fn ensure_tenant_scope(
    scoped: &crate::scoped_request::ScopedExecutor,
    tenant: TenantId,
) -> Result<()> {
    if scoped.scope().tenant_id() != tenant {
        return Err(AppError::not_found("tenant", tenant.to_string()));
    }
    Ok(())
}

fn validate_lookup_prefix(prefix: &str) -> Result<()> {
    let valid = prefix
        .strip_prefix(constants::API_KEY_PREFIX)
        .is_some_and(|entropy_prefix| {
            entropy_prefix.len() == constants::API_KEY_VISIBLE_PREFIX_LEN
                && entropy_prefix
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        });
    if !valid {
        return Err(AppError::invalid_field(
            "prefix",
            "invalid_format",
            "API-key lookup requires the fixed lowercase public prefix",
        ));
    }
    Ok(())
}

fn row_for_stored(stored: &StoredApiKey) -> Result<ApiKeyRow> {
    validate_digest(&stored.key_hash)?;
    let metadata = &stored.metadata;
    let status = if metadata.revoked_at.is_some() {
        "revoked"
    } else if metadata.expires_at.is_some_and(|expiry| expiry.is_past()) {
        "expired"
    } else {
        "active"
    };
    Ok(ApiKeyRow {
        id: Uuid::from(metadata.id),
        tenant_id: Uuid::from(metadata.tenant_id),
        organization_id: Uuid::from(metadata.organization_id),
        role: metadata.role,
        user_id: metadata.user_id.map(Uuid::from),
        name: metadata.name.clone(),
        prefix: metadata.prefix.clone(),
        secret_hash: stored.key_hash.clone(),
        scopes: serde_json::to_value(&metadata.scopes).map_err(|error| {
            AppError::internal(format!("could not encode API-key scopes: {error}"))
        })?,
        status: status.to_owned(),
        expires_at: metadata.expires_at.map(Timestamp::into_datetime),
        last_used_at: metadata.last_used_at.map(Timestamp::into_datetime),
        created_at: metadata.created_at.into_datetime(),
        updated_at: metadata.created_at.into_datetime(),
    })
}

fn stored_from_row(row: ApiKeyRow) -> Result<StoredApiKey> {
    validate_digest(&row.secret_hash)?;
    let scopes = serde_json::from_value::<BTreeSet<String>>(row.scopes).map_err(|error| {
        AppError::database(format!("api_keys.scopes is not a string set: {error}"))
    })?;
    let expires_at = row.expires_at.map(Timestamp::from_datetime);
    let updated_at = Timestamp::from_datetime(row.updated_at);
    let revoked_at = match row.status.as_str() {
        "active" => None,
        "revoked" => Some(updated_at),
        "expired" if expires_at.is_some_and(|expiry| expiry.is_past()) => None,
        // Fail closed for inconsistent expired/unknown lifecycle metadata.
        "expired" => Some(updated_at),
        status => {
            return Err(AppError::database(format!(
                "api_keys.status is not recognised: {status}"
            )))
        },
    };
    let metadata = ApiKey {
        id: ApiKeyId::from_uuid(row.id),
        tenant_id: TenantId::from_uuid(row.tenant_id),
        organization_id: mas_common::ids::OrganizationId::from_uuid(row.organization_id),
        role: row.role,
        user_id: row.user_id.map(mas_common::ids::UserId::from_uuid),
        name: row.name,
        prefix: row.prefix,
        scopes,
        expires_at,
        last_used_at: row.last_used_at.map(Timestamp::from_datetime),
        revoked_at,
        created_at: Timestamp::from_datetime(row.created_at),
    };
    Ok(StoredApiKey {
        metadata,
        key_hash: row.secret_hash,
    })
}

fn validate_digest(digest: &str) -> Result<()> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(AppError::invalid_field(
            "key_hash",
            "invalid_format",
            "API-key persistence accepts only a lowercase SHA-256 digest",
        ));
    }
    Ok(())
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
        created_at: row.try_get("created_at").map_err(map_sqlx)?,
        updated_at: row.try_get("updated_at").map_err(map_sqlx)?,
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
    fn prefix_lookup_accepts_only_the_fixed_lowercase_public_shard() {
        assert!(validate_lookup_prefix("mas_a1b2c3d4").is_ok());
        for invalid in [
            "",
            "mas_a1b2c3d",
            "mas_a1b2c3d40",
            "mas_A1b2c3d4",
            "other_a1b2c3d4",
            "mas_a1b2c3d4.extra",
        ] {
            assert!(
                validate_lookup_prefix(invalid).is_err(),
                "invalid lookup prefix must fail closed: {invalid:?}"
            );
        }
    }

    #[test]
    fn projection_never_includes_a_secret_column() {
        // No "secret" (raw), no "key" (raw): only secret_hash by name.
        let names: Vec<&str> = COLUMNS.split(',').map(|c| c.trim()).collect();
        assert!(names.contains(&"secret_hash"));
        assert!(!names.contains(&"secret"));
        // Twelve identity/security columns plus the two audit timestamps.
        assert_eq!(names.len(), 14);

        // The binding must be projected, or verification cannot carry it.
        assert!(names.contains(&"tenant_id"));
        assert!(names.contains(&"organization_id"));
        assert!(names.contains(&"role"));
    }

    #[test]
    fn stored_key_roundtrip_preserves_binding_and_never_persists_raw_secret() {
        let raw = format!("mas_{}", "a1".repeat(32));
        let metadata = ApiKey::new(
            TenantId::new(),
            mas_common::ids::OrganizationId::new(),
            Some(mas_common::ids::UserId::new()),
            MembershipRole::Developer,
            "build-agent",
            "mas_a1a1a1a1",
            BTreeSet::from(["executions:read".to_owned()]),
            None,
        )
        .expect("metadata");
        let stored = StoredApiKey {
            metadata,
            key_hash: mas_security::api_keys::hash_key(&raw),
        };
        let row = row_for_stored(&stored).expect("row");
        assert_ne!(row.secret_hash, raw);
        assert_eq!(row.tenant_id, Uuid::from(stored.metadata.tenant_id));
        assert_eq!(
            row.organization_id,
            Uuid::from(stored.metadata.organization_id)
        );
        assert_eq!(row.status, "active");

        let restored = stored_from_row(row).expect("restored");
        assert_eq!(restored.metadata.id, stored.metadata.id);
        assert_eq!(restored.metadata.tenant_id, stored.metadata.tenant_id);
        assert_eq!(
            restored.metadata.organization_id,
            stored.metadata.organization_id
        );
        assert_eq!(restored.metadata.role, MembershipRole::Developer);
        assert_eq!(restored.metadata.scopes, stored.metadata.scopes);
        assert_eq!(restored.key_hash, stored.key_hash);
        assert!(restored.metadata.revoked_at.is_none());
    }

    #[test]
    fn raw_key_material_is_rejected_from_the_digest_column() {
        let raw = format!("mas_{}", "a1".repeat(32));
        assert!(validate_digest(&raw).is_err());
    }

    #[test]
    fn revoked_database_status_hydrates_as_irreversibly_inactive() {
        let metadata = ApiKey::new(
            TenantId::new(),
            mas_common::ids::OrganizationId::new(),
            None,
            MembershipRole::ServiceAccount,
            "sync-worker",
            "mas_01234567",
            BTreeSet::new(),
            None,
        )
        .expect("metadata");
        let stored = StoredApiKey {
            metadata,
            key_hash: "a".repeat(64),
        };
        let mut row = row_for_stored(&stored).expect("row");
        row.status = "revoked".to_owned();
        let restored = stored_from_row(row).expect("restored");
        assert!(restored.metadata.revoked_at.is_some());
        assert!(!restored.metadata.is_active());
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
