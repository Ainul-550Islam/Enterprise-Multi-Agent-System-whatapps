//! PURPOSE: PostgreSQL adapters for canonical secret-reference metadata.
//!
//! Owning layer: `mas-persistence` infrastructure adapter.
//!
//! Production role: hydrate tenant-bound `SecretReference` metadata for the
//! dedicated integrations relay without exposing secret material, and provide
//! the request-plane scoped lookup used by authorized integration services.
//!
//! Security boundary: metadata is protected by FORCE RLS in request traffic.
//! The relay lookup is explicitly separate and must use the narrowly granted
//! `mas_integrations` pool; it can read reference metadata only, never
//! credentials or raw secret values.
//!
//! Data ownership: PostgreSQL owns reference lifecycle metadata. The external
//! secret manager owns the actual bytes and version contents.
//!
//! Prohibited responsibilities: material resolution, caching, writing secrets,
//! granting a request access based solely on an id, or using mas_worker.

use mas_common::error::AppError;
use mas_common::ids::{SecretReferenceId, TenantId};
use mas_common::result::Result;
use mas_domain::{SecretAvailability, SecretProviderKind, SecretReference, SecretRotationState};
use mas_integrations::secrets::{SecretReferenceLookupPort, SecretReferenceRecord};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;

const REFERENCE_COLUMNS: &str =
    "id, tenant_id, provider, path, key_version, rotation_state, availability";

/// Request-plane reference metadata store. It borrows the task-local scoped
/// transaction and therefore cannot enumerate another tenant's references.
#[derive(Debug, Clone)]
pub struct PostgresSecretReferenceStore {
    pool: PgPool,
}

impl PostgresSecretReferenceStore {
    /// Binds the scoped lookup adapter to the application's pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Pool used by composition to open an authoritative request scope.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Relay-plane reference metadata lookup. This type is intentionally distinct
/// from [`PostgresSecretReferenceStore`] so a cross-tenant query cannot be
/// called accidentally from a request handler.
#[derive(Debug, Clone)]
pub struct PostgresRelaySecretReferenceLookup {
    pool: PgPool,
}

impl PostgresRelaySecretReferenceLookup {
    /// Binds the cross-tenant lookup to the dedicated integrations pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl SecretReferenceLookupPort for PostgresSecretReferenceStore {
    async fn get(&self, id: SecretReferenceId) -> Result<Option<SecretReferenceRecord>> {
        let tenant = crate::scoped_request::current_scope()?.tenant_id();
        let mut scoped = crate::scoped_request::connection().await?;
        let row = sqlx::query(&format!(
            "SELECT {REFERENCE_COLUMNS}
             FROM secret_references
             WHERE tenant_id = $1 AND id = $2"
        ))
        .bind(tenant.into_uuid())
        .bind(id.into_uuid())
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(reference_from_row).transpose()
    }
}

#[async_trait::async_trait]
impl SecretReferenceLookupPort for PostgresRelaySecretReferenceLookup {
    async fn get(&self, id: SecretReferenceId) -> Result<Option<SecretReferenceRecord>> {
        let row = sqlx::query(&format!(
            "SELECT {REFERENCE_COLUMNS}
             FROM secret_references
             WHERE id = $1"
        ))
        .bind(id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        row.map(reference_from_row).transpose()
    }
}

pub(crate) fn reference_from_row(row: sqlx::postgres::PgRow) -> Result<SecretReferenceRecord> {
    let provider: String = row.try_get("provider").map_err(map_sqlx)?;
    let rotation_state: String = row.try_get("rotation_state").map_err(map_sqlx)?;
    let availability: String = row.try_get("availability").map_err(map_sqlx)?;
    let reference = SecretReference {
        id: SecretReferenceId::from_uuid(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?),
        provider: provider
            .parse::<SecretProviderKind>()
            .map_err(|_| AppError::database("stored secret provider is invalid"))?,
        path: row.try_get("path").map_err(map_sqlx)?,
        key_version: row.try_get("key_version").map_err(map_sqlx)?,
        rotation_state: rotation_state
            .parse::<SecretRotationState>()
            .map_err(|_| AppError::database("stored secret rotation state is invalid"))?,
        availability: availability
            .parse::<SecretAvailability>()
            .map_err(|_| AppError::database("stored secret availability is invalid"))?,
    };
    if reference.path.is_empty()
        || reference.path.len() > 1024
        || reference.path.chars().any(char::is_control)
    {
        return Err(AppError::database(
            "stored secret reference path violates domain safety bounds",
        ));
    }
    Ok(SecretReferenceRecord {
        tenant_id: TenantId::from_uuid(row.try_get::<Uuid, _>("tenant_id").map_err(map_sqlx)?),
        reference,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adapters_have_distinct_types_for_request_and_relay_planes() {
        let request = std::any::type_name::<PostgresSecretReferenceStore>();
        let relay = std::any::type_name::<PostgresRelaySecretReferenceLookup>();
        assert_ne!(request, relay);
    }
}
