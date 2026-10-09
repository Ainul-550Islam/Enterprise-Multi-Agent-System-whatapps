//! PURPOSE: PostgreSQL metadata adapter for externally stored OAuth PKCE verifiers.
//!
//! Owning layer: `mas-persistence` OAuth request/callback boundary.
//!
//! Production role: create tenant-scoped canonical `secret_references` for
//! one-time PKCE material in the request transaction, then retrieve/delete its
//! external material only after an OAuth state has been atomically consumed.
//!
//! Security boundary: the request pool is RLS-scoped; callback reads/deletes
//! use only the restricted `mas_oauth` pool. Raw verifier bytes never enter SQL,
//! logs, errors, or this type's fields. A missing reference is indistinguishable
//! from one already deleted.
//!
//! Data ownership: PostgreSQL owns reference lifecycle metadata; the injected
//! OAuthPkceMaterialPort owns bytes in the external secret manager.
//!
//! Prohibited responsibilities: token persistence, OAuth code exchange,
//! callback state lookup, generic secret-manager path access, or a fallback
//! in-memory verifier cache.

use mas_common::error::AppError;
use mas_common::ids::{SecretReferenceId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{SecretAvailability, SecretProviderKind, SecretReference, SecretRotationState};
use mas_integrations::durable_oauth::{
    oauth_pkce_reference_locator, OAuthPkceMaterialPort, OAuthPkceSecretPort,
};
use mas_integrations::oauth::TokenSecret;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;

/// Durable PKCE verifier adapter with deliberately split request/callback pools.
#[derive(Debug)]
pub struct PostgresOAuthPkceSecretStore<M>
where
    M: OAuthPkceMaterialPort,
{
    request_pool: PgPool,
    callback_pool: PgPool,
    material: M,
}

impl<M> PostgresOAuthPkceSecretStore<M>
where
    M: OAuthPkceMaterialPort,
{
    /// `request_pool` must be the normal `mas_app` pool used through
    /// `scoped_request`; `callback_pool` must authenticate as `mas_oauth`.
    #[must_use]
    pub const fn new(request_pool: PgPool, callback_pool: PgPool, material: M) -> Self {
        Self {
            request_pool,
            callback_pool,
            material,
        }
    }

    /// Request-pool health/composition accessor. Callers still must establish
    /// a task-local authoritative RLS scope before using the write port.
    #[must_use]
    pub const fn request_pool(&self) -> &PgPool {
        &self.request_pool
    }

    /// Callback-pool health accessor; it exposes no arbitrary query API.
    #[must_use]
    pub const fn callback_pool(&self) -> &PgPool {
        &self.callback_pool
    }

    async fn callback_reference(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
    ) -> Result<Option<SecretReference>> {
        let row = sqlx::query(
            "SELECT id, tenant_id, provider, key_version, rotation_state, availability
             FROM secret_references
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant_id.into_uuid())
        .bind(reference_id.into_uuid())
        .fetch_optional(&self.callback_pool)
        .await
        .map_err(map_sqlx)?;
        row.map(|row| {
            let stored_id =
                SecretReferenceId::from_uuid(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?);
            let stored_tenant =
                TenantId::from_uuid(row.try_get::<Uuid, _>("tenant_id").map_err(map_sqlx)?);
            if stored_id != reference_id || stored_tenant != tenant_id {
                return Err(AppError::database(
                    "OAuth PKCE reference lookup returned a foreign or mismatched row",
                ));
            }
            let provider: String = row.try_get("provider").map_err(map_sqlx)?;
            let provider = provider
                .parse::<SecretProviderKind>()
                .map_err(|_| AppError::database("stored OAuth PKCE secret provider is invalid"))?;
            let rotation_state: String = row.try_get("rotation_state").map_err(map_sqlx)?;
            let rotation_state = rotation_state
                .parse::<SecretRotationState>()
                .map_err(|_| AppError::database("stored OAuth PKCE rotation state is invalid"))?;
            let availability: String = row.try_get("availability").map_err(map_sqlx)?;
            let availability = availability
                .parse::<SecretAvailability>()
                .map_err(|_| AppError::database("stored OAuth PKCE availability is invalid"))?;
            Ok(SecretReference {
                id: stored_id,
                provider,
                path: oauth_pkce_reference_locator(stored_id),
                key_version: row.try_get("key_version").map_err(map_sqlx)?,
                rotation_state,
                availability,
            })
        })
        .transpose()
    }

    async fn mark_deleted(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
    ) -> Result<()> {
        let result = sqlx::query(
            "UPDATE secret_references
             SET availability = 'deleted', updated_at = now()
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant_id.into_uuid())
        .bind(reference_id.into_uuid())
        .execute(&self.callback_pool)
        .await
        .map_err(map_sqlx)?;
        if result.rows_affected() != 1 {
            return Err(AppError::database(
                "OAuth PKCE reference disappeared before lifecycle settlement",
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<M> OAuthPkceSecretPort for PostgresOAuthPkceSecretStore<M>
where
    M: OAuthPkceMaterialPort + 'static,
{
    async fn store_verifier(
        &self,
        tenant_id: TenantId,
        verifier: &TokenSecret,
        expires_at: Timestamp,
    ) -> Result<SecretReferenceId> {
        if crate::scoped_request::current_scope()?.tenant_id() != tenant_id {
            return Err(AppError::not_found("oauth_pkce", "reference unavailable"));
        }
        let reference_id = SecretReferenceId::new();
        let reference = self
            .material
            .write_verifier(tenant_id, reference_id, verifier, expires_at)
            .await?;
        if let Err(error) = validate_material_reference(&reference, reference_id) {
            let _ = self.material.delete_verifier(tenant_id, &reference).await;
            return Err(error);
        }

        let mut scoped = match crate::scoped_request::connection().await {
            Ok(scoped) => scoped,
            Err(error) => {
                // The verifier was written before SQL metadata so the durable
                // state can refer only to a successfully materialized secret.
                // If acquiring the authoritative RLS transaction fails, this
                // adapter—not its caller, which never received the id—still
                // owns compensating the deterministic external write.
                if self
                    .material
                    .delete_verifier(tenant_id, &reference)
                    .await
                    .is_err()
                {
                    tracing::warn!(
                        outcome = "pkce_metadata_connection_cleanup_failed",
                        "OAuth PKCE verifier compensation failed"
                    );
                }
                return Err(error);
            },
        };
        let inserted = sqlx::query(
            "INSERT INTO secret_references (
                 id, tenant_id, provider, path, key_version, rotation_state,
                 availability, created_at, updated_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, now(), now())",
        )
        .bind(reference.id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(reference.provider.to_string())
        .bind(&reference.path)
        .bind(&reference.key_version)
        .bind(reference.rotation_state.to_string())
        .bind(reference.availability.to_string())
        .execute(scoped.connection().as_mut())
        .await;
        match inserted {
            Ok(result) if result.rows_affected() == 1 => Ok(reference_id),
            Ok(_) => {
                let _ = self.material.delete_verifier(tenant_id, &reference).await;
                Err(AppError::database(
                    "OAuth PKCE metadata insert did not affect exactly one row",
                ))
            },
            Err(error) => {
                let _ = self.material.delete_verifier(tenant_id, &reference).await;
                Err(map_sqlx(error))
            },
        }
    }

    async fn consume_verifier(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
    ) -> Result<Option<TokenSecret>> {
        let Some(reference) = self.callback_reference(tenant_id, reference_id).await? else {
            return Ok(None);
        };
        if !reference.is_readable() {
            return Ok(None);
        }
        let verifier = self
            .material
            .consume_verifier(tenant_id, &reference)
            .await?;
        // The external manager delete is confirmed before this update. Marking
        // metadata deleted makes any later resolver fail closed even if an
        // administrator replays a stale reference id.
        self.mark_deleted(tenant_id, reference_id).await?;
        Ok(verifier)
    }

    async fn discard_verifier(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
    ) -> Result<()> {
        let Some(reference) = self.callback_reference(tenant_id, reference_id).await? else {
            // State creation can fail after external write but before the RLS
            // transaction commits metadata. The material adapter derives a
            // constrained path from the generated id and can still erase it.
            return self
                .material
                .delete_unstored_verifier(tenant_id, reference_id)
                .await;
        };
        self.material.delete_verifier(tenant_id, &reference).await?;
        self.mark_deleted(tenant_id, reference_id).await
    }
}

fn validate_material_reference(
    reference: &SecretReference,
    expected_id: SecretReferenceId,
) -> Result<()> {
    if reference.id != expected_id
        || reference.path != oauth_pkce_reference_locator(expected_id)
        || reference.path.len() > 1024
        || reference.path.chars().any(char::is_control)
        || !reference.is_readable()
        || reference.key_version.as_deref().is_some_and(|version| {
            version.is_empty() || version.len() > 128 || version.chars().any(char::is_control)
        })
    {
        return Err(AppError::invalid_field(
            "pkce_verifier_reference",
            "invalid_reference",
            "secret manager returned invalid OAuth PKCE reference metadata",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn material_reference_validation_rejects_unreadable_or_mismatched_metadata() {
        let expected = SecretReferenceId::new();
        let mut reference = SecretReference {
            id: expected,
            provider: mas_domain::SecretProviderKind::Vault,
            path: oauth_pkce_reference_locator(expected),
            key_version: None,
            rotation_state: mas_domain::SecretRotationState::Current,
            availability: SecretAvailability::Available,
        };
        assert!(validate_material_reference(&reference, expected).is_ok());
        reference.mark_deleted();
        assert!(validate_material_reference(&reference, expected).is_err());
    }
}
