//! PURPOSE: PostgreSQL OAuth callback credential lifecycle adapter backed by
//! external OAuth token material storage.
//!
//! Owning layer: `mas-persistence` OAuth callback security boundary.
//!
//! Production role: write OAuth access/refresh token bytes to the injected
//! secret manager, then atomically create canonical secret-reference metadata,
//! create/revoke credential lifecycle metadata, and bind the new credential to
//! the tenant-proven OAuth2 connector using the dedicated `mas_oauth` pool.
//!
//! Security boundary: this type never receives token strings directly; they
//! remain in TokenBundle/secret-manager ports. PostgreSQL gets reference
//! metadata only. On SQL failure it compensates the deterministic external
//! path. Callback role SQL is limited by migration 0043.
//!
//! Data ownership: secret manager owns token material; PostgreSQL owns
//! reference, credential lifecycle, and connector binding metadata.
//!
//! Prohibited responsibilities: generic credential administration, raw-token
//! logging/persistence, provider HTTP exchange, callback state consumption,
//! request-plane RLS bypass, or silently retaining a successful token bundle
//! without a bound canonical credential.

use mas_common::error::AppError;
use mas_common::ids::{ConnectorId, CredentialId, SecretReferenceId, TenantId};
use mas_common::result::Result;
use mas_domain::{
    ConnectorAuthMode, SecretAvailability, SecretProviderKind, SecretReference, SecretRotationState,
};
use mas_integrations::durable_oauth::{
    oauth_token_reference_locator, DurableAuthorizationState, OAuthCredentialBinding,
    OAuthCredentialPort, OAuthTokenMaterialPort,
};
use mas_integrations::oauth::TokenBundle;
use serde_json::json;
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;

const MATERIALIZATION_GRACE_SECONDS: i64 = 300;

/// Callback-plane OAuth credential lifecycle with external token material.
#[derive(Debug)]
pub struct PostgresOAuthCredentialStore<M>
where
    M: OAuthTokenMaterialPort,
{
    callback_pool: PgPool,
    material: M,
}

impl<M> PostgresOAuthCredentialStore<M>
where
    M: OAuthTokenMaterialPort,
{
    /// `callback_pool` must authenticate as `mas_oauth`; it is never the
    /// normal request pool and callers cannot supply a transaction/tenant.
    #[must_use]
    pub const fn new(callback_pool: PgPool, material: M) -> Self {
        Self {
            callback_pool,
            material,
        }
    }

    /// Callback-pool readiness/composition accessor only.
    #[must_use]
    pub const fn callback_pool(&self) -> &PgPool {
        &self.callback_pool
    }

    /// Persists the callback-to-secret-manager intent before external material
    /// is written. A later process/connection failure cannot strand material:
    /// the fenced reconciler either proves its credential binding or performs
    /// deterministic idempotent deletion after this grace window.
    async fn prepare_materialization_intent(
        &self,
        tenant_id: TenantId,
        connector_id: ConnectorId,
        reference_id: SecretReferenceId,
    ) -> Result<()> {
        let inserted = sqlx::query(
            "INSERT INTO oauth_token_materialization_jobs (
                 secret_reference_id, tenant_id, connector_id, state, attempts,
                 next_attempt_at, created_at, updated_at
             ) VALUES (
                 $1, $2, $3, 'pending', 0,
                 now() + ($4 * interval '1 second'), now(), now()
             )",
        )
        .bind(reference_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(connector_id.into_uuid())
        .bind(MATERIALIZATION_GRACE_SECONDS)
        .execute(&self.callback_pool)
        .await
        .map_err(map_sqlx)?;
        if inserted.rows_affected() != 1 {
            return Err(AppError::database(
                "OAuth materialization intent insert did not affect exactly one row",
            ));
        }
        Ok(())
    }

    /// Locks the prepared intent for the complete credential-binding
    /// transaction. A reconciler that already leased/settled the intent makes
    /// the callback fail closed instead of binding material it may have deleted.
    async fn lock_materialization_intent(
        connection: &mut PgConnection,
        tenant_id: TenantId,
        connector_id: ConnectorId,
        reference_id: SecretReferenceId,
    ) -> Result<()> {
        let row = sqlx::query(
            "SELECT state
             FROM oauth_token_materialization_jobs
             WHERE secret_reference_id = $1
               AND tenant_id = $2
               AND connector_id = $3
             FOR UPDATE",
        )
        .bind(reference_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(connector_id.into_uuid())
        .fetch_optional(&mut *connection)
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| AppError::conflict("OAuth materialization intent is unavailable"))?;
        let state: String = row.try_get("state").map_err(map_sqlx)?;
        if state != "pending" {
            return Err(AppError::conflict(
                "OAuth materialization intent was already reconciled",
            ));
        }
        Ok(())
    }

    /// Terminally binds the intent in the same transaction as the canonical
    /// credential/reference rows. This update must never be moved post-commit.
    async fn bind_materialization_intent(
        connection: &mut PgConnection,
        tenant_id: TenantId,
        connector_id: ConnectorId,
        credential_id: CredentialId,
        reference_id: SecretReferenceId,
    ) -> Result<()> {
        let updated = sqlx::query(
            "UPDATE oauth_token_materialization_jobs
             SET credential_id = $4,
                 state = 'bound',
                 next_attempt_at = NULL,
                 completed_at = now(),
                 updated_at = now()
             WHERE secret_reference_id = $1
               AND tenant_id = $2
               AND connector_id = $3
               AND state = 'pending'",
        )
        .bind(reference_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(connector_id.into_uuid())
        .bind(credential_id.into_uuid())
        .execute(&mut *connection)
        .await
        .map_err(map_sqlx)?;
        if updated.rows_affected() != 1 {
            return Err(AppError::conflict(
                "OAuth materialization intent was superseded before binding",
            ));
        }
        Ok(())
    }

    async fn old_reference(
        connection: &mut PgConnection,
        tenant_id: TenantId,
        connector_id: ConnectorId,
        credential_id: CredentialId,
    ) -> Result<Option<SecretReference>> {
        let row = sqlx::query(
            "SELECT
                 credential.purpose AS credential_purpose,
                 reference.id AS id,
                 reference.tenant_id AS tenant_id,
                 reference.provider AS provider,
                 reference.key_version AS key_version,
                 reference.rotation_state AS rotation_state,
                 reference.availability AS availability
             FROM credentials AS credential
             JOIN secret_references AS reference
               ON reference.tenant_id = credential.tenant_id
              AND reference.id = credential.secret_reference_id
             WHERE credential.tenant_id = $1
               AND credential.connector_id = $2
               AND credential.id = $3
             FOR UPDATE",
        )
        .bind(tenant_id.into_uuid())
        .bind(connector_id.into_uuid())
        .bind(credential_id.into_uuid())
        .fetch_optional(&mut *connection)
        .await
        .map_err(map_sqlx)?;
        row.map(|row| {
            let purpose: String = row.try_get("credential_purpose").map_err(map_sqlx)?;
            // A pre-canonical generic secret must never be reinterpreted as an
            // OAuth token just because a connector is being re-authorized.
            if purpose != "oauth2_token" {
                return Ok(None);
            }
            let reference_id =
                SecretReferenceId::from_uuid(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?);
            let stored_tenant =
                TenantId::from_uuid(row.try_get::<Uuid, _>("tenant_id").map_err(map_sqlx)?);
            if stored_tenant != tenant_id {
                return Err(AppError::database(
                    "OAuth credential lookup returned a foreign tenant reference",
                ));
            }
            let provider: String = row.try_get("provider").map_err(map_sqlx)?;
            let provider = provider.parse::<SecretProviderKind>().map_err(|_| {
                AppError::database("stored OAuth credential secret provider is invalid")
            })?;
            let rotation_state: String = row.try_get("rotation_state").map_err(map_sqlx)?;
            let rotation_state = rotation_state.parse::<SecretRotationState>().map_err(|_| {
                AppError::database("stored OAuth credential secret rotation state is invalid")
            })?;
            let availability: String = row.try_get("availability").map_err(map_sqlx)?;
            let availability = availability.parse::<SecretAvailability>().map_err(|_| {
                AppError::database("stored OAuth credential secret availability is invalid")
            })?;
            Ok(Some(SecretReference {
                id: reference_id,
                provider,
                path: oauth_token_reference_locator(reference_id),
                key_version: row.try_get("key_version").map_err(map_sqlx)?,
                rotation_state,
                availability,
            }))
        })
        .transpose()
        .map(|reference| reference.flatten())
    }

    async fn bind_in_transaction(
        &self,
        connection: &mut PgConnection,
        authorization: &DurableAuthorizationState,
        reference: &SecretReference,
        tokens: &TokenBundle,
    ) -> Result<CredentialId> {
        let connector_id = authorization.connector_id.ok_or_else(|| {
            AppError::invalid_field(
                "connector_id",
                "required",
                "OAuth token materialization requires a connector-bound authorization state",
            )
        })?;
        Self::lock_materialization_intent(
            connection,
            authorization.tenant_id,
            connector_id,
            reference.id,
        )
        .await?;
        let connector = sqlx::query(
            "SELECT id, tenant_id, auth_mode, credential_id
             FROM connectors
             WHERE tenant_id = $1 AND id = $2
             FOR UPDATE",
        )
        .bind(authorization.tenant_id.into_uuid())
        .bind(connector_id.into_uuid())
        .fetch_optional(&mut *connection)
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| AppError::not_found("connector", connector_id.to_string()))?;
        let stored_tenant = TenantId::from_uuid(connector.try_get("tenant_id").map_err(map_sqlx)?);
        let auth_mode: String = connector.try_get("auth_mode").map_err(map_sqlx)?;
        let auth_mode = auth_mode
            .parse::<ConnectorAuthMode>()
            .map_err(|_| AppError::database("stored connector auth mode is invalid"))?;
        if stored_tenant != authorization.tenant_id || auth_mode != ConnectorAuthMode::OAuth2 {
            return Err(AppError::conflict(
                "OAuth callback cannot bind a token to this connector",
            ));
        }
        let prior_credential_id = connector
            .try_get::<Option<Uuid>, _>("credential_id")
            .map_err(map_sqlx)?
            .map(CredentialId::from_uuid);
        let prior_reference = match prior_credential_id {
            Some(credential_id) => {
                Self::old_reference(
                    connection,
                    authorization.tenant_id,
                    connector_id,
                    credential_id,
                )
                .await?
            },
            None => None,
        };

        let inserted_reference = sqlx::query(
            "INSERT INTO secret_references (
                 id, tenant_id, provider, path, key_version, rotation_state,
                 availability, created_at, updated_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, now(), now())",
        )
        .bind(reference.id.into_uuid())
        .bind(authorization.tenant_id.into_uuid())
        .bind(reference.provider.to_string())
        .bind(&reference.path)
        .bind(&reference.key_version)
        .bind(reference.rotation_state.to_string())
        .bind(reference.availability.to_string())
        .execute(&mut *connection)
        .await
        .map_err(map_sqlx)?;
        if inserted_reference.rows_affected() != 1 {
            return Err(AppError::database(
                "OAuth token reference metadata insert did not affect exactly one row",
            ));
        }

        let credential_id = CredentialId::new();
        // `credentials.secret_ref` is a legacy NOT NULL compatibility column.
        // New OAuth records retain only the opaque canonical id there; provider,
        // lifecycle, and the non-Vault opaque locator live in the dedicated
        // reference row. In particular, no Vault route is duplicated in JSON.
        let secret_ref = json!({ "id": reference.id.to_string() });
        let name = format!("oauth2-{}", credential_id);
        let inserted_credential = sqlx::query(
            "INSERT INTO credentials (
                 id, tenant_id, connector_id, name, purpose, secret_ref,
                 status, expires_at, secret_reference_id, secret_version,
                 created_at, updated_at
             ) VALUES (
                 $1, $2, $3, $4, 'oauth2_token', $5,
                 'active', $6, $7, $8, now(), now()
             )",
        )
        .bind(credential_id.into_uuid())
        .bind(authorization.tenant_id.into_uuid())
        .bind(connector_id.into_uuid())
        .bind(name)
        .bind(secret_ref)
        .bind(tokens.expires_at().map(|value| value.into_datetime()))
        .bind(reference.id.into_uuid())
        .bind(&reference.key_version)
        .execute(&mut *connection)
        .await
        .map_err(map_sqlx)?;
        if inserted_credential.rows_affected() != 1 {
            return Err(AppError::database(
                "OAuth credential insert did not affect exactly one row",
            ));
        }
        Self::bind_materialization_intent(
            connection,
            authorization.tenant_id,
            connector_id,
            credential_id,
            reference.id,
        )
        .await?;
        if let Some(expiry) = tokens.expires_at() {
            let refresh_job = sqlx::query(
                "INSERT INTO oauth_token_refresh_jobs (
                     credential_id, tenant_id, connector_id, state, attempts,
                     next_attempt_at, created_at, updated_at
                 ) VALUES (
                     $1, $2, $3, 'pending', 0,
                     GREATEST(now(), $4 - interval '300 seconds'), now(), now()
                 )",
            )
            .bind(credential_id.into_uuid())
            .bind(authorization.tenant_id.into_uuid())
            .bind(connector_id.into_uuid())
            .bind(expiry.into_datetime())
            .execute(&mut *connection)
            .await
            .map_err(map_sqlx)?;
            if refresh_job.rows_affected() != 1 {
                return Err(AppError::database(
                    "OAuth refresh job insert did not affect exactly one row",
                ));
            }
        }

        if let Some(prior_credential_id) = prior_credential_id {
            let revoked = sqlx::query(
                "UPDATE credentials
                 SET status = 'revoked', revoked_at = now(), updated_at = now()
                 WHERE tenant_id = $1
                   AND connector_id = $2
                   AND id = $3
                   AND status IN ('active', 'rotating')",
            )
            .bind(authorization.tenant_id.into_uuid())
            .bind(connector_id.into_uuid())
            .bind(prior_credential_id.into_uuid())
            .execute(&mut *connection)
            .await
            .map_err(map_sqlx)?;
            if revoked.rows_affected() > 1 {
                return Err(AppError::database(
                    "OAuth credential revocation affected multiple rows",
                ));
            }
            if revoked.rows_affected() == 1 {
                if let Some(prior_reference) = prior_reference.as_ref() {
                    // A credential-specific canonical OAuth reference is never
                    // synchronously deleted after commit. Persisting this job
                    // in the same transaction makes external cleanup durable
                    // across process failures and marks metadata deleted only
                    // after the separate fenced worker succeeds.
                    if self.material.owns_reference(prior_reference) {
                        let cleanup_job = sqlx::query(
                            "INSERT INTO oauth_token_cleanup_jobs (
                                 credential_id, tenant_id, connector_id,
                                 secret_reference_id, state, attempts,
                                 next_attempt_at, created_at, updated_at
                             ) VALUES (
                                 $1, $2, $3, $4, 'pending', 0,
                                 now(), now(), now()
                             ) ON CONFLICT (credential_id) DO NOTHING",
                        )
                        .bind(prior_credential_id.into_uuid())
                        .bind(authorization.tenant_id.into_uuid())
                        .bind(connector_id.into_uuid())
                        .bind(prior_reference.id.into_uuid())
                        .execute(&mut *connection)
                        .await
                        .map_err(map_sqlx)?;
                        if cleanup_job.rows_affected() > 1 {
                            return Err(AppError::database(
                                "OAuth token cleanup enqueue affected multiple rows",
                            ));
                        }
                    }
                }
            }
        }

        let bound = sqlx::query(
            "UPDATE connectors
             SET credential_id = $3, updated_at = now()
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(authorization.tenant_id.into_uuid())
        .bind(connector_id.into_uuid())
        .bind(credential_id.into_uuid())
        .execute(&mut *connection)
        .await
        .map_err(map_sqlx)?;
        if bound.rows_affected() != 1 {
            return Err(AppError::database(
                "OAuth connector credential binding did not affect exactly one row",
            ));
        }
        Ok(credential_id)
    }
}

#[async_trait::async_trait]
impl<M> OAuthCredentialPort for PostgresOAuthCredentialStore<M>
where
    M: OAuthTokenMaterialPort + 'static,
{
    async fn persist_tokens(
        &self,
        authorization: &DurableAuthorizationState,
        tokens: &TokenBundle,
    ) -> Result<OAuthCredentialBinding> {
        authorization.validate()?;
        if authorization.connector_id.is_none() {
            return Err(AppError::invalid_field(
                "connector_id",
                "required",
                "OAuth token materialization requires a connector-bound authorization state",
            ));
        }
        let connector_id = authorization.connector_id.ok_or_else(|| {
            AppError::invalid_field(
                "connector_id",
                "required",
                "OAuth token materialization requires a connector-bound authorization state",
            )
        })?;
        let reference_id = SecretReferenceId::new();
        self.prepare_materialization_intent(authorization.tenant_id, connector_id, reference_id)
            .await?;
        let reference = self
            .material
            .write_tokens(authorization.tenant_id, reference_id, tokens)
            .await?;
        // The prepared intent is now the durable cleanup authority. Do not
        // make a best-effort delete the only recovery path after an
        // untrusted/invalid adapter response.
        validate_token_reference(&reference, reference_id)?;
        if !self.material.owns_reference(&reference) {
            return Err(AppError::invalid_field(
                "oauth_token_reference",
                "unexpected_reference",
                "secret manager returned an OAuth token reference outside its canonical prefix",
            ));
        }

        let mut transaction = match self.callback_pool.begin().await.map_err(map_sqlx) {
            Ok(transaction) => transaction,
            Err(error) => {
                // The prepared intent remains durable and will reconcile this
                // deterministic location after the grace window.
                return Err(error);
            },
        };
        let result = self
            .bind_in_transaction(transaction.as_mut(), authorization, &reference, tokens)
            .await;
        match result {
            Ok(credential_id) => {
                if let Err(error) = transaction.commit().await.map_err(map_sqlx) {
                    // PostgreSQL can report a connection failure after commit.
                    // Do not delete material here: doing so could invalidate a
                    // committed credential binding. A reconciliation job uses
                    // deterministic reference ids to handle the rare unknown
                    // outcome without exposing token values.
                    tracing::warn!(
                        outcome = "commit_unknown",
                        "OAuth credential commit outcome is unknown"
                    );
                    return Err(error);
                }
                Ok(OAuthCredentialBinding {
                    credential_id,
                    secret_reference_id: reference.id,
                })
            },
            Err(error) => {
                let _ = transaction.rollback().await;
                // The pre-written intent, not this error path, owns recovery.
                // It will prove that no credential committed and perform an
                // idempotent deterministic external deletion after the grace.
                Err(error)
            },
        }
    }
}

fn validate_token_reference(
    reference: &SecretReference,
    expected_id: SecretReferenceId,
) -> Result<()> {
    if reference.id != expected_id
        || reference.path != oauth_token_reference_locator(expected_id)
        || reference.path.len() > 1024
        || reference.path.chars().any(char::is_control)
        || !reference.is_readable()
        || reference.key_version.as_deref().is_some_and(|version| {
            version.is_empty() || version.len() > 128 || version.chars().any(char::is_control)
        })
    {
        return Err(AppError::invalid_field(
            "oauth_token_reference",
            "invalid_reference",
            "secret manager returned invalid OAuth token reference metadata",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_reference_validation_rejects_unreadable_metadata() {
        let expected = SecretReferenceId::new();
        let reference = SecretReference {
            id: expected,
            provider: mas_domain::SecretProviderKind::Vault,
            path: oauth_token_reference_locator(expected),
            key_version: None,
            rotation_state: mas_domain::SecretRotationState::Retired,
            availability: mas_domain::SecretAvailability::Available,
        };
        assert!(validate_token_reference(&reference, expected).is_err());
    }

    #[test]
    fn token_reference_validation_rejects_a_vault_route_in_metadata() {
        let expected = SecretReferenceId::new();
        let reference = SecretReference {
            id: expected,
            provider: mas_domain::SecretProviderKind::Vault,
            path: "untrusted-non-opaque-locator".to_owned(),
            key_version: None,
            rotation_state: mas_domain::SecretRotationState::Current,
            availability: mas_domain::SecretAvailability::Available,
        };
        assert!(validate_token_reference(&reference, expected).is_err());
    }
}
