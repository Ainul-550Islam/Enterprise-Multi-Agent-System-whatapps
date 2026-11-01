//! PURPOSE: PostgreSQL fenced OAuth token-material cleanup job repository.
//!
//! Owning layer: `mas-persistence` OAuth background-lifecycle boundary.
//!
//! Production role: atomically claim revoked/superseded OAuth material cleanup
//! jobs with `SKIP LOCKED` and monotonic fences, then durably settle external
//! deletion success, retry, or dead-letter evidence.
//!
//! Security boundary: this adapter requires the dedicated `mas_oauth_refresh`
//! pool. It never selects legacy credential JSON or token material. The only
//! reference mutation marks availability deleted after the integrations worker
//! confirms its narrow secret-manager deletion call.
//!
//! Data ownership: PostgreSQL owns cleanup job state, lease/fence/retry and
//! canonical reference metadata; secret managers own external token bytes.
//!
//! Prohibited responsibilities: token parsing/storage, Vault/provider HTTP,
//! browser callbacks, generic secret cleanup, best-effort-only deletion, or
//! treating a missing fenced row as successful settlement.

use mas_common::error::AppError;
use mas_common::ids::{ConnectorId, CredentialId, SecretReferenceId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{SecretAvailability, SecretProviderKind, SecretReference, SecretRotationState};
use mas_integrations::durable_oauth::oauth_token_reference_locator;
use mas_integrations::oauth_cleanup::{
    OAuthTokenCleanupClaim, OAuthTokenCleanupFailure, OAuthTokenCleanupStorePort,
    MAX_OAUTH_TOKEN_CLEANUP_BATCH,
};
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;

const LEASE_SECONDS: i64 = 120;
const MAX_ATTEMPTS: i32 = 8;

/// Dedicated cross-tenant OAuth token cleanup repository.
#[derive(Debug, Clone)]
pub struct PostgresOAuthTokenCleanupStore {
    refresh_pool: PgPool,
}

impl PostgresOAuthTokenCleanupStore {
    /// `refresh_pool` must authenticate as `mas_oauth_refresh`; request/RLS and
    /// callback-only pools are not valid substitutes.
    #[must_use]
    pub const fn new(refresh_pool: PgPool) -> Self {
        Self { refresh_pool }
    }

    /// Background-pool accessor for readiness/composition checks only.
    #[must_use]
    pub const fn refresh_pool(&self) -> &PgPool {
        &self.refresh_pool
    }

    async fn transaction(&self) -> Result<sqlx::Transaction<'_, sqlx::Postgres>> {
        self.refresh_pool.begin().await.map_err(map_sqlx)
    }
}

#[async_trait::async_trait]
impl OAuthTokenCleanupStorePort for PostgresOAuthTokenCleanupStore {
    async fn claim_due(
        &self,
        worker_id: &str,
        _now: &Timestamp,
        limit: u16,
    ) -> Result<Vec<OAuthTokenCleanupClaim>> {
        if !(1..=MAX_OAUTH_TOKEN_CLEANUP_BATCH).contains(&limit) {
            return Err(AppError::invalid_field(
                "limit",
                "out_of_range",
                "OAuth token cleanup claim limit must be between 1 and 100",
            ));
        }
        let mut transaction = self.transaction().await?;
        reap_expired_exhausted_leases(transaction.as_mut(), limit).await?;
        let rows = sqlx::query(
            "WITH candidates AS (
                 SELECT job.credential_id
                 FROM oauth_token_cleanup_jobs AS job
                 JOIN credentials AS credential
                   ON credential.tenant_id = job.tenant_id
                  AND credential.id = job.credential_id
                 JOIN connectors AS connector
                   ON connector.tenant_id = job.tenant_id
                  AND connector.id = job.connector_id
                 JOIN secret_references AS reference
                   ON reference.tenant_id = job.tenant_id
                  AND reference.id = job.secret_reference_id
                 WHERE (
                         (job.state IN ('pending', 'failed') AND job.next_attempt_at <= now())
                      OR (job.state = 'leased' AND job.lease_expires_at <= now())
                 )
                   AND job.attempts < $2
                   AND credential.purpose = 'oauth2_token'
                   AND credential.status = 'revoked'
                   AND credential.revoked_at IS NOT NULL
                   AND connector.auth_mode = 'oauth2'
                 ORDER BY job.next_attempt_at ASC NULLS FIRST, job.credential_id ASC
                 FOR UPDATE OF job SKIP LOCKED
                 LIMIT $3
             ), claimed AS (
                 UPDATE oauth_token_cleanup_jobs AS job
                 SET state = 'leased',
                     attempts = job.attempts + 1,
                     next_attempt_at = NULL,
                     lease_owner = $1,
                     lease_expires_at = now() + ($4 * interval '1 second'),
                     fence = job.fence + 1,
                     last_error = NULL,
                     updated_at = now()
                 FROM candidates
                 WHERE job.credential_id = candidates.credential_id
                 RETURNING job.credential_id, job.tenant_id, job.connector_id,
                           job.secret_reference_id, job.attempts, job.fence
             )
             SELECT
                 claimed.credential_id AS credential_id,
                 claimed.tenant_id AS tenant_id,
                 claimed.connector_id AS connector_id,
                 reference.id AS reference_id,
                 reference.provider AS reference_provider,
                 reference.key_version AS reference_key_version,
                 reference.rotation_state AS reference_rotation_state,
                 reference.availability AS reference_availability,
                 claimed.attempts AS attempts,
                 claimed.fence AS fence
             FROM claimed
             JOIN secret_references AS reference
               ON reference.tenant_id = claimed.tenant_id
              AND reference.id = claimed.secret_reference_id
             ORDER BY claimed.credential_id ASC",
        )
        .bind(worker_id)
        .bind(MAX_ATTEMPTS)
        .bind(i64::from(limit))
        .bind(LEASE_SECONDS)
        .fetch_all(transaction.as_mut())
        .await
        .map_err(map_sqlx)?;
        transaction.commit().await.map_err(map_sqlx)?;
        rows.into_iter().map(cleanup_claim_from_row).collect()
    }

    async fn complete(&self, claim: &OAuthTokenCleanupClaim, _now: &Timestamp) -> Result<()> {
        claim.validate()?;
        let mut transaction = self.transaction().await?;
        lock_current_lease(transaction.as_mut(), claim).await?;

        let reference = sqlx::query(
            "SELECT availability
             FROM secret_references
             WHERE tenant_id = $1 AND id = $2
             FOR UPDATE",
        )
        .bind(claim.tenant_id.into_uuid())
        .bind(claim.secret_reference.id.into_uuid())
        .fetch_optional(transaction.as_mut())
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| {
            AppError::conflict("OAuth cleanup reference is unavailable for settlement")
        })?;
        let availability: String = reference.try_get("availability").map_err(map_sqlx)?;
        if availability != "deleted" {
            let deleted = sqlx::query(
                "UPDATE secret_references
                 SET availability = 'deleted', updated_at = now()
                 WHERE tenant_id = $1
                   AND id = $2
                   AND availability <> 'deleted'",
            )
            .bind(claim.tenant_id.into_uuid())
            .bind(claim.secret_reference.id.into_uuid())
            .execute(transaction.as_mut())
            .await
            .map_err(map_sqlx)?;
            if deleted.rows_affected() != 1 {
                return Err(AppError::conflict(
                    "OAuth cleanup reference changed before fenced settlement",
                ));
            }
        }

        let completed = sqlx::query(
            "UPDATE oauth_token_cleanup_jobs
             SET state = 'completed',
                 next_attempt_at = NULL,
                 lease_owner = NULL,
                 lease_expires_at = NULL,
                 completed_at = now(),
                 dead_at = NULL,
                 last_error = NULL,
                 updated_at = now()
             WHERE credential_id = $1
               AND tenant_id = $2
               AND connector_id = $3
               AND secret_reference_id = $4
               AND state = 'leased'
               AND fence = $5",
        )
        .bind(claim.credential_id.into_uuid())
        .bind(claim.tenant_id.into_uuid())
        .bind(claim.connector_id.into_uuid())
        .bind(claim.secret_reference.id.into_uuid())
        .bind(claim.fence)
        .execute(transaction.as_mut())
        .await
        .map_err(map_sqlx)?;
        if completed.rows_affected() != 1 {
            return Err(AppError::conflict(
                "OAuth token cleanup lease was superseded before settlement",
            ));
        }
        transaction.commit().await.map_err(map_sqlx)
    }

    async fn fail(
        &self,
        claim: &OAuthTokenCleanupClaim,
        failure: OAuthTokenCleanupFailure,
        _now: &Timestamp,
    ) -> Result<()> {
        claim.validate()?;
        let mut transaction = self.transaction().await?;
        lock_current_lease(transaction.as_mut(), claim).await?;
        if failure.retryable && i32::from(claim.attempts) < MAX_ATTEMPTS {
            let retried = sqlx::query(
                "UPDATE oauth_token_cleanup_jobs
                 SET state = 'failed',
                     next_attempt_at = now() + (
                         LEAST(
                             3600::bigint,
                             30::bigint * (1::bigint << LEAST(attempts - 1, 6))
                         ) * interval '1 second'
                     ),
                     lease_owner = NULL,
                     lease_expires_at = NULL,
                     last_error = $5,
                     completed_at = NULL,
                     dead_at = NULL,
                     updated_at = now()
                 WHERE credential_id = $1
                   AND tenant_id = $2
                   AND connector_id = $3
                   AND secret_reference_id = $4
                   AND state = 'leased'
                   AND fence = $6",
            )
            .bind(claim.credential_id.into_uuid())
            .bind(claim.tenant_id.into_uuid())
            .bind(claim.connector_id.into_uuid())
            .bind(claim.secret_reference.id.into_uuid())
            .bind(failure.code)
            .bind(claim.fence)
            .execute(transaction.as_mut())
            .await
            .map_err(map_sqlx)?;
            if retried.rows_affected() != 1 {
                return Err(AppError::conflict(
                    "OAuth token cleanup lease was superseded before retry settlement",
                ));
            }
            return transaction.commit().await.map_err(map_sqlx);
        }

        let reason = if failure.retryable {
            "oauth_token_cleanup_retry_exhausted"
        } else {
            failure.code
        };
        let dead = sqlx::query(
            "UPDATE oauth_token_cleanup_jobs
             SET state = 'dead',
                 next_attempt_at = NULL,
                 lease_owner = NULL,
                 lease_expires_at = NULL,
                 last_error = $5,
                 completed_at = NULL,
                 dead_at = now(),
                 updated_at = now()
             WHERE credential_id = $1
               AND tenant_id = $2
               AND connector_id = $3
               AND secret_reference_id = $4
               AND state = 'leased'
               AND fence = $6",
        )
        .bind(claim.credential_id.into_uuid())
        .bind(claim.tenant_id.into_uuid())
        .bind(claim.connector_id.into_uuid())
        .bind(claim.secret_reference.id.into_uuid())
        .bind(reason)
        .bind(claim.fence)
        .execute(transaction.as_mut())
        .await
        .map_err(map_sqlx)?;
        if dead.rows_affected() != 1 {
            return Err(AppError::conflict(
                "OAuth token cleanup lease was superseded before dead-letter settlement",
            ));
        }
        let dead_letter = sqlx::query(
            "INSERT INTO oauth_token_cleanup_dead_letters (
                 id, credential_id, tenant_id, connector_id, secret_reference_id,
                 attempts, fence, reason, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
             ON CONFLICT (credential_id, fence) DO NOTHING",
        )
        .bind(Uuid::now_v7())
        .bind(claim.credential_id.into_uuid())
        .bind(claim.tenant_id.into_uuid())
        .bind(claim.connector_id.into_uuid())
        .bind(claim.secret_reference.id.into_uuid())
        .bind(i32::from(claim.attempts))
        .bind(claim.fence)
        .bind(reason)
        .execute(transaction.as_mut())
        .await
        .map_err(map_sqlx)?;
        if dead_letter.rows_affected() != 1 {
            return Err(AppError::database(
                "OAuth token cleanup dead-letter insert did not affect exactly one row",
            ));
        }
        transaction.commit().await.map_err(map_sqlx)
    }
}

/// Dead-letters an expired final cleanup attempt rather than leaving its lease
/// invisible forever. Reference availability is intentionally not changed here:
/// external deletion was not durably confirmed and a human can reconcile from
/// the durable evidence.
async fn reap_expired_exhausted_leases(connection: &mut PgConnection, limit: u16) -> Result<()> {
    let rows = sqlx::query(
        "SELECT credential_id, tenant_id, connector_id, secret_reference_id,
                attempts, fence
         FROM oauth_token_cleanup_jobs
         WHERE state = 'leased'
           AND lease_expires_at <= now()
           AND attempts >= $1
         ORDER BY lease_expires_at ASC, credential_id ASC
         FOR UPDATE SKIP LOCKED
         LIMIT $2",
    )
    .bind(MAX_ATTEMPTS)
    .bind(i64::from(limit))
    .fetch_all(&mut *connection)
    .await
    .map_err(map_sqlx)?;
    for row in rows {
        let credential_id =
            CredentialId::from_uuid(row.try_get::<Uuid, _>("credential_id").map_err(map_sqlx)?);
        let tenant_id = TenantId::from_uuid(row.try_get::<Uuid, _>("tenant_id").map_err(map_sqlx)?);
        let connector_id =
            ConnectorId::from_uuid(row.try_get::<Uuid, _>("connector_id").map_err(map_sqlx)?);
        let reference_id = SecretReferenceId::from_uuid(
            row.try_get::<Uuid, _>("secret_reference_id")
                .map_err(map_sqlx)?,
        );
        let attempts: i32 = row.try_get("attempts").map_err(map_sqlx)?;
        let fence: i64 = row.try_get("fence").map_err(map_sqlx)?;
        let dead = sqlx::query(
            "UPDATE oauth_token_cleanup_jobs
             SET state = 'dead',
                 next_attempt_at = NULL,
                 lease_owner = NULL,
                 lease_expires_at = NULL,
                 last_error = 'oauth_token_cleanup_lease_expired_attempt_exhausted',
                 completed_at = NULL,
                 dead_at = now(),
                 updated_at = now()
             WHERE credential_id = $1
               AND tenant_id = $2
               AND connector_id = $3
               AND secret_reference_id = $4
               AND state = 'leased'
               AND fence = $5",
        )
        .bind(credential_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(connector_id.into_uuid())
        .bind(reference_id.into_uuid())
        .bind(fence)
        .execute(&mut *connection)
        .await
        .map_err(map_sqlx)?;
        if dead.rows_affected() != 1 {
            return Err(AppError::conflict(
                "OAuth token cleanup exhausted lease was superseded during reaping",
            ));
        }
        sqlx::query(
            "INSERT INTO oauth_token_cleanup_dead_letters (
                 id, credential_id, tenant_id, connector_id, secret_reference_id,
                 attempts, fence, reason, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7,
                       'oauth_token_cleanup_lease_expired_attempt_exhausted', now())
             ON CONFLICT (credential_id, fence) DO NOTHING",
        )
        .bind(Uuid::now_v7())
        .bind(credential_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(connector_id.into_uuid())
        .bind(reference_id.into_uuid())
        .bind(attempts)
        .bind(fence)
        .execute(&mut *connection)
        .await
        .map_err(map_sqlx)?;
    }
    Ok(())
}

async fn lock_current_lease(
    connection: &mut PgConnection,
    claim: &OAuthTokenCleanupClaim,
) -> Result<()> {
    let row = sqlx::query(
        "SELECT 1
         FROM oauth_token_cleanup_jobs
         WHERE credential_id = $1
           AND tenant_id = $2
           AND connector_id = $3
           AND secret_reference_id = $4
           AND state = 'leased'
           AND fence = $5
           AND lease_expires_at > now()
         FOR UPDATE",
    )
    .bind(claim.credential_id.into_uuid())
    .bind(claim.tenant_id.into_uuid())
    .bind(claim.connector_id.into_uuid())
    .bind(claim.secret_reference.id.into_uuid())
    .bind(claim.fence)
    .fetch_optional(&mut *connection)
    .await
    .map_err(map_sqlx)?;
    if row.is_none() {
        return Err(AppError::conflict(
            "OAuth token cleanup lease was superseded before settlement",
        ));
    }
    Ok(())
}

fn cleanup_claim_from_row(row: sqlx::postgres::PgRow) -> Result<OAuthTokenCleanupClaim> {
    let provider: String = row.try_get("reference_provider").map_err(map_sqlx)?;
    let provider = provider
        .parse::<SecretProviderKind>()
        .map_err(|_| AppError::database("stored OAuth cleanup secret provider is invalid"))?;
    let rotation_state: String = row.try_get("reference_rotation_state").map_err(map_sqlx)?;
    let rotation_state = rotation_state
        .parse::<SecretRotationState>()
        .map_err(|_| AppError::database("stored OAuth cleanup secret rotation state is invalid"))?;
    let availability: String = row.try_get("reference_availability").map_err(map_sqlx)?;
    let availability = availability
        .parse::<SecretAvailability>()
        .map_err(|_| AppError::database("stored OAuth cleanup secret availability is invalid"))?;
    let attempts: i32 = row.try_get("attempts").map_err(map_sqlx)?;
    let reference_id =
        SecretReferenceId::from_uuid(row.try_get::<Uuid, _>("reference_id").map_err(map_sqlx)?);
    let claim = OAuthTokenCleanupClaim {
        credential_id: CredentialId::from_uuid(
            row.try_get::<Uuid, _>("credential_id").map_err(map_sqlx)?,
        ),
        tenant_id: TenantId::from_uuid(row.try_get::<Uuid, _>("tenant_id").map_err(map_sqlx)?),
        connector_id: ConnectorId::from_uuid(
            row.try_get::<Uuid, _>("connector_id").map_err(map_sqlx)?,
        ),
        secret_reference: SecretReference {
            id: reference_id,
            provider,
            path: oauth_token_reference_locator(reference_id),
            key_version: row.try_get("reference_key_version").map_err(map_sqlx)?,
            rotation_state,
            availability,
        },
        attempts: u16::try_from(attempts)
            .map_err(|_| AppError::database("stored OAuth cleanup attempts are invalid"))?,
        fence: row.try_get("fence").map_err(map_sqlx)?,
    };
    claim.validate()?;
    Ok(claim)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constants_keep_cleanup_lease_and_attempts_bounded() {
        assert_eq!(LEASE_SECONDS, 120);
        assert_eq!(MAX_ATTEMPTS, 8);
    }
}
