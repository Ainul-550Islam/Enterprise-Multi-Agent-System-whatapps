//! PURPOSE: PostgreSQL fenced OAuth token-refresh job repository.
//!
//! Owning layer: `mas-persistence` OAuth background-lifecycle boundary.
//!
//! Production role: atomically claim due refresh jobs with SKIP LOCKED and a
//! monotonically increasing fence, expose only required provider/reference
//! metadata, and settle success/retry/revocation/dead-letter state durably.
//!
//! Security boundary: this adapter requires the dedicated mas_oauth_refresh
//! pool from migration 0044. It never selects credential `secret_ref` JSON,
//! connector config, or raw secret/token material, and stale leases cannot
//! mutate newer job state because every settlement matches its fence.
//!
//! Data ownership: PostgreSQL owns lease/fence/retry/dead-letter and canonical
//! metadata; the injected integrations worker owns secret-manager/provider I/O.
//!
//! Prohibited responsibilities: token parsing/storage, Vault HTTP, provider
//! HTTP, browser callback handling, generic connector lookup, or treating a
//! missing fenced row as a successful settlement.

use mas_common::error::AppError;
use mas_common::ids::{ConnectorId, CredentialId, SecretReferenceId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{SecretAvailability, SecretProviderKind, SecretReference, SecretRotationState};
use mas_integrations::durable_oauth::oauth_token_reference_locator;
use mas_integrations::oauth_refresh::{
    OAuthRefreshClaim, OAuthRefreshFailure, OAuthTokenRefreshStorePort, MAX_OAUTH_REFRESH_BATCH,
};
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;

const LEASE_SECONDS: i64 = 120;
const MAX_ATTEMPTS: i32 = 8;

/// Dedicated cross-tenant OAuth refresh repository.
#[derive(Debug, Clone)]
pub struct PostgresOAuthTokenRefreshStore {
    refresh_pool: PgPool,
}

impl PostgresOAuthTokenRefreshStore {
    /// `refresh_pool` must authenticate as `mas_oauth_refresh`; a request/RLS
    /// pool or the callback-only `mas_oauth` role is not a valid substitute.
    #[must_use]
    pub const fn new(refresh_pool: PgPool) -> Self {
        Self { refresh_pool }
    }

    /// Returns the dedicated lifecycle pool for bootstrap readiness checks.
    #[must_use]
    pub const fn refresh_pool(&self) -> &PgPool {
        &self.refresh_pool
    }

    async fn transaction(&self) -> Result<sqlx::Transaction<'_, sqlx::Postgres>> {
        self.refresh_pool.begin().await.map_err(map_sqlx)
    }
}

#[async_trait::async_trait]
impl OAuthTokenRefreshStorePort for PostgresOAuthTokenRefreshStore {
    async fn claim_due(
        &self,
        worker_id: &str,
        _now: &Timestamp,
        limit: u16,
    ) -> Result<Vec<OAuthRefreshClaim>> {
        if !(1..=MAX_OAUTH_REFRESH_BATCH).contains(&limit) {
            return Err(AppError::invalid_field(
                "limit",
                "out_of_range",
                "OAuth refresh claim limit must be between 1 and 100",
            ));
        }
        let mut transaction = self.transaction().await?;
        reap_expired_exhausted_leases(transaction.as_mut(), limit).await?;
        let rows = sqlx::query(
            "WITH candidates AS (
                 SELECT job.credential_id
                 FROM oauth_token_refresh_jobs AS job
                 JOIN credentials AS credential
                   ON credential.tenant_id = job.tenant_id
                  AND credential.id = job.credential_id
                 JOIN connectors AS connector
                   ON connector.tenant_id = job.tenant_id
                  AND connector.id = job.connector_id
                 JOIN secret_references AS reference
                   ON reference.tenant_id = credential.tenant_id
                  AND reference.id = credential.secret_reference_id
                 WHERE (
                         (job.state IN ('pending', 'failed') AND job.next_attempt_at <= now())
                      OR (job.state = 'leased' AND job.lease_expires_at <= now())
                 )
                   AND job.attempts < $2
                   AND credential.purpose = 'oauth2_token'
                   AND credential.status IN ('active', 'rotating')
                   AND credential.revoked_at IS NULL
                   AND connector.auth_mode = 'oauth2'
                   AND reference.availability = 'available'
                   AND reference.rotation_state <> 'retired'
                 ORDER BY job.next_attempt_at ASC NULLS FIRST, job.credential_id ASC
                 FOR UPDATE OF job SKIP LOCKED
                 LIMIT $3
             ), claimed AS (
                 UPDATE oauth_token_refresh_jobs AS job
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
                           job.attempts, job.fence
             )
             SELECT
                 claimed.credential_id AS credential_id,
                 claimed.tenant_id AS tenant_id,
                 claimed.connector_id AS connector_id,
                 connector.provider AS provider,
                 reference.id AS reference_id,
                 reference.provider AS reference_provider,
                 reference.key_version AS reference_key_version,
                 reference.rotation_state AS reference_rotation_state,
                 reference.availability AS reference_availability,
                 claimed.attempts AS attempts,
                 claimed.fence AS fence
             FROM claimed
             JOIN credentials AS credential
               ON credential.tenant_id = claimed.tenant_id
              AND credential.id = claimed.credential_id
             JOIN connectors AS connector
               ON connector.tenant_id = claimed.tenant_id
              AND connector.id = claimed.connector_id
             JOIN secret_references AS reference
               ON reference.tenant_id = credential.tenant_id
              AND reference.id = credential.secret_reference_id
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
        rows.into_iter().map(refresh_claim_from_row).collect()
    }

    async fn complete(
        &self,
        claim: &OAuthRefreshClaim,
        updated_reference: &SecretReference,
        expires_at: Option<Timestamp>,
        _now: &Timestamp,
    ) -> Result<()> {
        claim.validate()?;
        if updated_reference.id != claim.secret_reference.id || !updated_reference.is_readable() {
            return Err(AppError::invalid_field(
                "updated_reference",
                "invalid_reference",
                "OAuth refresh settlement received invalid reference metadata",
            ));
        }
        let mut transaction = self.transaction().await?;
        lock_current_lease(transaction.as_mut(), claim).await?;
        let updated_reference_row = sqlx::query(
            "UPDATE secret_references
             SET key_version = $4, updated_at = now()
             WHERE tenant_id = $1
               AND id = $2
               AND availability = 'available'
               AND rotation_state <> 'retired'
               AND provider = $3",
        )
        .bind(claim.tenant_id.into_uuid())
        .bind(updated_reference.id.into_uuid())
        .bind(updated_reference.provider.to_string())
        .bind(&updated_reference.key_version)
        .execute(transaction.as_mut())
        .await
        .map_err(map_sqlx)?;
        if updated_reference_row.rows_affected() != 1 {
            return Err(AppError::conflict(
                "OAuth refresh reference is unavailable for fenced settlement",
            ));
        }
        let credential = sqlx::query(
            "UPDATE credentials
             SET expires_at = $4,
                 secret_version = $5,
                 status = 'active',
                 updated_at = now()
             WHERE tenant_id = $1
               AND id = $2
               AND connector_id = $3
               AND purpose = 'oauth2_token'
               AND status IN ('active', 'rotating')
               AND revoked_at IS NULL",
        )
        .bind(claim.tenant_id.into_uuid())
        .bind(claim.credential_id.into_uuid())
        .bind(claim.connector_id.into_uuid())
        .bind(expires_at.map(Timestamp::into_datetime))
        .bind(&updated_reference.key_version)
        .execute(transaction.as_mut())
        .await
        .map_err(map_sqlx)?;
        if credential.rows_affected() != 1 {
            return Err(AppError::conflict(
                "OAuth refresh credential is unavailable for fenced settlement",
            ));
        }
        let settled = if let Some(expiry) = expires_at {
            sqlx::query(
                "UPDATE oauth_token_refresh_jobs
                 SET state = 'pending',
                     attempts = 0,
                     next_attempt_at = GREATEST(now(), $4 - interval '300 seconds'),
                     lease_owner = NULL,
                     lease_expires_at = NULL,
                     completed_at = NULL,
                     dead_at = NULL,
                     last_error = NULL,
                     updated_at = now()
                 WHERE credential_id = $1
                   AND tenant_id = $2
                   AND connector_id = $3
                   AND state = 'leased'
                   AND fence = $5",
            )
            .bind(claim.credential_id.into_uuid())
            .bind(claim.tenant_id.into_uuid())
            .bind(claim.connector_id.into_uuid())
            .bind(expiry.into_datetime())
            .bind(claim.fence)
            .execute(transaction.as_mut())
            .await
            .map_err(map_sqlx)?
        } else {
            sqlx::query(
                "UPDATE oauth_token_refresh_jobs
                 SET state = 'completed',
                     attempts = 0,
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
                   AND state = 'leased'
                   AND fence = $4",
            )
            .bind(claim.credential_id.into_uuid())
            .bind(claim.tenant_id.into_uuid())
            .bind(claim.connector_id.into_uuid())
            .bind(claim.fence)
            .execute(transaction.as_mut())
            .await
            .map_err(map_sqlx)?
        };
        if settled.rows_affected() != 1 {
            return Err(AppError::conflict(
                "OAuth refresh lease was superseded before settlement",
            ));
        }
        transaction.commit().await.map_err(map_sqlx)
    }

    async fn fail(
        &self,
        claim: &OAuthRefreshClaim,
        failure: OAuthRefreshFailure,
        _now: &Timestamp,
    ) -> Result<()> {
        claim.validate()?;
        let mut transaction = self.transaction().await?;
        lock_current_lease(transaction.as_mut(), claim).await?;
        if failure.retryable && i32::from(claim.attempts) < MAX_ATTEMPTS {
            let retried = sqlx::query(
                "UPDATE oauth_token_refresh_jobs
                 SET state = 'failed',
                     next_attempt_at = now() + (
                         LEAST(
                             3600::bigint,
                             30::bigint * (1::bigint << LEAST(attempts - 1, 6))
                         ) * interval '1 second'
                     ),
                     lease_owner = NULL,
                     lease_expires_at = NULL,
                     last_error = $4,
                     completed_at = NULL,
                     dead_at = NULL,
                     updated_at = now()
                 WHERE credential_id = $1
                   AND tenant_id = $2
                   AND connector_id = $3
                   AND state = 'leased'
                   AND fence = $5",
            )
            .bind(claim.credential_id.into_uuid())
            .bind(claim.tenant_id.into_uuid())
            .bind(claim.connector_id.into_uuid())
            .bind(failure.code)
            .bind(claim.fence)
            .execute(transaction.as_mut())
            .await
            .map_err(map_sqlx)?;
            if retried.rows_affected() != 1 {
                return Err(AppError::conflict(
                    "OAuth refresh lease was superseded before retry settlement",
                ));
            }
            return transaction.commit().await.map_err(map_sqlx);
        }

        let reason = if failure.retryable {
            "oauth_refresh_retry_exhausted"
        } else {
            failure.code
        };
        let revoked = sqlx::query(
            "UPDATE credentials
             SET status = 'revoked', revoked_at = now(), updated_at = now()
             WHERE tenant_id = $1
               AND id = $2
               AND connector_id = $3
               AND purpose = 'oauth2_token'
               AND status IN ('active', 'rotating')
               AND revoked_at IS NULL",
        )
        .bind(claim.tenant_id.into_uuid())
        .bind(claim.credential_id.into_uuid())
        .bind(claim.connector_id.into_uuid())
        .execute(transaction.as_mut())
        .await
        .map_err(map_sqlx)?;
        if revoked.rows_affected() != 1 {
            return Err(AppError::conflict(
                "OAuth refresh credential was superseded before revocation",
            ));
        }
        // Revocation and external-material cleanup are one durable transition.
        // The cleanup worker alone performs the idempotent secret-manager call
        // and changes reference availability after that call succeeds.
        let cleanup_job = sqlx::query(
            "INSERT INTO oauth_token_cleanup_jobs (
                 credential_id, tenant_id, connector_id, secret_reference_id,
                 state, attempts, next_attempt_at, created_at, updated_at
             ) VALUES (
                 $1, $2, $3, $4, 'pending', 0, now(), now(), now()
             ) ON CONFLICT (credential_id) DO NOTHING",
        )
        .bind(claim.credential_id.into_uuid())
        .bind(claim.tenant_id.into_uuid())
        .bind(claim.connector_id.into_uuid())
        .bind(claim.secret_reference.id.into_uuid())
        .execute(transaction.as_mut())
        .await
        .map_err(map_sqlx)?;
        if cleanup_job.rows_affected() > 1 {
            return Err(AppError::database(
                "OAuth token cleanup enqueue affected multiple rows",
            ));
        }
        let dead = sqlx::query(
            "UPDATE oauth_token_refresh_jobs
             SET state = 'dead',
                 next_attempt_at = NULL,
                 lease_owner = NULL,
                 lease_expires_at = NULL,
                 last_error = $4,
                 completed_at = NULL,
                 dead_at = now(),
                 updated_at = now()
             WHERE credential_id = $1
               AND tenant_id = $2
               AND connector_id = $3
               AND state = 'leased'
               AND fence = $5",
        )
        .bind(claim.credential_id.into_uuid())
        .bind(claim.tenant_id.into_uuid())
        .bind(claim.connector_id.into_uuid())
        .bind(reason)
        .bind(claim.fence)
        .execute(transaction.as_mut())
        .await
        .map_err(map_sqlx)?;
        if dead.rows_affected() != 1 {
            return Err(AppError::conflict(
                "OAuth refresh lease was superseded before dead-letter settlement",
            ));
        }
        let dead_letter = sqlx::query(
            "INSERT INTO oauth_token_refresh_dead_letters (
                 id, credential_id, tenant_id, connector_id, attempts, fence,
                 reason, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, now())
             ON CONFLICT (credential_id, fence) DO NOTHING",
        )
        .bind(Uuid::now_v7())
        .bind(claim.credential_id.into_uuid())
        .bind(claim.tenant_id.into_uuid())
        .bind(claim.connector_id.into_uuid())
        .bind(i32::from(claim.attempts))
        .bind(claim.fence)
        .bind(reason)
        .execute(transaction.as_mut())
        .await
        .map_err(map_sqlx)?;
        if dead_letter.rows_affected() != 1 {
            return Err(AppError::database(
                "OAuth refresh dead-letter insert did not affect exactly one row",
            ));
        }
        transaction.commit().await.map_err(map_sqlx)
    }
}

/// Turns an expired final-attempt lease into a fail-closed terminal transition.
/// A worker can lose its process/connection after provider/Vault I/O, so this
/// path cannot claim success. It revokes the credential, schedules durable
/// external cleanup, and records evidence rather than leaving a `leased` row
/// permanently invisible to future workers.
async fn reap_expired_exhausted_leases(connection: &mut PgConnection, limit: u16) -> Result<()> {
    let rows = sqlx::query(
        "SELECT
             job.credential_id AS credential_id,
             job.tenant_id AS tenant_id,
             job.connector_id AS connector_id,
             job.attempts AS attempts,
             job.fence AS fence,
             reference.id AS reference_id
         FROM oauth_token_refresh_jobs AS job
         JOIN credentials AS credential
           ON credential.tenant_id = job.tenant_id
          AND credential.id = job.credential_id
         JOIN secret_references AS reference
           ON reference.tenant_id = credential.tenant_id
          AND reference.id = credential.secret_reference_id
         WHERE job.state = 'leased'
           AND job.lease_expires_at <= now()
           AND job.attempts >= $1
         ORDER BY job.lease_expires_at ASC, job.credential_id ASC
         FOR UPDATE OF job, credential SKIP LOCKED
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
        let reference_id =
            SecretReferenceId::from_uuid(row.try_get::<Uuid, _>("reference_id").map_err(map_sqlx)?);
        let attempts: i32 = row.try_get("attempts").map_err(map_sqlx)?;
        let fence: i64 = row.try_get("fence").map_err(map_sqlx)?;
        let revoked = sqlx::query(
            "UPDATE credentials
             SET status = 'revoked', revoked_at = now(), updated_at = now()
             WHERE tenant_id = $1
               AND id = $2
               AND connector_id = $3
               AND purpose = 'oauth2_token'
               AND status IN ('active', 'rotating')
               AND revoked_at IS NULL",
        )
        .bind(tenant_id.into_uuid())
        .bind(credential_id.into_uuid())
        .bind(connector_id.into_uuid())
        .execute(&mut *connection)
        .await
        .map_err(map_sqlx)?;
        if revoked.rows_affected() > 1 {
            return Err(AppError::database(
                "OAuth refresh exhausted-lease revocation affected multiple rows",
            ));
        }
        let cleanup = sqlx::query(
            "INSERT INTO oauth_token_cleanup_jobs (
                 credential_id, tenant_id, connector_id, secret_reference_id,
                 state, attempts, next_attempt_at, created_at, updated_at
             ) VALUES (
                 $1, $2, $3, $4, 'pending', 0, now(), now(), now()
             ) ON CONFLICT (credential_id) DO NOTHING",
        )
        .bind(credential_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(connector_id.into_uuid())
        .bind(reference_id.into_uuid())
        .execute(&mut *connection)
        .await
        .map_err(map_sqlx)?;
        if cleanup.rows_affected() > 1 {
            return Err(AppError::database(
                "OAuth refresh exhausted-lease cleanup enqueue affected multiple rows",
            ));
        }
        let dead = sqlx::query(
            "UPDATE oauth_token_refresh_jobs
             SET state = 'dead',
                 next_attempt_at = NULL,
                 lease_owner = NULL,
                 lease_expires_at = NULL,
                 last_error = 'oauth_refresh_lease_expired_attempt_exhausted',
                 completed_at = NULL,
                 dead_at = now(),
                 updated_at = now()
             WHERE credential_id = $1
               AND tenant_id = $2
               AND connector_id = $3
               AND state = 'leased'
               AND fence = $4",
        )
        .bind(credential_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(connector_id.into_uuid())
        .bind(fence)
        .execute(&mut *connection)
        .await
        .map_err(map_sqlx)?;
        if dead.rows_affected() != 1 {
            return Err(AppError::conflict(
                "OAuth refresh exhausted lease was superseded during reaping",
            ));
        }
        sqlx::query(
            "INSERT INTO oauth_token_refresh_dead_letters (
                 id, credential_id, tenant_id, connector_id, attempts, fence,
                 reason, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6,
                       'oauth_refresh_lease_expired_attempt_exhausted', now())
             ON CONFLICT (credential_id, fence) DO NOTHING",
        )
        .bind(Uuid::now_v7())
        .bind(credential_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(connector_id.into_uuid())
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
    claim: &OAuthRefreshClaim,
) -> Result<()> {
    let row = sqlx::query(
        "SELECT 1
         FROM oauth_token_refresh_jobs
         WHERE credential_id = $1
           AND tenant_id = $2
           AND connector_id = $3
           AND state = 'leased'
           AND fence = $4
           AND lease_expires_at > now()
         FOR UPDATE",
    )
    .bind(claim.credential_id.into_uuid())
    .bind(claim.tenant_id.into_uuid())
    .bind(claim.connector_id.into_uuid())
    .bind(claim.fence)
    .fetch_optional(&mut *connection)
    .await
    .map_err(map_sqlx)?;
    if row.is_none() {
        return Err(AppError::conflict(
            "OAuth refresh lease was superseded before settlement",
        ));
    }
    Ok(())
}

fn refresh_claim_from_row(row: sqlx::postgres::PgRow) -> Result<OAuthRefreshClaim> {
    let provider: String = row.try_get("reference_provider").map_err(map_sqlx)?;
    let reference_provider = provider.parse::<SecretProviderKind>().map_err(|_| {
        AppError::database("stored OAuth refresh secret reference provider is invalid")
    })?;
    let rotation_state: String = row.try_get("reference_rotation_state").map_err(map_sqlx)?;
    let rotation_state = rotation_state.parse::<SecretRotationState>().map_err(|_| {
        AppError::database("stored OAuth refresh secret reference rotation state is invalid")
    })?;
    let availability: String = row.try_get("reference_availability").map_err(map_sqlx)?;
    let availability = availability.parse::<SecretAvailability>().map_err(|_| {
        AppError::database("stored OAuth refresh secret reference availability is invalid")
    })?;
    let attempts: i32 = row.try_get("attempts").map_err(map_sqlx)?;
    let reference_id =
        SecretReferenceId::from_uuid(row.try_get::<Uuid, _>("reference_id").map_err(map_sqlx)?);
    let claim = OAuthRefreshClaim {
        credential_id: CredentialId::from_uuid(
            row.try_get::<Uuid, _>("credential_id").map_err(map_sqlx)?,
        ),
        tenant_id: TenantId::from_uuid(row.try_get::<Uuid, _>("tenant_id").map_err(map_sqlx)?),
        connector_id: ConnectorId::from_uuid(
            row.try_get::<Uuid, _>("connector_id").map_err(map_sqlx)?,
        ),
        provider: row.try_get("provider").map_err(map_sqlx)?,
        secret_reference: SecretReference {
            id: reference_id,
            provider: reference_provider,
            path: oauth_token_reference_locator(reference_id),
            key_version: row.try_get("reference_key_version").map_err(map_sqlx)?,
            rotation_state,
            availability,
        },
        attempts: u16::try_from(attempts)
            .map_err(|_| AppError::database("stored OAuth refresh attempts are invalid"))?,
        fence: row.try_get("fence").map_err(map_sqlx)?,
    };
    claim.validate()?;
    Ok(claim)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constants_keep_refresh_lease_and_attempts_bounded() {
        assert_eq!(LEASE_SECONDS, 120);
        assert_eq!(MAX_ATTEMPTS, 8);
    }
}
