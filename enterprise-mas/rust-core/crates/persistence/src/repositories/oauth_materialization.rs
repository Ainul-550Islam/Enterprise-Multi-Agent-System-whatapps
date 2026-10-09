//! PURPOSE: PostgreSQL fenced OAuth materialization-intent reconciliation repository.
//!
//! Owning layer: `mas-persistence` OAuth callback/background-lifecycle boundary.
//!
//! Production role: claim callback materialization intents whose durable grace
//! window elapsed, prove a credential binding or settle deterministic orphan
//! cleanup, and retain bounded retry/dead-letter evidence with monotonic fences.
//!
//! Security boundary: the dedicated `mas_oauth_refresh` pool sees only UUID
//! lifecycle metadata. This adapter never selects legacy credential JSON,
//! provider configuration, Vault paths, raw token material, OAuth codes, or
//! PKCE verifiers.
//!
//! Data ownership: PostgreSQL owns intent lease/fence/retry state and credential
//! binding truth; the integrations material adapter owns deterministic external
//! deletion; the callback plane owns preparing/binding its intent.
//!
//! Prohibited responsibilities: Vault/provider HTTP, arbitrary secret lookup,
//! silent best-effort-only cleanup, request-plane use, or accepting stale lease
//! settlement as success.

use mas_common::error::AppError;
use mas_common::ids::{ConnectorId, SecretReferenceId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_integrations::oauth_materialization::{
    OAuthMaterializationClaim, OAuthMaterializationFailure, OAuthMaterializationStorePort,
    MAX_OAUTH_MATERIALIZATION_BATCH,
};
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;

const LEASE_SECONDS: i64 = 120;
const MAX_ATTEMPTS: i32 = 8;

/// Dedicated cross-tenant materialization reconciliation repository.
#[derive(Debug, Clone)]
pub struct PostgresOAuthMaterializationStore {
    refresh_pool: PgPool,
}

impl PostgresOAuthMaterializationStore {
    /// `refresh_pool` must use the narrowly granted `mas_oauth_refresh` role.
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
impl OAuthMaterializationStorePort for PostgresOAuthMaterializationStore {
    async fn claim_due(
        &self,
        worker_id: &str,
        _now: &Timestamp,
        limit: u16,
    ) -> Result<Vec<OAuthMaterializationClaim>> {
        if !(1..=MAX_OAUTH_MATERIALIZATION_BATCH).contains(&limit) {
            return Err(AppError::invalid_field(
                "limit",
                "out_of_range",
                "OAuth materialization claim limit must be between 1 and 100",
            ));
        }
        let mut transaction = self.transaction().await?;
        reap_expired_exhausted_leases(transaction.as_mut(), limit).await?;
        let rows = sqlx::query(
            "WITH candidates AS (
                 SELECT job.secret_reference_id
                 FROM oauth_token_materialization_jobs AS job
                 JOIN connectors AS connector
                   ON connector.tenant_id = job.tenant_id
                  AND connector.id = job.connector_id
                 WHERE (
                         (job.state IN ('pending', 'failed') AND job.next_attempt_at <= now())
                      OR (job.state = 'leased' AND job.lease_expires_at <= now())
                 )
                   AND job.attempts < $2
                   AND connector.auth_mode = 'oauth2'
                 ORDER BY job.next_attempt_at ASC NULLS FIRST, job.secret_reference_id ASC
                 FOR UPDATE OF job SKIP LOCKED
                 LIMIT $3
             ), claimed AS (
                 UPDATE oauth_token_materialization_jobs AS job
                 SET state = 'leased',
                     attempts = job.attempts + 1,
                     next_attempt_at = NULL,
                     lease_owner = $1,
                     lease_expires_at = now() + ($4 * interval '1 second'),
                     fence = job.fence + 1,
                     last_error = NULL,
                     updated_at = now()
                 FROM candidates
                 WHERE job.secret_reference_id = candidates.secret_reference_id
                 RETURNING job.secret_reference_id, job.tenant_id, job.connector_id,
                           job.attempts, job.fence
             )
             SELECT
                 claimed.secret_reference_id AS secret_reference_id,
                 claimed.tenant_id AS tenant_id,
                 claimed.connector_id AS connector_id,
                 claimed.attempts AS attempts,
                 claimed.fence AS fence,
                 EXISTS (
                     SELECT 1
                     FROM credentials AS credential
                     WHERE credential.tenant_id = claimed.tenant_id
                       AND credential.connector_id = claimed.connector_id
                       AND credential.secret_reference_id = claimed.secret_reference_id
                       AND credential.purpose = 'oauth2_token'
                 ) AS has_bound_credential
             FROM claimed
             ORDER BY claimed.secret_reference_id ASC",
        )
        .bind(worker_id)
        .bind(MAX_ATTEMPTS)
        .bind(i64::from(limit))
        .bind(LEASE_SECONDS)
        .fetch_all(transaction.as_mut())
        .await
        .map_err(map_sqlx)?;
        transaction.commit().await.map_err(map_sqlx)?;
        rows.into_iter().map(claim_from_row).collect()
    }

    async fn complete_bound(
        &self,
        claim: &OAuthMaterializationClaim,
        _now: &Timestamp,
    ) -> Result<()> {
        claim.validate()?;
        let mut transaction = self.transaction().await?;
        lock_current_lease(transaction.as_mut(), claim).await?;
        let credential = sqlx::query(
            "SELECT id
             FROM credentials
             WHERE tenant_id = $1
               AND connector_id = $2
               AND secret_reference_id = $3
               AND purpose = 'oauth2_token'
             ORDER BY id DESC
             LIMIT 1
             FOR UPDATE",
        )
        .bind(claim.tenant_id.into_uuid())
        .bind(claim.connector_id.into_uuid())
        .bind(claim.secret_reference_id.into_uuid())
        .fetch_optional(transaction.as_mut())
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| {
            AppError::conflict("OAuth materialization binding disappeared before fenced settlement")
        })?;
        let credential_id: Uuid = credential.try_get("id").map_err(map_sqlx)?;
        let settled = sqlx::query(
            "UPDATE oauth_token_materialization_jobs
             SET credential_id = $4,
                 state = 'bound',
                 next_attempt_at = NULL,
                 lease_owner = NULL,
                 lease_expires_at = NULL,
                 completed_at = now(),
                 dead_at = NULL,
                 last_error = NULL,
                 updated_at = now()
             WHERE secret_reference_id = $1
               AND tenant_id = $2
               AND connector_id = $3
               AND state = 'leased'
               AND fence = $5",
        )
        .bind(claim.secret_reference_id.into_uuid())
        .bind(claim.tenant_id.into_uuid())
        .bind(claim.connector_id.into_uuid())
        .bind(credential_id)
        .bind(claim.fence)
        .execute(transaction.as_mut())
        .await
        .map_err(map_sqlx)?;
        if settled.rows_affected() != 1 {
            return Err(AppError::conflict(
                "OAuth materialization lease was superseded before bound settlement",
            ));
        }
        transaction.commit().await.map_err(map_sqlx)
    }

    async fn complete_deleted(
        &self,
        claim: &OAuthMaterializationClaim,
        _now: &Timestamp,
    ) -> Result<()> {
        claim.validate()?;
        let mut transaction = self.transaction().await?;
        lock_current_lease(transaction.as_mut(), claim).await?;
        let bound = sqlx::query(
            "SELECT 1
             FROM credentials
             WHERE tenant_id = $1
               AND connector_id = $2
               AND secret_reference_id = $3
               AND purpose = 'oauth2_token'
             LIMIT 1
             FOR UPDATE",
        )
        .bind(claim.tenant_id.into_uuid())
        .bind(claim.connector_id.into_uuid())
        .bind(claim.secret_reference_id.into_uuid())
        .fetch_optional(transaction.as_mut())
        .await
        .map_err(map_sqlx)?;
        if bound.is_some() {
            return Err(AppError::conflict(
                "OAuth materialization acquired a credential binding before deletion settlement",
            ));
        }
        let settled = sqlx::query(
            "UPDATE oauth_token_materialization_jobs
             SET state = 'deleted',
                 next_attempt_at = NULL,
                 lease_owner = NULL,
                 lease_expires_at = NULL,
                 completed_at = now(),
                 dead_at = NULL,
                 last_error = NULL,
                 updated_at = now()
             WHERE secret_reference_id = $1
               AND tenant_id = $2
               AND connector_id = $3
               AND state = 'leased'
               AND fence = $4",
        )
        .bind(claim.secret_reference_id.into_uuid())
        .bind(claim.tenant_id.into_uuid())
        .bind(claim.connector_id.into_uuid())
        .bind(claim.fence)
        .execute(transaction.as_mut())
        .await
        .map_err(map_sqlx)?;
        if settled.rows_affected() != 1 {
            return Err(AppError::conflict(
                "OAuth materialization lease was superseded before delete settlement",
            ));
        }
        transaction.commit().await.map_err(map_sqlx)
    }

    async fn fail(
        &self,
        claim: &OAuthMaterializationClaim,
        failure: OAuthMaterializationFailure,
        _now: &Timestamp,
    ) -> Result<()> {
        claim.validate()?;
        let mut transaction = self.transaction().await?;
        lock_current_lease(transaction.as_mut(), claim).await?;
        if failure.retryable && i32::from(claim.attempts) < MAX_ATTEMPTS {
            let retried = sqlx::query(
                "UPDATE oauth_token_materialization_jobs
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
                 WHERE secret_reference_id = $1
                   AND tenant_id = $2
                   AND connector_id = $3
                   AND state = 'leased'
                   AND fence = $5",
            )
            .bind(claim.secret_reference_id.into_uuid())
            .bind(claim.tenant_id.into_uuid())
            .bind(claim.connector_id.into_uuid())
            .bind(failure.code)
            .bind(claim.fence)
            .execute(transaction.as_mut())
            .await
            .map_err(map_sqlx)?;
            if retried.rows_affected() != 1 {
                return Err(AppError::conflict(
                    "OAuth materialization lease was superseded before retry settlement",
                ));
            }
            return transaction.commit().await.map_err(map_sqlx);
        }

        let reason = if failure.retryable {
            "oauth_materialization_reconciliation_retry_exhausted"
        } else {
            failure.code
        };
        let dead = sqlx::query(
            "UPDATE oauth_token_materialization_jobs
             SET state = 'dead',
                 next_attempt_at = NULL,
                 lease_owner = NULL,
                 lease_expires_at = NULL,
                 last_error = $4,
                 completed_at = NULL,
                 dead_at = now(),
                 updated_at = now()
             WHERE secret_reference_id = $1
               AND tenant_id = $2
               AND connector_id = $3
               AND state = 'leased'
               AND fence = $5",
        )
        .bind(claim.secret_reference_id.into_uuid())
        .bind(claim.tenant_id.into_uuid())
        .bind(claim.connector_id.into_uuid())
        .bind(reason)
        .bind(claim.fence)
        .execute(transaction.as_mut())
        .await
        .map_err(map_sqlx)?;
        if dead.rows_affected() != 1 {
            return Err(AppError::conflict(
                "OAuth materialization lease was superseded before dead-letter settlement",
            ));
        }
        let dead_letter = sqlx::query(
            "INSERT INTO oauth_token_materialization_dead_letters (
                 id, secret_reference_id, tenant_id, connector_id, attempts,
                 fence, reason, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, now())
             ON CONFLICT (secret_reference_id, fence) DO NOTHING",
        )
        .bind(Uuid::now_v7())
        .bind(claim.secret_reference_id.into_uuid())
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
                "OAuth materialization dead-letter insert did not affect exactly one row",
            ));
        }
        transaction.commit().await.map_err(map_sqlx)
    }
}

/// Dead-letters an expired final reconciliation attempt. It deliberately does
/// not infer whether the external delete occurred; the durable evidence is the
/// escalation point for an operator rather than a fabricated success state.
async fn reap_expired_exhausted_leases(connection: &mut PgConnection, limit: u16) -> Result<()> {
    let rows = sqlx::query(
        "SELECT secret_reference_id, tenant_id, connector_id, attempts, fence
         FROM oauth_token_materialization_jobs
         WHERE state = 'leased'
           AND lease_expires_at <= now()
           AND attempts >= $1
         ORDER BY lease_expires_at ASC, secret_reference_id ASC
         FOR UPDATE SKIP LOCKED
         LIMIT $2",
    )
    .bind(MAX_ATTEMPTS)
    .bind(i64::from(limit))
    .fetch_all(&mut *connection)
    .await
    .map_err(map_sqlx)?;
    for row in rows {
        let reference_id = SecretReferenceId::from_uuid(
            row.try_get::<Uuid, _>("secret_reference_id")
                .map_err(map_sqlx)?,
        );
        let tenant_id = TenantId::from_uuid(row.try_get::<Uuid, _>("tenant_id").map_err(map_sqlx)?);
        let connector_id =
            ConnectorId::from_uuid(row.try_get::<Uuid, _>("connector_id").map_err(map_sqlx)?);
        let attempts: i32 = row.try_get("attempts").map_err(map_sqlx)?;
        let fence: i64 = row.try_get("fence").map_err(map_sqlx)?;
        let dead = sqlx::query(
            "UPDATE oauth_token_materialization_jobs
             SET state = 'dead',
                 next_attempt_at = NULL,
                 lease_owner = NULL,
                 lease_expires_at = NULL,
                 last_error = 'oauth_materialization_lease_expired_attempt_exhausted',
                 completed_at = NULL,
                 dead_at = now(),
                 updated_at = now()
             WHERE secret_reference_id = $1
               AND tenant_id = $2
               AND connector_id = $3
               AND state = 'leased'
               AND fence = $4",
        )
        .bind(reference_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(connector_id.into_uuid())
        .bind(fence)
        .execute(&mut *connection)
        .await
        .map_err(map_sqlx)?;
        if dead.rows_affected() != 1 {
            return Err(AppError::conflict(
                "OAuth materialization exhausted lease was superseded during reaping",
            ));
        }
        sqlx::query(
            "INSERT INTO oauth_token_materialization_dead_letters (
                 id, secret_reference_id, tenant_id, connector_id, attempts,
                 fence, reason, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6,
                       'oauth_materialization_lease_expired_attempt_exhausted', now())
             ON CONFLICT (secret_reference_id, fence) DO NOTHING",
        )
        .bind(Uuid::now_v7())
        .bind(reference_id.into_uuid())
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
    claim: &OAuthMaterializationClaim,
) -> Result<()> {
    let row = sqlx::query(
        "SELECT 1
         FROM oauth_token_materialization_jobs
         WHERE secret_reference_id = $1
           AND tenant_id = $2
           AND connector_id = $3
           AND state = 'leased'
           AND fence = $4
           AND lease_expires_at > now()
         FOR UPDATE",
    )
    .bind(claim.secret_reference_id.into_uuid())
    .bind(claim.tenant_id.into_uuid())
    .bind(claim.connector_id.into_uuid())
    .bind(claim.fence)
    .fetch_optional(&mut *connection)
    .await
    .map_err(map_sqlx)?;
    if row.is_none() {
        return Err(AppError::conflict(
            "OAuth materialization lease was superseded before settlement",
        ));
    }
    Ok(())
}

fn claim_from_row(row: sqlx::postgres::PgRow) -> Result<OAuthMaterializationClaim> {
    let attempts: i32 = row.try_get("attempts").map_err(map_sqlx)?;
    let claim = OAuthMaterializationClaim {
        secret_reference_id: SecretReferenceId::from_uuid(
            row.try_get::<Uuid, _>("secret_reference_id")
                .map_err(map_sqlx)?,
        ),
        tenant_id: TenantId::from_uuid(row.try_get::<Uuid, _>("tenant_id").map_err(map_sqlx)?),
        connector_id: ConnectorId::from_uuid(
            row.try_get::<Uuid, _>("connector_id").map_err(map_sqlx)?,
        ),
        attempts: u16::try_from(attempts)
            .map_err(|_| AppError::database("stored OAuth materialization attempts are invalid"))?,
        fence: row.try_get("fence").map_err(map_sqlx)?,
        has_bound_credential: row.try_get("has_bound_credential").map_err(map_sqlx)?,
    };
    claim.validate()?;
    Ok(claim)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constants_keep_materialization_leases_and_attempts_bounded() {
        assert_eq!(LEASE_SECONDS, 120);
        assert_eq!(MAX_ATTEMPTS, 8);
    }
}
