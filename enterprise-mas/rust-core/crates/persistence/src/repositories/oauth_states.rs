//! PURPOSE: PostgreSQL durable OAuth state-hash repository.
//!
//! Owning layer: `mas-persistence` split request/callback adapter.
//!
//! Production role: write tenant-scoped authorization state inside the normal
//! RLS request transaction and consume a callback state atomically through the
//! narrowly granted `mas_oauth` pool before a tenant can be known.
//!
//! Security boundary: the callback pool is explicitly separate because raw
//! OAuth state is tenant-opaque by design. It can SELECT/UPDATE only state
//! hashes and secret-reference metadata (migration 0039); no token, credential,
//! user, or general tenant table is accessible through that pool.
//!
//! Data ownership: OAuth state rows are durable one-shot evidence; PKCE bytes
//! live in an external secret-manager reference and are never returned here.
//!
//! Prohibited responsibilities: secret resolution, OAuth HTTP exchange, token
//! persistence, accepting raw state in SQL, or request-plane cross-tenant read.

use chrono::{DateTime, Utc};
use mas_common::error::AppError;
use mas_common::ids::{ConnectorId, SecretReferenceId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_integrations::durable_oauth::{DurableAuthorizationState, OAuthAuthorizationStatePort};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;

/// OAuth state adapter with deliberately separate request and callback pools.
#[derive(Debug, Clone)]
pub struct PostgresOAuthStateStore {
    request_pool: PgPool,
    callback_pool: PgPool,
}

impl PostgresOAuthStateStore {
    /// `request_pool` is the normal `mas_app` pool (and must be used inside
    /// `scoped_request`); `callback_pool` is authenticated as `mas_oauth`.
    #[must_use]
    pub const fn new(request_pool: PgPool, callback_pool: PgPool) -> Self {
        Self {
            request_pool,
            callback_pool,
        }
    }

    /// Pool used by bootstrap to establish authoritative request scopes.
    #[must_use]
    pub const fn request_pool(&self) -> &PgPool {
        &self.request_pool
    }

    /// Callback-pool health check accessor. Handlers never execute arbitrary
    /// SQL through this accessor.
    #[must_use]
    pub const fn callback_pool(&self) -> &PgPool {
        &self.callback_pool
    }
}

#[async_trait::async_trait]
impl OAuthAuthorizationStatePort for PostgresOAuthStateStore {
    async fn begin(&self, state: &DurableAuthorizationState) -> Result<()> {
        state.validate()?;
        if crate::scoped_request::current_scope()?.tenant_id() != state.tenant_id {
            return Err(AppError::not_found(
                "oauth_authorization",
                state.id.to_string(),
            ));
        }
        let scopes: Vec<String> = state.requested_scopes.iter().cloned().collect();
        let ttl_millis = state
            .expires_at
            .duration_since(&state.created_at)
            .and_then(|duration| i64::try_from(duration.as_millis()).ok())
            .filter(|millis| (1_000..=600_000).contains(millis))
            .ok_or_else(|| {
                AppError::invalid_field(
                    "expires_at",
                    "invalid_range",
                    "OAuth state lifetime must be between one second and ten minutes",
                )
            })?;
        let mut scoped = crate::scoped_request::connection().await?;
        sqlx::query(
            "INSERT INTO oauth_authorization_states (
                 id, tenant_id, connector_id, provider, state_hash,
                 pkce_verifier_secret_id, redirect_uri, requested_scopes,
                 expires_at, created_at
             ) VALUES (
                 $1, $2, $3, $4, $5, $6, $7, $8,
                 now() + ($9 * interval '1 millisecond'), now()
             )",
        )
        .bind(state.id)
        .bind(state.tenant_id.into_uuid())
        .bind(state.connector_id.map(ConnectorId::into_uuid))
        .bind(&state.provider)
        .bind(&state.state_hash)
        .bind(state.pkce_verifier_secret_id.into_uuid())
        .bind(&state.redirect_uri)
        .bind(&scopes)
        .bind(ttl_millis)
        .execute(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn consume(
        &self,
        state_hash: &str,
        _now: &Timestamp,
    ) -> Result<Option<DurableAuthorizationState>> {
        validate_hash(state_hash)?;
        // One statement makes state one-shot even with concurrent callbacks.
        // PostgreSQL time is authoritative; an API replica's skew cannot extend
        // an authorization window.
        let row = sqlx::query(
            "UPDATE oauth_authorization_states
             SET consumed_at = now()
             WHERE state_hash = $1
               AND consumed_at IS NULL
               AND expires_at > now()
             RETURNING id, tenant_id, connector_id, provider, state_hash,
                       pkce_verifier_secret_id, redirect_uri, requested_scopes,
                       expires_at, created_at",
        )
        .bind(state_hash)
        .fetch_optional(&self.callback_pool)
        .await
        .map_err(map_sqlx)?;
        row.map(state_from_row).transpose()
    }
}

fn validate_hash(hash: &str) -> Result<()> {
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(AppError::invalid_field(
            "state_hash",
            "invalid_format",
            "OAuth state hash must be exactly 64 hexadecimal characters",
        ));
    }
    Ok(())
}

fn state_from_row(row: sqlx::postgres::PgRow) -> Result<DurableAuthorizationState> {
    let state = DurableAuthorizationState {
        id: row.try_get::<Uuid, _>("id").map_err(map_sqlx)?,
        tenant_id: TenantId::from_uuid(row.try_get::<Uuid, _>("tenant_id").map_err(map_sqlx)?),
        connector_id: row
            .try_get::<Option<Uuid>, _>("connector_id")
            .map_err(map_sqlx)?
            .map(ConnectorId::from_uuid),
        provider: row.try_get("provider").map_err(map_sqlx)?,
        state_hash: row.try_get("state_hash").map_err(map_sqlx)?,
        pkce_verifier_secret_id: SecretReferenceId::from_uuid(
            row.try_get::<Uuid, _>("pkce_verifier_secret_id")
                .map_err(map_sqlx)?,
        ),
        redirect_uri: row.try_get("redirect_uri").map_err(map_sqlx)?,
        requested_scopes: row
            .try_get::<Vec<String>, _>("requested_scopes")
            .map_err(map_sqlx)?
            .into_iter()
            .collect(),
        expires_at: Timestamp::from_datetime(
            row.try_get::<DateTime<Utc>, _>("expires_at")
                .map_err(map_sqlx)?,
        ),
        created_at: Timestamp::from_datetime(
            row.try_get::<DateTime<Utc>, _>("created_at")
                .map_err(map_sqlx)?,
        ),
    };
    state
        .validate()
        .map_err(|_| AppError::database("stored OAuth authorization state violates invariants"))?;
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_hash_validation_is_fail_closed() {
        assert!(validate_hash(&"a".repeat(64)).is_ok());
        assert!(validate_hash("not-a-state").is_err());
    }
}
