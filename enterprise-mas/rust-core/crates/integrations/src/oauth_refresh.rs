//! PURPOSE: Durable OAuth access-token refresh worker contracts and orchestration.
//!
//! Owning layer: `mas-integrations` background credential-lifecycle boundary.
//!
//! Production role: lease due connector OAuth credentials, read structured
//! refresh material from the audited secret-manager port, call a guarded
//! provider token endpoint, rotate external token material, and settle durable
//! expiry/version/retry or revocation state using a monotonic fence.
//!
//! Security boundary: this worker never reads generic credential JSON or raw
//! tokens from PostgreSQL; it accepts no tenant/provider URL from a queue; only
//! the stored connector provider selects an operator-enabled static profile.
//! Material reads/writes are audited by the injected OAuth material port.
//!
//! Data ownership: PostgreSQL refresh jobs own leases/fences/retries and
//! credential metadata; secret managers own token bytes; providers own refresh
//! grant validity. This module owns transient orchestration only.
//!
//! Prohibited responsibilities: browser callbacks, client secrets, arbitrary
//! HTTP, in-memory retry state, unbounded retry, token logging, or claiming a
//! credential refreshed until durable settlement succeeds.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::ids::{ConnectorId, CredentialId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::SecretReference;

use crate::durable_oauth::OAuthTokenMaterialPort;
use crate::oauth::TokenEndpointPort;
use crate::oauth_providers::OAuthProviderCatalogPort;

/// Maximum number of jobs one worker transaction-free pass may claim.
pub const MAX_OAUTH_REFRESH_BATCH: u16 = 100;

/// A fenced leased credential refresh job. All fields are non-secret metadata.
#[derive(Debug, Clone)]
pub struct OAuthRefreshClaim {
    pub credential_id: CredentialId,
    pub tenant_id: TenantId,
    pub connector_id: ConnectorId,
    pub provider: String,
    pub secret_reference: SecretReference,
    pub attempts: u16,
    pub fence: i64,
}

impl OAuthRefreshClaim {
    pub fn validate(&self) -> Result<()> {
        if self.credential_id.is_nil()
            || self.tenant_id.is_nil()
            || self.connector_id.is_nil()
            || self.fence <= 0
            || self.provider.is_empty()
            || self.provider.len() > 64
            || !self.provider.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'.' | b'_' | b'-')
            })
            || !self.secret_reference.is_readable()
        {
            return Err(AppError::database(
                "stored OAuth refresh claim violates lifecycle invariants",
            ));
        }
        Ok(())
    }
}

/// A redacted refresh settlement reason. No provider response, endpoint, token,
/// Vault path, or raw internal error is carried into durable job state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OAuthRefreshFailure {
    pub code: &'static str,
    pub retryable: bool,
}

impl OAuthRefreshFailure {
    #[must_use]
    pub const fn transient() -> Self {
        Self {
            code: "oauth_refresh_transient_failure",
            retryable: true,
        }
    }

    #[must_use]
    pub const fn reauthorization_required() -> Self {
        Self {
            code: "oauth_reauthorization_required",
            retryable: false,
        }
    }
}

/// Durable lease/fence and credential-metadata settlement boundary.
#[async_trait]
pub trait OAuthTokenRefreshStorePort: fmt::Debug + Send + Sync {
    /// Claims up to `limit` due work for `worker_id`. Implementations must use
    /// `FOR UPDATE SKIP LOCKED`/equivalent and monotonically increment `fence`.
    async fn claim_due(
        &self,
        worker_id: &str,
        now: &Timestamp,
        limit: u16,
    ) -> Result<Vec<OAuthRefreshClaim>>;

    /// Fenced success settlement after external token replacement. It updates
    /// canonical reference version, credential expiry/version metadata, and
    /// schedules the next refresh or completes a non-expiring credential.
    async fn complete(
        &self,
        claim: &OAuthRefreshClaim,
        updated_reference: &SecretReference,
        expires_at: Option<Timestamp>,
        now: &Timestamp,
    ) -> Result<()>;

    /// Fenced failure settlement. A non-retryable reason revokes the
    /// credential and dead-letters its job; retryable failures use bounded
    /// database-owned backoff and eventually dead-letter.
    async fn fail(
        &self,
        claim: &OAuthRefreshClaim,
        failure: OAuthRefreshFailure,
        now: &Timestamp,
    ) -> Result<()>;
}

/// Per-pass counters suitable for low-cardinality operational metrics.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OAuthRefreshRunStats {
    pub claimed: u64,
    pub refreshed: u64,
    pub retry_scheduled: u64,
    pub revoked_or_dead_lettered: u64,
}

/// Separates a normal externally-originated refresh failure from an unknown
/// durable settlement outcome. The latter must never be overwritten by a
/// second state transition after a possible PostgreSQL commit.
enum RefreshClaimError {
    Failure(OAuthRefreshFailure),
    Settlement(AppError),
}

/// Real durable OAuth refresh worker. Every capability is a narrow port; no
/// concrete HTTP/database manager can be substituted by request input.
#[derive(Debug)]
pub struct OAuthTokenRefreshWorker {
    catalog: Arc<dyn OAuthProviderCatalogPort>,
    endpoint: Arc<dyn TokenEndpointPort>,
    material: Arc<dyn OAuthTokenMaterialPort>,
    store: Arc<dyn OAuthTokenRefreshStorePort>,
}

impl OAuthTokenRefreshWorker {
    #[must_use]
    pub fn new(
        catalog: Arc<dyn OAuthProviderCatalogPort>,
        endpoint: Arc<dyn TokenEndpointPort>,
        material: Arc<dyn OAuthTokenMaterialPort>,
        store: Arc<dyn OAuthTokenRefreshStorePort>,
    ) -> Self {
        Self {
            catalog,
            endpoint,
            material,
            store,
        }
    }

    /// Claims and settles one bounded batch. A settlement error is returned so
    /// supervisors cannot record a false healthy pass; unprocessed leased work
    /// remains retryable after its durable lease expires.
    pub async fn run_once(
        &self,
        worker_id: &str,
        now: &Timestamp,
        limit: u16,
    ) -> Result<OAuthRefreshRunStats> {
        validate_worker_id(worker_id)?;
        if !(1..=MAX_OAUTH_REFRESH_BATCH).contains(&limit) {
            return Err(AppError::invalid_field(
                "limit",
                "out_of_range",
                "OAuth refresh batch limit must be between 1 and 100",
            ));
        }
        let claims = self.store.claim_due(worker_id, now, limit).await?;
        let mut stats = OAuthRefreshRunStats {
            claimed: claims.len() as u64,
            ..OAuthRefreshRunStats::default()
        };
        for claim in claims {
            claim.validate()?;
            match self.refresh_claim(&claim, now).await {
                Ok(()) => stats.refreshed += 1,
                Err(RefreshClaimError::Failure(failure)) if failure.retryable => {
                    self.store.fail(&claim, failure, now).await?;
                    stats.retry_scheduled += 1;
                },
                Err(RefreshClaimError::Failure(failure)) => {
                    self.store.fail(&claim, failure, now).await?;
                    stats.revoked_or_dead_lettered += 1;
                },
                Err(RefreshClaimError::Settlement(error)) => {
                    // A database connection can fail after a commit. Never
                    // overwrite that unknown outcome with a failure/revocation
                    // settlement; the fenced lease will recover/reconcile.
                    return Err(error);
                },
            }
        }
        Ok(stats)
    }

    async fn refresh_claim(
        &self,
        claim: &OAuthRefreshClaim,
        now: &Timestamp,
    ) -> std::result::Result<(), RefreshClaimError> {
        let config = self
            .catalog
            .config(&claim.provider)
            .await
            .map_err(|_| RefreshClaimError::Failure(OAuthRefreshFailure::transient()))?
            .ok_or_else(|| {
                RefreshClaimError::Failure(OAuthRefreshFailure::reauthorization_required())
            })?;
        if !self.material.owns_reference(&claim.secret_reference) {
            return Err(RefreshClaimError::Failure(
                OAuthRefreshFailure::reauthorization_required(),
            ));
        }
        let material = self
            .material
            .read_tokens(claim.tenant_id, &claim.secret_reference)
            .await
            .map_err(|error| RefreshClaimError::Failure(classify_refresh_error(error)))?
            .ok_or_else(|| {
                RefreshClaimError::Failure(OAuthRefreshFailure::reauthorization_required())
            })?;
        let refresh_token = material.refresh_token().ok_or_else(|| {
            RefreshClaimError::Failure(OAuthRefreshFailure::reauthorization_required())
        })?;
        let mut replacement = self
            .endpoint
            .refresh(&config, refresh_token.expose_secret())
            .await
            .map_err(|error| RefreshClaimError::Failure(classify_refresh_error(error)))?;
        replacement.preserve_refresh_token_if_absent(refresh_token);
        if replacement.is_expired(now) {
            return Err(RefreshClaimError::Failure(OAuthRefreshFailure::transient()));
        }
        let updated_reference = self
            .material
            .replace_tokens(claim.tenant_id, &claim.secret_reference, &replacement)
            .await
            .map_err(|error| RefreshClaimError::Failure(classify_refresh_error(error)))?;
        if updated_reference.id != claim.secret_reference.id
            || !self.material.owns_reference(&updated_reference)
            || !updated_reference.is_readable()
        {
            return Err(RefreshClaimError::Failure(OAuthRefreshFailure::transient()));
        }
        self.store
            .complete(claim, &updated_reference, replacement.expires_at(), now)
            .await
            .map_err(RefreshClaimError::Settlement)
    }
}

fn validate_worker_id(worker_id: &str) -> Result<()> {
    if worker_id.is_empty()
        || worker_id.len() > 128
        || !worker_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(AppError::invalid_field(
            "worker_id",
            "invalid_format",
            "OAuth refresh worker id must be a bounded safe token",
        ));
    }
    Ok(())
}

fn classify_refresh_error(error: AppError) -> OAuthRefreshFailure {
    match error {
        AppError::Unauthorized(_)
        | AppError::Forbidden(_)
        | AppError::Validation { .. }
        | AppError::NotFound { .. }
        | AppError::Conflict(_) => OAuthRefreshFailure::reauthorization_required(),
        AppError::RateLimited(_)
        | AppError::Timeout(_)
        | AppError::Cancelled(_)
        | AppError::Database(_)
        | AppError::Messaging(_)
        | AppError::Serialization(_)
        | AppError::ExternalService { .. }
        | AppError::Internal(_)
        | AppError::ExternalFailure { .. }
        | AppError::QuotaExceeded { .. }
        | AppError::PolicyDenied { .. }
        | AppError::EntitlementRequired { .. } => OAuthRefreshFailure::transient(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_id_and_error_classification_fail_closed() {
        assert!(validate_worker_id("oauth-refresh-1").is_ok());
        assert!(validate_worker_id("bad worker").is_err());
        assert!(!classify_refresh_error(AppError::unauthorized("grant rejected")).retryable);
        assert!(classify_refresh_error(AppError::timeout("network")).retryable);
    }
}
