//! PURPOSE: Durable OAuth token-material cleanup worker contracts and orchestration.
//!
//! Owning layer: `mas-integrations` background credential-lifecycle boundary.
//!
//! Production role: lease revocation/supersession cleanup work, delete only
//! canonical OAuth material through the audited secret-manager port, and then
//! durably mark the corresponding reference deleted using a monotonic fence.
//!
//! Security boundary: PostgreSQL never supplies token bytes and this worker
//! never accepts arbitrary secret paths. It only receives a canonical,
//! tenant-owned reference claimed from a fenced cleanup queue. Reference
//! metadata is marked deleted only after the external deletion call succeeds.
//!
//! Data ownership: PostgreSQL cleanup jobs own leases, retries, fences, and
//! terminal evidence; the secret manager owns token bytes and their deletion;
//! secret-reference metadata records completed deletion only.
//!
//! Prohibited responsibilities: provider HTTP, browser OAuth callbacks,
//! generic secret deletion, best-effort post-commit cleanup, token logging,
//! unbounded retry, or overwriting an unknown durable settlement outcome.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::ids::{ConnectorId, CredentialId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{SecretAvailability, SecretReference};

use crate::durable_oauth::OAuthTokenMaterialPort;

/// Maximum cleanup claims processed by one bounded worker pass.
pub const MAX_OAUTH_TOKEN_CLEANUP_BATCH: u16 = 100;

/// A fenced OAuth material deletion claim. It intentionally carries metadata
/// only; secret-manager adapters resolve their deterministic private location
/// from the tenant and reference rather than from request input.
#[derive(Debug, Clone)]
pub struct OAuthTokenCleanupClaim {
    pub credential_id: CredentialId,
    pub tenant_id: TenantId,
    pub connector_id: ConnectorId,
    pub secret_reference: SecretReference,
    pub attempts: u16,
    pub fence: i64,
}

impl OAuthTokenCleanupClaim {
    /// Rejects impossible stored state before it reaches the external secret
    /// manager. A database corruption is a hard operational failure, not a
    /// reason to guess a deletion target.
    pub fn validate(&self) -> Result<()> {
        if self.credential_id.is_nil()
            || self.tenant_id.is_nil()
            || self.connector_id.is_nil()
            || self.secret_reference.id.is_nil()
            || self.fence <= 0
            || self.secret_reference.path.is_empty()
            || self.secret_reference.path.len() > 1_024
            || self.secret_reference.path.chars().any(char::is_control)
        {
            return Err(AppError::database(
                "stored OAuth token cleanup claim violates lifecycle invariants",
            ));
        }
        Ok(())
    }
}

/// A redacted durable cleanup outcome. No secret-manager response, path, or
/// token value is copied into job/error rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OAuthTokenCleanupFailure {
    pub code: &'static str,
    pub retryable: bool,
}

impl OAuthTokenCleanupFailure {
    #[must_use]
    pub const fn transient() -> Self {
        Self {
            code: "oauth_token_cleanup_transient_failure",
            retryable: true,
        }
    }

    #[must_use]
    pub const fn unowned_reference() -> Self {
        Self {
            code: "oauth_token_cleanup_unowned_reference",
            retryable: false,
        }
    }
}

/// Durable cleanup lease/fence and secret-reference deletion settlement port.
#[async_trait]
pub trait OAuthTokenCleanupStorePort: fmt::Debug + Send + Sync {
    /// Claims due cleanup work. Implementations must reclaim only expired
    /// leases, use `FOR UPDATE SKIP LOCKED`/equivalent, and increment `fence`.
    async fn claim_due(
        &self,
        worker_id: &str,
        now: &Timestamp,
        limit: u16,
    ) -> Result<Vec<OAuthTokenCleanupClaim>>;

    /// Fenced completion after an idempotent external deletion call. This must
    /// be the only path that marks `secret_references.availability = deleted`.
    async fn complete(&self, claim: &OAuthTokenCleanupClaim, now: &Timestamp) -> Result<()>;

    /// Fenced retry/dead-letter settlement after an external deletion failure.
    async fn fail(
        &self,
        claim: &OAuthTokenCleanupClaim,
        failure: OAuthTokenCleanupFailure,
        now: &Timestamp,
    ) -> Result<()>;
}

/// Low-cardinality operational results for a bounded cleanup pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OAuthTokenCleanupRunStats {
    pub claimed: u64,
    pub cleaned: u64,
    pub retry_scheduled: u64,
    pub dead_lettered: u64,
}

/// A regular external deletion error is safe to settle as a retry/dead-letter.
/// A completion error is not: the SQL transaction might have committed just
/// before a connection failure. The lease therefore remains for deterministic
/// reconciliation rather than being overwritten by a fabricated failure.
enum CleanupClaimError {
    Failure(OAuthTokenCleanupFailure),
    Settlement(AppError),
}

/// Durable cleanup worker. The injected material capability is deliberately
/// narrower than a generic secret manager and refuses references outside its
/// canonical OAuth namespace.
#[derive(Debug)]
pub struct OAuthTokenCleanupWorker {
    material: Arc<dyn OAuthTokenMaterialPort>,
    store: Arc<dyn OAuthTokenCleanupStorePort>,
}

impl OAuthTokenCleanupWorker {
    #[must_use]
    pub fn new(
        material: Arc<dyn OAuthTokenMaterialPort>,
        store: Arc<dyn OAuthTokenCleanupStorePort>,
    ) -> Self {
        Self { material, store }
    }

    /// Runs one finite cleanup pass. A completion uncertainty is surfaced to
    /// the supervisor without a compensating state transition; a later lease
    /// reclamation repeats the idempotent external delete and converges.
    pub async fn run_once(
        &self,
        worker_id: &str,
        now: &Timestamp,
        limit: u16,
    ) -> Result<OAuthTokenCleanupRunStats> {
        validate_worker_id(worker_id)?;
        if !(1..=MAX_OAUTH_TOKEN_CLEANUP_BATCH).contains(&limit) {
            return Err(AppError::invalid_field(
                "limit",
                "out_of_range",
                "OAuth token cleanup batch limit must be between 1 and 100",
            ));
        }

        let claims = self.store.claim_due(worker_id, now, limit).await?;
        let mut stats = OAuthTokenCleanupRunStats {
            claimed: claims.len() as u64,
            ..OAuthTokenCleanupRunStats::default()
        };
        for claim in claims {
            claim.validate()?;
            match self.cleanup_claim(&claim, now).await {
                Ok(()) => stats.cleaned += 1,
                Err(CleanupClaimError::Failure(failure)) if failure.retryable => {
                    self.store.fail(&claim, failure, now).await?;
                    stats.retry_scheduled += 1;
                },
                Err(CleanupClaimError::Failure(failure)) => {
                    self.store.fail(&claim, failure, now).await?;
                    stats.dead_lettered += 1;
                },
                Err(CleanupClaimError::Settlement(error)) => return Err(error),
            }
        }
        Ok(stats)
    }

    async fn cleanup_claim(
        &self,
        claim: &OAuthTokenCleanupClaim,
        now: &Timestamp,
    ) -> std::result::Result<(), CleanupClaimError> {
        // A preceding successful delete may have committed its metadata update
        // before the client lost the response. Completion is idempotent and
        // records the queue terminal state without another secret-manager call.
        if claim.secret_reference.availability == SecretAvailability::Deleted {
            return self
                .store
                .complete(claim, now)
                .await
                .map_err(CleanupClaimError::Settlement);
        }
        if !self.material.owns_reference(&claim.secret_reference) {
            return Err(CleanupClaimError::Failure(
                OAuthTokenCleanupFailure::unowned_reference(),
            ));
        }
        self.material
            .delete_tokens(claim.tenant_id, &claim.secret_reference)
            .await
            .map_err(|_| CleanupClaimError::Failure(OAuthTokenCleanupFailure::transient()))?;
        self.store
            .complete(claim, now)
            .await
            .map_err(CleanupClaimError::Settlement)
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
            "OAuth token cleanup worker id must be a bounded safe token",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_batch_limit_is_bounded() {
        assert_eq!(MAX_OAUTH_TOKEN_CLEANUP_BATCH, 100);
        assert!(validate_worker_id("oauth-refresh.api_1").is_ok());
        assert!(validate_worker_id("unsafe worker id").is_err());
    }
}
