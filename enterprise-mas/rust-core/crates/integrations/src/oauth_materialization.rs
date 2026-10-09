//! PURPOSE: Durable reconciliation worker for OAuth token materialization intents.
//!
//! Owning layer: `mas-integrations` OAuth credential-lifecycle boundary.
//!
//! Production role: reconcile the unavoidable external-write/SQL-commit gap
//! when an OAuth callback writes deterministic secret-manager material before
//! its credential binding transaction commits. It proves a bound credential or
//! idempotently deletes an unbound deterministic location after a grace period.
//!
//! Security boundary: claims contain only tenant/connector/reference UUID
//! metadata, never a Vault path or token. The material port derives the sole
//! deletion target from its operator-owned prefix and generated reference id.
//!
//! Data ownership: PostgreSQL intent jobs own grace windows, leases, fences,
//! retries, and evidence; secret managers own material bytes; credentials own
//! whether a callback binding actually committed.
//!
//! Prohibited responsibilities: provider HTTP, token reads/logging, deleting a
//! materialized token before a durable grace window, accepting arbitrary paths,
//! fake callback success, or converting unknown settlement outcomes into a
//! destructive retry state.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::ids::{ConnectorId, SecretReferenceId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;

use crate::durable_oauth::OAuthTokenMaterialPort;

/// Maximum materialization-reconciliation claims processed in one pass.
pub const MAX_OAUTH_MATERIALIZATION_BATCH: u16 = 100;

/// A fenced prepared materialization intent. `has_bound_credential` is derived
/// by the store at claim time and must be revalidated at settlement.
#[derive(Debug, Clone)]
pub struct OAuthMaterializationClaim {
    pub tenant_id: TenantId,
    pub connector_id: ConnectorId,
    pub secret_reference_id: SecretReferenceId,
    pub attempts: u16,
    pub fence: i64,
    pub has_bound_credential: bool,
}

impl OAuthMaterializationClaim {
    pub fn validate(&self) -> Result<()> {
        if self.tenant_id.is_nil()
            || self.connector_id.is_nil()
            || self.secret_reference_id.is_nil()
            || self.fence <= 0
        {
            return Err(AppError::database(
                "stored OAuth materialization claim violates lifecycle invariants",
            ));
        }
        Ok(())
    }
}

/// Redacted external cleanup failure. Durable rows store this stable code only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OAuthMaterializationFailure {
    pub code: &'static str,
    pub retryable: bool,
}

impl OAuthMaterializationFailure {
    #[must_use]
    pub const fn transient() -> Self {
        Self {
            code: "oauth_materialization_reconciliation_transient_failure",
            retryable: true,
        }
    }
}

/// Durable prepared-materialization reconciliation boundary.
#[async_trait]
pub trait OAuthMaterializationStorePort: fmt::Debug + Send + Sync {
    /// Claims only intents whose callback grace period elapsed. Implementations
    /// must use `FOR UPDATE SKIP LOCKED` and monotonically advance `fence`.
    async fn claim_due(
        &self,
        worker_id: &str,
        now: &Timestamp,
        limit: u16,
    ) -> Result<Vec<OAuthMaterializationClaim>>;

    /// Fenced terminal settlement when a committed credential now references
    /// this deterministic material id; no external deletion is performed.
    async fn complete_bound(
        &self,
        claim: &OAuthMaterializationClaim,
        now: &Timestamp,
    ) -> Result<()>;

    /// Fenced terminal settlement after idempotently deleting material proven
    /// to have no committed OAuth credential binding.
    async fn complete_deleted(
        &self,
        claim: &OAuthMaterializationClaim,
        now: &Timestamp,
    ) -> Result<()>;

    /// Fenced retry/dead-letter settlement for an external deletion failure.
    async fn fail(
        &self,
        claim: &OAuthMaterializationClaim,
        failure: OAuthMaterializationFailure,
        now: &Timestamp,
    ) -> Result<()>;
}

/// Low-cardinality result counters for one reconciliation pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OAuthMaterializationRunStats {
    pub claimed: u64,
    pub bindings_confirmed: u64,
    pub orphan_material_deleted: u64,
    pub retry_scheduled: u64,
    pub dead_lettered: u64,
}

/// Completion ambiguity cannot safely be converted into a failure transition:
/// a PostgreSQL commit can succeed before the caller observes connection loss.
enum MaterializationClaimError {
    Failure(OAuthMaterializationFailure),
    Settlement(AppError),
}

/// Reconciles callback materialization intents. This closes the unknown-outcome
/// hole between secret-manager write and PostgreSQL credential binding without
/// ever deleting material on the callback's error path alone.
#[derive(Debug)]
pub struct OAuthMaterializationReconciler {
    material: Arc<dyn OAuthTokenMaterialPort>,
    store: Arc<dyn OAuthMaterializationStorePort>,
}

impl OAuthMaterializationReconciler {
    #[must_use]
    pub fn new(
        material: Arc<dyn OAuthTokenMaterialPort>,
        store: Arc<dyn OAuthMaterializationStorePort>,
    ) -> Self {
        Self { material, store }
    }

    pub async fn run_once(
        &self,
        worker_id: &str,
        now: &Timestamp,
        limit: u16,
    ) -> Result<OAuthMaterializationRunStats> {
        validate_worker_id(worker_id)?;
        if !(1..=MAX_OAUTH_MATERIALIZATION_BATCH).contains(&limit) {
            return Err(AppError::invalid_field(
                "limit",
                "out_of_range",
                "OAuth materialization batch limit must be between 1 and 100",
            ));
        }
        let claims = self.store.claim_due(worker_id, now, limit).await?;
        let mut stats = OAuthMaterializationRunStats {
            claimed: claims.len() as u64,
            ..OAuthMaterializationRunStats::default()
        };
        for claim in claims {
            claim.validate()?;
            match self.reconcile_claim(&claim, now).await {
                Ok(ReconciliationOutcome::Bound) => stats.bindings_confirmed += 1,
                Ok(ReconciliationOutcome::Deleted) => stats.orphan_material_deleted += 1,
                Err(MaterializationClaimError::Failure(failure)) if failure.retryable => {
                    self.store.fail(&claim, failure, now).await?;
                    stats.retry_scheduled += 1;
                },
                Err(MaterializationClaimError::Failure(failure)) => {
                    self.store.fail(&claim, failure, now).await?;
                    stats.dead_lettered += 1;
                },
                Err(MaterializationClaimError::Settlement(error)) => return Err(error),
            }
        }
        Ok(stats)
    }

    async fn reconcile_claim(
        &self,
        claim: &OAuthMaterializationClaim,
        now: &Timestamp,
    ) -> std::result::Result<ReconciliationOutcome, MaterializationClaimError> {
        if claim.has_bound_credential {
            self.store
                .complete_bound(claim, now)
                .await
                .map_err(MaterializationClaimError::Settlement)?;
            return Ok(ReconciliationOutcome::Bound);
        }
        self.material
            .delete_unstored_tokens(claim.tenant_id, claim.secret_reference_id)
            .await
            .map_err(|_| {
                MaterializationClaimError::Failure(OAuthMaterializationFailure::transient())
            })?;
        self.store
            .complete_deleted(claim, now)
            .await
            .map_err(MaterializationClaimError::Settlement)?;
        Ok(ReconciliationOutcome::Deleted)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconciliationOutcome {
    Bound,
    Deleted,
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
            "OAuth materialization worker id must be a bounded safe token",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::{
        validate_worker_id, OAuthMaterializationClaim, OAuthMaterializationFailure,
        OAuthMaterializationReconciler, OAuthMaterializationStorePort,
        MAX_OAUTH_MATERIALIZATION_BATCH,
    };
    use crate::durable_oauth::{OAuthTokenMaterial, OAuthTokenMaterialPort};
    use crate::oauth::TokenBundle;
    use async_trait::async_trait;
    use mas_common::error::AppError;
    use mas_common::ids::{ConnectorId, SecretReferenceId, TenantId};
    use mas_common::result::Result;
    use mas_common::timestamps::Timestamp;
    use mas_domain::SecretReference;

    #[derive(Debug)]
    struct FailingCompletionStore {
        failures: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl OAuthMaterializationStorePort for FailingCompletionStore {
        async fn claim_due(
            &self,
            _worker_id: &str,
            _now: &Timestamp,
            _limit: u16,
        ) -> Result<Vec<OAuthMaterializationClaim>> {
            Ok(vec![OAuthMaterializationClaim {
                tenant_id: TenantId::new(),
                connector_id: ConnectorId::new(),
                secret_reference_id: SecretReferenceId::new(),
                attempts: 1,
                fence: 1,
                has_bound_credential: true,
            }])
        }

        async fn complete_bound(
            &self,
            _claim: &OAuthMaterializationClaim,
            _now: &Timestamp,
        ) -> Result<()> {
            Err(AppError::database(
                "simulated post-commit completion uncertainty",
            ))
        }

        async fn complete_deleted(
            &self,
            _claim: &OAuthMaterializationClaim,
            _now: &Timestamp,
        ) -> Result<()> {
            unreachable!("bound claim must not attempt deletion")
        }

        async fn fail(
            &self,
            _claim: &OAuthMaterializationClaim,
            _failure: OAuthMaterializationFailure,
            _now: &Timestamp,
        ) -> Result<()> {
            self.failures.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Debug)]
    struct UnusedMaterial;

    #[async_trait]
    impl OAuthTokenMaterialPort for UnusedMaterial {
        fn owns_reference(&self, _reference: &SecretReference) -> bool {
            false
        }

        async fn write_tokens(
            &self,
            _tenant_id: TenantId,
            _reference_id: SecretReferenceId,
            _tokens: &TokenBundle,
        ) -> Result<SecretReference> {
            Err(AppError::internal("unused test material write"))
        }

        async fn read_tokens(
            &self,
            _tenant_id: TenantId,
            _reference: &SecretReference,
        ) -> Result<Option<OAuthTokenMaterial>> {
            Err(AppError::internal("unused test material read"))
        }

        async fn replace_tokens(
            &self,
            _tenant_id: TenantId,
            _reference: &SecretReference,
            _tokens: &TokenBundle,
        ) -> Result<SecretReference> {
            Err(AppError::internal("unused test material replace"))
        }

        async fn delete_tokens(
            &self,
            _tenant_id: TenantId,
            _reference: &SecretReference,
        ) -> Result<()> {
            Err(AppError::internal("unused test material delete"))
        }

        async fn delete_unstored_tokens(
            &self,
            _tenant_id: TenantId,
            _reference_id: SecretReferenceId,
        ) -> Result<()> {
            Err(AppError::internal("unused test material unstored delete"))
        }
    }

    #[test]
    fn materialization_batch_and_worker_id_are_bounded() {
        assert_eq!(MAX_OAUTH_MATERIALIZATION_BATCH, 100);
        assert!(validate_worker_id("oauth-materialization.api_1").is_ok());
        assert!(validate_worker_id("not safe").is_err());
    }

    #[tokio::test]
    async fn unknown_completion_outcome_never_becomes_a_failure_settlement() {
        let failures = Arc::new(AtomicUsize::new(0));
        let worker = OAuthMaterializationReconciler::new(
            Arc::new(UnusedMaterial),
            Arc::new(FailingCompletionStore {
                failures: failures.clone(),
            }),
        );
        let result = worker
            .run_once("materialization-test", &Timestamp::now(), 1)
            .await;
        assert!(result.is_err());
        assert_eq!(failures.load(Ordering::SeqCst), 0);
    }
}
