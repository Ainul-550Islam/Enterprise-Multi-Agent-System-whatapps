//! PURPOSE:
//! The durable idempotency contract for execution submission.
//!
//! This module owns the *decision* — given the record a key collision found,
//! is this submission new, a safe replay, or a deterministic conflict? It does
//! not own the storage (that is a persistence concern) and it does not own the
//! hashing (that lives one layer up, in `mas-application`, because it needs a
//! digest implementation and this crate stays dependency-light).
//!
//! Layer / ownership
//! -----------------
//! `mas-domain`. Pure data + pure decision logic. The Postgres implementation
//! and the in-memory reference implementation of the durable port both call
//! [`IdempotencyRecord::decide`], so the three outcomes cannot drift between
//! them.
//!
//! Production responsibility
//! -------------------------
//! * Make duplicate submission **safe**: the same key with the same request
//!   fingerprint returns the original execution instead of running paid AI
//!   work twice.
//! * Make conflicting submission **deterministic**: the same key with a
//!   *different* fingerprint is a hard conflict, never a silent overwrite and
//!   never a silent replay of someone else's request.
//! * Keep the fingerprint **secret-free**: only a digest is ever stored, so
//!   the ledger cannot leak request bodies.
//!
//! What this module must NOT do
//! ----------------------------
//! * It must NOT perform a SELECT-then-INSERT. The caller inserts with a
//!   database-enforced unique constraint and only then consults this module to
//!   classify the outcome.
//! * It must NOT store raw request payloads, secrets or credentials.
//! * It must NOT decide *how long* keys are retained — retention is an
//!   operational concern of the persistence layer.

use mas_common::error::AppError;
use mas_common::ids::{ExecutionId, TaskId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

/// Maximum accepted length of a client-supplied idempotency key.
///
/// Deliberately reuses the platform constant so a key accepted by the API can
/// always be stored by the ledger.
pub const MAX_KEY_LENGTH: usize = mas_common::constants::MAX_IDEMPOTENCY_KEY_LENGTH;

// ---------------------------------------------------------------------------
// The record
// ---------------------------------------------------------------------------

/// One row of the idempotency ledger.
///
/// Only digests are stored. `fingerprint` identifies *what* was requested and
/// `response_digest` identifies *what* was returned, so a replay can be proven
/// to be the same answer rather than merely "an answer".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdempotencyRecord {
    /// Owning tenant — the uniqueness scope is (tenant, key).
    pub tenant_id: TenantId,
    /// Client-supplied duplicate-submission guard.
    pub idempotency_key: String,
    /// Hex digest of the canonical request. Never the request itself.
    pub fingerprint: String,
    /// Digest of the response returned for the original submission.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_digest: Option<String>,
    /// Execution the original submission created.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<ExecutionId>,
    /// Task the original submission created.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<TaskId>,
    /// When the original submission committed.
    pub created_at: Timestamp,
    /// Last time the record was read or written.
    pub updated_at: Timestamp,
}

impl IdempotencyRecord {
    /// Builds a record, validating the key and the digest shape.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: TenantId,
        idempotency_key: impl Into<String>,
        fingerprint: impl Into<String>,
        response_digest: Option<String>,
        execution_id: Option<ExecutionId>,
        task_id: Option<TaskId>,
        now: &Timestamp,
    ) -> Result<Self> {
        let idempotency_key = idempotency_key.into();
        let fingerprint = fingerprint.into();
        validation::validate_non_empty("idempotency_key", &idempotency_key)?;
        validation::validate_length("idempotency_key", &idempotency_key, 1, MAX_KEY_LENGTH)?;
        Self::validate_fingerprint(&fingerprint)?;
        if let Some(digest) = response_digest.as_deref() {
            Self::validate_fingerprint(digest)?;
        }
        if execution_id.is_none() && task_id.is_none() {
            return Err(AppError::invalid_field(
                "idempotency_record",
                "missing_target",
                "an idempotency record must reference the execution or task it guards",
            ));
        }
        Ok(Self {
            tenant_id,
            idempotency_key,
            fingerprint,
            response_digest,
            execution_id,
            task_id,
            created_at: *now,
            updated_at: *now,
        })
    }

    /// Whether a fingerprint is structurally a plausible digest.
    ///
    /// The ledger must never store raw request bodies, so a value that is not
    /// a bounded hex token is rejected rather than persisted.
    pub fn validate_fingerprint(fingerprint: &str) -> Result<()> {
        validation::validate_non_empty("fingerprint", fingerprint)?;
        validation::validate_length("fingerprint", fingerprint, 1, 128)?;
        if !fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(AppError::invalid_field(
                "fingerprint",
                "not_a_digest",
                "fingerprints must be hex digests; raw request data is never stored",
            ));
        }
        Ok(())
    }

    /// Classification of a key collision.
    ///
    /// The three outcomes are exhaustive and mutually exclusive:
    ///
    /// * [`IdempotencyDecision::New`] — no record; the caller may proceed.
    /// * [`IdempotencyDecision::Replay`] — same key, same fingerprint: return
    ///   the original result, run nothing.
    /// * [`IdempotencyDecision::Conflict`] — same key, different fingerprint:
    ///   the client reused a key for a different request. This is a client
    ///   error and must surface as `409 Conflict`, never as a silent replay.
    #[must_use]
    pub fn decide(existing: Option<&IdempotencyRecord>, fingerprint: &str) -> IdempotencyDecision {
        match existing {
            None => IdempotencyDecision::New,
            Some(record) if record.fingerprint == fingerprint => IdempotencyDecision::Replay {
                record: record.clone(),
            },
            Some(record) => IdempotencyDecision::Conflict {
                record: record.clone(),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// The decision
// ---------------------------------------------------------------------------

/// Outcome of presenting an idempotency key + fingerprint to the ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdempotencyDecision {
    /// Nothing stored under this key — proceed with submission.
    New,
    /// Same key, same request: replay the recorded result.
    Replay {
        /// The stored record.
        record: IdempotencyRecord,
    },
    /// Same key, different request: a deterministic conflict.
    Conflict {
        /// The stored record (its fingerprint differs from the presented one).
        record: IdempotencyRecord,
    },
}

impl IdempotencyDecision {
    /// `true` when the caller may create new durable work.
    #[must_use]
    pub const fn is_new(&self) -> bool {
        matches!(self, Self::New)
    }

    /// `true` when the caller must return the previously recorded result.
    #[must_use]
    pub const fn is_replay(&self) -> bool {
        matches!(self, Self::Replay { .. })
    }

    /// `true` when the caller must reject the submission.
    #[must_use]
    pub const fn is_conflict(&self) -> bool {
        matches!(self, Self::Conflict { .. })
    }

    /// A stable token for metrics and logs.
    #[must_use]
    pub const fn metric_token(&self) -> &'static str {
        match self {
            Self::New => "new",
            Self::Replay { .. } => "replay",
            Self::Conflict { .. } => "conflict",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> Timestamp {
        Timestamp::from_unix_seconds(1_700_000_000).expect("timestamp")
    }

    fn record(key: &str, fingerprint: &str) -> IdempotencyRecord {
        IdempotencyRecord::new(
            TenantId::new(),
            key,
            fingerprint,
            None,
            Some(ExecutionId::new()),
            Some(TaskId::new()),
            &now(),
        )
        .expect("record")
    }

    const DIGEST_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const DIGEST_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    #[test]
    fn no_record_means_new() {
        let decision = IdempotencyRecord::decide(None, DIGEST_A);
        assert!(decision.is_new());
        assert_eq!(decision.metric_token(), "new");
    }

    #[test]
    fn same_key_same_fingerprint_is_a_safe_replay() {
        let stored = record("k", DIGEST_A);
        let decision = IdempotencyRecord::decide(Some(&stored), DIGEST_A);
        assert!(decision.is_replay());
        assert!(!decision.is_new());
        assert_eq!(decision.metric_token(), "replay");
        match decision {
            IdempotencyDecision::Replay { record } => {
                assert_eq!(record.execution_id, stored.execution_id);
            },
            other => panic!("expected replay, got {other:?}"),
        }
    }

    #[test]
    fn same_key_different_fingerprint_is_a_deterministic_conflict() {
        let stored = record("k", DIGEST_A);
        let decision = IdempotencyRecord::decide(Some(&stored), DIGEST_B);
        assert!(decision.is_conflict());
        assert!(
            !decision.is_replay(),
            "a conflicting request must never replay"
        );
        assert_eq!(decision.metric_token(), "conflict");
        match decision {
            IdempotencyDecision::Conflict { record } => {
                assert_eq!(record.fingerprint, DIGEST_A);
            },
            other => panic!("expected conflict, got {other:?}"),
        }
    }

    #[test]
    fn records_reject_raw_request_data_instead_of_a_digest() {
        let error = IdempotencyRecord::new(
            TenantId::new(),
            "k",
            "{\"prompt\":\"secret\"}",
            None,
            Some(ExecutionId::new()),
            None,
            &now(),
        )
        .expect_err("raw JSON must not be storable as a fingerprint");
        assert!(
            matches!(error, AppError::Validation { .. }),
            "got {error:?}"
        );
    }

    #[test]
    fn records_require_a_target_and_a_non_empty_key() {
        assert!(IdempotencyRecord::new(
            TenantId::new(),
            "",
            DIGEST_A,
            None,
            Some(ExecutionId::new()),
            None,
            &now(),
        )
        .is_err());
        assert!(
            IdempotencyRecord::new(TenantId::new(), "k", DIGEST_A, None, None, None, &now(),)
                .is_err()
        );
        assert!(IdempotencyRecord::new(
            TenantId::new(),
            "k",
            DIGEST_A,
            None,
            None,
            Some(TaskId::new()),
            &now(),
        )
        .is_ok());
        assert!(IdempotencyRecord::new(
            TenantId::new(),
            "x".repeat(MAX_KEY_LENGTH + 1),
            DIGEST_A,
            None,
            Some(ExecutionId::new()),
            None,
            &now(),
        )
        .is_err());
    }

    #[test]
    fn response_digest_is_validated_when_present() {
        assert!(IdempotencyRecord::new(
            TenantId::new(),
            "k",
            DIGEST_A,
            Some("not-hex".to_owned()),
            Some(ExecutionId::new()),
            None,
            &now(),
        )
        .is_err());
        assert!(IdempotencyRecord::new(
            TenantId::new(),
            "k",
            DIGEST_A,
            Some(DIGEST_B.to_owned()),
            Some(ExecutionId::new()),
            None,
            &now(),
        )
        .is_ok());
    }
}
