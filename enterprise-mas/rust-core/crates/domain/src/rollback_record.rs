//! PURPOSE: Append-only governance rollback history value object.
//!
//! A rollback does not rewrite the selected version. It records the current
//! version and the newly published version, together with actor, reason and an
//! idempotency key. The selected source version is captured by the new
//! version's `rollback_of` link.

use mas_common::error::AppError;
use mas_common::ids::{RollbackRecordId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::audit_event::AuditActor;
use mas_common::string_enum;

string_enum! {
    /// Governed resource family whose published version was rolled back.
    RollbackResourceKind {
        /// Agent configuration/version.
        Agent => "agent",
        /// Workflow graph/version.
        Workflow => "workflow",
        /// Policy rule-set/version.
        Policy => "policy",
    }
}

/// One immutable record of a forward-versioned rollback operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RollbackRecord {
    pub id: RollbackRecordId,
    pub tenant_id: TenantId,
    pub resource_kind: RollbackResourceKind,
    /// Raw UUID shared by the three resource families; interpreted by kind.
    pub resource_id: Uuid,
    /// Version that was current before this rollback was published.
    pub from_version: u64,
    /// Newly created version; rollback never decrements a version counter.
    pub to_version: u64,
    pub actor: AuditActor,
    pub reason: String,
    pub idempotency_key: String,
    pub created_at: Timestamp,
}

impl RollbackRecord {
    /// Constructs and validates a new history record.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: TenantId,
        resource_kind: RollbackResourceKind,
        resource_id: Uuid,
        from_version: u64,
        to_version: u64,
        actor: AuditActor,
        reason: impl Into<String>,
        idempotency_key: impl Into<String>,
    ) -> Result<Self> {
        let reason = reason.into();
        let idempotency_key = idempotency_key.into();
        validate_fields(
            tenant_id,
            resource_id,
            from_version,
            to_version,
            &actor,
            &reason,
            &idempotency_key,
        )?;
        Ok(Self {
            id: RollbackRecordId::new(),
            tenant_id,
            resource_kind,
            resource_id,
            from_version,
            to_version,
            actor,
            reason,
            idempotency_key,
            created_at: Timestamp::now(),
        })
    }

    /// Rehydrates an immutable history row and reruns its invariants.
    #[allow(clippy::too_many_arguments)]
    pub fn rehydrate(
        id: RollbackRecordId,
        tenant_id: TenantId,
        resource_kind: RollbackResourceKind,
        resource_id: Uuid,
        from_version: u64,
        to_version: u64,
        actor: AuditActor,
        reason: impl Into<String>,
        idempotency_key: impl Into<String>,
        created_at: Timestamp,
    ) -> Result<Self> {
        let reason = reason.into();
        let idempotency_key = idempotency_key.into();
        if id.is_nil() {
            return Err(AppError::database("rollback record has a nil identifier"));
        }
        validate_fields(
            tenant_id,
            resource_id,
            from_version,
            to_version,
            &actor,
            &reason,
            &idempotency_key,
        )
        .map_err(|_| AppError::database("rollback record violates its persisted invariants"))?;
        Ok(Self {
            id,
            tenant_id,
            resource_kind,
            resource_id,
            from_version,
            to_version,
            actor,
            reason,
            idempotency_key,
            created_at,
        })
    }
}

fn validate_fields(
    tenant_id: TenantId,
    resource_id: Uuid,
    from_version: u64,
    to_version: u64,
    actor: &AuditActor,
    reason: &str,
    idempotency_key: &str,
) -> Result<()> {
    if tenant_id.is_nil() || resource_id.is_nil() {
        return Err(AppError::invalid_field(
            "rollback_record",
            "required",
            "tenant and resource identifiers must be concrete",
        ));
    }
    if from_version == 0 || to_version <= from_version || to_version > i64::MAX as u64 {
        return Err(AppError::invalid_field(
            "version",
            "out_of_range",
            "rollback must create a new, greater version number",
        ));
    }
    validation::validate_length("actor.id", &actor.id, 1, 256)?;
    validation::validate_length("reason", reason, 1, 1024)?;
    validation::validate_length("idempotency_key", idempotency_key, 1, 256)?;
    if reason.chars().any(char::is_control) || idempotency_key.chars().any(char::is_control) {
        return Err(AppError::invalid_field(
            "rollback_record",
            "invalid_format",
            "reason and idempotency key must not contain control characters",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_rollback_records_advance_versions_and_are_idempotency_keyed() {
        let record = RollbackRecord::new(
            TenantId::new(),
            RollbackResourceKind::Agent,
            Uuid::new_v4(),
            2,
            3,
            AuditActor::system(),
            "Revert unsafe configuration",
            "rollback-req-1",
        )
        .expect("record");
        assert_eq!(record.from_version, 2);
        assert_eq!(record.to_version, 3);
        assert_eq!(record.idempotency_key, "rollback-req-1");
    }

    #[test]
    fn rollback_cannot_decrement_or_reuse_a_version_number() {
        let error = RollbackRecord::new(
            TenantId::new(),
            RollbackResourceKind::Workflow,
            Uuid::new_v4(),
            4,
            2,
            AuditActor::system(),
            "rollback",
            "key",
        )
        .expect_err("new version must be monotonic");
        assert!(matches!(error, AppError::Validation { .. }));
    }
}
