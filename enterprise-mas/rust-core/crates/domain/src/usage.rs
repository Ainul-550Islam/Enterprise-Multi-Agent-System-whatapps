//! Append-only usage records (metering/billing source) and their
//! aggregation keys.

use mas_common::ids::{ExecutionId, OrganizationId, ProjectId, TenantId, UsageRecordId};
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use serde::{Deserialize, Serialize};

use crate::quota::QuotaDimension;

string_enum! {
    /// Who/what consumed the resource.
    ActorKind {
        User => "user",
        ApiKey => "api_key",
        Service => "service",
        System => "system",
    }
}

/// Generic actor reference (typed IDs belong to the originating subsystem;
/// the reference stores the canonical string form).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorRef {
    pub kind: ActorKind,
    /// Canonical id string (UUID or service name).
    pub id: String,
}

impl ActorRef {
    pub fn new(kind: ActorKind, id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        mas_common::validation::validate_non_empty("actor_id", &id)?;
        Ok(Self { kind, id })
    }

    /// System actor (scheduled/derived work).
    #[must_use]
    pub fn system() -> Self {
        Self {
            kind: ActorKind::System,
            id: "system".to_owned(),
        }
    }
}

/// Deterministic key for rollups: `tenant:dimension:bucket`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct UsageAggregationKey {
    pub tenant_id: TenantId,
    pub dimension: QuotaDimension,
    /// Hour bucket `YYYYMMDDHH` (UTC).
    pub bucket: String,
}

impl UsageAggregationKey {
    #[must_use]
    pub fn hourly(tenant_id: TenantId, dimension: QuotaDimension, at: &Timestamp) -> Self {
        let dt = at.as_datetime();
        Self {
            tenant_id,
            dimension,
            bucket: dt.format("%Y%m%d%H").to_string(),
        }
    }

    #[must_use]
    pub fn day(tenant_id: TenantId, dimension: QuotaDimension, at: &Timestamp) -> Self {
        let dt = at.as_datetime();
        Self {
            tenant_id,
            dimension,
            bucket: dt.format("%Y%m%d").to_string(),
        }
    }

    #[must_use]
    pub fn as_string(&self) -> String {
        format!("{}:{}:{}", self.tenant_id, self.dimension, self.bucket)
    }
}

/// One immutable usage observation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageRecord {
    pub id: UsageRecordId,
    pub tenant_id: TenantId,
    pub organization_id: OrganizationId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<ProjectId>,
    pub actor: ActorRef,
    pub dimension: QuotaDimension,
    /// Amount consumed (units depend on dimension: bytes, tokens, calls, …).
    pub quantity: u64,
    /// Causing execution, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<ExecutionId>,
    /// Non-secret provider/detail metadata (model id, tool id, endpoint).
    #[serde(default)]
    pub metadata: serde_json::Map<String, serde_json::Value>,
    pub occurred_at: Timestamp,
}

impl UsageRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: TenantId,
        organization_id: OrganizationId,
        project_id: Option<ProjectId>,
        actor: ActorRef,
        dimension: QuotaDimension,
        quantity: u64,
        execution_id: Option<ExecutionId>,
    ) -> Result<Self> {
        if quantity == 0 {
            return Err(mas_common::error::AppError::invalid_field(
                "quantity",
                "out_of_range",
                "usage quantity must be positive",
            ));
        }
        Ok(Self {
            id: UsageRecordId::new(),
            tenant_id,
            organization_id,
            project_id,
            actor,
            dimension,
            quantity,
            execution_id,
            metadata: serde_json::Map::new(),
            occurred_at: Timestamp::now(),
        })
    }

    #[must_use]
    pub fn with_metadata(mut self, metadata: serde_json::Map<String, serde_json::Value>) -> Self {
        self.metadata = metadata;
        self
    }

    /// Hourly rollup key for this record.
    #[must_use]
    pub fn hourly_key(&self) -> UsageAggregationKey {
        UsageAggregationKey::hourly(self.tenant_id, self.dimension, &self.occurred_at)
    }

    /// Daily rollup key for this record.
    #[must_use]
    pub fn daily_key(&self) -> UsageAggregationKey {
        UsageAggregationKey::day(self.tenant_id, self.dimension, &self.occurred_at)
    }
}
