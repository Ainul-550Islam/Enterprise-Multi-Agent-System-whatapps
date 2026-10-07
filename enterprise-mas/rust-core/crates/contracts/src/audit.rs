//! Read-only audit-log query DTOs.

use mas_common::ids::AuditEventId;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

/// Filter and keyset page for audit events.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuditQuery {
    /// Exact actor identifier filter.
    pub actor: Option<String>,
    /// Action prefix, e.g. `agent.`.
    pub action: Option<String>,
    /// Resource type filter.
    pub resource: Option<String>,
    /// `success`, `denied`, or `failure`.
    pub outcome: Option<String>,
    /// Inclusive lower bound.
    pub from: Option<Timestamp>,
    /// Exclusive upper bound.
    pub to: Option<Timestamp>,
    /// Opaque keyset cursor returned by the prior page.
    pub cursor: Option<String>,
    /// Maximum page size (default is applied by the service).
    pub limit: Option<u16>,
}

impl AuditQuery {
    /// Validates bounded filters and pagination parameters.
    pub fn validate(&self) -> Result<()> {
        for (field, value) in [
            ("actor", self.actor.as_deref()),
            ("action", self.action.as_deref()),
            ("resource", self.resource.as_deref()),
        ] {
            if let Some(value) = value {
                validation::validate_length(field, value, 1, 256)?;
            }
        }
        if let Some(outcome) = &self.outcome {
            match outcome.as_str() {
                "success" | "denied" | "failure" => {},
                other => {
                    return Err(mas_common::error::AppError::invalid_field(
                        "outcome",
                        "invalid_enum_value",
                        format!("unknown audit outcome '{other}'"),
                    ));
                },
            }
        }
        if let (Some(from), Some(to)) = (self.from, self.to) {
            if !from.is_before(&to) {
                return Err(mas_common::error::AppError::invalid_field(
                    "from",
                    "invalid_range",
                    "audit query from must be before to",
                ));
            }
        }
        if self.limit.is_some_and(|limit| !(1..=200).contains(&limit)) {
            return Err(mas_common::error::AppError::invalid_field(
                "limit",
                "out_of_range",
                "audit page limit must be between 1 and 200",
            ));
        }
        if let Some(cursor) = &self.cursor {
            validation::validate_length("cursor", cursor, 1, 512)?;
        }
        Ok(())
    }
}

/// Safe projection of an append-only audit event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEventDto {
    /// Event identifier.
    pub id: AuditEventId,
    /// Human-readable actor kind.
    pub actor_kind: String,
    /// Actor identifier.
    pub actor_id: String,
    /// Action name.
    pub action: String,
    /// Resource family.
    pub resource_type: String,
    /// Optional resource identifier.
    pub resource_id: Option<String>,
    /// Outcome label.
    pub outcome: String,
    /// Correlation ID.
    pub correlation_id: String,
    /// Pre-redacted event metadata.
    pub metadata: serde_json::Map<String, serde_json::Value>,
    /// Event timestamp.
    pub occurred_at: Timestamp,
}

/// Keyset page of audit events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditPageDto {
    /// Events in reverse chronological order.
    pub items: Vec<AuditEventDto>,
    /// Next page cursor, absent when the result is exhausted.
    pub next_cursor: Option<String>,
}
