//! Immutable audit events.
//!
//! Audit records are **append-only**: there are no mutating methods and no
//! update/delete paths anywhere in the platform. Metadata attached here must
//! already be redacted (callers run `mas_common::redaction` first).

use mas_common::enums::AuditSeverity;
use mas_common::ids::{AuditEventId, OrganizationId, TenantId};
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

string_enum! {
    /// Kind of actor that performed the audited action.
    AuditActorKind {
        User => "user",
        ApiKey => "api_key",
        Service => "service",
        System => "system",
        Anonymous => "anonymous",
    }
}

string_enum! {
    /// Outcome of the audited action.
    AuditOutcome {
        Success => "success",
        Failure => "failure",
        Denied => "denied",
    }
}

string_enum! {
    /// Compliance classification for retention/scoping rules.
    AuditComplianceClass {
        General => "general",
        Security => "security",
        Privacy => "privacy",
        Financial => "financial",
    }
}

/// The actor of an audited action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditActor {
    pub kind: AuditActorKind,
    /// Canonical id string (user id / api key id / service name).
    pub id: String,
    /// Optional display hint (never secrets; emails must be redacted).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display: Option<String>,
}

impl AuditActor {
    pub fn new(kind: AuditActorKind, id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        validation::validate_non_empty("actor_id", &id)?;
        Ok(Self {
            kind,
            id,
            display: None,
        })
    }

    #[must_use]
    pub fn system() -> Self {
        Self {
            kind: AuditActorKind::System,
            id: "system".to_owned(),
            display: None,
        }
    }
}

/// One immutable audit record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub id: AuditEventId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<TenantId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organization_id: Option<OrganizationId>,
    pub actor: AuditActor,
    /// Verb in `subject.verb` form (e.g. `agent.publish`, `policy.evaluate`).
    pub action: String,
    /// Resource category (e.g. `agent`, `workflow`, `policy`).
    pub resource_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_id: Option<String>,
    pub outcome: AuditOutcome,
    pub severity: AuditSeverity,
    pub compliance_class: AuditComplianceClass,
    /// End-to-end correlation (request/execution) id.
    pub correlation_id: String,
    /// Attached, pre-redacted metadata.
    #[serde(default)]
    pub metadata: serde_json::Map<String, serde_json::Value>,
    pub occurred_at: Timestamp,
}

impl AuditEvent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: Option<TenantId>,
        organization_id: Option<OrganizationId>,
        actor: AuditActor,
        action: impl Into<String>,
        resource_type: impl Into<String>,
        resource_id: Option<String>,
        outcome: AuditOutcome,
        correlation_id: impl Into<String>,
    ) -> Result<Self> {
        let action = action.into();
        let resource_type = resource_type.into();
        let correlation_id = correlation_id.into();
        validation::validate_non_empty("action", &action)?;
        validation::validate_length("action", &action, 1, 128)?;
        validation::validate_non_empty("resource_type", &resource_type)?;
        validation::validate_non_empty("correlation_id", &correlation_id)?;

        let mut event = Self {
            id: AuditEventId::new(),
            tenant_id,
            organization_id,
            actor,
            action,
            resource_type,
            resource_id,
            outcome,
            severity: AuditSeverity::Info,
            compliance_class: AuditComplianceClass::General,
            correlation_id,
            metadata: serde_json::Map::new(),
            occurred_at: Timestamp::now(),
        };
        event.apply_default_classification();
        Ok(event)
    }

    #[must_use]
    pub fn with_severity(mut self, severity: AuditSeverity) -> Self {
        self.severity = severity;
        self
    }

    #[must_use]
    pub fn with_compliance_class(mut self, class: AuditComplianceClass) -> Self {
        self.compliance_class = class;
        self
    }

    #[must_use]
    pub fn with_metadata(mut self, metadata: serde_json::Map<String, serde_json::Value>) -> Self {
        self.metadata = metadata;
        self
    }

    /// Classification heuristic: security-relevant actions/verbs get
    /// `Security` + higher floor severity unless the caller overrides.
    fn apply_default_classification(&mut self) {
        let security_verbs = [
            "auth.",
            "login",
            "authorize",
            "policy.deny",
            "policy.evaluate",
            "token",
            "api_key.",
            "secret.",
            "credential.",
            "tenant.",
            "membership.",
        ];
        if self.outcome == AuditOutcome::Denied
            || security_verbs
                .iter()
                .any(|verb| self.action.starts_with(verb) || self.action.contains(verb))
        {
            self.compliance_class = AuditComplianceClass::Security;
            if self.outcome == AuditOutcome::Denied && self.severity < AuditSeverity::Warning {
                self.severity = AuditSeverity::Warning;
            }
        }
        if self.outcome == AuditOutcome::Failure && self.severity < AuditSeverity::Notice {
            self.severity = AuditSeverity::Notice;
        }
    }

    /// Whether the event belongs to a compliance-defended category (stricter
    /// retention, export controls).
    #[must_use]
    pub const fn is_compliance_significant(&self) -> bool {
        !matches!(self.compliance_class, AuditComplianceClass::General)
    }
}
