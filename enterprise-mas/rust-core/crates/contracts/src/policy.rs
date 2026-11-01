//! Policy DTOs.

use mas_common::enums::PolicyDecision;
use mas_common::ids::PolicyId;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

/// Request to create a (initially inactive) policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatePolicyRequest {
    /// Ownership options are interpreted by scope; at least the scope's
    /// required ownership fields must be present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<mas_common::ids::ProjectId>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// `authorization` | `execution` | `data_governance` | `quota` | `compliance`
    pub policy_type: String,
    /// `global` | `organization` | `tenant` | `project` | `agent`
    pub scope: String,
    #[serde(default)]
    pub priority: i32,
    /// Policy DSL document with a non-empty `rules` array.
    pub definition: serde_json::Value,
}

impl CreatePolicyRequest {
    pub fn validate(&self) -> Result<()> {
        validation::validate_resource_name("name", &self.name)?;
        if let Some(description) = &self.description {
            validation::validate_length(
                "description",
                description,
                0,
                mas_common::constants::MAX_DESCRIPTION_LENGTH,
            )?;
        }
        match self.policy_type.as_str() {
            "authorization" | "execution" | "data_governance" | "quota" | "compliance" => {},
            other => {
                return Err(mas_common::error::AppError::invalid_field(
                    "policy_type",
                    "invalid_enum_value",
                    format!("unknown policy type '{other}'"),
                ));
            },
        }
        match self.scope.as_str() {
            "global" | "organization" | "tenant" | "project" | "agent" => {},
            other => {
                return Err(mas_common::error::AppError::invalid_field(
                    "scope",
                    "invalid_enum_value",
                    format!("unknown policy scope '{other}'"),
                ));
            },
        }
        if !(-1000..=1000).contains(&self.priority) {
            return Err(mas_common::error::AppError::invalid_field(
                "priority",
                "out_of_range",
                "priority must be -1000..=1000",
            ));
        }
        let definition = self.definition.as_object().ok_or_else(|| {
            mas_common::error::AppError::invalid_field(
                "definition",
                "invalid_format",
                "definition must be a JSON object",
            )
        })?;
        match definition.get("rules") {
            Some(serde_json::Value::Array(rules)) if !rules.is_empty() => Ok(()),
            _ => Err(mas_common::error::AppError::invalid_field(
                "definition.rules",
                "required",
                "definition must contain a non-empty 'rules' array",
            )),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdatePolicyRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<bool>,
}

impl UpdatePolicyRequest {
    pub fn validate(&self) -> Result<()> {
        if let Some(name) = &self.name {
            validation::validate_resource_name("name", name)?;
        }
        if let Some(priority) = self.priority {
            if !(-1000..=1000).contains(&priority) {
                return Err(mas_common::error::AppError::invalid_field(
                    "priority",
                    "out_of_range",
                    "priority must be -1000..=1000",
                ));
            }
        }
        if let Some(definition) = &self.definition {
            if !definition.is_object() {
                return Err(mas_common::error::AppError::invalid_field(
                    "definition",
                    "invalid_format",
                    "definition must be a JSON object",
                ));
            }
        }
        if self.name.is_none()
            && self.description.is_none()
            && self.priority.is_none()
            && self.definition.is_none()
            && self.active.is_none()
        {
            return Err(mas_common::error::AppError::invalid_field(
                "update",
                "empty_patch",
                "update requests must change at least one field",
            ));
        }
        Ok(())
    }
}

/// Wire representation of a policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyResponse {
    pub id: PolicyId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<mas_common::ids::TenantId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<mas_common::ids::ProjectId>,
    pub name: String,
    pub policy_type: String,
    pub scope: String,
    pub priority: i32,
    pub active: bool,
    pub version: u64,
    #[serde(default)]
    pub definition: serde_json::Value,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

/// Direct policy evaluation request (also used by the Python orchestrator
/// over gRPC).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluatePolicyRequest {
    /// Subject attributes (actor, roles, …).
    #[serde(default)]
    pub subject: serde_json::Value,
    /// Resource attributes (type, id, owner, tags, …).
    #[serde(default)]
    pub resource: serde_json::Value,
    /// Requested action (e.g. `tool.invoke`, `workflow.publish`).
    pub action: String,
    /// Evaluation environment attributes (ip, time, environment, risk).
    #[serde(default)]
    pub environment: serde_json::Value,
}

impl EvaluatePolicyRequest {
    pub fn validate(&self) -> Result<()> {
        validation::validate_non_empty("action", &self.action)?;
        validation::validate_length("action", &self.action, 1, 128)?;
        if !self.subject.is_object() || !self.resource.is_object() || !self.environment.is_object()
        {
            return Err(mas_common::error::AppError::invalid_field(
                "context",
                "invalid_format",
                "subject, resource and environment must be JSON objects",
            ));
        }
        Ok(())
    }
}

/// Outcome of a policy evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyDecisionResponse {
    pub decision: PolicyDecision,
    /// Machine-parsable reason codes (e.g. `rule_matched:deny-external-http`).
    pub reasons: Vec<String>,
    /// Policies that contributed to the decision.
    #[serde(default)]
    pub policy_ids: Vec<PolicyId>,
    /// When `RequireApproval`: the approval reference to honor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_required: Option<ApprovalRequirementDto>,
    /// When `RateLimit`: the hint to apply.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    /// When `Transform`: the modified payload to use instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transformed: Option<serde_json::Value>,
}

/// Approval gate detail.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalRequirementDto {
    /// Who must approve (role name).
    pub approver_role: String,
    /// Deadline of the approval request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<Timestamp>,
}
