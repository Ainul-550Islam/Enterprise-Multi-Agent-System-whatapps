//! Agent lifecycle DTOs.

use mas_common::ids::{AgentId, AgentVersionId, ProjectId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

/// Request to create an agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateAgentRequest {
    pub project_id: ProjectId,
    pub name: String,
    pub slug: String,
    /// `standard` | `supervisor` | `tool_user` (string on the wire).
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_hint: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
}

impl CreateAgentRequest {
    pub fn validate(&self) -> Result<()> {
        validation::validate_resource_name("name", &self.name)?;
        validation::validate_slug("slug", &self.slug)?;
        match self.kind.as_str() {
            "standard" | "supervisor" | "tool_user" => {},
            other => {
                return Err(mas_common::error::AppError::invalid_field(
                    "kind",
                    "invalid_enum_value",
                    format!("unknown agent kind '{other}'"),
                ));
            },
        }
        if let Some(description) = &self.description {
            validation::validate_length(
                "description",
                description,
                0,
                mas_common::constants::MAX_DESCRIPTION_LENGTH,
            )?;
        }
        if self.tags.len() > mas_common::constants::MAX_TAG_COUNT {
            return Err(mas_common::error::AppError::invalid_field(
                "tags",
                "too_many",
                "too many tags",
            ));
        }
        Ok(())
    }
}

/// Request to update an agent (draft/disabled only).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdateAgentRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_hint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    /// Full replacement of the capability list when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Vec<serde_json::Value>>,
}

impl UpdateAgentRequest {
    pub fn validate(&self) -> Result<()> {
        if let Some(name) = &self.name {
            validation::validate_resource_name("name", name)?;
        }
        if let Some(description) = &self.description {
            validation::validate_length(
                "description",
                description,
                0,
                mas_common::constants::MAX_DESCRIPTION_LENGTH,
            )?;
        }
        if !self.has_changes() {
            return Err(mas_common::error::AppError::invalid_field(
                "update",
                "empty_patch",
                "update requests must change at least one field",
            ));
        }
        Ok(())
    }

    #[must_use]
    pub const fn has_changes(&self) -> bool {
        self.name.is_some()
            || self.description.is_some()
            || self.model_hint.is_some()
            || self.tags.is_some()
            || self.capabilities.is_some()
    }
}

/// Request to publish the current draft config as a new immutable version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishAgentRequest {
    pub agent_id: AgentId,
    /// Optional human changelog attached to the version record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub changelog: Option<String>,
}

impl PublishAgentRequest {
    pub fn validate(&self) -> Result<()> {
        if let Some(changelog) = &self.changelog {
            validation::validate_length("changelog", changelog, 0, 4096)?;
        }
        Ok(())
    }
}

/// Request to deploy a published version into an environment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployAgentRequest {
    pub agent_id: AgentId,
    /// Defaults to the latest published version when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_number: Option<u64>,
    pub environment: mas_common::enums::Environment,
}

impl DeployAgentRequest {
    pub fn validate(&self) -> Result<()> {
        if let Some(0) = self.version_number {
            return Err(mas_common::error::AppError::invalid_field(
                "version_number",
                "out_of_range",
                "version numbers start at 1",
            ));
        }
        Ok(())
    }
}

/// Request to roll an agent back to a previously published version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackAgentRequest {
    pub agent_id: AgentId,
    pub target_version_number: u64,
    pub reason: String,
}

impl RollbackAgentRequest {
    pub fn validate(&self) -> Result<()> {
        if self.target_version_number == 0 {
            return Err(mas_common::error::AppError::invalid_field(
                "target_version_number",
                "out_of_range",
                "version numbers start at 1",
            ));
        }
        validation::validate_non_empty("reason", &self.reason)?;
        validation::validate_length("reason", &self.reason, 1, 1024)?;
        Ok(())
    }
}

/// Wire representation of an agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentResponse {
    pub id: AgentId,
    pub project_id: ProjectId,
    pub tenant_id: mas_common::ids::TenantId,
    pub name: String,
    pub slug: String,
    pub kind: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_version_id: Option<AgentVersionId>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}
