//! Tool DTOs and schema payloads.

use mas_common::enums::ToolStatus;
use mas_common::ids::{AgentId, ToolId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

/// JSON-Schema payload for tool inputs/outputs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ToolSchemaDto(pub serde_json::Value);

impl ToolSchemaDto {
    pub fn validate(&self) -> Result<()> {
        if !self.0.is_object() {
            return Err(mas_common::error::AppError::invalid_field(
                "schema",
                "invalid_format",
                "tool schemas must be JSON objects",
            ));
        }
        let size = serde_json::to_vec(&self.0)
            .map_err(|_| mas_common::error::AppError::validation("schema is not serializable"))?
            .len();
        if size > 256 * 1024 {
            return Err(mas_common::error::AppError::invalid_field(
                "schema",
                "too_large",
                "tool schemas are limited to 256 KiB",
            ));
        }
        Ok(())
    }
}

/// Request to register a tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterToolRequest {
    /// When absent, the tool is tenant-global.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<mas_common::ids::ProjectId>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// `internal` | `http` | `database` | `code` | `connector` | `custom`
    pub kind: String,
    pub input_schema: ToolSchemaDto,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<ToolSchemaDto>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    /// `standard` | `sensitive` | `destructive`
    #[serde(default = "default_safety")]
    pub safety: String,
}

fn default_safety() -> String {
    "standard".to_owned()
}

impl RegisterToolRequest {
    pub fn validate(&self) -> Result<()> {
        validation::validate_resource_name("name", &self.name)?;
        match self.kind.as_str() {
            "internal" | "http" | "database" | "code" | "connector" | "custom" => {},
            other => {
                return Err(mas_common::error::AppError::invalid_field(
                    "kind",
                    "invalid_enum_value",
                    format!("unknown tool kind '{other}'"),
                ));
            },
        }
        match self.safety.as_str() {
            "standard" | "sensitive" | "destructive" => {},
            other => {
                return Err(mas_common::error::AppError::invalid_field(
                    "safety",
                    "invalid_enum_value",
                    format!("unknown safety class '{other}'"),
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
        self.input_schema.validate()?;
        if let Some(schema) = &self.output_schema {
            schema.validate()?;
        }
        if let Some(endpoint) = &self.endpoint_url {
            validation::validate_url("endpoint_url", endpoint)?;
        }
        if let Some(timeout_ms) = self.timeout_ms {
            if timeout_ms == 0 {
                return Err(mas_common::error::AppError::invalid_field(
                    "timeout_ms",
                    "out_of_range",
                    "timeout must be positive",
                ));
            }
        }
        Ok(())
    }
}

/// Wire representation of a tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResponse {
    pub id: ToolId,
    pub tenant_id: mas_common::ids::TenantId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<mas_common::ids::ProjectId>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub kind: String,
    pub safety: String,
    pub status: ToolStatus,
    pub input_schema: ToolSchemaDto,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<ToolSchemaDto>,
    pub version: u64,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

/// Request to invoke a tool (policy/quota-checked server-side).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvokeToolRequest {
    pub tool_id: ToolId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    #[serde(default)]
    pub arguments: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

impl InvokeToolRequest {
    pub fn validate(&self) -> Result<()> {
        let size = serde_json::to_vec(&self.arguments)
            .map_err(|_| mas_common::error::AppError::validation("arguments are not serializable"))?
            .len();
        if size > mas_common::constants::MAX_TOOL_ARGUMENT_BYTES {
            return Err(mas_common::error::AppError::invalid_field(
                "arguments",
                "too_large",
                "tool arguments exceed the limit",
            ));
        }
        if let Some(timeout_ms) = self.timeout_ms {
            if timeout_ms == 0 {
                return Err(mas_common::error::AppError::invalid_field(
                    "timeout_ms",
                    "out_of_range",
                    "timeout must be positive",
                ));
            }
        }
        Ok(())
    }
}

/// Result of one tool invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResultResponse {
    pub tool_id: ToolId,
    /// Monotonic tool definition version that served the invocation.
    pub tool_version: u64,
    pub success: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
    /// Stable error when `success == false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<crate::errors::StableApiError>,
    pub duration_ms: u64,
}
