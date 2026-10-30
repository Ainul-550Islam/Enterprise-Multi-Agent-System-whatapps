//! Tool DTOs and schema payloads.

use mas_common::enums::ToolStatus;
use mas_common::ids::{AgentId, ToolId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

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

/// Partial update for a tool definition (`PATCH /v1/tools/{id}`).
///
/// An absent field is left unchanged; an explicit `null` on
/// `description`, `output_schema`, or `endpoint_url` clears that field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateToolRequest {
    /// New catalogue name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// `None` ⇒ unchanged, `Some(None)` ⇒ cleared.
    #[serde(
        default,
        deserialize_with = "double_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub description: Option<Option<String>>,
    /// Replacement input JSON-Schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<ToolSchemaDto>,
    /// `None` ⇒ unchanged, `Some(None)` ⇒ cleared.
    #[serde(
        default,
        deserialize_with = "double_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub output_schema: Option<Option<ToolSchemaDto>>,
    /// `None` ⇒ unchanged, `Some(None)` ⇒ cleared, `Some(Some(url))` ⇒ set.
    #[serde(
        default,
        deserialize_with = "double_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub endpoint_url: Option<Option<String>>,
    /// New per-invocation timeout bound.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    /// `standard` | `sensitive` | `destructive`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub safety: Option<String>,
}

/// Deserializes `Option<Option<T>>` so an explicit JSON `null` becomes
/// `Some(None)` (clear) while an absent field stays `None` (unchanged).
fn double_option<'de, D, T>(deserializer: D) -> std::result::Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Some(Option::deserialize(deserializer)?))
}

impl UpdateToolRequest {
    /// Validates only the fields that are present.
    pub fn validate(&self) -> Result<()> {
        if let Some(name) = &self.name {
            validation::validate_resource_name("name", name)?;
        }
        if let Some(Some(description)) = &self.description {
            validation::validate_length(
                "description",
                description,
                0,
                mas_common::constants::MAX_DESCRIPTION_LENGTH,
            )?;
        }
        if let Some(schema) = &self.input_schema {
            schema.validate()?;
        }
        if let Some(Some(schema)) = &self.output_schema {
            schema.validate()?;
        }
        if let Some(Some(endpoint)) = &self.endpoint_url {
            validation::validate_url("endpoint_url", endpoint)?;
        }
        if let Some(0) = self.timeout_ms {
            return Err(mas_common::error::AppError::invalid_field(
                "timeout_ms",
                "out_of_range",
                "timeout must be positive",
            ));
        }
        if let Some(safety) = &self.safety {
            match safety.as_str() {
                "standard" | "sensitive" | "destructive" => {},
                other => {
                    return Err(mas_common::error::AppError::invalid_field(
                        "safety",
                        "invalid_enum_value",
                        format!("unknown safety class '{other}'"),
                    ));
                },
            }
        }
        Ok(())
    }
}

/// Query parameters for a bounded tool catalogue page.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ToolListQuery {
    /// Restrict the page to one project (`None` ⇒ the whole tenant).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<mas_common::ids::ProjectId>,
    /// Maximum number of tools to return.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u16>,
    /// Opaque URL-safe base64 cursor returned by the preceding page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

impl ToolListQuery {
    /// Bounds the page size and the cursor shape before any store is touched.
    pub fn validate(&self) -> Result<()> {
        if self.limit.is_some_and(|limit| !(1..=200).contains(&limit)) {
            return Err(mas_common::error::AppError::invalid_field(
                "limit",
                "out_of_range",
                "tool page limit must be between 1 and 200",
            ));
        }
        if let Some(cursor) = &self.cursor {
            validation::validate_length("cursor", cursor, 1, 512)?;
            if !cursor
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            {
                return Err(mas_common::error::AppError::invalid_field(
                    "cursor",
                    "invalid_format",
                    "tool cursor must be URL-safe base64 without padding",
                ));
            }
        }
        Ok(())
    }

    /// Page size with the default applied.
    #[must_use]
    pub const fn page_limit(&self) -> u16 {
        match self.limit {
            Some(limit) => limit,
            None => 50,
        }
    }
}

/// Stable tenant tool page (metadata only — never runtime endpoints).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolPageResponse {
    pub items: Vec<ToolResponse>,
    pub next_cursor: Option<String>,
}

/// One requested agent tool-permission grant inside a replace call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolPermissionGrantDto {
    /// Tool the grant binds.
    pub tool_id: ToolId,
    /// Project narrowing (`None` ⇒ any project of the tenant).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<mas_common::ids::ProjectId>,
    /// Role narrowing parsed from its canonical lowercase name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// Allowed action names (`invoke`, `read`, … or `*`).
    pub allowed_actions: BTreeSet<String>,
}

/// Full replacement of one agent's tool-permission set
/// (`PUT /v1/agents/{agent_id}/tool-permissions`).
///
/// An empty `grants` list clears the agent's set; the replacement itself is
/// applied atomically by the application service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplaceToolPermissionsRequest {
    pub grants: Vec<ToolPermissionGrantDto>,
}

impl ReplaceToolPermissionsRequest {
    /// Bounds the replacement size; per-grant semantics are validated by the
    /// domain constructor so the same rules hold on every entry path.
    pub fn validate(&self) -> Result<()> {
        if self.grants.len() > 500 {
            return Err(mas_common::error::AppError::invalid_field(
                "grants",
                "too_many_entries",
                "an agent may receive at most 500 tool grants per replacement",
            ));
        }
        Ok(())
    }
}

/// Stable projection of one persisted tool-permission grant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolPermissionResponse {
    pub tool_id: ToolId,
    pub tenant_id: mas_common::ids::TenantId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<mas_common::ids::ProjectId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    pub allowed_actions: BTreeSet<String>,
    pub created_at: Timestamp,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_null_clears_while_absent_leaves_unchanged() {
        let cleared: UpdateToolRequest =
            serde_json::from_value(serde_json::json!({"description": null}))
                .expect("explicit null parses");
        assert!(matches!(cleared.description, Some(None)));
        let unchanged: UpdateToolRequest =
            serde_json::from_value(serde_json::json!({})).expect("absent field parses");
        assert!(unchanged.description.is_none());
        let set: UpdateToolRequest =
            serde_json::from_value(serde_json::json!({"description": "hi"})).expect("value parses");
        assert!(matches!(set.description, Some(Some(ref value)) if value == "hi"));
    }

    #[test]
    fn tool_list_query_bounds_limit_and_cursor() {
        let mut query = ToolListQuery {
            limit: Some(50),
            ..ToolListQuery::default()
        };
        assert!(query.validate().is_ok());
        assert_eq!(query.page_limit(), 50);
        query.limit = Some(0);
        assert!(query.validate().is_err());
        query.limit = None;
        query.cursor = Some("../escape".to_owned());
        assert!(query.validate().is_err());
    }

    #[test]
    fn permission_replace_bounds_grant_count_but_allows_a_clearing_put() {
        let empty = ReplaceToolPermissionsRequest { grants: Vec::new() };
        assert!(empty.validate().is_ok());
        let too_many = ReplaceToolPermissionsRequest {
            grants: vec![
                ToolPermissionGrantDto {
                    tool_id: ToolId::new(),
                    project_id: None,
                    role: Some("viewer".to_owned()),
                    allowed_actions: BTreeSet::from(["invoke".to_owned()]),
                };
                501
            ],
        };
        assert!(too_many.validate().is_err());
    }
}
