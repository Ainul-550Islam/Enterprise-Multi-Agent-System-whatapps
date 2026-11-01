//! Tool aggregate: registered invocable capabilities (HTTP endpoints, code,
//! connector-mediated operations, …).

use mas_common::enums::ToolStatus;
use mas_common::error::AppError;
use mas_common::ids::{OrganizationId, ProjectId, TenantId, ToolId};
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

use crate::value_objects::SafeUrl;

string_enum! {
    /// Implementation kind of a tool.
    ToolKind {
        /// Built into the platform (state transforms etc.).
        Internal => "internal",
        /// Invoked over HTTP(S).
        Http => "http",
        /// Backed by a database connector.
        Database => "database",
        /// Sandboxed code execution.
        Code => "code",
        /// Mediated by an external connector.
        Connector => "connector",
        /// Tenant-supplied custom runtime.
        Custom => "custom",
    }
}

string_enum! {
    /// Risk classification driving approval gates and sandboxing.
    SafetyClassification {
        /// Read-only, no side effects.
        Standard => "standard",
        /// Has side effects / carries tenant data.
        Sensitive => "sensitive",
        /// Irreversible or destructive operations.
        Destructive => "destructive",
    }
}

/// Runtime metadata for invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRuntime {
    /// Invocation endpoint (required for `Http`/`Connector`, forbidden-ish for `Internal`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<SafeUrl>,
    /// Invocation timeout (ms).
    pub timeout_ms: u64,
    /// Whether execution must happen inside the sandbox.
    pub sandbox_required: bool,
    /// Extra, non-secret runtime configuration.
    #[serde(default)]
    pub settings: serde_json::Map<String, serde_json::Value>,
}

impl Default for ToolRuntime {
    fn default() -> Self {
        Self {
            endpoint: None,
            timeout_ms: mas_common::constants::DEFAULT_TOOL_TIMEOUT_MS,
            sandbox_required: false,
            settings: serde_json::Map::new(),
        }
    }
}

/// The tool aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tool {
    pub id: ToolId,
    pub tenant_id: TenantId,
    pub organization_id: OrganizationId,
    /// Owning project; `None` for tenant-global tools.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<ProjectId>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub kind: ToolKind,
    /// JSON Schema for the invocation arguments.
    pub input_schema: serde_json::Value,
    /// JSON Schema for the result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<serde_json::Value>,
    pub runtime: ToolRuntime,
    pub safety: SafetyClassification,
    pub status: ToolStatus,
    /// Monotonic definition version, bumped on every change.
    pub version: u64,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Tool {
    /// Creates a tool in `Draft`.
    #[allow(clippy::too_many_arguments)]
    pub fn register(
        tenant_id: TenantId,
        organization_id: OrganizationId,
        project_id: Option<ProjectId>,
        name: impl Into<String>,
        kind: ToolKind,
        input_schema: serde_json::Value,
        output_schema: Option<serde_json::Value>,
    ) -> Result<Self> {
        let tool = Self {
            id: ToolId::new(),
            tenant_id,
            organization_id,
            project_id,
            name: name.into(),
            description: None,
            kind,
            input_schema,
            output_schema,
            runtime: ToolRuntime::default(),
            safety: SafetyClassification::Standard,
            status: ToolStatus::Draft,
            version: 1,
            created_at: Timestamp::now(),
            updated_at: Timestamp::now(),
        };
        tool.validate()?;
        Ok(tool)
    }

    /// Structural validation (schemas as JSON-Schema objects, runtime/kind
    /// coherence, safety defaults).
    pub fn validate(&self) -> Result<()> {
        validation::validate_resource_name("name", &self.name)?;
        Self::validate_schema(&self.input_schema, "input_schema")?;
        if let Some(schema) = &self.output_schema {
            Self::validate_schema(schema, "output_schema")?;
        }
        match self.kind {
            ToolKind::Http | ToolKind::Connector | ToolKind::Custom => {
                if self.runtime.endpoint.is_none() {
                    return Err(AppError::invalid_field(
                        "runtime.endpoint",
                        "required",
                        format!("{} tools require an endpoint", self.kind),
                    ));
                }
            },
            ToolKind::Code => {
                if !self.runtime.sandbox_required {
                    return Err(AppError::invalid_field(
                        "runtime.sandbox_required",
                        "required",
                        "code execution tools must run sandboxed",
                    ));
                }
            },
            ToolKind::Internal | ToolKind::Database => {},
        }
        if self.runtime.timeout_ms == 0
            || self.runtime.timeout_ms > mas_common::constants::MAX_EXECUTION_DURATION_MS
        {
            return Err(AppError::invalid_field(
                "runtime.timeout_ms",
                "out_of_range",
                "tool timeout out of range",
            ));
        }
        Ok(())
    }

    fn validate_schema(schema: &serde_json::Value, field: &'static str) -> Result<()> {
        let object = schema.as_object().ok_or_else(|| {
            AppError::invalid_field(field, "invalid_schema", "schema must be a JSON object")
        })?;
        if let Some(schema_type) = object.get("type") {
            if schema_type.as_str() != Some("object") {
                return Err(AppError::invalid_field(
                    field,
                    "invalid_schema",
                    "top-level schema type must be 'object'",
                ));
            }
        }
        Ok(())
    }

    pub fn set_runtime(&mut self, runtime: ToolRuntime) -> Result<()> {
        let before = std::mem::replace(&mut self.runtime, runtime);
        if let Err(err) = self.validate() {
            self.runtime = before; // rollback on invalid runtime
            return Err(err);
        }
        self.bump();
        Ok(())
    }

    /// Draft → Published.
    pub fn publish(&mut self) -> Result<()> {
        match self.status {
            ToolStatus::Draft | ToolStatus::Deprecated => {
                self.validate()?;
                self.status = ToolStatus::Published;
                self.bump();
                Ok(())
            },
            ToolStatus::Published => Ok(()),
            ToolStatus::Disabled => Err(AppError::conflict(
                "disabled tools must not be re-published directly; re-create a draft",
            )),
        }
    }

    /// Published → Deprecated (still invocable, hidden from new bindings).
    pub fn deprecate(&mut self) -> Result<()> {
        match self.status {
            ToolStatus::Published => {
                self.status = ToolStatus::Deprecated;
                self.bump();
                Ok(())
            },
            ToolStatus::Deprecated => Ok(()),
            other => Err(AppError::conflict(format!(
                "tool in status '{other}' cannot be deprecated"
            ))),
        }
    }

    /// Any → Disabled (terminal for this tool record).
    pub fn disable(&mut self) -> Result<()> {
        match self.status {
            ToolStatus::Disabled => Ok(()),
            _ => {
                self.status = ToolStatus::Disabled;
                self.bump();
                Ok(())
            },
        }
    }

    /// Whether the tool may be invoked at runtime.
    #[must_use]
    pub fn is_invocable(&self) -> bool {
        matches!(self.status, ToolStatus::Published | ToolStatus::Deprecated)
    }

    fn bump(&mut self) {
        self.version += 1;
        self.updated_at = Timestamp::now();
    }
}
