//! Workflow nodes: the executable units of a workflow graph.

use mas_common::constants::DEFAULT_TOOL_TIMEOUT_MS;
use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::validation;
use serde::{Deserialize, Serialize};

string_enum! {
    /// Kind of a workflow node.
    WorkflowNodeType {
        /// Exactly one per graph; the implicit entry point.
        Start => "start",
        /// Invokes an agent.
        Agent => "agent",
        /// Invokes a tool.
        Tool => "tool",
        /// Branches on an evaluated expression.
        Condition => "condition",
        /// Fans out into parallel branches.
        Parallel => "parallel",
        /// Waits for parallel branches to converge.
        Join => "join",
        /// Suspends execution for a duration.
        Delay => "delay",
        /// Pure data transformation of the execution context.
        Transform => "transform",
        /// Blocks until a human approves/rejects.
        Approval => "approval",
        /// At least one per graph; terminates a path.
        End => "end",
    }
}

impl WorkflowNodeType {
    /// Nodes that produce outgoing control flow.
    #[must_use]
    pub const fn has_outgoing(&self) -> bool {
        !matches!(self, Self::End)
    }

    /// Nodes that accept incoming control flow.
    #[must_use]
    pub const fn has_incoming(&self) -> bool {
        !matches!(self, Self::Start)
    }
}

/// Per-node execution limits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCapability {
    /// Timeout for this node in milliseconds.
    pub timeout_ms: u64,
    /// Retries for retryable failures of this node.
    pub max_retries: u32,
}

impl Default for NodeCapability {
    fn default() -> Self {
        Self {
            timeout_ms: DEFAULT_TOOL_TIMEOUT_MS,
            max_retries: mas_common::constants::DEFAULT_MAX_RETRIES,
        }
    }
}

/// One node of a workflow graph.
///
/// `node_key` is the stable human-authored identifier referenced by edges
/// (unique per graph); `id` (persistence) is assigned by the repository.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowNode {
    pub node_key: String,
    pub node_type: WorkflowNodeType,
    pub name: String,
    /// Type-specific configuration, validated by [`WorkflowNode::validate`]:
    /// * `agent`: `{ "agent_ref": "<id|slug>" }`
    /// * `tool`: `{ "tool_ref": "<id|slug>", "arguments_template"?: … }`
    /// * `condition`: `{ "expression": "…" }`
    /// * `delay`: `{ "duration_ms": <u64> }`
    /// * `approval`: `{ "approver_role": "…", "instructions"?: "…" }`
    /// * `transform`: `{ "template": … }`
    /// * others: free-form metadata
    #[serde(default)]
    pub config: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    pub capability: NodeCapability,
}

impl WorkflowNode {
    pub fn new(
        node_key: impl Into<String>,
        node_type: WorkflowNodeType,
        name: impl Into<String>,
    ) -> Result<Self> {
        let node = Self {
            node_key: node_key.into(),
            node_type,
            name: name.into(),
            config: serde_json::Map::new(),
            capability: NodeCapability::default(),
        };
        // Config-dependent requirements (agent_ref/tool_ref/…) are enforced by
        // `validate()` — via `WorkflowGraph::validate` at publish/run time —
        // because `with_config` is applied *after* construction.
        node.validate_structure()?;
        Ok(node)
    }

    #[must_use]
    pub fn with_config(mut self, config: serde_json::Map<String, serde_json::Value>) -> Self {
        self.config = config;
        self
    }

    #[must_use]
    pub fn with_capability(mut self, capability: NodeCapability) -> Self {
        self.capability = capability;
        self
    }

    /// Structural + type-specific validation.
    pub fn validate(&self) -> Result<()> {
        self.validate_structure()?;
        self.validate_required_config()
    }

    /// Config-independent structural checks (runs in [`WorkflowNode::new`]).
    fn validate_structure(&self) -> Result<()> {
        if self.node_key.is_empty()
            || self.node_key.len() > 128
            || !self
                .node_key
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
        {
            return Err(AppError::invalid_field(
                "node_key",
                "invalid_format",
                "node keys must be 1..=128 chars of [A-Za-z0-9_-]",
            ));
        }
        validation::validate_resource_name("name", &self.name)?;
        if self.capability.timeout_ms == 0 {
            return Err(AppError::invalid_field(
                "capability.timeout_ms",
                "out_of_range",
                "node timeout must be positive",
            ));
        }
        if self.capability.max_retries > mas_common::constants::MAX_ALLOWED_RETRIES {
            return Err(AppError::invalid_field(
                "capability.max_retries",
                "out_of_range",
                format!(
                    "node retries may not exceed {}",
                    mas_common::constants::MAX_ALLOWED_RETRIES
                ),
            ));
        }
        Ok(())
    }

    /// Type-specific required configuration (runs in [`WorkflowNode::validate`]).
    fn validate_required_config(&self) -> Result<()> {
        // Type-specific required configuration.
        match self.node_type {
            WorkflowNodeType::Agent => {
                self.required_string("agent_ref")?;
            },
            WorkflowNodeType::Tool => {
                self.required_string("tool_ref")?;
            },
            WorkflowNodeType::Condition => {
                let expression = self.required_string("expression")?;
                if expression.len() > 4096 {
                    return Err(AppError::invalid_field(
                        "config.expression",
                        "too_long",
                        "condition expressions are limited to 4096 characters",
                    ));
                }
            },
            WorkflowNodeType::Delay => {
                let duration = self
                    .config
                    .get("duration_ms")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| {
                        AppError::invalid_field(
                            "config.duration_ms",
                            "required",
                            "delay nodes require an integer duration_ms",
                        )
                    })?;
                if duration == 0 || duration > mas_common::constants::MAX_EXECUTION_DURATION_MS {
                    return Err(AppError::invalid_field(
                        "config.duration_ms",
                        "out_of_range",
                        "delay must be within execution duration limits",
                    ));
                }
            },
            WorkflowNodeType::Approval => {
                self.required_string("approver_role")?;
            },
            WorkflowNodeType::Transform => {
                if !self.config.contains_key("template") {
                    return Err(AppError::invalid_field(
                        "config.template",
                        "required",
                        "transform nodes require a template",
                    ));
                }
            },
            WorkflowNodeType::Start
            | WorkflowNodeType::Parallel
            | WorkflowNodeType::Join
            | WorkflowNodeType::End => {},
        }
        Ok(())
    }

    fn required_string(&self, key: &str) -> Result<String> {
        self.config
            .get(key)
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
            .ok_or_else(|| {
                AppError::invalid_field(
                    format!("config.{key}"),
                    "required",
                    format!("{} nodes require a non-empty '{key}'", self.node_type),
                )
            })
    }
}
