//! Agent capabilities: what an agent is allowed to access/execute.

use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::validation;
use serde::{Deserialize, Serialize};

string_enum! {
    /// Category of an agent capability.
    CapabilityType {
        /// May invoke specified tools.
        ToolAccess => "tool_access",
        /// May trigger specified workflows.
        WorkflowAccess => "workflow_access",
        /// May reach specified external systems through connectors.
        ExternalSystemAccess => "external_system_access",
        /// May delegate work to other agents (supervision).
        AgentDelegation => "agent_delegation",
        /// Overrides execution limits (higher ceilings).
        ExecutionLimit => "execution_limit",
    }
}

/// One granted capability.
///
/// `target` identifies the resource being granted:
/// * for `ToolAccess`/`WorkflowAccess`/`AgentDelegation`: a resource ID string
///   or `*` (all, allowed only when `constraints.allow_wildcard`),
/// * for `ExternalSystemAccess`: a connector ID or provider key,
/// * for `ExecutionLimit`: `None` (limits live in `constraints`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCapability {
    pub capability_type: CapabilityType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// Machine-readable constraints (e.g. `{"max_calls": 10}`,
    /// `{"environments": ["staging"]}`, `{"allow_wildcard": false}`).
    #[serde(default)]
    pub constraints: serde_json::Map<String, serde_json::Value>,
}

impl AgentCapability {
    pub fn new(capability_type: CapabilityType, target: Option<String>) -> Result<Self> {
        let capability = Self {
            capability_type,
            target,
            constraints: serde_json::Map::new(),
        };
        capability.validate()?;
        Ok(capability)
    }

    #[must_use]
    pub fn with_constraints(
        mut self,
        constraints: serde_json::Map<String, serde_json::Value>,
    ) -> Self {
        self.constraints = constraints;
        self
    }

    /// Structural validation (semantic authorization happens in policy).
    pub fn validate(&self) -> Result<()> {
        let needs_target = !matches!(self.capability_type, CapabilityType::ExecutionLimit);
        if needs_target {
            let target = self.target.as_deref().ok_or_else(|| {
                AppError::invalid_field(
                    "target",
                    "required",
                    format!("{} capabilities require a target", self.capability_type),
                )
            })?;
            if target != "*" {
                validation::validate_length("target", target, 1, 256)?;
            }
        }
        if self.target.as_deref() == Some("*") && !self.allows_wildcard() {
            return Err(AppError::invalid_field(
                "target",
                "wildcard_not_allowed",
                "wildcard capability requires constraints.allow_wildcard = true",
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn allows_wildcard(&self) -> bool {
        self.constraints
            .get("allow_wildcard")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    }

    /// Whether this capability permits supervising/delegating work items.
    #[must_use]
    pub fn allows_delegation(&self) -> bool {
        matches!(
            self.capability_type,
            CapabilityType::AgentDelegation | CapabilityType::WorkflowAccess
        )
    }

    /// Whether this capability grants access to `target` (exact or wildcard).
    #[must_use]
    pub fn covers_target(&self, target: &str) -> bool {
        match self.target.as_deref() {
            Some("*") => self.allows_wildcard(),
            Some(own) => own == target,
            None => false,
        }
    }

    /// Two capabilities conflict when an `ExecutionLimit` capability and a
    /// restrictive capability of identical type+target coexist with opposing
    /// intent. For now the deterministic rule: duplicates on (type, target)
    /// with different constraints are in conflict; identical pairs are fine.
    pub fn conflicts_with(&self, other: &Self) -> bool {
        self.capability_type == other.capability_type
            && self.target == other.target
            && self.constraints != other.constraints
    }

    /// Fails when `other` conflicts with `self`.
    pub fn assert_compatible(&self, other: &Self) -> Result<()> {
        if self.conflicts_with(other) {
            return Err(AppError::conflict(format!(
                "conflicting {} capabilities for target {:?}",
                self.capability_type, self.target
            )));
        }
        Ok(())
    }
}
