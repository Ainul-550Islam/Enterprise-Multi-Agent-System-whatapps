//! Tool permissions: bindings deciding who may invoke which tool, how.

use mas_common::error::AppError;
use mas_common::ids::{AgentId, ProjectId, TenantId, ToolId};
use mas_common::result::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

use crate::membership::MembershipRole;

/// Caller context used to answer "may this invocation happen?".
///
/// `None` scopes in a [`ToolPermission`] are wildcards; the *most specific*
/// matching permission wins in the application layer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolInvocationContext {
    pub tenant_id: TenantId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<ProjectId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<MembershipRole>,
    /// Requested action (e.g. `invoke`, `read`, `write`).
    pub action: String,
}

/// A scoped allow-rule binding a tool to tenant/project/agent/role + actions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolPermission {
    pub tool_id: ToolId,
    pub tenant_id: TenantId,
    /// `None` ⇒ any project of the tenant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<ProjectId>,
    /// `None` ⇒ any agent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    /// `None` ⇒ any role.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<MembershipRole>,
    pub allowed_actions: BTreeSet<String>,
    pub created_at: mas_common::timestamps::Timestamp,
}

impl ToolPermission {
    /// Creates a permission; at least one action and one non-tenant scope
    /// narrowing is required to avoid accidental tenant-wide tools.
    pub fn new(
        tool_id: ToolId,
        tenant_id: TenantId,
        project_id: Option<ProjectId>,
        agent_id: Option<AgentId>,
        role: Option<MembershipRole>,
        allowed_actions: BTreeSet<String>,
    ) -> Result<Self> {
        if allowed_actions.is_empty() {
            return Err(AppError::invalid_field(
                "allowed_actions",
                "required",
                "a permission must allow at least one action",
            ));
        }
        for action in &allowed_actions {
            if action.is_empty()
                || action.len() > 64
                || !action.chars().all(|ch| {
                    ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-' | ':' | '*')
                })
            {
                return Err(AppError::invalid_field(
                    "allowed_actions",
                    "invalid_format",
                    format!("invalid action name {action:?}"),
                ));
            }
        }
        if project_id.is_none() && agent_id.is_none() && role.is_none() {
            return Err(AppError::invalid_field(
                "scope",
                "too_broad",
                "tenant-wide permissions require at least one narrowing scope (project, agent or role)",
            ));
        }
        Ok(Self {
            tool_id,
            tenant_id,
            project_id,
            agent_id,
            role,
            allowed_actions,
            created_at: mas_common::timestamps::Timestamp::now(),
        })
    }

    /// Whether `context` is covered by this permission (scope equality or
    /// wildcard) and the action is allowed (exact or `*`).
    #[must_use]
    pub fn can_invoke(&self, context: &ToolInvocationContext) -> bool {
        if self.tenant_id != context.tenant_id {
            return false;
        }
        if let Some(required) = self.project_id {
            if context.project_id != Some(required) {
                return false;
            }
        }
        if let Some(required) = self.agent_id {
            if context.agent_id != Some(required) {
                return false;
            }
        }
        if let Some(required) = self.role {
            if context.role != Some(required) {
                return false;
            }
        }
        self.allowed_actions.contains(&context.action) || self.allowed_actions.contains("*")
    }

    /// Specificity score (more narrowed scopes ⇒ higher precedence).
    #[must_use]
    pub const fn specificity(&self) -> u8 {
        (self.project_id.is_some() as u8)
            + (self.agent_id.is_some() as u8)
            + (self.role.is_some() as u8)
    }
}
