//! Rust-owned agent snapshot contract for the execution bridge.
//!
//! A snapshot carries an immutable published configuration and authoritative
//! scope. It does not grant access to a Python-side agent catalog.

use std::collections::HashSet;
use std::fmt;

use mas_common::error::AppError;
use mas_common::ids::{AgentId, AgentVersionId, OrganizationId, ProjectId, TenantId};
use mas_common::result::Result;
use mas_domain::{Agent, AgentOrigin, AgentVersion};
use serde::{Deserialize, Serialize};

use crate::agent_bridge::AgentRunRequest;

/// Resource ceilings understood by the Python agent runtime.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AgentRuntimeLimits {
    pub max_steps: u32,
    pub max_tool_calls: u32,
    pub max_delegation_depth: u32,
    pub timeout_seconds: Option<f64>,
}

impl Default for AgentRuntimeLimits {
    fn default() -> Self {
        Self {
            max_steps: 25,
            max_tool_calls: 500,
            max_delegation_depth: 5,
            timeout_seconds: None,
        }
    }
}

/// The closed, versioned subset needed to execute an agent.
#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AgentRuntimeConfiguration {
    pub description: Option<String>,
    pub model_hint: Option<String>,
    pub system_prompt: String,
    pub limits: AgentRuntimeLimits,
    pub tags: Vec<String>,
    pub allowed_tools: Vec<String>,
}

impl fmt::Debug for AgentRuntimeConfiguration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentRuntimeConfiguration")
            .field("contents", &"[REDACTED]")
            .finish()
    }
}

impl AgentRuntimeConfiguration {
    fn validate(&self) -> Result<()> {
        let limits = &self.limits;
        if !(1..=1_000).contains(&limits.max_steps)
            || limits.max_tool_calls > 500
            || limits.max_delegation_depth > 5
            || limits
                .timeout_seconds
                .is_some_and(|value| !value.is_finite() || value <= 0.0 || value > 86_400.0)
        {
            return Err(AppError::validation(
                "agent runtime limits are outside supported bounds",
            ));
        }
        if self.description.as_ref().is_some_and(|s| s.len() > 4096)
            || self
                .model_hint
                .as_ref()
                .is_some_and(|s| s.trim().is_empty() || s.len() > 256)
            || self.system_prompt.len() > 128 * 1024
            || self.tags.len() > 32
            || self.tags.iter().any(|s| s.is_empty() || s.len() > 64)
            || self.allowed_tools.len() > 128
            || self
                .allowed_tools
                .iter()
                .any(|s| s.is_empty() || s.len() > 256)
            || self.allowed_tools.iter().collect::<HashSet<_>>().len() != self.allowed_tools.len()
        {
            return Err(AppError::validation(
                "agent runtime configuration is outside supported bounds",
            ));
        }
        Ok(())
    }
}

/// An exact agent/version selection, including authoritative scope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentRuntimeSnapshot {
    pub agent_id: AgentId,
    pub tenant_id: TenantId,
    pub organization_id: OrganizationId,
    pub project_id: ProjectId,
    pub agent_version_id: AgentVersionId,
    pub version_number: u64,
    pub configuration_checksum: String,
    pub name: String,
    pub slug: String,
    pub kind: String,
    pub status: String,
    pub origin: String,
    pub read_only: bool,
    pub configuration: AgentRuntimeConfiguration,
}

impl AgentRuntimeSnapshot {
    /// Materialize a closed runtime configuration from a published version.
    pub fn from_domain(agent: &Agent, version: &AgentVersion) -> Result<Self> {
        if !agent.is_executable() || version.agent_id() != agent.id {
            return Err(AppError::validation(
                "agent version is not executable for this agent",
            ));
        }
        if agent.id.is_nil()
            || agent.tenant_id.is_nil()
            || agent.organization_id.is_nil()
            || agent.project_id.is_nil()
            || version.id().is_nil()
        {
            return Err(AppError::validation(
                "agent runtime snapshot requires concrete scope",
            ));
        }
        let configuration: AgentRuntimeConfiguration =
            serde_json::from_value(version.configuration_snapshot().clone()).map_err(|_| {
                AppError::validation(
                    "published agent configuration is not a valid runtime snapshot",
                )
            })?;
        configuration.validate()?;
        if agent.name.is_empty() || agent.name.len() > 128 || agent.slug.as_str().len() > 64 {
            return Err(AppError::validation(
                "agent identity is outside runtime bounds",
            ));
        }
        let snapshot = Self {
            agent_id: agent.id,
            tenant_id: agent.tenant_id,
            organization_id: agent.organization_id,
            project_id: agent.project_id,
            agent_version_id: version.id(),
            version_number: version.version_number(),
            configuration_checksum: version.configuration_checksum().to_owned(),
            name: agent.name.clone(),
            slug: agent.slug.as_str().to_owned(),
            kind: agent.kind.to_string(),
            status: agent.status.to_string(),
            origin: agent.origin.to_string(),
            read_only: agent.origin == AgentOrigin::System,
            configuration,
        };
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|_| AppError::serialization("agent snapshot serialization failed"))?;
        if bytes.len() > 512 * 1024 {
            return Err(AppError::validation(
                "agent runtime snapshot exceeds maximum size",
            ));
        }
        Ok(snapshot)
    }
}

/// Resolves a request under the worker's authoritative tenant scope.
#[async_trait::async_trait]
pub trait AgentRuntimeSnapshotResolver: Send + Sync + fmt::Debug {
    async fn resolve(&self, request: &AgentRunRequest) -> Result<AgentRuntimeSnapshot>;
}
