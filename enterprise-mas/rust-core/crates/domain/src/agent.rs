//! Agent aggregate: orchestration metadata for an executable agent.
//!
//! This aggregate never performs or configures LLM calls directly; runtime
//! model/provider invocation lives in the Python orchestrator. What lives
//! here is identity, lifecycle, capabilities and execution metadata.

use mas_common::enums::AgentStatus;
use mas_common::error::AppError;
use mas_common::ids::{AgentId, OrganizationId, ProjectId, TenantId};
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

use crate::agent_capability::AgentCapability;
use crate::value_objects::{ResourceLimits, Slug};

string_enum! {
    /// Execution role of an agent.
    AgentKind {
        /// Plain worker agent.
        Standard => "standard",
        /// Coordinates/delegates to other agents.
        Supervisor => "supervisor",
        /// Agent whose primary purpose is mediating tool access.
        ToolUser => "tool_user",
    }
}

/// Orchestration-level agent configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Opaque reference resolved by the Python orchestrator to a model/endpoint
    /// (e.g. `model-catalog:gpt-5-mini`). Rust never interprets it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_hint: Option<String>,
    /// Default resource envelope applied to runs of this agent.
    pub resource_limits: ResourceLimits,
    #[serde(default)]
    pub tags: Vec<String>,
    /// Free-form orchestration metadata (timeouts, retry hints, routing).
    #[serde(default)]
    pub metadata: serde_json::Map<String, serde_json::Value>,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            description: None,
            model_hint: None,
            resource_limits: ResourceLimits::default(),
            tags: Vec::new(),
            metadata: serde_json::Map::new(),
        }
    }
}

impl AgentConfig {
    pub fn validate(&self) -> Result<()> {
        self.resource_limits.validate()?;
        if self.tags.len() > mas_common::constants::MAX_TAG_COUNT {
            return Err(AppError::invalid_field(
                "tags",
                "too_many",
                format!(
                    "at most {} tags allowed",
                    mas_common::constants::MAX_TAG_COUNT
                ),
            ));
        }
        for tag in &self.tags {
            validation::validate_length("tag", tag, 1, mas_common::constants::MAX_TAG_LENGTH)?;
        }
        if let Some(description) = &self.description {
            validation::validate_length(
                "description",
                description,
                0,
                mas_common::constants::MAX_DESCRIPTION_LENGTH,
            )?;
        }
        Ok(())
    }
}

/// The agent aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Agent {
    pub id: AgentId,
    pub tenant_id: TenantId,
    pub organization_id: OrganizationId,
    pub project_id: ProjectId,
    pub name: String,
    pub slug: Slug,
    pub kind: AgentKind,
    pub status: AgentStatus,
    pub config: AgentConfig,
    /// Capability references defining what this agent may access.
    #[serde(default)]
    pub capabilities: Vec<AgentCapability>,
    /// Latest published version number (monotonic); `None` while unpublished.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_version: Option<u64>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Agent {
    /// Creates an agent in `Draft` status.
    pub fn create(
        tenant_id: TenantId,
        organization_id: OrganizationId,
        project_id: ProjectId,
        name: impl Into<String>,
        slug: Slug,
        kind: AgentKind,
    ) -> Result<Self> {
        for (field, id_nil) in [
            ("tenant_id", tenant_id.is_nil()),
            ("organization_id", organization_id.is_nil()),
            ("project_id", project_id.is_nil()),
        ] {
            if id_nil {
                return Err(AppError::invalid_field(
                    field,
                    "required",
                    "must be concrete",
                ));
            }
        }
        let name = name.into();
        validation::validate_resource_name("name", &name)?;
        let now = Timestamp::now();
        Ok(Self {
            id: AgentId::new(),
            tenant_id,
            organization_id,
            project_id,
            name,
            slug,
            kind,
            status: AgentStatus::Draft,
            config: AgentConfig::default(),
            capabilities: Vec::new(),
            current_version: None,
            created_at: now,
            updated_at: now,
        })
    }

    /// Full aggregate invariant check (used pre-publish and by repositories).
    pub fn validate(&self) -> Result<()> {
        validation::validate_resource_name("name", &self.name)?;
        self.config.validate()?;
        // Capability pairs must be mutually compatible.
        for (index, capability) in self.capabilities.iter().enumerate() {
            capability.validate()?;
            for other in &self.capabilities[index + 1..] {
                capability.assert_compatible(other)?;
            }
        }
        // Supervisors must be able to delegate: they need at least one
        // agent/workflow capability before publication.
        if self.kind == AgentKind::Supervisor
            && !self.capabilities.iter().any(|cap| cap.allows_delegation())
        {
            return Err(AppError::invalid_field(
                "capabilities",
                "missing_capability",
                "supervisor agents need at least one delegation capability (agent or workflow access)",
            ));
        }
        Ok(())
    }

    /// Grants a capability after compatibility checks.
    pub fn grant_capability(&mut self, capability: AgentCapability) -> Result<()> {
        self.assert_mutable("grant capabilities")?;
        capability.validate()?;
        for existing in &self.capabilities {
            existing.assert_compatible(&capability)?;
        }
        if !self.capabilities.contains(&capability) {
            self.capabilities.push(capability);
            self.touch();
        }
        Ok(())
    }

    /// Revokes all capabilities exactly equal to `capability`.
    pub fn revoke_capability(&mut self, capability: &AgentCapability) -> Result<()> {
        self.assert_mutable("revoke capabilities")?;
        let before = self.capabilities.len();
        self.capabilities.retain(|cap| cap != capability);
        if self.capabilities.len() != before {
            self.touch();
        }
        Ok(())
    }

    /// Replaces configuration after validation. Only in `Draft`/`Disabled`.
    pub fn update_configuration(&mut self, config: AgentConfig) -> Result<()> {
        self.assert_mutable("update configuration")?;
        config.validate()?;
        self.config = config;
        self.touch();
        Ok(())
    }

    /// Draft → Active. Requires a validated configuration.
    pub fn enable(&mut self) -> Result<()> {
        self.validate()?;
        match self.status {
            AgentStatus::Draft | AgentStatus::Disabled => {
                self.status = AgentStatus::Active;
                self.touch();
                Ok(())
            },
            AgentStatus::Active => Ok(()),
            AgentStatus::Archived => Err(AppError::conflict("archived agents cannot be enabled")),
        }
    }

    pub fn disable(&mut self) -> Result<()> {
        match self.status {
            AgentStatus::Active => {
                self.status = AgentStatus::Disabled;
                self.touch();
                Ok(())
            },
            AgentStatus::Disabled => Ok(()),
            other => Err(AppError::conflict(format!(
                "agent in status '{other}' cannot be disabled"
            ))),
        }
    }

    pub fn archive(&mut self) -> Result<()> {
        match self.status {
            AgentStatus::Archived => Ok(()),
            AgentStatus::Active => Err(AppError::conflict(
                "active agents must be disabled before archiving",
            )),
            _ => {
                self.status = AgentStatus::Archived;
                self.touch();
                Ok(())
            },
        }
    }

    /// Records a new published version number (monotonic, set by application
    /// layer after inserting the `AgentVersion` row).
    pub fn record_published_version(&mut self, version: u64) -> Result<()> {
        if let Some(current) = self.current_version {
            if version <= current {
                return Err(AppError::conflict(format!(
                    "version {version} does not advance current version {current}"
                )));
            }
        }
        self.current_version = Some(version);
        self.touch();
        Ok(())
    }

    /// Whether runtime execution may reference this agent.
    #[must_use]
    pub fn is_executable(&self) -> bool {
        self.status == AgentStatus::Active && self.current_version.is_some()
    }

    fn assert_mutable(&self, action: &str) -> Result<()> {
        match self.status {
            AgentStatus::Draft | AgentStatus::Disabled => Ok(()),
            other => Err(AppError::conflict(format!(
                "agent in status '{other}' cannot {action}; disable it first"
            ))),
        }
    }

    fn touch(&mut self) {
        self.updated_at = Timestamp::now();
    }
}
