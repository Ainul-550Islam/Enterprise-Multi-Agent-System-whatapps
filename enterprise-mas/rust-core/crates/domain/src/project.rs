//! Project aggregate: a logical workspace grouping agents, workflows, tools
//! and executions under a tenant.

use mas_common::error::AppError;
use mas_common::ids::{OrganizationId, ProjectId, TenantId};
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

use crate::value_objects::{ResourceLimits, Slug};

string_enum! {
    /// Lifecycle status of a project.
    ProjectStatus {
        Active => "active",
        Archived => "archived",
    }
}

/// Project-level configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectConfig {
    /// Default limits applied to executions launched inside the project.
    pub default_resource_limits: ResourceLimits,
    /// Free-form non-secret labels (team, cost center, …).
    #[serde(default)]
    pub labels: serde_json::Map<String, serde_json::Value>,
}

impl Default for ProjectConfig {
    fn default() -> Self {
        Self {
            default_resource_limits: ResourceLimits::default(),
            labels: serde_json::Map::new(),
        }
    }
}

/// The project aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub id: ProjectId,
    pub tenant_id: TenantId,
    pub organization_id: OrganizationId,
    pub name: String,
    pub slug: Slug,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub status: ProjectStatus,
    pub config: ProjectConfig,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Project {
    /// Creates an active project. `tenant_id` and `organization_id` must be
    /// consistent (verified by the caller; re-verified at every trust
    /// boundary by the tenancy layer).
    pub fn create(
        tenant_id: TenantId,
        organization_id: OrganizationId,
        name: impl Into<String>,
        slug: Slug,
    ) -> Result<Self> {
        if tenant_id.is_nil() || organization_id.is_nil() {
            return Err(AppError::invalid_field(
                "project",
                "required",
                "project must belong to a concrete tenant and organization",
            ));
        }
        let name = name.into();
        validation::validate_resource_name("name", &name)?;
        let now = Timestamp::now();
        Ok(Self {
            id: ProjectId::new(),
            tenant_id,
            organization_id,
            name,
            slug,
            description: None,
            status: ProjectStatus::Active,
            config: ProjectConfig::default(),
            created_at: now,
            updated_at: now,
        })
    }

    /// Replaces project configuration after validating it.
    pub fn update_config(&mut self, config: ProjectConfig) -> Result<()> {
        self.assert_active("update configuration")?;
        config.default_resource_limits.validate()?;
        if config.labels.len() > mas_common::constants::MAX_TAG_COUNT {
            return Err(AppError::invalid_field(
                "labels",
                "too_many",
                format!(
                    "at most {} labels are allowed",
                    mas_common::constants::MAX_TAG_COUNT
                ),
            ));
        }
        self.config = config;
        self.touch();
        Ok(())
    }

    pub fn set_description(&mut self, description: Option<String>) -> Result<()> {
        self.assert_active("update description")?;
        if let Some(text) = &description {
            validation::validate_length(
                "description",
                text,
                0,
                mas_common::constants::MAX_DESCRIPTION_LENGTH,
            )?;
        }
        self.description = description;
        self.touch();
        Ok(())
    }

    pub fn archive(&mut self) -> Result<()> {
        match self.status {
            ProjectStatus::Archived => Ok(()),
            ProjectStatus::Active => {
                self.status = ProjectStatus::Archived;
                self.touch();
                Ok(())
            },
        }
    }

    #[must_use]
    pub fn is_active(&self) -> bool {
        self.status == ProjectStatus::Active
    }

    fn assert_active(&self, action: &str) -> Result<()> {
        if self.is_active() {
            Ok(())
        } else {
            Err(AppError::conflict(format!(
                "archived projects cannot {action}"
            )))
        }
    }

    fn touch(&mut self) {
        self.updated_at = Timestamp::now();
    }
}
