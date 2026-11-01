//! Organization aggregate: the top-level administrative/billing boundary.

use mas_common::error::AppError;
use mas_common::ids::OrganizationId;
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

use crate::value_objects::Slug;

string_enum! {
    /// Lifecycle status of an organization.
    OrganizationStatus {
        Active => "active",
        Suspended => "suspended",
        Archived => "archived",
    }
}

impl OrganizationStatus {
    /// Whether tenants under this organization may operate.
    #[must_use]
    pub const fn is_operational(&self) -> bool {
        matches!(self, Self::Active)
    }
}

/// Free-form, non-secret organization settings (plan feature toggles,
/// branding metadata, default region, …).
pub type OrganizationSettings = serde_json::Map<String, serde_json::Value>;

/// The organization aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Organization {
    pub id: OrganizationId,
    /// Registered legal entity name.
    pub legal_name: String,
    /// Human-friendly name shown in UIs.
    pub display_name: String,
    /// Globally unique URL-safe short name.
    pub slug: Slug,
    pub status: OrganizationStatus,
    #[serde(default)]
    pub settings: OrganizationSettings,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Organization {
    /// Creates an active organization after validating identity fields.
    pub fn create(
        legal_name: impl Into<String>,
        display_name: impl Into<String>,
        slug: Slug,
    ) -> Result<Self> {
        let legal_name = legal_name.into();
        let display_name = display_name.into();
        validation::validate_resource_name("legal_name", &legal_name)?;
        validation::validate_resource_name("display_name", &display_name)?;

        let now = Timestamp::now();
        Ok(Self {
            id: OrganizationId::new(),
            legal_name,
            display_name,
            slug,
            status: OrganizationStatus::Active,
            settings: OrganizationSettings::default(),
            created_at: now,
            updated_at: now,
        })
    }

    /// Invariants that must hold for any persisted organization.
    pub fn validate(&self) -> Result<()> {
        validation::validate_resource_name("legal_name", &self.legal_name)?;
        validation::validate_resource_name("display_name", &self.display_name)?;
        if self.settings.len() > 256 {
            return Err(AppError::invalid_field(
                "settings",
                "too_large",
                "settings may contain at most 256 keys",
            ));
        }
        Ok(())
    }

    /// Suspended → Active. Archived is terminal.
    pub fn activate(&mut self) -> Result<()> {
        match self.status {
            OrganizationStatus::Suspended => {
                self.status = OrganizationStatus::Active;
                self.touch();
                Ok(())
            },
            OrganizationStatus::Active => Ok(()), // idempotent
            OrganizationStatus::Archived => Err(AppError::conflict(
                "archived organizations cannot be reactivated",
            )),
        }
    }

    /// Active → Suspended (stops tenant operations; data preserved).
    pub fn suspend(&mut self) -> Result<()> {
        match self.status {
            OrganizationStatus::Active => {
                self.status = OrganizationStatus::Suspended;
                self.touch();
                Ok(())
            },
            OrganizationStatus::Suspended => Ok(()),
            OrganizationStatus::Archived => Err(AppError::conflict(
                "archived organizations cannot be suspended",
            )),
        }
    }

    /// Active|Suspended → Archived (terminal).
    pub fn archive(&mut self) -> Result<()> {
        match self.status {
            OrganizationStatus::Archived => Ok(()),
            _ => {
                self.status = OrganizationStatus::Archived;
                self.touch();
                Ok(())
            },
        }
    }

    /// Renames the display name (legal name changes go through compliance).
    pub fn rename_display(&mut self, display_name: impl Into<String>) -> Result<()> {
        let display_name = display_name.into();
        validation::validate_resource_name("display_name", &display_name)?;
        self.display_name = display_name;
        self.touch();
        Ok(())
    }

    fn touch(&mut self) {
        self.updated_at = Timestamp::now();
    }
}
