//! Tenant aggregate: an isolated customer workspace inside an organization.

use mas_common::enums::{Environment, TenantStatus};
use mas_common::error::AppError;
use mas_common::ids::{OrganizationId, TenantId};
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

use crate::value_objects::Slug;

string_enum! {
    /// Data-isolation mode applied to the tenant.
    IsolationMode {
        /// Row-level security inside the shared schema.
        SharedRls => "shared_rls",
        /// Dedicated schema inside the shared database cluster.
        SchemaPerTenant => "schema_per_tenant",
        /// Fully dedicated database (enterprise tier).
        DedicatedDatabase => "dedicated_database",
    }
}

/// The tenant aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tenant {
    pub id: TenantId,
    /// Owning organization; immutable after creation.
    organization_id: OrganizationId,
    pub name: String,
    pub slug: Slug,
    pub status: TenantStatus,
    pub environment: Environment,
    pub isolation: IsolationMode,
    #[serde(default)]
    pub settings: serde_json::Map<String, serde_json::Value>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Tenant {
    /// Creates a tenant owned by `organization_id`. The owning organization is
    /// fixed at creation — cross-organization moves are not permitted.
    pub fn create(
        organization_id: OrganizationId,
        name: impl Into<String>,
        slug: Slug,
        environment: Environment,
        isolation: IsolationMode,
    ) -> Result<Self> {
        if organization_id.is_nil() {
            return Err(AppError::invalid_field(
                "organization_id",
                "required",
                "tenant must belong to a concrete organization",
            ));
        }
        let name = name.into();
        validation::validate_resource_name("name", &name)?;

        let now = Timestamp::now();
        Ok(Self {
            id: TenantId::new(),
            organization_id,
            name,
            slug,
            status: TenantStatus::Provisioning,
            environment,
            isolation,
            settings: serde_json::Map::new(),
            created_at: now,
            updated_at: now,
        })
    }

    /// Read-only accessor; reassignment is intentionally impossible.
    #[must_use]
    pub const fn organization_id(&self) -> OrganizationId {
        self.organization_id
    }

    /// Defense-in-depth: asserts this tenant belongs to `organization_id`.
    /// Call at trust boundaries when both were supplied independently
    /// (prevents cross-organization tenant ownership confusion).
    pub fn assert_belongs_to(&self, organization_id: OrganizationId) -> Result<()> {
        if self.organization_id != organization_id {
            return Err(AppError::forbidden(
                "tenant does not belong to the claimed organization",
            ));
        }
        Ok(())
    }

    pub fn activate(&mut self) -> Result<()> {
        match self.status {
            TenantStatus::Provisioning | TenantStatus::Suspended => {
                self.status = TenantStatus::Active;
                self.touch();
                Ok(())
            },
            TenantStatus::Active => Ok(()),
            TenantStatus::Archived => {
                Err(AppError::conflict("archived tenants cannot be reactivated"))
            },
        }
    }

    pub fn suspend(&mut self) -> Result<()> {
        match self.status {
            TenantStatus::Active => {
                self.status = TenantStatus::Suspended;
                self.touch();
                Ok(())
            },
            TenantStatus::Suspended => Ok(()),
            other => Err(AppError::conflict(format!(
                "tenant in status '{other}' cannot be suspended"
            ))),
        }
    }

    pub fn archive(&mut self) -> Result<()> {
        match self.status {
            TenantStatus::Archived => Ok(()),
            TenantStatus::Provisioning => Err(AppError::conflict(
                "provisioning tenants cannot be archived; finish or abort provisioning first",
            )),
            _ => {
                self.status = TenantStatus::Archived;
                self.touch();
                Ok(())
            },
        }
    }

    /// Whether workloads may execute under this tenant.
    #[must_use]
    pub fn is_operational(&self) -> bool {
        self.status.is_operational()
    }

    fn touch(&mut self) {
        self.updated_at = Timestamp::now();
    }
}
