//! Policy aggregate: versioned, scoped rule sets evaluated by the policy
//! engine. This file holds identity/lifecycle/validation; evaluation lives
//! in `mas-policy`.

use mas_common::ids::{OrganizationId, PolicyId, ProjectId, TenantId};
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

string_enum! {
    /// Functional category of a policy.
    PolicyType {
        Authorization => "authorization",
        Execution => "execution",
        DataGovernance => "data_governance",
        Quota => "quota",
        Compliance => "compliance",
    }
}

string_enum! {
    /// Scope at which the policy binds.
    PolicyScope {
        Global => "global",
        Organization => "organization",
        Tenant => "tenant",
        Project => "project",
        Agent => "agent",
    }
}

/// The policy aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    pub id: PolicyId,
    /// Owner of the policy; `None` for platform-global policies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<TenantId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organization_id: Option<OrganizationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<ProjectId>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub policy_type: PolicyType,
    pub scope: PolicyScope,
    /// Evaluation priority: higher wins on conflict. Range -1000..=1000.
    pub priority: i32,
    pub active: bool,
    /// Rule definition in the policy DSL (compiled by `mas-policy`).
    pub definition: serde_json::Value,
    /// Fingerprint of the compiled form (hex), set by the policy engine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compiled_checksum: Option<String>,
    /// Monotonic version, bumped on every definition change.
    pub version: u64,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Policy {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: Option<TenantId>,
        organization_id: Option<OrganizationId>,
        project_id: Option<ProjectId>,
        name: impl Into<String>,
        policy_type: PolicyType,
        scope: PolicyScope,
        priority: i32,
        definition: serde_json::Value,
    ) -> Result<Self> {
        let policy = Self {
            id: PolicyId::new(),
            tenant_id,
            organization_id,
            project_id,
            name: name.into(),
            description: None,
            policy_type,
            scope,
            priority,
            active: false, // policies start inactive until published
            definition,
            compiled_checksum: None,
            version: 1,
            created_at: Timestamp::now(),
            updated_at: Timestamp::now(),
        };
        policy.validate_definition()?;
        Ok(policy)
    }

    /// Deterministic structural validation of scope/ownership coherence and
    /// the definition envelope. DSL compilation happens in `mas-policy`.
    pub fn validate_definition(&self) -> Result<()> {
        validation::validate_resource_name("name", &self.name)?;
        if !(-1000..=1000).contains(&self.priority) {
            return Err(mas_common::error::AppError::invalid_field(
                "priority",
                "out_of_range",
                "policy priority must be -1000..=1000",
            ));
        }
        // Scope/ownership coherence.
        match self.scope {
            PolicyScope::Global => {
                if self.tenant_id.is_some() || self.project_id.is_some() {
                    return Err(mas_common::error::AppError::invalid_field(
                        "scope",
                        "incoherent_scope",
                        "global policies must not carry tenant/project ownership",
                    ));
                }
            },
            PolicyScope::Organization => {
                if self.organization_id.is_none() {
                    return Err(mas_common::error::AppError::invalid_field(
                        "organization_id",
                        "required",
                        "organization-scoped policies need an organization",
                    ));
                }
            },
            PolicyScope::Tenant => {
                if self.tenant_id.is_none() {
                    return Err(mas_common::error::AppError::invalid_field(
                        "tenant_id",
                        "required",
                        "tenant-scoped policies need a tenant",
                    ));
                }
            },
            PolicyScope::Project | PolicyScope::Agent => {
                if self.project_id.is_none() || self.tenant_id.is_none() {
                    return Err(mas_common::error::AppError::invalid_field(
                        "scope",
                        "incoherent_scope",
                        "project/agent-scoped policies need tenant and project ownership",
                    ));
                }
            },
        }
        // Definition envelope: object with a non-empty `rules` array.
        let object = self.definition.as_object().ok_or_else(|| {
            mas_common::error::AppError::invalid_field(
                "definition",
                "invalid_format",
                "policy definition must be a JSON object",
            )
        })?;
        match object.get("rules") {
            Some(serde_json::Value::Array(rules)) if !rules.is_empty() => Ok(()),
            _ => Err(mas_common::error::AppError::invalid_field(
                "definition.rules",
                "required",
                "policy definition must contain a non-empty 'rules' array",
            )),
        }
    }

    /// Replaces the definition (bumps version, clears compiled state,
    /// requires re-publication).
    pub fn update_definition(&mut self, definition: serde_json::Value) -> Result<()> {
        self.definition = definition;
        self.validate_definition()?;
        self.compiled_checksum = None;
        self.active = false;
        self.version += 1;
        self.updated_at = Timestamp::now();
        Ok(())
    }

    pub fn set_priority(&mut self, priority: i32) -> Result<()> {
        if !(-1000..=1000).contains(&priority) {
            return Err(mas_common::error::AppError::invalid_field(
                "priority",
                "out_of_range",
                "policy priority must be -1000..=1000",
            ));
        }
        self.priority = priority;
        self.updated_at = Timestamp::now();
        Ok(())
    }

    /// Marks the policy active with the compiled fingerprint from the engine.
    pub fn publish(&mut self, compiled_checksum: impl Into<String>) -> Result<()> {
        let compiled_checksum = compiled_checksum.into();
        validation::validate_non_empty("compiled_checksum", &compiled_checksum)?;
        self.validate_definition()?;
        self.compiled_checksum = Some(compiled_checksum);
        self.active = true;
        self.updated_at = Timestamp::now();
        Ok(())
    }

    /// Deactivates without deleting (decisions fall back to other policies).
    pub fn deactivate(&mut self) {
        self.active = false;
        self.updated_at = Timestamp::now();
    }

    #[must_use]
    pub const fn is_enforced(&self) -> bool {
        self.active
    }
}
