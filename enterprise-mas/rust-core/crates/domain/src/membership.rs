//! Membership aggregate: joins a user to an organization (and optionally one
//! of its tenants) with a set of roles.

use mas_common::error::AppError;
use mas_common::ids::{MembershipId, OrganizationId, TenantId, UserId};
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

string_enum! {
    /// Authorization role granted by a membership.
    MembershipRole {
        Owner => "owner",
        Admin => "admin",
        Developer => "developer",
        Operator => "operator",
        Viewer => "viewer",
        ServiceAccount => "service_account",
    }
}

impl MembershipRole {
    /// Capability rank used by `can_manage`. Higher manages lower.
    #[must_use]
    pub const fn rank(&self) -> u8 {
        match self {
            Self::Owner => 100,
            Self::Admin => 80,
            Self::Developer => 50,
            Self::Operator => 40,
            Self::Viewer => 10,
            Self::ServiceAccount => 0,
        }
    }
}

string_enum! {
    /// Status of a membership.
    MembershipStatus {
        Invited => "invited",
        Active => "active",
        Suspended => "suspended",
    }
}

/// The membership aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Membership {
    pub id: MembershipId,
    pub user_id: UserId,
    pub organization_id: OrganizationId,
    /// When `Some`, scopes the membership to one tenant of the organization;
    /// `None` means organization-wide.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<TenantId>,
    pub roles: BTreeSet<MembershipRole>,
    pub status: MembershipStatus,
    pub invited_at: Timestamp,
    pub joined_at: Option<Timestamp>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Membership {
    /// Creates an *invited* membership with an initial role.
    pub fn invite(
        user_id: UserId,
        organization_id: OrganizationId,
        tenant_id: Option<TenantId>,
        initial_role: MembershipRole,
    ) -> Result<Self> {
        if user_id.is_nil() || organization_id.is_nil() {
            return Err(AppError::invalid_field(
                "membership",
                "required",
                "user and organization must be concrete",
            ));
        }
        let now = Timestamp::now();
        Ok(Self {
            id: MembershipId::new(),
            user_id,
            organization_id,
            tenant_id,
            roles: BTreeSet::from([initial_role]),
            status: MembershipStatus::Invited,
            invited_at: now,
            joined_at: None,
            created_at: now,
            updated_at: now,
        })
    }

    /// Accepts an invitation (Invited → Active).
    pub fn accept_invitation(&mut self) -> Result<()> {
        match self.status {
            MembershipStatus::Invited => {
                self.status = MembershipStatus::Active;
                self.joined_at = Some(Timestamp::now());
                self.touch();
                Ok(())
            },
            MembershipStatus::Active => Ok(()),
            MembershipStatus::Suspended => Err(AppError::conflict(
                "suspended memberships must be restored, not re-accepted",
            )),
        }
    }

    /// Adds a role; requires an active membership.
    pub fn add_role(&mut self, role: MembershipRole) -> Result<()> {
        self.assert_active("assign roles")?;
        self.roles.insert(role);
        self.touch();
        Ok(())
    }

    /// Removes a role. A membership must keep at least one role; the last
    /// `Owner` role of an organization is protected by the application layer
    /// (it has the full membership list), not here.
    pub fn remove_role(&mut self, role: MembershipRole) -> Result<()> {
        self.assert_active("remove roles")?;
        if !self.roles.contains(&role) {
            return Ok(()); // idempotent
        }
        if self.roles.len() == 1 {
            return Err(AppError::conflict(
                "cannot remove the last role of a membership; suspend it instead",
            ));
        }
        self.roles.remove(&role);
        self.touch();
        Ok(())
    }

    pub fn suspend(&mut self) -> Result<()> {
        match self.status {
            MembershipStatus::Active => {
                self.status = MembershipStatus::Suspended;
                self.touch();
                Ok(())
            },
            MembershipStatus::Suspended => Ok(()),
            MembershipStatus::Invited => Err(AppError::conflict(
                "invited memberships cannot be suspended; revoke the invitation instead",
            )),
        }
    }

    pub fn restore(&mut self) -> Result<()> {
        match self.status {
            MembershipStatus::Suspended => {
                self.status = MembershipStatus::Active;
                self.touch();
                Ok(())
            },
            MembershipStatus::Active => Ok(()),
            MembershipStatus::Invited => {
                Err(AppError::conflict("invited memberships cannot be restored"))
            },
        }
    }

    // -- authorization check helpers ----------------------------------------

    #[must_use]
    pub fn is_active(&self) -> bool {
        self.status == MembershipStatus::Active
    }

    #[must_use]
    pub fn has_role(&self, role: MembershipRole) -> bool {
        self.roles.contains(&role)
    }

    /// `true` when any held role outranks `minimum`.
    #[must_use]
    pub fn has_at_least(&self, minimum: MembershipRole) -> bool {
        self.roles.iter().any(|role| role.rank() >= minimum.rank())
    }

    /// Whether this membership may manage a member holding `other` (purely by
    /// rank; tenancy checks happen in the security layer).
    #[must_use]
    pub fn can_manage(&self, other: MembershipRole) -> bool {
        self.highest_rank() > other.rank()
    }

    #[must_use]
    pub fn highest_rank(&self) -> u8 {
        self.roles
            .iter()
            .map(MembershipRole::rank)
            .max()
            .unwrap_or(0)
    }

    fn assert_active(&self, action: &str) -> Result<()> {
        if self.is_active() {
            Ok(())
        } else {
            Err(AppError::conflict(format!(
                "membership in status '{}' cannot {action}",
                self.status
            )))
        }
    }

    fn touch(&mut self) {
        self.updated_at = Timestamp::now();
    }
}
