//! User aggregate: identity metadata only.
//!
//! **Security invariant:** password hashes, MFA secrets and all other secret
//! material live exclusively in the security layer; this aggregate must never
//! hold them.

use mas_common::enums::UserStatus;
use mas_common::error::AppError;
use mas_common::ids::UserId;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

use crate::value_objects::Email;

/// The user aggregate (identity metadata only).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: UserId,
    /// Primary, case-normalized email.
    pub email: Email,
    pub display_name: String,
    pub status: UserStatus,
    /// Subject claim of the external identity provider, when federated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_subject: Option<String>,
    /// Identity provider key (e.g. `oidc:okta`), when federated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_login_at: Option<Timestamp>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl User {
    /// Creates a user in `PendingActivation`.
    pub fn register(email: Email, display_name: impl Into<String>) -> Result<Self> {
        let display_name = display_name.into();
        validation::validate_resource_name("display_name", &display_name)?;
        let now = Timestamp::now();
        Ok(Self {
            id: UserId::new(),
            email,
            display_name,
            status: UserStatus::PendingActivation,
            external_subject: None,
            identity_provider: None,
            last_login_at: None,
            created_at: now,
            updated_at: now,
        })
    }

    /// Links an external identity (IdP) subject to this user.
    pub fn link_external_identity(
        &mut self,
        provider: impl Into<String>,
        subject: impl Into<String>,
    ) -> Result<()> {
        let provider = provider.into();
        let subject = subject.into();
        validation::validate_non_empty("identity_provider", &provider)?;
        validation::validate_non_empty("external_subject", &subject)?;
        self.identity_provider = Some(provider);
        self.external_subject = Some(subject);
        self.touch();
        Ok(())
    }

    pub fn activate(&mut self) -> Result<()> {
        match self.status {
            UserStatus::PendingActivation | UserStatus::Disabled => {
                self.status = UserStatus::Active;
                self.touch();
                Ok(())
            },
            UserStatus::Active => Ok(()),
            UserStatus::Locked => Err(AppError::conflict(
                "locked users must be unlocked before activation",
            )),
        }
    }

    pub fn disable(&mut self) -> Result<()> {
        match self.status {
            UserStatus::Disabled => Ok(()),
            _ => {
                self.status = UserStatus::Disabled;
                self.touch();
                Ok(())
            },
        }
    }

    /// Locks the account (e.g. after repeated failed logins). Terminal until
    /// explicit `unlock`.
    pub fn lock(&mut self) -> Result<()> {
        match self.status {
            UserStatus::Locked => Ok(()),
            UserStatus::Active | UserStatus::PendingActivation => {
                self.status = UserStatus::Locked;
                self.touch();
                Ok(())
            },
            UserStatus::Disabled => Err(AppError::conflict(
                "disabled users cannot be locked; activate them first",
            )),
        }
    }

    pub fn unlock(&mut self) -> Result<()> {
        match self.status {
            UserStatus::Locked => {
                self.status = UserStatus::Active;
                self.touch();
                Ok(())
            },
            _ => Err(AppError::conflict("only locked users can be unlocked")),
        }
    }

    /// Records a successful login. Locked/disabled accounts must not log in.
    pub fn record_login(&mut self, at: Timestamp) -> Result<()> {
        match self.status {
            UserStatus::Active => {
                self.last_login_at = Some(at);
                self.touch();
                Ok(())
            },
            other => Err(AppError::forbidden(format!(
                "user in status '{other}' must not authenticate"
            ))),
        }
    }

    #[must_use]
    pub fn is_active(&self) -> bool {
        self.status == UserStatus::Active
    }

    fn touch(&mut self) {
        self.updated_at = Timestamp::now();
    }
}
