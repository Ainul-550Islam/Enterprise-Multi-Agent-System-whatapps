//! Session aggregate: authentication session metadata.

use mas_common::ids::{SessionId, TenantId, UserId};
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use serde::{Deserialize, Serialize};
use std::time::Duration;

string_enum! {
    /// How the session was established.
    AuthMethod {
        Password => "password",
        ApiKey => "api_key",
        Oidc => "oidc",
        Service => "service",
    }
}

/// Absolute longest a session may be extended to (24h).
pub const MAX_SESSION_LIFETIME: Duration = Duration::from_secs(86_400);

/// Session metadata aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: SessionId,
    pub user_id: UserId,
    /// Active tenant of the session, when tenant-bound.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<TenantId>,
    pub auth_method: AuthMethod,
    pub issued_at: Timestamp,
    pub expires_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<Timestamp>,
    /// Network metadata for anomaly detection (never logged raw).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip_address: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
}

impl Session {
    pub fn issue(
        user_id: UserId,
        tenant_id: Option<TenantId>,
        auth_method: AuthMethod,
        lifetime: Duration,
    ) -> Result<Self> {
        if lifetime.is_zero() || lifetime > MAX_SESSION_LIFETIME {
            return Err(mas_common::error::AppError::invalid_field(
                "lifetime",
                "out_of_range",
                "session lifetime must be between 1s and 24h",
            ));
        }
        let issued_at = Timestamp::now();
        let expires_at = issued_at.checked_add(lifetime).ok_or_else(|| {
            mas_common::error::AppError::invalid_field(
                "lifetime",
                "out_of_range",
                "session expiry overflows representable time",
            )
        })?;
        Ok(Self {
            id: SessionId::new(),
            user_id,
            tenant_id,
            auth_method,
            issued_at,
            expires_at,
            revoked_at: None,
            ip_address: None,
            user_agent: None,
        })
    }

    /// Attaches network context (best-effort metadata).
    #[must_use]
    pub fn with_network_context(
        mut self,
        ip_address: Option<String>,
        user_agent: Option<String>,
    ) -> Self {
        self.ip_address = ip_address;
        self.user_agent = user_agent;
        self
    }

    /// Whether the session is usable at `now`.
    #[must_use]
    pub fn is_valid(&self, now: &Timestamp) -> bool {
        self.revoked_at.is_none() && self.expires_at.is_after(now)
    }

    /// Immediately revokes the session (logout / security event).
    pub fn revoke(&mut self) {
        if self.revoked_at.is_none() {
            self.revoked_at = Some(Timestamp::now());
        }
    }

    /// Extends expiry to `new_expiry`, capped at `issued_at + MAX_SESSION_LIFETIME`.
    pub fn extend(&mut self, new_expiry: Timestamp) -> Result<()> {
        if self.revoked_at.is_some() {
            return Err(mas_common::error::AppError::conflict(
                "revoked sessions cannot be extended",
            ));
        }
        let hard_cap = self
            .issued_at
            .checked_add(MAX_SESSION_LIFETIME)
            .ok_or_else(|| mas_common::error::AppError::internal("session cap overflow"))?;
        if new_expiry.is_after(&hard_cap) {
            return Err(mas_common::error::AppError::invalid_field(
                "expires_at",
                "out_of_range",
                "session may not extend beyond 24h from issuance",
            ));
        }
        if !new_expiry.is_future() {
            return Err(mas_common::error::AppError::invalid_field(
                "expires_at",
                "out_of_range",
                "new expiry must be in the future",
            ));
        }
        self.expires_at = new_expiry;
        Ok(())
    }

    /// Remaining validity at `now`.
    #[must_use]
    pub fn remaining(&self, now: &Timestamp) -> Option<Duration> {
        self.expires_at.duration_since(now)
    }
}
