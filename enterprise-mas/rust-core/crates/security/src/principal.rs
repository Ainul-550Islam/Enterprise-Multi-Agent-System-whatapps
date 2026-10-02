//! The authenticated principal: *who* a request acts as, tenant-bind,
//! roles, scopes, expiry.

use crate::scopes::ScopeSet;
use mas_common::ids::{OrganizationId, TenantId};
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use serde::{Deserialize, Serialize};

string_enum! {
    /// Credential class backing a principal.
    PrincipalKind {
        User => "user",
        ServiceAccount => "service_account",
        ApiKey => "api_key",
        /// Short-lived actor token (JWT) principal.
        Token => "token",
        /// Internal system identity (`"system"`).
        System => "system",
    }
}

/// An authenticated caller with its capability surface.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityPrincipal {
    /// Who authenticated (user id, token `sub`, `system`).
    pub actor_id: String,
    pub kind: PrincipalKind,
    /// Tenant the credential is bound to (`None` only for platform-global
    /// system principals outside any tenant context).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<TenantId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organization_id: Option<OrganizationId>,
    #[serde(default)]
    pub roles: Vec<String>,
    pub scopes: ScopeSet,
    /// Credential expiry; `checked()` fails past this point.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<Timestamp>,
}

impl SecurityPrincipal {
    pub fn new(
        actor_id: impl Into<String>,
        kind: PrincipalKind,
        tenant_id: Option<TenantId>,
        scopes: ScopeSet,
    ) -> Self {
        Self {
            actor_id: actor_id.into(),
            kind,
            tenant_id,
            organization_id: None,
            roles: Vec::new(),
            scopes,
            expires_at: None,
        }
    }

    /// The internal platform identity (used by reapers/sweepers).
    #[must_use]
    pub fn system() -> Self {
        Self::new("system", PrincipalKind::System, None, ScopeSet::all())
    }

    #[must_use]
    pub const fn with_organization(mut self, organization_id: OrganizationId) -> Self {
        self.organization_id = Some(organization_id);
        self
    }

    #[must_use]
    pub fn with_roles<I, S>(mut self, roles: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.roles = roles.into_iter().map(Into::into).collect();
        self
    }

    #[must_use]
    pub const fn with_expiry(mut self, expires_at: Timestamp) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Whether the credential itself is temporally usable *now*.
    #[must_use]
    pub fn is_usable(&self) -> bool {
        self.expires_at.as_ref().is_none_or(Timestamp::is_future)
    }

    #[must_use]
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|held| held == role)
    }

    #[must_use]
    pub fn permits(&self, scope: &str) -> bool {
        self.scopes.permits(scope)
    }

    /// The tenant invariant: cross-tenant principals are impossible — a
    /// principal bound to `tenant A` cannot be checked against
    /// `tenant B`'s context by construction.
    #[must_use]
    pub fn tenant_matches(&self, tenant_id: TenantId) -> bool {
        self.tenant_id == Some(tenant_id)
    }

    /// The single gate used everywhere: usable + tenant match + scope.
    #[must_use]
    pub fn checked(&self, tenant_id: TenantId, scope: &str) -> bool {
        self.is_usable() && self.tenant_matches(tenant_id) && self.permits(scope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn gate_composes_expiry_tenant_and_scope() {
        let tenant = TenantId::new();
        let principal = SecurityPrincipal::new(
            "user-1",
            PrincipalKind::User,
            Some(tenant),
            ScopeSet::new(["executions:read"]).expect("scopes"),
        );
        assert!(principal.checked(tenant, "executions:read"));
        assert!(!principal.checked(tenant, "executions:cancel"));
        assert!(!principal.checked(TenantId::new(), "executions:read"));
        assert!(principal.is_usable());

        let expired = principal.clone().with_expiry(
            Timestamp::now()
                .checked_sub(Duration::from_secs(1))
                .expect("past"),
        );
        assert!(!expired.is_usable());
        assert!(!expired.checked(tenant, "executions:read"));
    }

    #[test]
    fn system_principal_is_break_glass() {
        let system = SecurityPrincipal::system();
        assert!(system.is_usable());
        assert!(system.permits("anything:here"));
        assert!(
            !system.tenant_matches(TenantId::new()),
            "system is tenant-agnostic"
        );
    }
}
