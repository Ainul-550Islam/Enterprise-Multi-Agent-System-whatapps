//! The request-scoped context every handler runs with: the verified principal,
//! the **resolved** scope, correlation id and request id. This is the ONLY
//! component allowed to translate transport metadata into a `mas-application`
//! `ServiceContext`.
//!
//! # Claims versus scope
//!
//! This type keeps both, and the distinction is the whole point:
//!
//! * [`RequestContext::claimed_tenant`] / [`RequestContext::claimed_organization`]
//!   are what the request *said*. They exist for diagnostics and audit — a
//!   mismatch worth recording is a mismatch someone attempted — and for the
//!   resolver to use as a selector.
//! * [`RequestContext::scope`] is what the request *gets*. It is `None` until
//!   the middleware has resolved it from identity, and
//!   [`RequestContext::require_scope`] hands out nothing else.
//!
//! Before this split, `tenant_id` was populated straight from `x-tenant-id`
//! and `require_scope()` only checked that the header was present, so any
//! authenticated caller could name any tenant. `require_scope()` now fails
//! unless a scope was *resolved*, which only the middleware can do.

use mas_application::context::ServiceContext;
use mas_common::error::AppError;
use mas_common::ids::{OrganizationId, ProjectId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_security::{AuthoritativeScope, SecurityPrincipal};

use crate::scope_resolution::ScopeClaims;

/// Correlation header name (HTTP + gRPC metadata share it).
pub const CORRELATION_HEADER: &str = "x-correlation-id";
/// Tenant scope header name. A **hint**; never authoritative.
pub const TENANT_HEADER: &str = "x-tenant-id";
/// Organization scope header name. A **hint**; never authoritative.
pub const ORGANIZATION_HEADER: &str = "x-organization-id";
/// Project scope header name. A **hint**; never authoritative.
pub const PROJECT_HEADER: &str = "x-project-id";
/// Execution idempotency header name.
pub const IDEMPOTENCY_HEADER: &str = "idempotency-key";

/// What a single request is entitled to do, proven by the middleware.
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// Verified caller. `None` on routes that are genuinely public; a
    /// protected route refuses to produce a scope without one.
    pub principal: Option<SecurityPrincipal>,
    /// The scope the server resolved from identity. `None` until
    /// [`Self::with_resolved`] is called by the middleware.
    pub scope: Option<AuthoritativeScope>,
    /// What the caller claimed, kept for audit and diagnostics only.
    pub claimed_tenant: Option<TenantId>,
    /// What the caller claimed, kept for audit and diagnostics only.
    pub claimed_organization: Option<OrganizationId>,
    /// Server-assignable-per-request uuid.
    pub request_id: uuid::Uuid,
    /// Correlation id (echoed from the caller or generated).
    pub correlation_id: String,
}

impl RequestContext {
    /// Builds an unauthenticated context carrying only claims.
    ///
    /// The scope is deliberately absent: nothing has proven anything yet.
    #[must_use]
    pub fn with_claims(claims: ScopeClaims, correlation_id: String) -> Self {
        Self {
            principal: None,
            scope: None,
            claimed_tenant: claims.tenant,
            claimed_organization: claims.organization,
            request_id: uuid::Uuid::now_v7(),
            correlation_id,
        }
    }

    /// Attaches the resolved scope and the principal as it was proven.
    ///
    /// Only the middleware calls this. It is the single point at which a
    /// request acquires a scope.
    #[must_use]
    pub fn with_resolved(
        mut self,
        principal: SecurityPrincipal,
        scope: AuthoritativeScope,
    ) -> Self {
        self.principal = Some(principal);
        self.scope = Some(scope);
        self
    }

    /// The scope, if one was resolved.
    #[must_use]
    pub const fn scope(&self) -> Option<&AuthoritativeScope> {
        self.scope.as_ref()
    }

    /// Builds the application-layer service context from the **resolved**
    /// scope.
    ///
    /// Tenant-owned routes call [`Self::require_scope`] first; the service's
    /// `require_organization()`/`require_tenant()` gates remain the final line
    /// of defense in `mas-application`.
    ///
    /// # Errors
    /// Fails when no scope was resolved, or when the principal is missing. It
    /// never falls back to the claimed values.
    pub fn service_context(&self) -> Result<ServiceContext> {
        let scope = self.require_scope()?;
        let principal = self
            .principal
            .as_ref()
            .ok_or_else(|| AppError::unauthorized("unauthenticated"))?;
        let actor_kind = match principal.kind {
            mas_security::PrincipalKind::User => mas_domain::AuditActorKind::User,
            mas_security::PrincipalKind::ApiKey => mas_domain::AuditActorKind::ApiKey,
            mas_security::PrincipalKind::ServiceAccount
            | mas_security::PrincipalKind::Worker
            | mas_security::PrincipalKind::Scheduler
            | mas_security::PrincipalKind::Token => mas_domain::AuditActorKind::Service,
            mas_security::PrincipalKind::System => mas_domain::AuditActorKind::System,
        };
        let actor = mas_domain::AuditActor::new(actor_kind, &principal.actor_id)?;
        let resolved_role = principal.role_in(scope.tenant_id(), scope.organization_id());
        ServiceContext::new(
            actor,
            Some(scope.tenant_id()),
            Some(scope.organization_id()),
            &self.correlation_id,
            &Timestamp::now(),
        )
        .map(|context| {
            context
                .with_request_id(self.request_id)
                .with_resolved_role(resolved_role)
        })
    }

    /// Errors `403 FORBIDDEN` when no scope was resolved.
    ///
    /// The old behaviour — accept the request if the tenant and organization
    /// *headers* were present — is gone. Presence of a claim is not proof of
    /// a grant, and a route that needs a scope must have had one resolved from
    /// identity by the middleware.
    pub fn require_scope(&self) -> Result<&AuthoritativeScope> {
        self.scope
            .as_ref()
            .ok_or_else(|| AppError::forbidden("no tenant scope was resolved for this request"))
    }

    /// The resolved project, when the request is project-scoped.
    #[must_use]
    pub fn project(&self) -> Option<ProjectId> {
        self.scope.as_ref().and_then(AuthoritativeScope::project_id)
    }

    /// Whether the caller claimed a different tenant than the one resolved.
    ///
    /// Used to record an attempted mismatch without changing the outcome.
    #[must_use]
    pub fn claimed_a_different_tenant(&self) -> bool {
        match (self.claimed_tenant, self.scope.as_ref()) {
            (Some(claimed), Some(scope)) => claimed != scope.tenant_id(),
            _ => false,
        }
    }

    /// Validates a correlation header value, mirroring the application
    /// layer's constraints (1–256 chars, no control bytes/whitespace-only).
    #[must_use]
    pub fn correlation_or_generated(raw: Option<&str>) -> String {
        let candidate = raw.map(str::trim).filter(|c| !c.is_empty());
        match candidate {
            Some(value) if value.len() <= 256 && !value.chars().any(char::is_control) => {
                value.to_owned()
            },
            _ => uuid::Uuid::now_v7().to_string(),
        }
    }

    /// Parses one scope header into a typed id; malformed values are 400s.
    pub fn parse_scope_header(name: &'static str, raw: Option<&str>) -> Result<Option<uuid::Uuid>> {
        let Some(value) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
            return Ok(None);
        };
        uuid::Uuid::parse_str(value).map(Some).map_err(|_| {
            AppError::invalid_field(name, "invalid", "must be a valid uuid (v7 recommended)")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_security::PrincipalKind;

    #[test]
    fn correlation_ids_are_validated_or_generated() {
        let kept = RequestContext::correlation_or_generated(Some("ticket-123"));
        assert_eq!(kept, "ticket-123");
        let generated = RequestContext::correlation_or_generated(None);
        assert!(
            uuid::Uuid::parse_str(&generated).is_ok(),
            "uuid v7 fallback"
        );
        let too_long = "x".repeat(257);
        assert_ne!(
            RequestContext::correlation_or_generated(Some(&too_long)),
            too_long
        );
        let dirty = RequestContext::correlation_or_generated(Some("bad\nvalue"));
        assert!(
            uuid::Uuid::parse_str(&dirty).is_ok(),
            "control bytes rejected"
        );
    }

    #[test]
    fn scope_headers_parse_or_reject() {
        let id = uuid::Uuid::now_v7().to_string();
        let parsed = RequestContext::parse_scope_header("x-tenant-id", Some(&id)).expect("ok");
        assert_eq!(parsed, Some(uuid::Uuid::parse_str(&id).expect("uuid")));
        assert!(RequestContext::parse_scope_header("x-tenant-id", Some("nope")).is_err());
        assert_eq!(
            RequestContext::parse_scope_header("x-tenant-id", None).expect("none"),
            None
        );
    }

    /// The core regression: a request that merely *claims* a tenant must not
    /// be able to produce a scope.
    #[test]
    fn a_claimed_tenant_is_not_a_scope() {
        let tenant = TenantId::new();
        let organization = OrganizationId::new();
        let context = RequestContext::with_claims(
            ScopeClaims {
                tenant: Some(tenant),
                organization: Some(organization),
                project: None,
            },
            "corr".to_owned(),
        );
        assert!(context.require_scope().is_err());
        assert_eq!(context.claimed_tenant, Some(tenant));
    }

    #[test]
    fn a_resolved_scope_is_what_require_scope_returns() {
        let tenant = TenantId::new();
        let organization = OrganizationId::new();
        let context = RequestContext::with_claims(ScopeClaims::default(), "corr".to_owned())
            .with_resolved(
                SecurityPrincipal::new(
                    "user-1",
                    PrincipalKind::User,
                    Some(tenant),
                    mas_security::ScopeSet::denied(),
                ),
                AuthoritativeScope::resolved(tenant, organization, "user-1"),
            );
        let scope = context.require_scope().expect("resolved");
        assert_eq!(scope.tenant_id(), tenant);
        assert_eq!(scope.organization_id(), organization);
    }

    #[test]
    fn service_context_preserves_the_transport_request_id() {
        let tenant = TenantId::new();
        let organization = OrganizationId::new();
        let request_id = uuid::Uuid::from_u128(0x1234);
        let context = RequestContext::with_claims(ScopeClaims::default(), "corr".to_owned())
            .with_resolved(
                SecurityPrincipal::new(
                    "user-1",
                    PrincipalKind::User,
                    Some(tenant),
                    mas_security::ScopeSet::denied(),
                ),
                AuthoritativeScope::resolved(tenant, organization, "user-1"),
            );
        let context = RequestContext {
            request_id,
            ..context
        };

        let service = context.service_context().expect("service context");
        assert_eq!(service.request_id, request_id);
    }

    /// A claim that disagrees with the resolved scope is recorded, not
    /// followed.
    #[test]
    fn a_divergent_claim_is_recorded_not_followed() {
        let real = TenantId::new();
        let claimed = TenantId::new();
        let context = RequestContext::with_claims(
            ScopeClaims {
                tenant: Some(claimed),
                ..ScopeClaims::default()
            },
            "corr".to_owned(),
        )
        .with_resolved(
            SecurityPrincipal::new(
                "user-1",
                PrincipalKind::User,
                Some(real),
                mas_security::ScopeSet::denied(),
            ),
            AuthoritativeScope::resolved(real, OrganizationId::new(), "user-1"),
        );
        assert!(context.claimed_a_different_tenant());
        assert_eq!(context.require_scope().expect("scope").tenant_id(), real);
    }
}
