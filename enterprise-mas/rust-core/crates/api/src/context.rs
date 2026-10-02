//! The request-scoped context every handler runs with: verified principal,
//! correlation id, and the tenant/organization scope extracted from the
//! transport headers. This is the ONLY component allowed to translate
//! transport metadata into a `mas-application` `ServiceContext`.

use mas_application::context::ServiceContext;
use mas_common::error::AppError;
use mas_common::ids::{OrganizationId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;

use crate::auth::Principal;

/// Correlation header name (HTTP + gRPC metadata share it).
pub const CORRELATION_HEADER: &str = "x-correlation-id";
/// Tenant scope header name.
pub const TENANT_HEADER: &str = "x-tenant-id";
/// Organization scope header name.
pub const ORGANIZATION_HEADER: &str = "x-organization-id";
/// Execution idempotency header name.
pub const IDEMPOTENCY_HEADER: &str = "idempotency-key";

/// What a single request is entitled to do, proven by the middleware.
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// Verified caller.
    pub principal: Principal,
    /// Tenant scope declared by the caller (None on org/system-level routes
    /// such as organization registration).
    pub tenant_id: Option<TenantId>,
    /// Organization scope declared by the caller.
    pub organization_id: Option<OrganizationId>,
    /// Server-assignable-per-request uuid.
    pub request_id: uuid::Uuid,
    /// Correlation id (echoed from the caller or generated).
    pub correlation_id: String,
}

impl RequestContext {
    /// Builds the application-layer service context from this request.
    ///
    /// Tenant-owned routes call [`Self::require_scope`] first; the
    /// service's `require_organization()`/`require_tenant()` gates remain
    /// the final line of defense in `mas-application`.
    pub fn service_context(&self) -> Result<ServiceContext> {
        let actor_kind = match self.principal.kind {
            crate::auth::PrincipalKind::User => mas_domain::AuditActorKind::User,
            crate::auth::PrincipalKind::Service => mas_domain::AuditActorKind::Service,
            crate::auth::PrincipalKind::ApiKey => mas_domain::AuditActorKind::ApiKey,
        };
        let actor = mas_domain::AuditActor::new(actor_kind, &self.principal.subject)?;
        let ctx = ServiceContext::new(
            actor,
            self.tenant_id,
            self.organization_id,
            &self.correlation_id,
            &Timestamp::now(),
        )?;
        Ok(ctx)
    }

    /// Errors `403 FORBIDDEN` when the request carried no tenant scope —
    /// tenant-owned routes must declare their scope up-front, and the
    /// application layer treats foreign ids as non-existent from here on.
    pub fn require_scope(&self) -> Result<&Self> {
        if self.tenant_id.is_none() || self.organization_id.is_none() {
            return Err(AppError::forbidden(
                "x-tenant-id and x-organization-id headers are required for this route",
            ));
        }
        Ok(self)
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
}
