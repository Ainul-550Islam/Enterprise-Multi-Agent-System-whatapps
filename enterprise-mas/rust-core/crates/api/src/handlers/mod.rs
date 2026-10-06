//! PURPOSE: The HTTP handler module root — the `Ctx` extractor and the body
//! size cap shared by every handler.
//!
//! Owning layer: `mas-api` (the process edge).
//!
//! Production responsibility: hand each handler the request context the
//! middleware attached, and cap request bodies so one request cannot exhaust
//! the process. Handlers themselves do no business logic: extract context,
//! build a `ServiceContext`, call one application service, wrap the result.
//!
//! Security boundary: `Ctx` is the only way a handler reaches the resolved
//! scope, and it fails closed — a missing context is a 500 from a
//! misconfigured router, never an unscoped request that quietly proceeds.
//!
//! It must NOT:
//!   • expose a way to build a `RequestContext` from request input;
//!   • let a handler run without a resolved scope;
//!   • carry business rules — those belong in `mas-application`.

//! HTTP handlers. Every handler: extract `RequestContext` (inserted by the
//! middleware) → build `ServiceContext` → call ONE application service
//! method → wrap in the stable envelope. No business logic here.

pub mod agents;
pub mod control_plane;
pub mod executions;
pub mod health;
pub mod internal;
pub mod schedules;
pub mod tenancy;
pub mod workflows;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;

use crate::context::RequestContext;
use crate::response::ApiError;

/// Default body size cap for all JSON routes (256 KiB).
pub const MAX_BODY_BYTES: usize = 256 * 1024;

/// Extractor: the middleware-attached request context (absence = 500, the
/// layer always runs first; this trips only on misconfigured routers).
#[derive(Debug)]
pub struct Ctx(pub RequestContext);

impl<S> FromRequestParts<S> for Ctx
where
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> std::result::Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<RequestContext>()
            .cloned()
            .map(Self)
            .ok_or_else(|| {
                let fallback = RequestContext::with_claims(
                    crate::scope_resolution::ScopeClaims::default(),
                    RequestContext::correlation_or_generated(None),
                );
                ApiError::new(
                    mas_common::error::AppError::internal(
                        "request context layer missing — router misconfiguration",
                    ),
                    &fallback,
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn extractor_surfaces_present_context() {
        let ctx = RequestContext {
            principal: Some(mas_security::SecurityPrincipal::new(
                "jane",
                mas_security::PrincipalKind::User,
                None,
                mas_security::ScopeSet::denied(),
            )),
            scope: None,
            claimed_tenant: None,
            claimed_organization: None,
            request_id: uuid::Uuid::now_v7(),
            correlation_id: "c-1".to_owned(),
        };
        let mut parts = axum::http::request::Request::new(()).into_parts().0;
        parts.extensions.insert(ctx.clone());
        let extracted: Ctx = <Ctx as FromRequestParts<()>>::from_request_parts(&mut parts, &())
            .await
            .expect("present");
        assert_eq!(
            extracted.0.principal.as_ref().map(|p| p.actor_id.as_str()),
            Some("jane")
        );
        assert_eq!(extracted.0.correlation_id, "c-1");
        let _ = ctx;
    }
}
