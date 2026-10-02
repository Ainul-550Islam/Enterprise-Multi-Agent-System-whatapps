//! HTTP handlers. Every handler: extract `RequestContext` (inserted by the
//! middleware) → build `ServiceContext` → call ONE application service
//! method → wrap in the stable envelope. No business logic here.

pub mod agents;
pub mod executions;
pub mod health;
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
                let fallback = RequestContext {
                    principal: crate::auth::Principal {
                        subject: "anonymous".to_owned(),
                        kind: crate::auth::PrincipalKind::User,
                    },
                    tenant_id: None,
                    organization_id: None,
                    request_id: uuid::Uuid::now_v7(),
                    correlation_id: RequestContext::correlation_or_generated(None),
                };
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
            principal: crate::auth::Principal {
                subject: "jane".to_owned(),
                kind: crate::auth::PrincipalKind::User,
            },
            tenant_id: None,
            organization_id: None,
            request_id: uuid::Uuid::now_v7(),
            correlation_id: "c-1".to_owned(),
        };
        let mut parts = axum::http::request::Request::new(()).into_parts().0;
        parts.extensions.insert(ctx.clone());
        let extracted: Ctx = <Ctx as FromRequestParts<()>>::from_request_parts(&mut parts, &())
            .await
            .expect("present");
        assert_eq!(extracted.0.principal.subject, "jane");
        assert_eq!(extracted.0.correlation_id, "c-1");
        let _ = ctx;
    }
}
