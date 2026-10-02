//! Two `from_fn` layers:
//!
//! * [`request_context_layer`] — public: correlation + scope parsing +
//!   request id. Runs on every route.
//! * [`authenticated_layer`] — additionally requires a valid bearer token
//!   and attachestheverifiedprincipal to the `RequestContext`.

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::auth::{bearer_token, Principal, PrincipalKind};
use crate::context::{RequestContext, CORRELATION_HEADER, ORGANIZATION_HEADER, TENANT_HEADER};
use crate::state::AppState;

/// Inserts an anonymous `RequestContext` extension into every request:
/// correlation id (echoed or generated), request id, parsed scope headers.
/// Scope parse failures eagerly answer `400`.
pub async fn request_context_layer(
    State(_state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    let headers = request.headers();
    let correlation_id = RequestContext::correlation_or_generated(
        headers
            .get(CORRELATION_HEADER)
            .and_then(|v| v.to_str().ok()),
    );
    let tenant = RequestContext::parse_scope_header(
        TENANT_HEADER,
        headers.get(TENANT_HEADER).and_then(|v| v.to_str().ok()),
    );
    let organization = RequestContext::parse_scope_header(
        ORGANIZATION_HEADER,
        headers
            .get(ORGANIZATION_HEADER)
            .and_then(|v| v.to_str().ok()),
    );

    let ctx = match (tenant, organization) {
        (Ok(tenant), Ok(organization)) => RequestContext {
            principal: Principal {
                subject: "anonymous".to_owned(),
                kind: PrincipalKind::User,
            },
            tenant_id: tenant.map(mas_common::ids::TenantId::from_uuid),
            organization_id: organization.map(mas_common::ids::OrganizationId::from_uuid),
            request_id: uuid::Uuid::now_v7(),
            correlation_id,
        },
        (Err(err), _) | (_, Err(err)) => {
            let failure_ctx = anonymous_failure_ctx(&correlation_id);
            return crate::response::ApiError::new(err, &failure_ctx).into_response();
        },
    };
    request.extensions_mut().insert(ctx);
    next.run(request).await
}

/// Requires a valid bearer token and replaces the anonymous principal.
/// Must run AFTER [`request_context_layer`] (layer order: outside-in, so
/// apply as `.layer` after `.layer(request_context)` — axum applies layers
/// in reverse declaration order).
pub async fn authenticated_layer(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    let existing = request
        .extensions()
        .get::<RequestContext>()
        .cloned()
        .unwrap_or_else(|| anonymous_failure_ctx("generated"));

    let token = match bearer_token(
        request
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok()),
    ) {
        Ok(token) => token,
        Err(err) => return crate::response::ApiError::new(err, &existing).into_response(),
    };
    let Some(token) = token else {
        let err = mas_common::error::AppError::unauthorized("missing bearer token");
        return crate::response::ApiError::new(err, &existing).into_response();
    };
    match state.verifier.verify(&token).await {
        Ok(principal) => {
            let ctx = RequestContext {
                principal: principal.clone(),
                ..existing
            };
            request.extensions_mut().insert(ctx);
            next.run(request).await
        },
        Err(err) => crate::response::ApiError::new(err, &existing).into_response(),
    }
}

fn anonymous_failure_ctx(correlation_id: &str) -> RequestContext {
    RequestContext {
        principal: Principal {
            subject: "anonymous".to_owned(),
            kind: PrincipalKind::User,
        },
        tenant_id: None,
        organization_id: None,
        request_id: uuid::Uuid::now_v7(),
        correlation_id: RequestContext::correlation_or_generated(Some(correlation_id)),
    }
}
