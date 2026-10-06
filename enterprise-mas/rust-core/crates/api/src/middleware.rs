//! PURPOSE: Per-request authentication, scope resolution, and the
//! transaction every repository call in that request runs inside.
//!
//! Owning layer: `mas-api` (the process edge), between the router and the
//! handlers.
//!
//! Production responsibility: verify the credential, resolve the scope from
//! the directory, then wrap the rest of the request in
//! `scoped_request::with_authoritative_scope` so row-level security is armed
//! for the whole request.
//!
//! Security boundary: this is where a caller-supplied header stops being a
//! hint and becomes either a proven scope or a refusal. It never falls back to
//! the claimed tenant, and in production it refuses to resolve scope without a
//! directory.
//!
//! It must NOT:
//!   • trust `x-tenant-id` / `x-organization-id` as authoritative;
//!   • fall back to an unscoped transaction when resolution fails;
//!   • let a request proceed without a scope unless the route is explicitly
//!     pre-tenant (organization registration).

//! Two `from_fn` layers:
//!
//! * [`request_context_layer`] — public: correlation + scope **claim**
//!   parsing + request id. Runs on every route. It resolves nothing: a claim
//!   is recorded, not believed.
//! * [`authenticated_layer`] — requires a valid bearer token, then **resolves**
//!   the request's scope from the principal's identity. This is where a
//!   request acquires a scope, and the only place it can.
//!
//! # Why the split matters
//!
//! The previous shape built a `RequestContext` from `x-tenant-id` /
//! `x-organization-id` with an `anonymous` principal, then merged in only the
//! verified principal (`..existing`) — so the scope was whatever the caller
//! typed, and authentication never revisited it. Identity and scope were
//! independent, which is the defect this module exists to remove.

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::auth::bearer_token;
use crate::context::{
    RequestContext, CORRELATION_HEADER, ORGANIZATION_HEADER, PROJECT_HEADER, TENANT_HEADER,
};
use crate::scope_resolution::{RequestScopeResolver, ScopeClaims};
use crate::state::AppState;

/// Inserts an unauthenticated `RequestContext` extension into every request:
/// correlation id (echoed or generated), request id, and the parsed scope
/// claims. Malformed claim headers eagerly answer `400`.
///
/// Deliberately does not set a scope.
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
    let project = RequestContext::parse_scope_header(
        PROJECT_HEADER,
        headers.get(PROJECT_HEADER).and_then(|v| v.to_str().ok()),
    );

    let (tenant, organization, project) = match (tenant, organization, project) {
        (Ok(tenant), Ok(organization), Ok(project)) => (tenant, organization, project),
        (Err(err), _, _) | (_, Err(err), _) | (_, _, Err(err)) => {
            let failure_ctx = anonymous_failure_ctx(&correlation_id);
            return crate::response::ApiError::new(err, &failure_ctx).into_response();
        },
    };

    // These are claims. Nothing here is a scope, and nothing here will become
    // one without a resolution step.
    let claims = ScopeClaims {
        tenant: tenant.map(mas_common::ids::TenantId::from_uuid),
        organization: organization.map(mas_common::ids::OrganizationId::from_uuid),
        project: project.map(mas_common::ids::ProjectId::from_uuid),
    };
    let ctx = RequestContext::with_claims(claims, correlation_id);
    request.extensions_mut().insert(ctx);
    next.run(request).await
}

/// Requires a valid bearer token, then resolves the scope from identity.
///
/// The claim headers are passed to the resolver as *hints* — they select among
/// the tenants the principal can already reach. Anything else is refused, and
/// a tenant the principal cannot reach is reported as absent rather than
/// forbidden.
///
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

    let principal = match state.verifier.verify(&token).await {
        Ok(principal) => principal,
        Err(err) => return crate::response::ApiError::new(err, &existing).into_response(),
    };

    // Recover the claims recorded by the outer layer and let identity decide.
    let claims = ScopeClaims {
        tenant: existing.claimed_tenant,
        organization: existing.claimed_organization,
        project: None,
    };

    let resolved = match resolve_guarded(&state.scope_resolver, &principal, claims).await {
        Ok(resolved) => resolved,
        Err(err) => return crate::response::ApiError::new(err, &existing).into_response(),
    };

    let ctx = existing.with_resolved(resolved.principal, resolved.scope.clone());
    request.extensions_mut().insert(ctx);

    // Everything downstream runs inside one transaction carrying this scope.
    // On PostgreSQL that is what arms row-level security: the GUCs are SET
    // LOCAL, so they are pinned to this transaction and die with it. A
    // connection returning to the pool afterwards carries nothing forward.
    //
    // On the in-memory lane there is no pool, so the handler just runs.
    let Some(pool) = &state.runtime_pool else {
        return next.run(request).await;
    };

    let scope = resolved.scope;
    let context_for_errors = request
        .extensions()
        .get::<RequestContext>()
        .cloned()
        .unwrap_or_else(|| anonymous_failure_ctx("generated"));

    // The handler's response is lifted into `Result` so a transaction failure
    // (commit error, lost connection) takes the same path as any other error:
    // through the stable envelope, with the correlation id attached.
    let outcome: mas_common::result::Result<Response> =
        mas_persistence::scoped_request::with_authoritative_scope(pool, scope, || async {
            Ok(next.run(request).await)
        })
        .await;

    match outcome {
        Ok(response) => response,
        Err(err) => crate::response::ApiError::new(err, &context_for_errors).into_response(),
    }
}

/// Resolves the scope, converting a missing resolver into a refusal.
///
/// The resolver is optional in [`AppState`] only because the in-memory
/// development lane has no directory to consult. A production process always
/// has one, and a process that somehow has neither must refuse rather than
/// serve: an unresolved scope is not "no scope", it is "no service".
async fn resolve_guarded(
    resolver: &Option<RequestScopeResolver>,
    principal: &mas_security::SecurityPrincipal,
    claims: ScopeClaims,
) -> mas_common::result::Result<crate::scope_resolution::ResolvedRequest> {
    match resolver {
        Some(resolver) => resolver.resolve(principal, claims).await,
        None => Err(mas_common::error::AppError::internal(
            "no tenancy directory is configured; refusing to resolve request scope",
        )),
    }
}

/// A context used only to shape an error response.
///
/// It carries no scope and no principal, which is the correct state for a
/// request that failed before either existed.
fn anonymous_failure_ctx(correlation_id: &str) -> RequestContext {
    RequestContext::with_claims(
        ScopeClaims::default(),
        RequestContext::correlation_or_generated(Some(correlation_id)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The failure context must never smuggle in a scope: an error response
    /// built from it is the last thing an attacker sees and must not confirm
    /// anything.
    #[test]
    fn the_failure_context_carries_no_scope_and_no_principal() {
        let ctx = anonymous_failure_ctx("corr-1");
        assert!(ctx.principal.is_none());
        assert!(ctx.scope.is_none());
        assert!(ctx.require_scope().is_err());
    }

    /// A missing directory is a refusal, not an unscoped pass-through.
    #[tokio::test]
    async fn an_unconfigured_resolver_refuses() {
        let principal = mas_security::SecurityPrincipal::system();
        let error = resolve_guarded(&None, &principal, ScopeClaims::default())
            .await
            .expect_err("must refuse");
        assert_eq!(error.error_code(), "INTERNAL_ERROR");
    }
}
