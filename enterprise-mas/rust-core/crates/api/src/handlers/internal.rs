// PURPOSE: Internal service routes — calls issued by the platform's own
// services (scheduler, worker) rather than by a tenant's API key.
//
// Owning layer: `mas-api` (the process edge), immediately below the router.
//
// Production responsibility: authenticate the *calling service* with the
// body-bound, time-bounded internal HMAC in `mas_security::internal_service`,
// then resolve the tenant from server-side truth (the schedule row) and run
// the execution inside the request's RLS-armed transaction, exactly as a
// tenant-facing call would.
//
// Security boundary: an internal caller is AUTHENTICATED BUT NOT TRUSTED WITH
// SCOPE. Being on the cluster network conveys no authority: every request must
// still be signed, still be authorized by `AuthorizationService`, and still
// run in a scope derived from the directory — never from the request body.
//
// It must NOT:
//   • accept an unsigned, stale, or unexpected-caller request;
//   • take the tenant, organization, project, or task template from the
//     request body as authoritative — they are claims, resolved server-side;
//   • mint or share a tenant-wide API key to let the "plane" reach every
//     tenant, which would turn one compromised scheduler into a platform-wide
//     breach;
//   • permit the caller to select an execution input: the task template comes
//     from the stored schedule.
//! Internal service routes: calls made by the platform's own services rather
//! than by a tenant's API key.
//!
//! # Why these are not on the protected router
//!
//! The protected router authenticates with a bearer API key and derives the
//! tenant from the key. The scheduler has no API key: it is a platform
//! service, and it acts on behalf of whichever tenant owns the schedule that
//! fired. Putting it on the protected router would mean minting a key that can
//! reach every tenant — a credential whose compromise is a platform-wide
//! breach.
//!
//! So these routes authenticate differently, with
//! [`mas_security::internal_service`]: a body-bound, time-bounded HMAC from a
//! service this process expects. That proves *which service* called. It does
//! not prove which tenant, and it must not: the tenant comes from the schedule
//! row, resolved server-side, exactly as it does everywhere else.
//!
//! # The rule that matters
//!
//! An internal caller is authenticated but not trusted with scope. A
//! `ScheduleRunRequest` names a tenant, and that name is a **claim** — it is
//! resolved against the directory and the resolved scope is what the request
//! runs under. A compromised scheduler that names a tenant it does not own
//! gets the same "not found" as any other caller.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use mas_common::ids::ScheduleId;
use mas_common::timestamps::Timestamp;
use mas_security::internal_service::{
    verify, InternalServiceIdentity, SERVICE_HEADER, SIGNATURE_HEADER,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::context::RequestContext;
use crate::response::{accepted, ApiError};
use crate::scope_resolution::ScopeClaims;
use crate::state::AppState;

/// Services allowed to call internal routes.
const ALLOWED_CALLERS: &[&str] = &["mas-scheduler", "mas-worker"];

/// Body of `POST /v1/internal/schedule-runs`.
///
/// Mirrors `mas-scheduler-service`'s `ScheduleRunRequest`. `tenant_id` is a
/// claim, not an authority: it is resolved against the directory before
/// anything runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleRunRequest {
    /// Schedule that fired.
    pub schedule_id: ScheduleId,
    /// The tenant the schedule belongs to (resolved server-side).
    pub tenant_id: mas_common::ids::TenantId,
    /// The planned fire time (idempotency material).
    pub planned_at: Timestamp,
    /// Opaque task template from the schedule aggregate.
    pub task_template: serde_json::Value,
}

/// Body of the `202` reply.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleRunAccepted {
    /// Created execution tracking id.
    pub execution_id: mas_common::ids::ExecutionId,
}

/// Accepts a fired schedule from the scheduler service.
///
/// Authentication is the internal HMAC; authorization is the ordinary scope
/// resolution, applied to the tenant named in the body.
///
/// # Responses
/// * `202` with an execution id — accepted;
/// * `401` — bad, stale, or missing signature, or an unexpected caller;
/// * `404` — the schedule, or its tenant, is not reachable. Deliberately
///   indistinguishable from "does not exist".
#[allow(clippy::too_many_lines)]
pub async fn schedule_runs(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // 1. Authenticate the caller against the raw bytes. The signature covers
    //    the body exactly as it arrived; deserializing first and re-serializing
    //    would verify a different message than the one being trusted.
    let identity = match authenticate_internal(&state, &headers, &body).await {
        Ok(identity) => identity,
        Err(error) => return error.into_response(),
    };

    let mut context = RequestContext::with_claims(
        ScopeClaims::default(),
        headers
            .get("x-correlation-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
            .unwrap_or_else(|| RequestContext::correlation_or_generated(None)),
    );

    let request: ScheduleRunRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(error) => {
            return error_response(
                mas_common::error::AppError::invalid_field(
                    "body",
                    "invalid_json",
                    format!("schedule run body is not valid JSON: {error}"),
                ),
                &fallback_context(&headers),
            )
            .into_response()
        },
    };

    // 2. Build a scheduler principal. It carries no tenant: the plane's
    //    authority is the row it names, resolved below.
    let mut principal = mas_security::SecurityPrincipal::scheduler(format!(
        "{}:{}",
        identity.service, request.schedule_id
    ));

    // 3. Resolve the scope from the directory. The tenant in the body is a
    //    claim; this is where it becomes a fact or a refusal.
    let claims = ScopeClaims {
        tenant: Some(request.tenant_id),
        organization: None,
        project: None,
    };
    context.claimed_tenant = claims.tenant;
    context.claimed_organization = claims.organization;
    let Some(resolver) = state.scope_resolver.as_ref() else {
        return internal_configuration_error().into_response();
    };
    let resolved = match resolver.resolve(&principal, claims).await {
        Ok(resolved) => resolved,
        Err(error) => return error_response(error, &context).into_response(),
    };
    principal = resolved.principal;
    context = context.with_resolved(principal.clone(), resolved.scope.clone());

    // 4. Authorize the plane at tenant scope before any state is touched.
    let decision = mas_security::AuthorizationService.authorize(
        &principal,
        &mas_security::AuthorizationRequest {
            resource: mas_security::ResourceKind::Execution,
            action: mas_security::Action::Create,
            tenant_id: resolved.scope.tenant_id(),
            organization_id: resolved.scope.organization_id(),
            project_id: None,
        },
    );
    if let mas_security::AuthorizationDecision::Deny(failure) = decision {
        tracing::warn!(
            schedule = %request.schedule_id,
            tenant = %request.tenant_id,
            code = %failure.code(),
            "internal schedule run denied"
        );
        return error_response(failure.to_app_error(), &context).into_response();
    }

    let service_context = match context.service_context() {
        Ok(service_context) => service_context,
        Err(error) => return error_response(error, &context).into_response(),
    };

    // 5. Load the schedule in the resolved scope. Its project and task
    //    template are authoritative; the body's `tenant_id` was only ever a
    //    claim, and the schedule's own `tenant_id` is compared against the
    //    resolved one below.
    let schedule = match state
        .schedules
        .get(&service_context, request.schedule_id)
        .await
    {
        Ok(schedule) => schedule,
        Err(error) => return error_response(error, &context).into_response(),
    };
    if schedule.tenant_id != resolved.scope.tenant_id() {
        // The scheduler named a tenant that does not own this schedule. There
        // is no legitimate reason for that pairing to exist, so it is not a
        // "not found" — it is a rejected request.
        return error_response(
            mas_common::error::AppError::forbidden(
                "the schedule does not belong to the tenant the run was requested for",
            ),
            &context,
        )
        .into_response();
    }
    let Some(project_id) = schedule.project_id else {
        return error_response(
            mas_common::error::AppError::conflict("schedule has no project, so nothing to run in"),
            &context,
        )
        .into_response();
    };

    // 6. Authorize the concrete project. Scope resolution proved the *tenant*;
    //    this proves the project is inside the tenant the schedule lives in.
    let project_decision = mas_security::AuthorizationService.authorize(
        &principal,
        &mas_security::AuthorizationRequest {
            resource: mas_security::ResourceKind::Execution,
            action: mas_security::Action::Create,
            tenant_id: resolved.scope.tenant_id(),
            organization_id: resolved.scope.organization_id(),
            project_id: Some(project_id),
        },
    );
    if let mas_security::AuthorizationDecision::Deny(failure) = project_decision {
        tracing::warn!(
            schedule = %request.schedule_id,
            project = %project_id,
            code = %failure.code(),
            "internal schedule run denied at project scope"
        );
        return error_response(failure.to_app_error(), &context).into_response();
    }

    // 7. Submit inside the request's scoped transaction, exactly as a
    //    tenant-facing call would. The template comes from the stored
    //    schedule, never from the caller's body.
    let idempotency_key = format!(
        "run:{}:{}",
        request.schedule_id,
        request.planned_at.to_unix_seconds()
    );
    let Some(pool) = state.runtime_pool.as_ref() else {
        return internal_configuration_error().into_response();
    };
    let executions = Arc::clone(&state.executions);
    let template = schedule.task_template;
    let outcome: mas_common::result::Result<mas_application::execution_service::SubmissionOutcome> =
        mas_persistence::scoped_request::with_authoritative_scope(
            pool,
            resolved.scope.clone(),
            move || async move {
                executions
                    .submit(
                        &service_context,
                        project_id,
                        None,
                        None,
                        template,
                        Some(&idempotency_key),
                    )
                    .await
            },
        )
        .await;

    match outcome {
        Ok(outcome) => accepted(
            &context,
            serde_json::json!(ScheduleRunAccepted {
                execution_id: outcome.execution.id,
            }),
        )
        .into_response(),
        Err(error) => error_response(error, &context).into_response(),
    }
}

/// Verifies the internal signature, returning a `401` response on failure.
async fn authenticate_internal(
    state: &AppState,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<InternalServiceIdentity, ApiError> {
    let Some(secret) = state.internal_secret.as_ref() else {
        // No secret configured: refuse rather than trust the network. An
        // operator sees this immediately because the scheduler stops working,
        // which is far better than it working without authentication.
        return Err(ApiError::new(
            mas_common::error::AppError::unauthorized("unauthenticated"),
            &fallback_context(headers),
        ));
    };
    let signature = headers
        .get(SIGNATURE_HEADER)
        .and_then(|value| value.to_str().ok());
    let service = headers
        .get(SERVICE_HEADER)
        .and_then(|value| value.to_str().ok());
    verify(
        secret.expose().as_bytes(),
        signature,
        service,
        &Timestamp::now(),
        body,
        ALLOWED_CALLERS,
    )
    .map_err(|error| ApiError::new(error, &fallback_context(headers)))
}

/// Every failure mode in `authenticate_internal` — missing header, malformed
/// header, stale timestamp, wrong secret, service not on the allowlist —
/// produces the same `UNAUTHENTICATED` error. A caller must not be able to
/// tell which check rejected it, because that difference is a probing oracle.
fn internal_configuration_error() -> ApiError {
    ApiError::new(
        mas_common::error::AppError::internal(
            "this process is not wired to resolve tenant scope; refusing the request",
        ),
        &fallback_context(&HeaderMap::new()),
    )
}

/// Builds an `ApiError` in the stable envelope.
fn error_response(error: mas_common::error::AppError, ctx: &RequestContext) -> ApiError {
    ApiError::new(error, ctx)
}

/// Minimal context for failures that happen before scope resolution exists.
fn fallback_context(headers: &HeaderMap) -> RequestContext {
    RequestContext::with_claims(
        ScopeClaims::default(),
        headers
            .get("x-correlation-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
            .unwrap_or_else(|| RequestContext::correlation_or_generated(None)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::response::ApiError;
    use crate::state::test_support::inmemory_state;
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use mas_application::context::ServiceContext;
    use mas_common::ids::TenantId;
    use mas_security::internal_service::sign;
    use tower::ServiceExt;

    const SECRET: &str = "inmemory-internal-secret";

    /// A signed request body, ready to post.
    fn signed_post(
        path: &str,
        service: &str,
        secret: &str,
        at: &Timestamp,
        body: &[u8],
    ) -> Request<Body> {
        let signature = sign(secret.as_bytes(), service, at, body);
        Request::builder()
            .method(Method::POST)
            .uri(path)
            .header("content-type", "application/json")
            .header(SIGNATURE_HEADER, signature)
            .header(SERVICE_HEADER, service)
            .body(Body::from(body.to_vec()))
            .expect("request")
    }

    fn run_body(tenant: TenantId, schedule: ScheduleId) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "schedule_id": schedule,
            "tenant_id": tenant,
            "planned_at": Timestamp::now(),
            "task_template": { "operation": "noop" },
        }))
        .expect("body")
    }

    #[tokio::test]
    async fn an_unsigned_run_is_refused() {
        let state = inmemory_state();
        let app = crate::router::router(state);
        let body = run_body(TenantId::new(), ScheduleId::new());
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/internal/schedule-runs")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn a_stale_signature_is_refused() {
        let state = inmemory_state();
        let app = crate::router::router(state);
        let body = run_body(TenantId::new(), ScheduleId::new());
        // Two hours old: far outside the 60-second window.
        let long_ago = Timestamp::now();
        let stale = Timestamp::from_unix_ms(long_ago.to_unix_ms().saturating_sub(7_200_000))
            .expect("stale timestamp");
        let response = app
            .oneshot(signed_post(
                "/v1/internal/schedule-runs",
                "mas-scheduler",
                SECRET,
                &stale,
                &body,
            ))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn a_signature_from_the_wrong_secret_is_refused() {
        let state = inmemory_state();
        let app = crate::router::router(state);
        let body = run_body(TenantId::new(), ScheduleId::new());
        let response = app
            .oneshot(signed_post(
                "/v1/internal/schedule-runs",
                "mas-scheduler",
                "not-the-secret",
                &Timestamp::now(),
                &body,
            ))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn a_service_that_is_not_on_the_allowlist_is_refused() {
        let state = inmemory_state();
        let app = crate::router::router(state);
        let body = run_body(TenantId::new(), ScheduleId::new());
        let response = app
            .oneshot(signed_post(
                "/v1/internal/schedule-runs",
                "mas-something-else",
                SECRET,
                &Timestamp::now(),
                &body,
            ))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn a_body_tampered_after_signing_is_refused() {
        let state = inmemory_state();
        let app = crate::router::router(state);
        let body = run_body(TenantId::new(), ScheduleId::new());
        // Sign one body, then send a different one. The MAC covers the body,
        // so swapping the message after signing must be rejected.
        let signature_for_other_body =
            sign(SECRET.as_bytes(), "mas-scheduler", &Timestamp::now(), b"{}");
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/internal/schedule-runs")
                    .header("content-type", "application/json")
                    .header(SIGNATURE_HEADER, signature_for_other_body)
                    .header(SERVICE_HEADER, "mas-scheduler")
                    .body(Body::from(body))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn a_process_without_a_secret_refuses_every_run() {
        let mut state = inmemory_state();
        state.internal_secret = None;
        let app = crate::router::router(state);
        let body = run_body(TenantId::new(), ScheduleId::new());
        let response = app
            .oneshot(signed_post(
                "/v1/internal/schedule-runs",
                "mas-scheduler",
                SECRET,
                &Timestamp::now(),
                &body,
            ))
            .await
            .expect("response");
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "no secret configured must fail closed, even for a correctly signed request"
        );
    }

    /// Regression test for the defect this phase exists to fix: a correctly
    /// signed run naming a tenant that does not exist must be indistinguishable
    /// from a route that does not exist.
    #[tokio::test]
    async fn a_signed_run_for_an_unreachable_tenant_is_not_found() {
        let state = inmemory_state();
        let app = crate::router::router(state);
        let body = run_body(TenantId::new(), ScheduleId::new());
        let response = app
            .oneshot(signed_post(
                "/v1/internal/schedule-runs",
                "mas-scheduler",
                SECRET,
                &Timestamp::now(),
                &body,
            ))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// A signed run that names tenant B while the schedule belongs to tenant A
    /// must not create anything in tenant A. The schedule lookup is scoped to
    /// the resolved tenant, so tenant A's schedule is invisible under a
    /// claim for tenant B.
    #[tokio::test]
    async fn a_signed_run_cannot_reach_another_tenants_schedule() {
        let state = inmemory_state();
        let system = ServiceContext::for_service("seed", "internal-it", &Timestamp::now())
            .expect("service context");
        let organization = state
            .tenancy
            .register_organization(&system, "Acme Ltd.", "Acme", "acme-internal")
            .await
            .expect("organization");
        let org_scoped = system.clone().with_scope(TenantId::new(), organization.id);
        let tenant_a = state
            .tenancy
            .register_tenant(
                &org_scoped,
                "A",
                "a-internal",
                mas_common::enums::Environment::Production,
                mas_domain::IsolationMode::SharedRls,
            )
            .await
            .expect("tenant a");
        state
            .tenancy
            .activate_tenant(&org_scoped, tenant_a.id)
            .await
            .expect("activate a");
        let tenant_b = state
            .tenancy
            .register_tenant(
                &org_scoped,
                "B",
                "b-internal",
                mas_common::enums::Environment::Production,
                mas_domain::IsolationMode::SharedRls,
            )
            .await
            .expect("tenant b");
        state
            .tenancy
            .activate_tenant(&org_scoped, tenant_b.id)
            .await
            .expect("activate b");

        let tenant_scoped = system.clone().with_scope(tenant_a.id, organization.id);
        let project = state
            .tenancy
            .register_project(&tenant_scoped, "Core", "core-internal")
            .await
            .expect("project");
        let schedule = state
            .schedules
            .register(
                &tenant_scoped,
                Some(project.id),
                "nightly",
                mas_domain::ScheduleKind::Interval { every_seconds: 60 },
                serde_json::json!({"operation": "noop"}),
            )
            .await
            .expect("schedule");

        // The real schedule id, but claimed for tenant B: tenant A's schedule
        // must not be reachable through that claim.
        let body = run_body(tenant_b.id, schedule.id);
        let app = crate::router::router(state);
        let response = app
            .oneshot(signed_post(
                "/v1/internal/schedule-runs",
                "mas-scheduler",
                SECRET,
                &Timestamp::now(),
                &body,
            ))
            .await
            .expect("response");
        assert_ne!(
            response.status(),
            StatusCode::ACCEPTED,
            "a schedule must never run under a tenant claim it does not belong to"
        );
    }

    #[tokio::test]
    async fn an_unparseable_body_is_a_validation_error_not_a_panic() {
        let state = inmemory_state();
        let app = crate::router::router(state);
        let body = b"{ not json".to_vec();
        let response = app
            .oneshot(signed_post(
                "/v1/internal/schedule-runs",
                "mas-scheduler",
                SECRET,
                &Timestamp::now(),
                &body,
            ))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let _: ApiError = ApiError::new(
            mas_common::error::AppError::internal("shape check"),
            &fallback_context(&HeaderMap::new()),
        );
    }
}
