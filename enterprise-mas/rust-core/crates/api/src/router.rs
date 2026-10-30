//! PURPOSE: The one HTTP route table for the api process.
//!
//! Owning layer: `mas-api` (the process edge).
//!
//! Production responsibility: define which paths exist, and — more
//! importantly — which authentication envelope each of them sits behind:
//! public (health), protected (bearer API key + resolved scope), and internal
//! (body-bound service HMAC, tenant resolved from server-side truth).
//!
//! Security boundary: a route registered on the wrong sub-router is a hole,
//! not a bug. The internal routes deliberately do NOT sit behind
//! `authenticated_layer`, because that layer's tenant comes from the API key
//! and a platform service has none; they authenticate with the internal
//! signature instead and resolve tenancy from the row they name.
//!
//! It must NOT:
//!   • add a route without deciding which of the three envelopes protects it;
//!   • put an internal route behind the API-key layer, or a tenant route
//!     behind the internal layer;
//!   • leave a route outside the request-context layer.

//! HTTP route table. Layer order (outside → in, per request):
//! `request_context_layer` → `authenticated_layer` (protected routes only)
//! → handler. Health routes stay public; the fallback answers v1-style 404s
//! in the stable envelope.

use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post, put};
use axum::Router;

use crate::handlers::{
    agents, analytics, api_keys, approvals, control_plane, data_hub, executions, health,
    inbound_webhooks, integrations, internal, notifications, oauth, rollbacks, schedules, search,
    tenancy, workflows, Ctx, MAX_BODY_BYTES,
};
use crate::middleware::{authenticated_layer, request_context_layer};
use crate::response::ApiError;
use crate::state::AppState;

/// Builds the full HTTP router for the api process.
pub fn router(state: AppState) -> Router {
    let public = Router::new()
        .route("/v1/health/live", get(health::live))
        .route("/v1/health/ready", get(health::ready))
        // Public only in the narrow provider-callback sense: its route id
        // selects server-owned metadata and its handler verifies an HMAC before
        // receipt insertion. No user/API-key authentication is applicable.
        .route(
            "/v1/public/inbound-webhooks/{route_id}",
            post(inbound_webhooks::ingest)
                .layer(DefaultBodyLimit::max(mas_integrations::inbound_webhooks::MAX_INBOUND_WEBHOOK_BYTES)),
        )
        // OAuth callbacks carry an opaque one-shot state; they never accept a
        // tenant, connector, redirect URI, or provider endpoint from query.
        .route(
            "/v1/public/oauth/{provider}/callback",
            get(oauth::callback),
        );

    let protected = Router::new()
        // ---- tenancy / identity ------------------------------------
        .route("/v1/organizations", post(tenancy::register_organization).get(tenancy::list_organizations))
        .route("/v1/tenants", post(tenancy::register_tenant).get(tenancy::list_tenants))
        .route("/v1/projects", post(tenancy::register_project))
        .route("/v1/users", post(tenancy::register_user))
        .route("/v1/memberships", post(tenancy::invite_membership))
        // ---- agents -------------------------------------------------
        .route("/v1/agents", post(agents::register).get(agents::list))
        .route("/v1/agents/{agent_id}", get(agents::get))
        .route(
            "/v1/agents/{agent_id}/versions",
            get(agents::list_versions).post(agents::publish_version),
        )
        .route(
            "/v1/agents/{agent_id}/versions/{version}/rollback",
            post(rollbacks::rollback_agent),
        )
        // ---- approvals -----------------------------------------------
        .route("/v1/approvals", get(approvals::list))
        .route("/v1/approvals/{id}/approve", post(approvals::approve))
        .route("/v1/approvals/{id}/deny", post(approvals::deny))
        // ---- API keys ------------------------------------------------
        .route("/v1/api-keys", post(api_keys::create).get(api_keys::list))
        .route("/v1/api-keys/{id}/revoke", post(api_keys::revoke))
        .route("/v1/api-keys/{id}/rotate", post(api_keys::rotate))
        // ---- recipient notifications --------------------------------
        .route(
            "/v1/notifications/recipients/{recipient_id}",
            get(notifications::list),
        )
        .route(
            "/v1/notifications/recipients/{recipient_id}/{notification_id}/read",
            post(notifications::mark_read),
        )
        .route(
            "/v1/notifications/recipients/{recipient_id}/{notification_id}/archive",
            post(notifications::archive),
        )
        // ---- canonical connectors and outbound webhooks --------------
        .route(
            "/v1/connectors",
            post(integrations::register_connector).get(integrations::list_connectors),
        )
        .route("/v1/connectors/test", post(integrations::test_connector))
        .route(
            "/v1/connectors/{connector_id}",
            get(integrations::get_connector),
        )
        .route(
            "/v1/webhooks",
            post(integrations::register_webhook).get(integrations::list_webhooks),
        )
        .route(
            "/v1/webhooks/{webhook_id}/enable",
            post(integrations::enable_webhook),
        )
        .route(
            "/v1/webhooks/{webhook_id}/disable",
            post(integrations::disable_webhook),
        )
        // ---- connector-bound OAuth authorization ---------------------
        .route(
            "/v1/connectors/{connector_id}/oauth/authorize",
            post(oauth::begin),
        )
        // ---- inbound webhook endpoint configuration -----------------
        .route(
            "/v1/inbound-webhooks",
            post(inbound_webhooks::register).get(inbound_webhooks::list),
        )
        .route(
            "/v1/inbound-webhooks/{endpoint_id}",
            get(inbound_webhooks::get),
        )
        .route(
            "/v1/inbound-webhooks/{endpoint_id}/enable",
            post(inbound_webhooks::enable),
        )
        .route(
            "/v1/inbound-webhooks/{endpoint_id}/disable",
            post(inbound_webhooks::disable),
        )
        // ---- workflows ----------------------------------------------
        .route("/v1/workflows", post(workflows::create).get(workflows::list))
        .route("/v1/workflows/{workflow_id}", get(workflows::get))
        .route("/v1/workflows/{workflow_id}/graph", put(workflows::update_graph))
        .route("/v1/workflows/{workflow_id}/publish", post(workflows::publish))
        .route("/v1/workflows/{workflow_id}/disable", post(workflows::disable))
        .route(
            "/v1/workflows/{workflow_id}/versions",
            get(workflows::list_versions),
        )
        .route(
            "/v1/workflows/{workflow_id}/versions/{version}/rollback",
            post(rollbacks::rollback_workflow),
        )
        .route("/v1/rollbacks", get(rollbacks::list))
        // ---- executions ---------------------------------------------
        .route("/v1/projects/{project_id}/executions", post(executions::submit))
        .route("/v1/executions", get(executions::list))
        .route("/v1/executions/{execution_id}", get(executions::get))
        .route("/v1/executions/{execution_id}/steps", get(executions::steps))
        .route("/v1/executions/{execution_id}/start", post(executions::start))
        .route("/v1/executions/{execution_id}/pause", post(executions::pause))
        .route("/v1/executions/{execution_id}/resume", post(executions::resume))
        .route("/v1/executions/{execution_id}/cancel", post(executions::cancel))
        // ---- schedules ----------------------------------------------
        .route("/v1/schedules", post(schedules::register).get(schedules::list))
        .route("/v1/schedules/{schedule_id}", get(schedules::get))
        .route("/v1/schedules/{schedule_id}/pause", post(schedules::pause))
        .route("/v1/schedules/{schedule_id}/resume", post(schedules::resume))
        .route("/v1/schedules/{schedule_id}/disable", post(schedules::disable))
        // ---- command palette search ---------------------------------
        .route("/v1/search", get(search::search))
        // ---- Data Hub -------------------------------------------------
        .route(
            "/v1/data-sources",
            post(data_hub::create_source).get(data_hub::list_sources),
        )
        .route("/v1/data-sources/{id}", get(data_hub::get_source))
        .route("/v1/data-sources/{id}/sync", post(data_hub::sync_source))
        .route(
            "/v1/data-sources/{id}/documents",
            get(data_hub::list_documents),
        )
        // This endpoint has its own bounded 64 MiB extractor limit; the Data
        // Hub application service applies the deployment-specific limit again
        // before retaining content or calling the Python bridge.
        .route(
            "/v1/documents/upload",
            post(data_hub::upload_document)
                .layer(DefaultBodyLimit::max(data_hub::MAX_DATA_HUB_MULTIPART_BYTES)),
        )
        .route(
            "/v1/documents/{id}",
            get(data_hub::get_document).delete(data_hub::delete_document),
        )
        // ---- dashboard / deterministic insights ---------------------
        .route("/v1/analytics/overview", get(analytics::overview))
        .route("/v1/analytics/performance", get(analytics::performance))
        .route("/v1/analytics/tasks-by-agent", get(analytics::tasks_by_agent))
        .route("/v1/analytics/usage-cost", get(analytics::usage_cost))
        .route("/v1/assistant/insights", get(analytics::insights))
        // ---- control plane (billing, entitlements, usage) -----------
        // Tenant-scoped: the tenant comes from the authenticated scope, never
        // from the path or the body, so no caller can read another tenant's
        // plan, usage or billing state by guessing an id.
        .route("/v1/billing/subscription", get(control_plane::get_subscription))
        .route("/v1/billing/entitlements", get(control_plane::list_entitlements))
        .route("/v1/billing/usage-totals", get(control_plane::usage_totals))
        .route("/v1/billing/receipts", get(control_plane::list_receipts))
        .route("/v1/billing/receipts/{provider}/{event_id}", get(control_plane::get_receipt))
        .route("/v1/usage", get(control_plane::list_usage))
        // ---- control-plane administration -------------------------
        // `platform:admin` only. These change what a tenant may do and what
        // it is billed; every one of them is audited, and every one of them
        // refuses rather than degrading when the caller is not authorised.
        .route("/v1/admin/plans", post(control_plane::create_plan))
        .route(
            "/v1/admin/plans/{code}/features",
            put(control_plane::grant_plan_feature),
        )
        .route(
            "/v1/admin/tenants/{tenant_id}/subscription",
            post(control_plane::create_subscription),
        )
        .route(
            "/v1/admin/tenants/{tenant_id}/subscription/transitions",
            post(control_plane::transition_subscription),
        )
        .route(
            "/v1/admin/tenants/{tenant_id}/overrides/features",
            post(control_plane::grant_feature_override),
        )
        .route(
            "/v1/admin/tenants/{tenant_id}/overrides/quotas",
            post(control_plane::grant_quota_override),
        )
        .route(
            "/v1/admin/tenants/{tenant_id}/overrides/features/{feature}/end",
            post(control_plane::end_feature_override),
        )
        // ---- governance (identity, audit, policies, quotas, usage) ----
        // Registered through the additive route group so this table keeps one
        // line per surface and the auth envelope above stays the only one.
        .merge(crate::routes_governance::routes())
        // ---- tooling (tools, connector admin, webhook ops, SSE, assistant)
        .merge(crate::routes_tooling::routes())
        .route_layer(axum::middleware::from_fn_with_state(state.clone(), authenticated_layer))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES));

    // ---- internal (service-to-service) -----------------------------
    // Deliberately NOT behind `authenticated_layer`: those routes take a
    // tenant API key, and a platform service has none. These take the
    // internal HMAC instead and resolve the tenant from server-side truth.
    let internal = Router::new()
        .route("/v1/internal/schedule-runs", post(internal::schedule_runs))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES));

    Router::new()
        .merge(public)
        .merge(protected)
        .merge(internal)
        .fallback(not_found)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            request_context_layer,
        ))
        .with_state(state)
}

/// `404` for unknown paths, stable-envelope shaped.
async fn not_found(Ctx(ctx): Ctx) -> ApiError {
    ApiError::new(
        mas_common::error::AppError::not_found("route", "the requested path"),
        &ctx,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::test_support::{inmemory_state, inmemory_state_with_store};
    use axum::body::Body;
    use axum::http::header;
    use axum::http::{Method, Request, StatusCode};
    use http_body_util::BodyExt;
    use mas_application::context::ServiceContext;
    use mas_application::stores::{ExecutionStepStorePort, InMemoryServices, MembershipStorePort};
    use mas_common::ids::{OrganizationId, TenantId};
    use mas_common::timestamps::Timestamp;
    use mas_domain::ExecutionStep;
    use std::sync::Arc;
    use tower::ServiceExt;

    struct Seeded {
        router: Router,
        store: Arc<InMemoryServices>,
        tenant: TenantId,
        organization: OrganizationId,
        project: mas_common::ids::ProjectId,
    }

    async fn seeded() -> Seeded {
        let (mut state, store) = inmemory_state_with_store();
        let system =
            ServiceContext::for_service("seed", "http-it-1", &Timestamp::now()).expect("ctx");
        let organization = state
            .tenancy
            .register_organization(&system, "Acme Ltd.", "Acme", "acme-http")
            .await
            .expect("org");
        let org_scoped = system.clone().with_scope(TenantId::new(), organization.id);
        let tenant = state
            .tenancy
            .register_tenant(
                &org_scoped,
                "Prod",
                "prod-http",
                mas_common::enums::Environment::Production,
                mas_domain::IsolationMode::SharedRls,
            )
            .await
            .expect("tenant");
        let org_scoped_activate = system.clone().with_scope(tenant.id, organization.id);
        // A tenant is provisioned, not operational, until activated. The
        // resolver refuses a non-operational tenant, so the happy path has to
        // activate it.
        state
            .tenancy
            .activate_tenant(&org_scoped_activate, tenant.id)
            .await
            .expect("activate");
        let tenant_scoped = system.clone().with_scope(tenant.id, organization.id);
        let project = state
            .tenancy
            .register_project(&tenant_scoped, "Core", "core-http")
            .await
            .expect("project");
        let owner = state
            .tenancy
            .register_user(&system, "router-owner@example.test", "Router Owner")
            .await
            .expect("owner user");
        let mut owner_membership = state
            .tenancy
            .invite_membership(&tenant_scoped, owner.id, mas_domain::MembershipRole::Owner)
            .await
            .expect("owner membership");
        owner_membership
            .accept_invitation()
            .expect("accept owner invitation");
        MembershipStorePort::save(&store, &owner_membership)
            .await
            .expect("activate owner membership");
        let principal = mas_security::SecurityPrincipal::new(
            owner.id.to_string(),
            mas_security::PrincipalKind::User,
            Some(tenant.id),
            mas_security::ScopeSet::denied(),
        )
        .with_organization(organization.id)
        .with_memberships(vec![mas_security::TenantMembership::new(
            organization.id,
            tenant.id,
            mas_domain::MembershipRole::Owner,
        )]);
        state.verifier =
            Arc::new(crate::auth::StaticTokenVerifier::new().with_token("test-token", principal));
        let router = router(state);
        Seeded {
            router,
            store,
            tenant: tenant.id,
            organization: organization.id,
            project: project.id,
        }
    }

    fn request(method: Method, path: &str, body: Option<serde_json::Value>) -> Request<Body> {
        let mut builder = Request::builder().method(method).uri(path);
        if body.is_some() {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
        }
        builder
            .header(header::AUTHORIZATION, "Bearer test-token")
            .body(match body {
                Some(json) => Body::from(serde_json::to_string(&json).expect("json")),
                None => Body::empty(),
            })
            .expect("request")
    }

    fn scoped_request(
        seeded: &Seeded,
        method: Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Request<Body> {
        let mut req = request(method, path, body);
        let headers = req.headers_mut();
        headers.insert("x-tenant-id", seeded.tenant.to_string().parse().expect("h"));
        headers.insert(
            "x-organization-id",
            seeded.organization.to_string().parse().expect("h"),
        );
        headers.insert("x-correlation-id", "http-corr-9".parse().expect("h"));
        req
    }

    async fn json_body(response: axum::response::Response) -> serde_json::Value {
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("collect")
            .to_bytes();
        serde_json::from_slice(&bytes).expect("json body")
    }

    #[tokio::test]
    async fn health_routes_are_public() {
        let state = inmemory_state();
        let router = router(state);
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/v1/health/live")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["status"], "ok");
    }

    #[tokio::test]
    async fn protected_routes_require_bearer_and_envelope_meta_is_stable() {
        let seeded = seeded().await;
        let missing_auth = seeded
            .router
            .clone()
            .oneshot(scoped_request(&seeded, Method::GET, "/v1/executions", None))
            .await
            .expect("response");
        // Builder adds Bearer test-token; replace with no-auth request.
        let mut no_auth = Request::builder()
            .method(Method::GET)
            .uri("/v1/executions")
            .body(Body::empty())
            .expect("r");
        no_auth
            .headers_mut()
            .insert("x-tenant-id", seeded.tenant.to_string().parse().expect("h"));
        no_auth.headers_mut().insert(
            "x-organization-id",
            seeded.organization.to_string().parse().expect("h"),
        );
        let response = seeded
            .router
            .clone()
            .oneshot(no_auth)
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = json_body(response).await;
        assert_eq!(body["error"]["code"], "UNAUTHENTICATED");
        assert!(body["meta"]["request_id"].is_string());
        let _ = missing_auth;
    }

    #[tokio::test]
    async fn platform_admin_can_reach_global_organization_catalog_without_forging_a_tenant() {
        let mut state = inmemory_state();
        state.verifier = Arc::new(
            crate::auth::StaticTokenVerifier::new()
                .with_token("platform-token", mas_security::SecurityPrincipal::system()),
        );
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/organizations")
                    .header(header::AUTHORIZATION, "Bearer platform-token")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(json_body(response).await["data"].is_array());
    }

    #[tokio::test]
    async fn search_route_validates_bounded_queries_and_uses_the_standard_envelope() {
        let seeded = seeded().await;
        let success = seeded
            .router
            .clone()
            .oneshot(scoped_request(
                &seeded,
                Method::GET,
                "/v1/search?q=route&limit=1&kinds=agents",
                None,
            ))
            .await
            .expect("search response");
        assert_eq!(success.status(), StatusCode::OK);
        let body = json_body(success).await;
        assert!(body["data"]["items"].is_array());
        assert_eq!(body["data"]["truncated"], false);
        assert_eq!(body["meta"]["correlation_id"], "http-corr-9");

        let too_short = seeded
            .router
            .clone()
            .oneshot(scoped_request(
                &seeded,
                Method::GET,
                "/v1/search?q=ab",
                None,
            ))
            .await
            .expect("validation response");
        assert_eq!(too_short.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            json_body(too_short).await["error"]["code"],
            "VALIDATION_FAILED"
        );

        let malformed = seeded
            .router
            .clone()
            .oneshot(scoped_request(
                &seeded,
                Method::GET,
                "/v1/search?q=route&limit=not-a-number",
                None,
            ))
            .await
            .expect("malformed-query response");
        assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            json_body(malformed).await["error"]["code"],
            "VALIDATION_FAILED"
        );
    }

    #[tokio::test]
    async fn full_http_happy_path_and_idempotent_replay() {
        let seeded = seeded().await;

        // Register an agent through the API.
        let create_agent = scoped_request(
            &seeded,
            Method::POST,
            "/v1/agents",
            Some(serde_json::json!({
                "project_id": seeded.project,
                "name": "Copilot",
                "slug": "copilot",
                "kind": "standard"
            })),
        );
        let response = seeded
            .router
            .clone()
            .oneshot(create_agent)
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);
        let agent_body = json_body(response).await;
        assert_eq!(agent_body["data"]["status"], "draft");
        assert_eq!(agent_body["meta"]["correlation_id"], "http-corr-9");
        let agent_id = agent_body["data"]["id"].as_str().expect("id").to_owned();

        // Create a workflow, then upload its graph through the API.
        let create_workflow = scoped_request(
            &seeded,
            Method::POST,
            "/v1/workflows",
            Some(serde_json::json!({"project_id": seeded.project, "name": "flow-1"})),
        );
        let response = seeded
            .router
            .clone()
            .oneshot(create_workflow)
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);
        let workflow_body = json_body(response).await;
        let workflow_id = workflow_body["data"]["id"].as_str().expect("id").to_owned();

        let graph_update = scoped_request(
            &seeded,
            Method::PUT,
            &format!("/v1/workflows/{workflow_id}/graph"),
            Some(serde_json::json!({
                "nodes": [
                    {"node_key": "start", "node_type": "start", "name": "start"},
                    {"node_key": "triage", "node_type": "agent", "name": "triage",
                     "config": {"agent_ref": "copilot"}},
                    {"node_key": "finish", "node_type": "end", "name": "end"}
                ],
                "edges": [{"from": "start", "to": "triage"}, {"from": "triage", "to": "finish"}]
            })),
        );
        let response = seeded
            .router
            .clone()
            .oneshot(graph_update)
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let graph_body = json_body(response).await;
        assert_eq!(
            graph_body["data"]["topological_order"],
            serde_json::json!(["start", "triage", "finish"]),
            "deterministic topological order returned"
        );

        // Publishing creates an immutable v1 snapshot visible through the
        // tenant-scoped version-history endpoint.
        let response = seeded
            .router
            .clone()
            .oneshot(scoped_request(
                &seeded,
                Method::POST,
                &format!("/v1/workflows/{workflow_id}/publish"),
                None,
            ))
            .await
            .expect("publish response");
        assert_eq!(response.status(), StatusCode::CREATED);
        let published_v1 = json_body(response).await;
        assert_eq!(published_v1["data"]["version_number"], 1);

        let response = seeded
            .router
            .clone()
            .oneshot(scoped_request(
                &seeded,
                Method::GET,
                &format!("/v1/workflows/{workflow_id}/versions"),
                None,
            ))
            .await
            .expect("version history response");
        assert_eq!(response.status(), StatusCode::OK);
        let versions = json_body(response).await;
        assert_eq!(versions["data"].as_array().expect("versions").len(), 1);
        assert_eq!(versions["data"][0]["version_number"], 1);
        assert_eq!(
            versions["data"][0]["graph"]["nodes"]
                .as_array()
                .expect("nodes")
                .len(),
            3
        );

        // Republish as v2, then roll back to v1. Rollback is forward-versioned
        // (v3) and the exact idempotency replay returns the same receipt.
        let response = seeded
            .router
            .clone()
            .oneshot(scoped_request(
                &seeded,
                Method::POST,
                &format!("/v1/workflows/{workflow_id}/disable"),
                None,
            ))
            .await
            .expect("disable response");
        assert_eq!(response.status(), StatusCode::OK);
        let response = seeded
            .router
            .clone()
            .oneshot(scoped_request(
                &seeded,
                Method::POST,
                &format!("/v1/workflows/{workflow_id}/publish"),
                None,
            ))
            .await
            .expect("republish response");
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(json_body(response).await["data"]["version_number"], 2);

        let rollback_body = serde_json::json!({
            "target_version_number": 1,
            "reason": "restore the approved graph",
            "idempotency_key": "http-workflow-rollback-1"
        });
        let rollback_path = format!("/v1/workflows/{workflow_id}/versions/1/rollback");
        let response = seeded
            .router
            .clone()
            .oneshot(scoped_request(
                &seeded,
                Method::POST,
                &rollback_path,
                Some(rollback_body.clone()),
            ))
            .await
            .expect("rollback response");
        assert_eq!(response.status(), StatusCode::CREATED);
        let rollback_receipt = json_body(response).await;
        assert_eq!(rollback_receipt["data"]["from_version"], 2);
        assert_eq!(rollback_receipt["data"]["to_version"], 3);
        let response = seeded
            .router
            .clone()
            .oneshot(scoped_request(
                &seeded,
                Method::POST,
                &rollback_path,
                Some(rollback_body),
            ))
            .await
            .expect("idempotent rollback response");
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            json_body(response).await["data"]["rollback_record_id"],
            rollback_receipt["data"]["rollback_record_id"]
        );
        let response = seeded
            .router
            .clone()
            .oneshot(scoped_request(
                &seeded,
                Method::GET,
                "/v1/rollbacks?limit=10",
                None,
            ))
            .await
            .expect("rollback history response");
        assert_eq!(response.status(), StatusCode::OK);
        let history = json_body(response).await;
        assert_eq!(history["data"].as_array().expect("history").len(), 1);
        assert!(history["data"][0].get("idempotency_key").is_none());

        // Submit an execution with an Idempotency-Key header.
        let submit_path = format!("/v1/projects/{}/executions", seeded.project);
        let make_submit = |workflow_id: &str| {
            let mut req = scoped_request(
                &seeded,
                Method::POST,
                &submit_path,
                Some(serde_json::json!({"workflow_id": workflow_id, "input": {"ticket": "T-1"}})),
            );
            req.headers_mut()
                .insert("idempotency-key", "http-idem-1".parse().expect("h"));
            req
        };
        let response = seeded
            .router
            .clone()
            .oneshot(make_submit(&workflow_id))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);
        assert!(response.headers().get("x-idempotent-replay").is_none());
        let first_body = json_body(response).await;
        let execution_id = first_body["data"]["id"].as_str().expect("id").to_owned();

        // Timeline reads are tenant-scoped and return an empty ordered list
        // until the runtime has persisted step rows.
        let response = seeded
            .router
            .clone()
            .oneshot(scoped_request(
                &seeded,
                Method::GET,
                &format!("/v1/executions/{execution_id}/steps"),
                None,
            ))
            .await
            .expect("execution steps response");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(json_body(response).await["data"]
            .as_array()
            .expect("timeline")
            .is_empty());

        // Seed runtime-owned step rows out of order, then verify the HTTP
        // projection is ordered and never exposes object-storage references.
        let execution_id_value = mas_common::ids::ExecutionId::from_uuid(
            uuid::Uuid::parse_str(&execution_id).expect("execution id"),
        );
        let mut later_step = ExecutionStep::new(
            execution_id_value,
            "agent.run",
            2,
            Some("finish".to_owned()),
        )
        .with_agent(mas_common::ids::AgentId::new())
        .with_input_ref("object://private/agent-input");
        later_step.begin().expect("begin later step");
        later_step
            .succeed(Some("object://private/agent-output".to_owned()))
            .expect("complete later step");
        let mut earlier_step = ExecutionStep::new(
            execution_id_value,
            "tool.invoke",
            0,
            Some("triage".to_owned()),
        )
        .with_tool(mas_common::ids::ToolId::new())
        .with_input_ref("object://private/tool-input");
        earlier_step.begin().expect("begin earlier step");
        earlier_step
            .succeed(Some("object://private/tool-output".to_owned()))
            .expect("complete earlier step");
        ExecutionStepStorePort::save(&seeded.store, &later_step)
            .await
            .expect("persist later runtime step");
        ExecutionStepStorePort::save(&seeded.store, &earlier_step)
            .await
            .expect("persist earlier runtime step");

        let response = seeded
            .router
            .clone()
            .oneshot(scoped_request(
                &seeded,
                Method::GET,
                &format!("/v1/executions/{execution_id}/steps"),
                None,
            ))
            .await
            .expect("populated execution steps response");
        assert_eq!(response.status(), StatusCode::OK);
        let timeline = json_body(response).await["data"]
            .as_array()
            .expect("populated timeline")
            .clone();
        assert_eq!(timeline.len(), 2);
        assert_eq!(timeline[0]["sequence"], 0);
        assert_eq!(timeline[0]["kind"], "tool.invoke");
        assert_eq!(timeline[0]["node_key"], "triage");
        assert_eq!(timeline[0]["status"], "completed");
        assert_eq!(timeline[0]["attempt"], 1);
        assert!(timeline[0]["tool_id"].is_string());
        assert!(timeline[0]["latency_ms"].is_u64());
        assert_eq!(timeline[1]["sequence"], 2);
        assert_eq!(timeline[1]["kind"], "agent.run");
        assert_eq!(timeline[1]["node_key"], "finish");
        assert_eq!(timeline[1]["status"], "completed");
        assert!(timeline[1]["agent_id"].is_string());
        for step in &timeline {
            assert!(step.get("input_ref").is_none());
            assert!(step.get("output_ref").is_none());
            assert!(step.get("tokens_input").is_none());
            assert!(step.get("tokens_output").is_none());
        }

        // Replay: same key ⇒ same execution with the replay header set.
        let response = seeded
            .router
            .clone()
            .oneshot(make_submit(&workflow_id))
            .await
            .expect("response");
        assert_eq!(
            response
                .headers()
                .get("x-idempotent-replay")
                .and_then(|v| v.to_str().ok()),
            Some("true"),
            "replay header set on idempotent hit"
        );
        let replay_body = json_body(response).await;
        assert_eq!(
            replay_body["data"]["id"].as_str(),
            Some(execution_id.as_str())
        );

        // Draft agents refuse submission (conflict surfaces the stable code).
        let bad_submit = scoped_request(
            &seeded,
            Method::POST,
            &submit_path,
            Some(serde_json::json!({"agent_id": agent_id, "input": {}})),
        );
        let response = seeded
            .router
            .clone()
            .oneshot(bad_submit)
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = json_body(response).await;
        assert_eq!(body["error"]["code"], "CONFLICT");

        // The publisher is derived from the authenticated user, never from
        // request JSON; list_versions returns the immutable safe projection.
        let response = seeded
            .router
            .clone()
            .oneshot(scoped_request(
                &seeded,
                Method::POST,
                &format!("/v1/agents/{agent_id}/versions"),
                Some(serde_json::json!({
                    "configuration_checksum": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                    "configuration_snapshot": {"model_hint": "test-model"}
                })),
            ))
            .await
            .expect("publish agent version response");
        assert_eq!(response.status(), StatusCode::CREATED);
        let published_agent_version = json_body(response).await;
        assert_eq!(published_agent_version["data"]["version_number"], 1);
        let response = seeded
            .router
            .clone()
            .oneshot(scoped_request(
                &seeded,
                Method::POST,
                &format!("/v1/agents/{agent_id}/versions"),
                Some(serde_json::json!({
                    "configuration_checksum": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                    "configuration_snapshot": {"model_hint": "forged"},
                    "published_by": "00000000-0000-0000-0000-000000000000"
                })),
            ))
            .await
            .expect("forged publisher response");
        assert!(
            response.status().is_client_error(),
            "clients may not submit the publisher identity"
        );
        let response = seeded
            .router
            .clone()
            .oneshot(scoped_request(
                &seeded,
                Method::GET,
                &format!("/v1/agents/{agent_id}/versions"),
                None,
            ))
            .await
            .expect("agent version history response");
        assert_eq!(response.status(), StatusCode::OK);
        let agent_versions = json_body(response).await;
        assert_eq!(agent_versions["data"][0]["version_number"], 1);
        assert_eq!(
            agent_versions["data"][0]["published_by"],
            published_agent_version["data"]["published_by"]
        );

        let response = seeded
            .router
            .clone()
            .oneshot(scoped_request(
                &seeded,
                Method::POST,
                &format!("/v1/agents/{agent_id}/versions"),
                Some(serde_json::json!({
                    "configuration_checksum": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
                    "configuration_snapshot": {"model_hint": "test-model-v2"}
                })),
            ))
            .await
            .expect("publish second agent version response");
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(json_body(response).await["data"]["version_number"], 2);

        let agent_rollback_body = serde_json::json!({
            "target_version_number": 1,
            "reason": "restore the approved agent configuration",
            "idempotency_key": "http-agent-rollback-1"
        });
        let agent_rollback_path = format!("/v1/agents/{agent_id}/versions/1/rollback");
        let response = seeded
            .router
            .clone()
            .oneshot(scoped_request(
                &seeded,
                Method::POST,
                &agent_rollback_path,
                Some(agent_rollback_body.clone()),
            ))
            .await
            .expect("agent rollback response");
        assert_eq!(response.status(), StatusCode::CREATED);
        let agent_rollback_receipt = json_body(response).await;
        assert_eq!(agent_rollback_receipt["data"]["from_version"], 2);
        assert_eq!(agent_rollback_receipt["data"]["to_version"], 3);
        assert_eq!(agent_rollback_receipt["data"]["source_version"], 1);
        let response = seeded
            .router
            .clone()
            .oneshot(scoped_request(
                &seeded,
                Method::POST,
                &agent_rollback_path,
                Some(agent_rollback_body),
            ))
            .await
            .expect("idempotent agent rollback response");
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            json_body(response).await["data"]["rollback_record_id"],
            agent_rollback_receipt["data"]["rollback_record_id"]
        );
        let response = seeded
            .router
            .clone()
            .oneshot(scoped_request(
                &seeded,
                Method::GET,
                "/v1/rollbacks?limit=10",
                None,
            ))
            .await
            .expect("combined rollback history response");
        assert_eq!(response.status(), StatusCode::OK);
        let combined_history = json_body(response).await;
        assert_eq!(
            combined_history["data"].as_array().expect("history").len(),
            2
        );
        assert!(combined_history["data"][0].get("idempotency_key").is_none());

        // Transition over HTTP: start → pause → resume.
        let do_post = |path: String| scoped_request(&seeded, Method::POST, &path, None);
        let response = seeded
            .router
            .clone()
            .oneshot(do_post(format!("/v1/executions/{execution_id}/start")))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["data"]["status"], "running");

        // Foreign tenant: hidden as 404, never 403.
        let mut foreign = scoped_request(
            &seeded,
            Method::GET,
            &format!("/v1/executions/{execution_id}"),
            None,
        );
        foreign.headers_mut().insert(
            "x-tenant-id",
            TenantId::new().to_string().parse().expect("h"),
        );
        let response = seeded
            .router
            .clone()
            .oneshot(foreign)
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // A principal bound to exactly one tenant needs no scope header: the
        // resolver selects its sole authoritative binding rather than
        // treating absent request metadata as an error.
        let no_scope = request(Method::GET, "/v1/executions", None);
        let response = seeded
            .router
            .clone()
            .oneshot(no_scope)
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["data"].as_array().expect("executions").len(), 1);
    }
}
