//! HTTP route table. Layer order (outside → in, per request):
//! `request_context_layer` → `authenticated_layer` (protected routes only)
//! → handler. Health routes stay public; the fallback answers v1-style 404s
//! in the stable envelope.

use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post, put};
use axum::Router;

use crate::handlers::{
    agents, executions, health, schedules, tenancy, workflows, Ctx, MAX_BODY_BYTES,
};
use crate::middleware::{authenticated_layer, request_context_layer};
use crate::response::ApiError;
use crate::state::AppState;

/// Builds the full HTTP router for the api process.
pub fn router(state: AppState) -> Router {
    let public = Router::new()
        .route("/v1/health/live", get(health::live))
        .route("/v1/health/ready", get(health::ready));

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
        .route("/v1/agents/{agent_id}/versions", post(agents::publish_version))
        // ---- workflows ----------------------------------------------
        .route("/v1/workflows", post(workflows::create).get(workflows::list))
        .route("/v1/workflows/{workflow_id}", get(workflows::get))
        .route("/v1/workflows/{workflow_id}/graph", put(workflows::update_graph))
        // ---- executions ---------------------------------------------
        .route("/v1/projects/{project_id}/executions", post(executions::submit))
        .route("/v1/executions", get(executions::list))
        .route("/v1/executions/{execution_id}", get(executions::get))
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
        .route_layer(axum::middleware::from_fn_with_state(state.clone(), authenticated_layer))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES));

    Router::new()
        .merge(public)
        .merge(protected)
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
    use crate::state::test_support::inmemory_state;
    use axum::body::Body;
    use axum::http::header;
    use axum::http::{Method, Request, StatusCode};
    use http_body_util::BodyExt;
    use mas_application::context::ServiceContext;
    use mas_common::ids::{OrganizationId, TenantId};
    use mas_common::timestamps::Timestamp;
    use tower::ServiceExt;

    struct Seeded {
        router: Router,
        tenant: TenantId,
        organization: OrganizationId,
        project: mas_common::ids::ProjectId,
    }

    async fn seeded() -> Seeded {
        let state = inmemory_state();
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
        let tenant_scoped = system.clone().with_scope(tenant.id, organization.id);
        let project = state
            .tenancy
            .register_project(&tenant_scoped, "Core", "core-http")
            .await
            .expect("project");
        let router = router(state);
        Seeded {
            router,
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

        // Scope headers are mandatory on tenant routes.
        let no_scope = request(Method::GET, "/v1/executions", None);
        let response = seeded
            .router
            .clone()
            .oneshot(no_scope)
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = json_body(response).await;
        assert_eq!(body["error"]["code"], "FORBIDDEN");
    }
}
