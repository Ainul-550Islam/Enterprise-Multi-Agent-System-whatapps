//! PURPOSE: Route and authorization contract tests for the BATCH-02 tooling,
//! connector, webhook-delivery, stream, and assistant surfaces.
//!
//! Owning layer: `mas-api` integration tests.
//!
//! Production responsibility: prove authentication, canonical role denial,
//! fail-closed service composition, safe URL rejection, and stable DTO shape at
//! the HTTP boundary; lower-layer suites verify RLS and durable transactions.
//!
//! Data ownership: test-only API state and pure contract fixtures.
//!
//! MUST NOT: replace production policy/SSRF checks with permissive test grants,
//! claim live-Postgres coverage, or register duplicate routes.

use axum::{
    body::Body,
    http::{Request, StatusCode},
    response::IntoResponse,
};
use mas_api::{
    context::RequestContext, router::router, scope_resolution::ScopeClaims,
    state::test_support::inmemory_state,
};
use mas_common::ids::{OrganizationId, TenantId};
use mas_contracts::tool::{ToolPageResponse, ToolResponse};
use mas_domain::MembershipRole;
use mas_security::{
    Action, AuthorizationRequest, AuthorizationService, PrincipalKind, ResourceKind, ScopeSet,
    SecurityPrincipal, TenantMembership,
};
use serde_json::json;
use tower::ServiceExt;

fn uuid() -> String {
    uuid::Uuid::now_v7().to_string()
}

#[tokio::test]
async fn every_new_tenant_surface_rejects_unauthenticated_requests() {
    let app = router(inmemory_state());
    let paths = vec![
        ("POST", "/v1/tools".to_owned()),
        ("GET", "/v1/tools".to_owned()),
        ("GET", format!("/v1/tools/{}", uuid())),
        ("PATCH", format!("/v1/tools/{}", uuid())),
        ("PUT", format!("/v1/agents/{}/tool-permissions", uuid())),
        ("GET", "/v1/connector-manifests".to_owned()),
        ("POST", format!("/v1/connectors/{}/probe", uuid())),
        ("POST", format!("/v1/connectors/{}/disable", uuid())),
        ("GET", format!("/v1/webhooks/{}/deliveries", uuid())),
        (
            "POST",
            format!("/v1/webhooks/{}/deliveries/{}/replay", uuid(), uuid()),
        ),
        ("GET", "/v1/stream".to_owned()),
        ("POST", "/v1/assistant/messages".to_owned()),
    ];
    for (method, path) in paths {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path.as_str())
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {path}"
        );
    }
}

#[test]
fn viewer_is_denied_tool_connector_and_webhook_mutations_by_the_canonical_matrix() {
    let tenant = TenantId::new();
    let organization = OrganizationId::new();
    let viewer = SecurityPrincipal::new(
        "viewer",
        PrincipalKind::User,
        Some(tenant),
        ScopeSet::denied(),
    )
    .with_memberships(vec![TenantMembership::new(
        organization,
        tenant,
        MembershipRole::Viewer,
    )]);
    for (resource, action) in [
        (ResourceKind::Tool, Action::Create),
        (ResourceKind::Tool, Action::Update),
        (ResourceKind::Agent, Action::Update),
        (ResourceKind::Connector, Action::Update),
        (ResourceKind::Webhook, Action::Update),
    ] {
        let request = AuthorizationRequest {
            resource,
            action,
            tenant_id: tenant,
            organization_id: organization,
            project_id: None,
        };
        assert!(
            matches!(
                AuthorizationService::new().authorize(&viewer, &request),
                mas_security::AuthorizationDecision::Deny(_)
            ),
            "viewer unexpectedly authorized for {resource}/{action}"
        );
    }
}

#[test]
fn private_and_metadata_destinations_are_rejected_before_a_connector_probe() {
    for url in [
        "http://127.0.0.1:8080/health",
        "https://10.20.30.40/internal",
        "http://169.254.169.254/latest/meta-data/",
        "http://[::1]/",
    ] {
        assert!(
            mas_domain::SafeUrl::parse(url).is_err(),
            "unsafe probe destination accepted: {url}"
        );
    }
}

#[tokio::test]
async fn omitted_tool_composition_fails_with_a_typed_service_unavailable() {
    let state = inmemory_state();
    let tenant = TenantId::new();
    let organization = OrganizationId::new();
    let principal = SecurityPrincipal::new(
        "test-user",
        PrincipalKind::User,
        Some(tenant),
        ScopeSet::denied(),
    );
    let ctx = RequestContext::with_claims(
        ScopeClaims {
            tenant: Some(tenant),
            organization: Some(organization),
            project: None,
        },
        "tooling-unavailable".to_owned(),
    )
    .with_resolved(
        principal,
        mas_security::AuthoritativeScope::resolved(tenant, organization, "test-user"),
    );
    let response = state
        .tool_service(&ctx)
        .expect_err("tool service is intentionally absent")
        .into_response();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[test]
fn tool_pages_and_tool_metadata_keep_the_stable_envelope_shape() {
    let page = ToolPageResponse {
        items: Vec::<ToolResponse>::new(),
        next_cursor: None,
    };
    let serialized = serde_json::to_value(page).expect("page JSON");
    assert_eq!(serialized, json!({"items": [], "next_cursor": null}));
    assert!(
        !serialized.to_string().contains("endpoint"),
        "runtime endpoints must not leak through tool metadata pages"
    );
}
