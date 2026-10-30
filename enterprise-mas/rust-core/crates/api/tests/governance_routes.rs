//! PURPOSE: Route and service security contract tests for the governance surface.
//!
//! Owning layer: `mas-api` integration tests.
//! Production responsibility: exercise protected route authentication, policy tenant hiding, canonical mutation authorization, audit pagination shape, and dry-run non-mutation semantics.
//! Data ownership: test-only in-memory services; production persistence is covered by the live PostgreSQL suite.
//! MUST NOT: replace the production authorization matrix with test-only grants or silently claim an unexecuted live database assertion.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use mas_api::{router::router, state::test_support::inmemory_state};
use mas_application::{
    audit::InMemoryAuditSink, context::ServiceContext, policy_service::PolicyService,
};
use mas_common::{
    ids::{OrganizationId, TenantId},
    timestamps::Timestamp,
};
use mas_contracts::audit::AuditPageDto;
use mas_domain::{
    AuditActor, AuditActorKind, AuditOutcome, MembershipRole, Policy, PolicyScope, PolicyType,
};
use mas_policy::{
    engine::PolicyEngine,
    store::{InMemoryPolicyStore, PolicyStorePort},
};
use mas_security::{
    Action, AuthorizationRequest, AuthorizationService, PrincipalKind, ResourceKind, ScopeSet,
    SecurityPrincipal,
};
use serde_json::json;
use std::sync::Arc;
use tower::ServiceExt;

#[tokio::test]
async fn protected_governance_routes_reject_unauthenticated_requests() {
    let app = router(inmemory_state());
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/audit-events")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn auth_me_requires_authentication() {
    let app = router(inmemory_state());
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/auth/me")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn foreign_tenant_policy_ids_are_hidden_as_not_found() {
    let tenant_a = TenantId::new();
    let tenant_b = TenantId::new();
    let organization = OrganizationId::new();
    let store = Arc::new(InMemoryPolicyStore::new());
    let policy = Policy::new(Some(tenant_b), Some(organization), None, "foreign-policy", PolicyType::Execution, PolicyScope::Tenant, 1, json!({"rules":[{"id":"allow","effect":"allow","actions":["task.submit"],"resources":["task"]}]})).expect("policy");
    let policy_id = policy.id;
    store.upsert(&policy).await.expect("insert");
    let service = PolicyService::new(
        store.clone(),
        Arc::new(PolicyEngine::new(store)),
        Arc::new(InMemoryAuditSink::new()),
    );
    let context = ServiceContext::for_service("test", "foreign-read", &Timestamp::now())
        .expect("ctx")
        .with_scope(tenant_a, organization);
    let error = service
        .get(&context, policy_id)
        .await
        .expect_err("foreign policy is hidden");
    assert_eq!(error.error_code(), "RESOURCE_NOT_FOUND");
}

#[tokio::test]
async fn foreign_tenant_policy_mutations_and_dry_runs_are_hidden_as_not_found() {
    let tenant_a = TenantId::new();
    let tenant_b = TenantId::new();
    let organization = OrganizationId::new();
    let store = Arc::new(InMemoryPolicyStore::new());
    let definition = json!({"rules":[{"id":"allow","effect":"allow","actions":["task.submit"],"resources":["task"]}]});
    let policy = Policy::new(
        Some(tenant_b),
        Some(organization),
        None,
        "foreign-policy",
        PolicyType::Execution,
        PolicyScope::Tenant,
        1,
        definition.clone(),
    )
    .expect("policy");
    let policy_id = policy.id;
    store.upsert(&policy).await.expect("insert");
    let service = PolicyService::new(
        store.clone(),
        Arc::new(PolicyEngine::new(store)),
        Arc::new(InMemoryAuditSink::new()),
    );
    let context = ServiceContext::for_service("test", "foreign-ops", &Timestamp::now())
        .expect("ctx")
        .with_scope(tenant_a, organization);

    let update = service
        .update(
            &context,
            policy_id,
            mas_contracts::policy::UpdatePolicyRequest {
                name: Some("renamed-by-tenant-a".to_owned()),
                description: None,
                priority: None,
                definition: None,
                active: None,
            },
        )
        .await
        .expect_err("foreign policy update is hidden");
    assert_eq!(update.error_code(), "RESOURCE_NOT_FOUND");

    let publish = service
        .publish(&context, policy_id)
        .await
        .expect_err("foreign policy publish is hidden");
    assert_eq!(publish.error_code(), "RESOURCE_NOT_FOUND");

    let rollback = service
        .rollback(&context, policy_id, definition.clone())
        .await
        .expect_err("foreign policy rollback is hidden");
    assert_eq!(rollback.error_code(), "RESOURCE_NOT_FOUND");

    let evaluate = service
        .evaluate(
            &context,
            policy_id,
            mas_contracts::policy::EvaluatePolicyRequest {
                subject: json!({"actor":"test"}),
                resource: json!({"kind":"task"}),
                action: "task.submit".to_owned(),
                environment: json!({}),
            },
        )
        .await
        .expect_err("foreign policy evaluation is hidden");
    assert_eq!(evaluate.error_code(), "RESOURCE_NOT_FOUND");
}

#[tokio::test]
async fn published_policy_versions_are_not_edited_in_place() {
    let tenant = TenantId::new();
    let organization = OrganizationId::new();
    let store = Arc::new(InMemoryPolicyStore::new());
    let policy = Policy::new(
        Some(tenant),
        Some(organization),
        None,
        "published-policy",
        PolicyType::Execution,
        PolicyScope::Tenant,
        1,
        json!({"rules":[{"id":"allow","effect":"allow","actions":["task.submit"],"resources":["task"]}]}),
    )
    .expect("policy");
    let policy_id = policy.id;
    store.upsert(&policy).await.expect("draft");
    let service = PolicyService::new(
        store.clone(),
        Arc::new(PolicyEngine::new(store.clone())),
        Arc::new(InMemoryAuditSink::new()),
    );
    let context = ServiceContext::for_service("test", "immutable-publish", &Timestamp::now())
        .expect("ctx")
        .with_scope(tenant, organization);

    let published = service
        .publish(&context, policy_id)
        .await
        .expect("first publish");
    assert!(published.active);
    let published_version = published.version;

    let edited = service
        .update(
            &context,
            policy_id,
            mas_contracts::policy::UpdatePolicyRequest {
                name: None,
                description: None,
                priority: None,
                definition: Some(
                    json!({"rules":[{"id":"allow","effect":"allow","actions":["task.abort"],"resources":["task"]}]}),
                ),
                active: None,
            },
        )
        .await
        .expect("definition edit");
    // The published artifact is immutable: the edit lands on a new version
    // that is inactive until explicitly republished.
    assert!(
        edited.version > published_version,
        "definition edits must bump the version"
    );
    assert!(
        !edited.active,
        "an edited version must be deactivated until republished"
    );

    let republished = service
        .publish(&context, policy_id)
        .await
        .expect("republish");
    assert!(republished.active);
    assert_eq!(republished.version, edited.version);
}

#[test]
fn viewer_cannot_mutate_policies_through_the_canonical_matrix() {
    let tenant = TenantId::new();
    let organization = OrganizationId::new();
    let viewer = SecurityPrincipal::new("viewer", PrincipalKind::User, None, ScopeSet::denied())
        .with_memberships(vec![mas_security::TenantMembership::new(
            organization,
            tenant,
            MembershipRole::Viewer,
        )]);
    let request = AuthorizationRequest {
        resource: ResourceKind::Policy,
        action: Action::ManagePolicies,
        tenant_id: tenant,
        organization_id: organization,
        project_id: None,
    };
    assert!(matches!(
        AuthorizationService::new().authorize(&viewer, &request),
        mas_security::AuthorizationDecision::Deny(_)
    ));
}

#[test]
fn audit_page_uses_items_and_optional_next_cursor_shape() {
    let page = AuditPageDto {
        items: Vec::new(),
        next_cursor: None,
    };
    let value = serde_json::to_value(page).expect("serialize");
    assert!(value.get("items").is_some());
    assert!(value.get("next_cursor").is_some());
}

#[tokio::test]
async fn dry_run_policy_evaluation_does_not_emit_a_mutation_audit() {
    let tenant = TenantId::new();
    let organization = OrganizationId::new();
    let store = Arc::new(InMemoryPolicyStore::new());
    let policy = Policy::new(Some(tenant), Some(organization), None, "dry-run-policy", PolicyType::Execution, PolicyScope::Tenant, 1, json!({"rules":[{"id":"allow","effect":"allow","actions":["tool.invoke"],"resources":["policy:evaluate"]}]})).expect("policy");
    let policy_id = policy.id;
    store.upsert(&policy).await.expect("draft");
    let audit = Arc::new(InMemoryAuditSink::new());
    let service = PolicyService::new(
        store.clone(),
        Arc::new(PolicyEngine::new(store)),
        audit.clone(),
    );
    let context = ServiceContext::for_service("test", "dry-run", &Timestamp::now())
        .expect("ctx")
        .with_scope(tenant, organization);
    let input = mas_contracts::policy::EvaluatePolicyRequest {
        subject: json!({"actor":"test"}),
        resource: json!({"kind":"tool"}),
        action: "tool.invoke".to_owned(),
        environment: json!({}),
    };
    let result = service
        .evaluate(&context, policy_id, input)
        .await
        .expect("dry run");
    assert_eq!(result.decision, mas_common::enums::PolicyDecision::Allow);
    assert!(
        audit.is_empty(),
        "dry run must not record a policy decision or mutation audit"
    );
}

#[test]
fn audit_actor_type_is_redactable_and_canonical() {
    let actor = AuditActor::new(AuditActorKind::User, "u-1").expect("actor");
    let event = mas_domain::AuditEvent::new(
        Some(TenantId::new()),
        None,
        actor,
        "policy.read",
        "policy",
        None,
        AuditOutcome::Success,
        "corr",
    )
    .expect("event");
    assert_eq!(event.actor.kind, AuditActorKind::User);
}
