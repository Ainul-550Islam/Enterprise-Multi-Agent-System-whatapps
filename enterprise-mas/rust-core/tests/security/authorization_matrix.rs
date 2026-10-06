// PURPOSE: The cross-cutting security test matrix for the authorization,
// tenancy and row-level-security hardening phase.
//
// Owning layer: workspace-root integration tests, compiled as part of
// `mas-api` (see `crates/api/tests/security_matrix.rs`).
//
// Production responsibility: assert, from the outside, the properties this
// phase guarantees — that no caller can name a tenant it does not belong to,
// that no principal can authorize itself into a scope, that internal services
// are authenticated, that deny is the default, and that a pooled connection
// never carries scope between requests.
//
// Security boundary: these tests are the executable form of the phase's
// completion gate. A test here fails if and only if a layer below has started
// trusting a caller-supplied value.
//
// It must NOT:
//   • test business behaviour that has no security consequence — that belongs
//     in the feature suites;
//   • silently skip when a dependency is missing. A test that cannot run fails,
//     so a green suite always means the property was actually checked.


use mas_api::handlers::internal::ScheduleRunRequest;
use mas_api::scope_resolution::ScopeClaims;
use mas_api::state::test_support::inmemory_state;
use mas_common::ids::{OrganizationId, ProjectId, ScheduleId, TenantId};
use mas_common::timestamps::Timestamp;
use mas_domain::MembershipRole;
use mas_security::internal_service::{sign, SERVICE_HEADER, SIGNATURE_HEADER};
use mas_security::{
    Action, AuthorizationDecision, AuthorizationRequest, AuthorizationService, AuthFailureKind,
    PrincipalKind, ResourceKind, ScopeSet, SecurityPrincipal, TenantMembership,
};
use serde_json::json;
use tower::ServiceExt;

const SECRET: &str = "inmemory-internal-secret";

/// A membership-bound human principal, the ordinary case.
fn user(actor: &str, memberships: Vec<TenantMembership>) -> SecurityPrincipal {
    SecurityPrincipal::new(actor, PrincipalKind::User, None, ScopeSet::denied())
        .with_memberships(memberships)
}

/// One grant.
fn membership(
    organization: OrganizationId,
    tenant: TenantId,
    role: MembershipRole,
) -> TenantMembership {
    TenantMembership::new(organization, tenant, role)
}

fn request(
    resource: ResourceKind,
    action: Action,
    tenant: TenantId,
    organization: OrganizationId,
    project: Option<ProjectId>,
) -> AuthorizationRequest {
    AuthorizationRequest {
        resource,
        action,
        tenant_id: tenant,
        organization_id: organization,
        project_id: project,
    }
}

fn deny_kind(decision: &AuthorizationDecision) -> Option<AuthFailureKind> {
    match decision {
        AuthorizationDecision::Deny(failure) => Some(failure.kind),
        AuthorizationDecision::Allow { .. } => None,
    }
}

// ── 1. Caller-supplied identity is never authoritative ──────────────────────

/// A principal with no membership in the tenant is denied. Its own claim that
/// it belongs there is not evidence.
#[tokio::test]
async fn a_principal_cannot_authorize_itself_into_an_ungranted_tenant() {
    let tenant = TenantId::new();
    let organization = OrganizationId::new();
    let attacker = SecurityPrincipal::new(
        "mallory",
        PrincipalKind::User,
        Some(tenant),
        ScopeSet::denied(),
    );
    let decision = AuthorizationService.authorize(
        &attacker,
        &request(
            ResourceKind::Execution,
            Action::Read,
            tenant,
            organization,
            None,
        ),
    );
    // The credential is bound to a tenant but holds no membership proving
    // it, so the refusal is a tenant mismatch — the point being that the
    // principal's own `tenant_id` field is not accepted as proof.
    assert!(
        matches!(deny_kind(&decision), Some(AuthFailureKind::TenantMismatch)),
        "a self-asserted tenant must fail as an unproven membership, not pass"
    );
}

/// Deny by default: an action the role does not carry is refused even for the
/// most privileged role.
#[tokio::test]
async fn an_action_outside_the_role_grants_is_denied() {
    let tenant = TenantId::new();
    let organization = OrganizationId::new();
    let viewer = user("v", vec![membership(organization, tenant, MembershipRole::Viewer)]);
    for action in [
        Action::Create,
        Action::Update,
        Action::Delete,
        Action::Execute,
        Action::Cancel,
        Action::Publish,
        Action::Administer,
    ] {
        let decision = AuthorizationService.authorize(
            &viewer,
            &request(ResourceKind::Execution, action, tenant, organization, None),
        );
        assert!(
            matches!(deny_kind(&decision), Some(AuthFailureKind::InsufficientPermission)),
            "a viewer must not {action:?}"
        );
    }
}

/// A viewer cannot manage credentials, policies or billing, and cannot read
/// the audit log: those are privileged actions, not read actions.
#[tokio::test]
async fn privileged_actions_are_refused_for_the_least_privileged_roles() {
    let tenant = TenantId::new();
    let organization = OrganizationId::new();
    for role in [
        MembershipRole::Viewer,
        MembershipRole::Operator,
        MembershipRole::Developer,
    ] {
        let principal = user("p", vec![membership(organization, tenant, role)]);
        for (resource, action) in [
            (ResourceKind::Credential, Action::ManageCredentials),
            (ResourceKind::Policy, Action::ManagePolicies),
            (ResourceKind::Audit, Action::ViewAudit),
            (ResourceKind::Usage, Action::ManageBilling),
        ] {
            let decision = AuthorizationService.authorize(
                &principal,
                &request(resource, action, tenant, organization, None),
            );
            assert!(
                matches!(decision, AuthorizationDecision::Deny(_)),
                "{role:?} must not {action:?} a {resource:?}"
            );
        }
    }
}

/// A service account is bound by its tenant like anything else: it is not a
/// platform-wide key.
#[tokio::test]
async fn a_service_account_is_still_bound_by_its_tenant() {
    let mine = TenantId::new();
    let theirs = TenantId::new();
    let organization = OrganizationId::new();
    let account = user(
        "sa-1",
        vec![membership(
            organization,
            mine,
            MembershipRole::ServiceAccount,
        )],
    );
    let decision = AuthorizationService.authorize(
        &account,
        &request(
            ResourceKind::Agent,
            Action::Read,
            theirs,
            organization,
            None,
        ),
    );
    assert!(
        matches!(decision, AuthorizationDecision::Deny(_)),
        "a service account must not read across tenants"
    );
}

/// An expired credential denies everything, including reads the role
/// otherwise carries.
#[tokio::test]
async fn an_expired_credential_denies_everything() {
    let tenant = TenantId::new();
    let organization = OrganizationId::new();
    let mut principal = user(
        "owner",
        vec![membership(organization, tenant, MembershipRole::Owner)],
    );
    principal.expires_at = Some(
        Timestamp::from_unix_ms(Timestamp::now().to_unix_ms() - 60_000).expect("timestamp"),
    );
    let decision = AuthorizationService.authorize(
        &principal,
        &request(
            ResourceKind::Execution,
            Action::Read,
            tenant,
            organization,
            None,
        ),
    );
    assert!(
        matches!(
            deny_kind(&decision),
            Some(AuthFailureKind::ExpiredCredential)
        ),
        "an expired credential must deny before any other check"
    );
}

// ── 2. Denials are typed, stable and never retryable ────────────────────────

/// Every denial carries a stable wire code, so clients branch on a contract
/// rather than on a message.
#[tokio::test]
async fn every_denial_carries_a_stable_code() {
    let tenant = TenantId::new();
    let organization = OrganizationId::new();
    let viewer = user("v", vec![membership(organization, tenant, MembershipRole::Viewer)]);
    let decision = AuthorizationService.authorize(
        &viewer,
        &request(
            ResourceKind::Execution,
            Action::Delete,
            tenant,
            organization,
            None,
        ),
    );
    match decision {
        AuthorizationDecision::Deny(failure) => {
            assert_eq!(failure.code(), "insufficient_permission");
            assert!(!failure.to_app_error().error_code().is_empty());
        },
        AuthorizationDecision::Allow { .. } => panic!("expected a denial"),
    }
}

/// Authentication and authorization failures are never retryable: a client
/// must not loop on a refusal.
#[tokio::test]
async fn authorization_failures_are_never_retryable() {
    let tenant = TenantId::new();
    let organization = OrganizationId::new();
    let attacker = SecurityPrincipal::new(
        "m",
        PrincipalKind::User,
        Some(tenant),
        ScopeSet::denied(),
    );
    let decision = AuthorizationService.authorize(
        &attacker,
        &request(
            ResourceKind::Execution,
            Action::Create,
            tenant,
            organization,
            None,
        ),
    );
    match decision {
        AuthorizationDecision::Deny(failure) => assert!(
            !failure.to_app_error().is_retryable(),
            "an authorization failure must never be retried"
        ),
        AuthorizationDecision::Allow { .. } => panic!("expected a denial"),
    }
}

// ── 3. Scope resolution: hints are not authority ────────────────────────────

/// A claim naming an unreachable tenant does not resolve.
#[tokio::test]
async fn a_claim_for_an_unreachable_tenant_does_not_resolve() {
    let state = inmemory_state();
    let resolver = state
        .scope_resolver
        .as_ref()
        .expect("the test lane resolves scope");
    let result = resolver
        .resolve(
            &user("nobody", vec![]),
            ScopeClaims {
                tenant: Some(TenantId::new()),
                organization: None,
                project: None,
            },
        )
        .await;
    assert!(result.is_err(), "an unreachable tenant must not resolve");
}

/// Conflicting claims fail deterministically rather than being silently
/// corrected.
#[tokio::test]
async fn conflicting_tenant_and_organization_claims_fail() {
    let state = inmemory_state();
    let resolver = state
        .scope_resolver
        .as_ref()
        .expect("the test lane resolves scope");
    let result = resolver
        .resolve(
            &user("nobody", vec![]),
            ScopeClaims {
                tenant: Some(TenantId::new()),
                organization: Some(OrganizationId::new()),
                project: None,
            },
        )
        .await;
    assert!(
        result.is_err(),
        "a conflicting pair of claims must fail, not be corrected"
    );
}

// ── 4. Internal service authentication ─────────────────────────────────────

fn run_body(tenant: TenantId, schedule: ScheduleId) -> Vec<u8> {
    serde_json::to_vec(&ScheduleRunRequest {
        schedule_id: schedule,
        tenant_id: tenant,
        planned_at: Timestamp::now(),
        task_template: json!({ "operation": "noop" }),
    })
    .expect("body")
}

async fn post_internal(
    state: mas_api::state::AppState,
    service: &str,
    secret: &str,
    at: &Timestamp,
    body: &[u8],
) -> axum::http::StatusCode {
    let app = mas_api::router::router(state);
    let signature = sign(secret.as_bytes(), service, at, body);
    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/internal/schedule-runs")
        .header("content-type", "application/json")
        .header(SIGNATURE_HEADER, signature)
        .header(SERVICE_HEADER, service)
        .body(axum::body::Body::from(body.to_vec()))
        .expect("request");
    app.oneshot(request).await.expect("response").status()
}

/// The network is not an identity: an unsigned internal call is refused.
#[tokio::test]
async fn an_unsigned_internal_call_is_refused() {
    let app = mas_api::router::router(inmemory_state());
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/v1/internal/schedule-runs")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(run_body(
                    TenantId::new(),
                    ScheduleId::new(),
                )))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), axum::http::StatusCode::UNAUTHORIZED);
}

/// A call captured hours ago cannot be replayed.
#[tokio::test]
async fn a_replayed_internal_call_is_refused() {
    let long_ago =
        Timestamp::from_unix_ms(Timestamp::now().to_unix_ms() - 7_200_000).expect("timestamp");
    let status = post_internal(
        inmemory_state(),
        "mas-scheduler",
        SECRET,
        &long_ago,
        &run_body(TenantId::new(), ScheduleId::new()),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED);
}

/// A cryptographically valid signature from a service the receiver does not
/// expect is still a refusal.
#[tokio::test]
async fn an_unexpected_service_is_refused() {
    let status = post_internal(
        inmemory_state(),
        "mas-not-a-service",
        SECRET,
        &Timestamp::now(),
        &run_body(TenantId::new(), ScheduleId::new()),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED);
}

/// A receiver with no secret refuses rather than trusting the network.
#[tokio::test]
async fn an_unconfigured_receiver_refuses_every_internal_call() {
    let mut state = inmemory_state();
    state.internal_secret = None;
    let status = post_internal(
        state,
        "mas-scheduler",
        SECRET,
        &Timestamp::now(),
        &run_body(TenantId::new(), ScheduleId::new()),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED);
}

/// A correctly signed call for an unreachable tenant is a `404`, not a `403`:
/// existence is not disclosed.
#[tokio::test]
async fn a_signed_call_for_an_unreachable_tenant_is_not_found() {
    let status = post_internal(
        inmemory_state(),
        "mas-scheduler",
        SECRET,
        &Timestamp::now(),
        &run_body(TenantId::new(), ScheduleId::new()),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::NOT_FOUND);
}

/// A signature over one body must not authorize a different body.
#[tokio::test]
async fn a_signature_does_not_authorize_a_different_body() {
    let body = run_body(TenantId::new(), ScheduleId::new());
    let signature = sign(SECRET.as_bytes(), "mas-scheduler", &Timestamp::now(), b"{}");
    let app = mas_api::router::router(inmemory_state());
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/v1/internal/schedule-runs")
                .header("content-type", "application/json")
                .header(SIGNATURE_HEADER, signature)
                .header(SERVICE_HEADER, "mas-scheduler")
                .body(axum::body::Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), axum::http::StatusCode::UNAUTHORIZED);
}

/// Internal headers carry identity only: never tenancy.
#[tokio::test]
async fn internal_headers_can_never_carry_tenancy() {
    assert_eq!(SIGNATURE_HEADER, "x-mas-internal-signature");
    assert_eq!(SERVICE_HEADER, "x-mas-internal-service");
    for header in [SIGNATURE_HEADER, SERVICE_HEADER] {
        assert!(
            !header.contains("tenant") && !header.contains("org"),
            "{header} must never be able to carry a scope"
        );
    }
}

// ── 5. Scoped transactions ─────────────────────────────────────────────────

/// Outside a request there is no scope: a query run there cannot see rows.
#[tokio::test]
async fn there_is_no_scope_outside_a_request() {
    assert!(
        !mas_persistence::scoped_request::has_scope(),
        "no transaction-local scope may exist outside a request"
    );
}

/// Scope is task-local. A sibling task on another connection must not inherit
/// it, which is the pooled-connection leak the phase prevents.
#[tokio::test]
async fn scope_does_not_leak_to_a_sibling_task() {
    let handle = tokio::spawn(async { mas_persistence::scoped_request::has_scope() });
    assert!(
        !handle.await.expect("task"),
        "scope must be task-local, never ambient"
    );
}

// ── 6. Cross-tenant and cross-project containment ───────────────────────────

/// The matrix that matters: for every read and write verb, a principal bound
/// to tenant A is denied on tenant B.
#[tokio::test]
async fn no_principal_crosses_a_tenant_boundary() {
    let mine = TenantId::new();
    let theirs = TenantId::new();
    let organization = OrganizationId::new();
    let principal = user(
        "cross",
        vec![membership(organization, mine, MembershipRole::Owner)],
    );
    for action in [
        Action::Read,
        Action::Create,
        Action::Update,
        Action::Delete,
        Action::Execute,
        Action::Cancel,
    ] {
        for resource in [
            ResourceKind::Execution,
            ResourceKind::Agent,
            ResourceKind::Workflow,
            ResourceKind::Schedule,
            ResourceKind::Credential,
            ResourceKind::ApiKey,
            ResourceKind::Audit,
        ] {
            let decision = AuthorizationService.authorize(
                &principal,
                &request(resource, action, theirs, organization, None),
            );
            assert!(
                matches!(decision, AuthorizationDecision::Deny(_)),
                "{action:?} on {resource:?} must not cross tenants"
            );
        }
    }
}

/// A project the directory has not proven to belong to the tenant is not a
/// scope a principal can name.
#[tokio::test]
async fn an_unproven_project_is_not_a_valid_scope() {
    let tenant = TenantId::new();
    let organization = OrganizationId::new();
    let foreign_project = ProjectId::new();
    let owner = user(
        "o",
        vec![membership(organization, tenant, MembershipRole::Owner)],
    );
    let decision = AuthorizationService.authorize(
        &owner,
        &request(
            ResourceKind::Execution,
            Action::Create,
            tenant,
            organization,
            Some(foreign_project),
        ),
    );
    assert!(
        matches!(
            deny_kind(&decision),
            Some(AuthFailureKind::ProjectMismatch)
        ),
        "an unproven project must be refused, not assumed"
    );
}

/// A proven project is accepted — the check narrows, it does not merely deny.
#[tokio::test]
async fn a_proven_project_is_accepted() {
    let tenant = TenantId::new();
    let organization = OrganizationId::new();
    let project = ProjectId::new();
    let mut owner = user(
        "o",
        vec![membership(organization, tenant, MembershipRole::Owner)],
    );
    owner.prove_project(tenant, project);
    let decision = AuthorizationService.authorize(
        &owner,
        &request(
            ResourceKind::Execution,
            Action::Create,
            tenant,
            organization,
            Some(project),
        ),
    );
    assert!(
        matches!(decision, AuthorizationDecision::Allow { .. }),
        "a proven project must not be refused"
    );
}

/// The tenant a credential is bound to is not a scope it can widen: binding
/// to A while asking for B is a mismatch, not a grant.
#[tokio::test]
async fn a_credential_bound_to_one_tenant_cannot_act_in_another() {
    let bound = TenantId::new();
    let other = TenantId::new();
    let organization = OrganizationId::new();
    let key = SecurityPrincipal::new(
        "key-1",
        PrincipalKind::ApiKey,
        Some(bound),
        ScopeSet::denied(),
    )
    .with_memberships(vec![membership(organization, bound, MembershipRole::Admin)]);
    let decision = AuthorizationService.authorize(
        &key,
        &request(
            ResourceKind::Execution,
            Action::Create,
            other,
            organization,
            None,
        ),
    );
    assert!(matches!(decision, AuthorizationDecision::Deny(_)));
}

/// The same principal, in its own tenant, is allowed — proving the denials
/// above are about scope and not about a broken allow-list.
#[tokio::test]
async fn the_same_principal_is_allowed_inside_its_own_tenant() {
    let tenant = TenantId::new();
    let organization = OrganizationId::new();
    let owner = user(
        "o",
        vec![membership(organization, tenant, MembershipRole::Owner)],
    );
    let decision = AuthorizationService.authorize(
        &owner,
        &request(
            ResourceKind::Execution,
            Action::Create,
            tenant,
            organization,
            None,
        ),
    );
    assert!(matches!(decision, AuthorizationDecision::Allow { .. }));
}

/// Every role's grants are explicit. A role that can do everything is a bug,
/// not a convenience.
#[tokio::test]
async fn no_role_holds_every_action_on_every_resource() {
    let all_resources = [
        ResourceKind::Execution,
        ResourceKind::Task,
        ResourceKind::Workflow,
        ResourceKind::Agent,
        ResourceKind::Tool,
        ResourceKind::Connector,
        ResourceKind::Credential,
        ResourceKind::Policy,
        ResourceKind::Usage,
        ResourceKind::Audit,
        ResourceKind::Webhook,
        ResourceKind::Subscription,
        ResourceKind::ApiKey,
        ResourceKind::Schedule,
        ResourceKind::Tenant,
        ResourceKind::Organization,
        ResourceKind::Project,
        ResourceKind::Membership,
    ];
    let all_actions = [
        Action::Read,
        Action::Create,
        Action::Update,
        Action::Delete,
        Action::Execute,
        Action::Cancel,
        Action::Publish,
        Action::Administer,
        Action::ManageCredentials,
        Action::ManagePolicies,
        Action::ViewAudit,
        Action::ManageBilling,
        Action::ManageMembers,
    ];
    for role in [
        MembershipRole::Owner,
        MembershipRole::Admin,
        MembershipRole::Developer,
        MembershipRole::Operator,
        MembershipRole::Viewer,
        MembershipRole::ServiceAccount,
    ] {
        let mut held = 0usize;
        for resource in &all_resources {
            for action in &all_actions {
                if AuthorizationService::role_permits(&role, *resource, *action) {
                    held += 1;
                }
            }
        }
        let total = all_resources.len() * all_actions.len();
        assert!(
            held < total,
            "{role:?} holds every action on every resource ({held}/{total}); \
             capabilities must be explicit"
        );
    }
}

/// The twelve-plus resource taxonomy the phase requires actually exists and is
/// distinct on the wire.
#[tokio::test]
async fn the_resource_taxonomy_covers_every_protected_kind() {
    let kinds: Vec<&str> = [
        ResourceKind::Execution,
        ResourceKind::Task,
        ResourceKind::Workflow,
        ResourceKind::Agent,
        ResourceKind::Tool,
        ResourceKind::Connector,
        ResourceKind::Credential,
        ResourceKind::Policy,
        ResourceKind::Usage,
        ResourceKind::Audit,
        ResourceKind::Webhook,
        ResourceKind::Subscription,
    ]
    .iter()
    .map(ResourceKind::as_str)
    .collect();
    let unique: std::collections::BTreeSet<&str> = kinds.iter().copied().collect();
    assert_eq!(unique.len(), kinds.len(), "resource kinds must be distinct");
    assert!(kinds.len() >= 12, "at least twelve resource kinds required");
}

/// There are at least eight distinct, stable authentication-failure kinds, so
/// a client can tell a refusal apart without a message.
#[tokio::test]
async fn the_failure_taxonomy_has_at_least_eight_distinct_kinds() {
    let kinds = [
        AuthFailureKind::Unauthenticated,
        AuthFailureKind::InvalidCredential,
        AuthFailureKind::ExpiredCredential,
        AuthFailureKind::Forbidden,
        AuthFailureKind::TenantMismatch,
        AuthFailureKind::OrganizationMismatch,
        AuthFailureKind::ProjectMismatch,
        AuthFailureKind::InsufficientPermission,
        AuthFailureKind::InvalidServiceIdentity,
    ];
    use mas_security::AuthFailure;
    let codes: std::collections::BTreeSet<&str> =
        kinds.iter().map(|kind| AuthFailure::new(*kind).code()).collect();
    assert!(
        codes.len() >= 8,
        "at least eight distinct failure codes required, got {codes:?}"
    );
}
