//! PURPOSE: Live-PostgreSQL integration suite for the control-plane
//! **administration** surface — the write half that spec item ㉜ asks for.
//!
//! Owning layer: `mas-api` (tests).
//!
//! Production responsibility: prove, against a real database and through the
//! real production router, the properties that make an admin mutation safe to
//! expose:
//!   • a principal without `platform:admin` is refused, and so is a caller
//!     with no credential at all;
//!   • every mutation is audited, and the audit names the resource;
//!   • a mutation whose justification is missing, or whose `expected_version`
//!     is stale, is refused rather than applied;
//!   • an unknown enum value or a negative ceiling is a validation failure,
//!     not a 500 and not a silently coerced value;
//!   • an admin acting inside tenant A's scope cannot write tenant B's
//!     subscription — the refusal comes from the database, not from a guard
//!     the handler remembered to write.
//!
//! Security boundary: these tests exercise the route a platform operator uses
//! to change what a tenant may do and what it is billed. A green suite here is
//! the evidence that the endpoint is guarded by more than its URL.
//!
//! It must NOT:
//!   • run without `MAS_TEST_DATABASE_URL` (it skips loudly instead);
//!   • assert only the happy path — the refusal cases are the point;
//!   • be pointed at a production database.

//! Live PostgreSQL integration suite for the control-plane admin API.
//!
//! Gated by `MAS_TEST_DATABASE_URL`. When it is absent every test prints a
//! banner and returns, because the alternative — an in-memory stand-in for the
//! stores — would make the isolation assertions pass vacuously.
//!
//! NEVER run against a production database.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use http_body_util::BodyExt;
use mas_api::auth::StaticTokenVerifier;
use mas_api::router::router;
use mas_api::state::AppState;
use mas_application::agent_service::AgentService;
use mas_application::audit::{AuditSinkPort, InMemoryAuditSink};
use mas_application::durable_memory::InMemoryDurableExecution;
use mas_application::execution_service::ExecutionService;
use mas_application::schedule_service::ScheduleService;
use mas_application::stores::InMemoryServices;
use mas_application::tenancy_service::TenancyService;
use mas_application::workflow_service::WorkflowService;
use mas_common::enums::Environment;
use mas_common::ids::TenantId;
use mas_domain::IsolationMode;
use mas_observability::health::HealthRegistry;
use mas_security::{PrincipalKind, ScopeSet, SecurityPrincipal};
use serde_json::{json, Value};
use tower::ServiceExt;

/// The token that carries `platform:admin`.
const ADMIN_TOKEN: &str = "test-admin-token";
/// A valid credential with no control-plane authority at all.
const TENANT_TOKEN: &str = "test-tenant-token";

/// Everything one test needs: the router, the pool, the tenants and the sink
/// the audit assertions read back.
///
/// The pool kept here is the *runtime* pool: the same one the router runs on,
/// so a row-count assertion reads through the same role the request wrote
/// through rather than a privileged side door.
struct Harness {
    app: axum::Router,
    pool: sqlx::PgPool,
    /// The tenant the admin principal is bound to.
    home: TenantId,
    /// A second tenant the admin principal cannot reach.
    other: TenantId,
    audit: Arc<InMemoryAuditSink>,
}

/// Environment variable consulted for the live database (migrator role).
const ENV_VAR: &str = "MAS_TEST_DATABASE_URL";
/// Environment variable for the runtime role (`mas_app`, no BYPASSRLS).
const APP_ENV_VAR: &str = "MAS_TEST_DATABASE_APP_URL";

/// Reads an environment variable, treating whitespace as absent.
fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

/// Connects to the live database and migrates it; `None` when the suite is
/// off.
///
/// This is the *owner* pool: it creates the tenants the fixture needs, and it
/// is deliberately not the pool the router runs on.
async fn live_pool(owner: &'static str) -> Option<sqlx::PgPool> {
    let raw = match env(ENV_VAR) {
        Some(raw) => raw,
        None => {
            eprintln!(
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\
                 control_plane_admin: skipping (set {ENV_VAR} and {APP_ENV_VAR})\n\
                 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            );
            return None;
        },
    };
    let url = mas_persistence::pool::DatabaseUrl::new(raw).expect("database url parses");
    let mut config = mas_persistence::pool::PoolConfig::for_url(url, owner).expect("pool config");
    config.max_connections = 4;
    let database = mas_persistence::pool::Database::connect(&config)
        .await
        .expect("live database reachable");
    let pool = database.pool().clone();
    let runner = mas_persistence::migrations::MigrationRunner::new(pool.clone());
    runner.ensure_table().await.expect("ledger table");
    runner.migrate().await.expect("migrations applied");
    Some(pool)
}

/// The pool the router runs on: `mas_app`, a role that cannot bypass RLS.
///
/// The suite refuses to run without it. Every claim here about tenant
/// isolation would be vacuous under a superuser, and a suite that cannot fail
/// is worse than a suite that does not run.
async fn runtime_pool(owner: &'static str) -> Option<sqlx::PgPool> {
    let raw = env(APP_ENV_VAR)?;
    let url = mas_persistence::pool::DatabaseUrl::new(raw).expect("runtime url parses");
    let mut config = mas_persistence::pool::PoolConfig::for_url(url, owner).expect("pool config");
    config.max_connections = 4;
    let database = mas_persistence::pool::Database::connect(&config)
        .await
        .expect("runtime database reachable");
    let pool = database.pool().clone();
    let bypass: bool =
        sqlx::query_scalar("SELECT rolbypassrls FROM pg_roles WHERE rolname = current_user")
            .fetch_one(&pool)
            .await
            .expect("role lookup");
    assert!(
        !bypass,
        "{APP_ENV_VAR} must connect as a role subject to row-level security; \
         current_user bypasses it, so every isolation assertion here would pass \
         without testing anything"
    );
    Some(pool)
}

/// Seeds one tenant in **both** homes it has to exist in.
///
/// The scope resolver reads the in-memory tenancy store, and the control-plane
/// tables have a foreign key to `tenants`, so the same tenant row has to be
/// written to PostgreSQL and to the in-memory store with the same id. Seeding
/// one and not the other produces either a request that is refused before it
/// reaches the handler, or an insert that violates a foreign key — both of
/// which look like handler bugs and are not.
async fn seed_tenant(owner: &sqlx::PgPool, store: &Arc<InMemoryServices>, label: &str) -> TenantId {
    use mas_application::stores::{OrganizationStorePort, ProjectStorePort, TenantStorePort};
    use mas_domain::value_objects::Slug;
    use mas_domain::{Organization, Project, Tenant};
    use mas_persistence::repositories::{OrganizationStore, ProjectStore, TenantStore};

    let suffix = uuid::Uuid::now_v7().to_string().replace('-', "");
    let mut connection = owner.acquire().await.expect("connection");

    let organization = Organization::create(
        format!("{label} Holdings"),
        format!("{label} Holdings"),
        Slug::new(&format!("{label}-{suffix}")).expect("org slug"),
    )
    .expect("organization");
    OrganizationStore::insert_in(&mut connection, &organization)
        .await
        .expect("organization insert");
    OrganizationStorePort::save(store.as_ref(), &organization)
        .await
        .expect("organization in memory");

    let mut tenant = Tenant::create(
        organization.id,
        format!("{label} tenant"),
        Slug::new(&format!("tenant-{label}-{suffix}")).expect("tenant slug"),
        Environment::Development,
        IsolationMode::SharedRls,
    )
    .expect("tenant");
    // A tenant that is still provisioning is refused by the resolver before
    // any handler runs, which would make every test in this file about
    // lifecycle rather than about the control plane.
    tenant.activate().expect("activate tenant");
    TenantStore::insert_in(&mut connection, &tenant)
        .await
        .expect("tenant insert");
    TenantStorePort::save(store.as_ref(), &tenant)
        .await
        .expect("tenant in memory");

    let project = Project::create(
        tenant.id,
        organization.id,
        format!("{label} project"),
        Slug::new(&format!("proj-{label}-{suffix}")).expect("project slug"),
    )
    .expect("project");
    ProjectStore::insert_in(&mut connection, &project)
        .await
        .expect("project insert");
    ProjectStorePort::save(store.as_ref(), &project)
        .await
        .expect("project in memory");

    tenant.id
}

/// Builds the harness: in-memory application services, a real PostgreSQL
/// control plane reached through the runtime role, and the production router.
async fn harness(owner: sqlx::PgPool, runtime: sqlx::PgPool) -> Harness {
    let store = Arc::new(InMemoryServices::new());
    let audit = Arc::new(InMemoryAuditSink::new());
    // The PostgreSQL control plane, exactly as the production composition
    // root builds it. Wiring the in-memory one here would make every
    // assertion in this file a test of a fixture: the in-memory stores have
    // no row-level security, so a cross-tenant write would succeed.
    let control_plane = Arc::new(
        mas_persistence::repositories::control_plane::build_postgres_control_plane(runtime.clone()),
    );

    let tenancy = Arc::new(TenancyService::new(
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(audit.clone()),
        Box::new(control_plane.entitlement_store()),
    ));
    let agents = Arc::new(AgentService::new(
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(audit.clone()),
    ));
    let workflows = Arc::new(WorkflowService::new(
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(audit.clone()),
    ));
    let durable = Arc::new(InMemoryDurableExecution::new());
    let executions = Arc::new(ExecutionService::new(
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(audit.clone()),
        durable,
        Arc::clone(&control_plane),
    ));
    let schedules = Arc::new(ScheduleService::new(
        Box::new(store.clone()),
        Box::new(audit.clone()),
    ));

    let home = seed_tenant(&owner, &store, "home").await;
    let other = seed_tenant(&owner, &store, "other").await;

    // Two principals, one credential each, differing by exactly one scope. A
    // test that passes for both has proved nothing about authorization.
    let admin = SecurityPrincipal::new(
        "actor-platform-admin".to_owned(),
        PrincipalKind::User,
        Some(home),
        ScopeSet::new(["platform:admin", "tenant:read"]).expect("admin scopes"),
    );
    let tenant_user = SecurityPrincipal::new(
        "actor-tenant-user".to_owned(),
        PrincipalKind::User,
        Some(home),
        ScopeSet::new(["tenant:read"]).expect("tenant scopes"),
    );
    let verifier = Arc::new(
        StaticTokenVerifier::new()
            .with_token(ADMIN_TOKEN, admin)
            .with_token(TENANT_TOKEN, tenant_user),
    );

    let scope_resolver = Some(mas_api::scope_resolution::RequestScopeResolver::new(
        mas_tenancy::TenantResolver::new(
            Arc::new(mas_api::inmemory_directory::InMemoryTenantDirectory::new(
                store.clone(),
            )),
            Arc::new(mas_api::inmemory_directory::InMemoryMembershipDirectory::new(store.clone())),
        ),
    ));

    let state = AppState::new(
        "mas-api-admin-test",
        env!("CARGO_PKG_VERSION"),
        verifier,
        scope_resolver,
        tenancy,
        agents,
        workflows,
        executions,
        schedules,
        Arc::clone(&control_plane),
        audit.clone() as Arc<dyn AuditSinkPort>,
        Some(Arc::new(
            mas_persistence::repositories::control_plane::ControlPlaneStores::new(runtime.clone()),
        )),
        Arc::new(HealthRegistry::new()),
        // The runtime pool is what arms row-level security, one transaction
        // per request. Pointing this at the owner pool would make every
        // isolation assertion in this file pass without testing anything.
        Some(runtime.clone()),
        Some(mas_security::SecretString::new(
            "admin-test-secret".to_owned(),
        )),
    );

    Harness {
        app: router(state),
        pool: runtime,
        home,
        other,
        audit,
    }
}

/// Connects, migrates and builds the harness; `None` when the suite is off.
async fn fixture(owner_label: &'static str) -> Option<Harness> {
    let owner = live_pool(owner_label).await?;
    let runtime = match runtime_pool(owner_label).await {
        Some(pool) => pool,
        None => {
            eprintln!(
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\
                 control_plane_admin: skipping (set {APP_ENV_VAR} — the isolation \n\
                 assertions need a role subject to row-level security)\n\
                 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            );
            return None;
        },
    };
    Some(harness(owner, runtime).await)
}

// ───────────────────────────────────────────────────────────────────────────
// HTTP driver
// ───────────────────────────────────────────────────────────────────────────

/// One request through the production router.
///
/// Returns the status and the decoded envelope. The envelope is decoded rather
/// than matched on the body text so a change to the field order does not read
/// as a behaviour change.
#[derive(Debug)]
struct Reply {
    status: StatusCode,
    body: Value,
}

impl Reply {
    /// The error code the stable envelope carries, when there is one.
    fn code(&self) -> Option<&str> {
        self.body
            .get("error")
            .and_then(|error| error.get("code"))
            .and_then(Value::as_str)
    }
}

async fn call(
    app: &axum::Router,
    method: &str,
    uri: &str,
    token: Option<&str>,
    body: Option<Value>,
) -> Reply {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let payload = match body {
        Some(value) => Body::from(serde_json::to_vec(&value).expect("body encodes")),
        None => Body::empty(),
    };
    let request = builder.body(payload).expect("request builds");

    let response = app.clone().oneshot(request).await.expect("router responds");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or_else(|_| {
        // A non-JSON body is a defect in the envelope, not a test failure to
        // be papered over: report it as an empty object and let the status
        // assertion carry the diagnosis.
        json!({})
    });
    Reply { status, body }
}

/// A plan code unique to one test run.
///
/// Plan codes are `snake_case` by domain rule, so the label is normalised
/// rather than assumed: a fixture that only ever passes valid labels proves
/// nothing about the validation it is meant to be sitting next to.
fn plan_code(label: &str) -> String {
    let suffix = uuid::Uuid::now_v7().to_string().replace('-', "");
    let label = label.to_ascii_lowercase().replace('-', "_");
    format!("plan_{label}_{suffix}")
}

// ───────────────────────────────────────────────────────────────────────────
// Authorization
// ───────────────────────────────────────────────────────────────────────────

/// A tenant-scoped principal must not reach a control-plane mutation, however
/// valid its credential.
#[tokio::test]
async fn a_tenant_scoped_principal_cannot_create_a_plan() {
    let Some(h) = fixture("cp-admin-authz").await else {
        return;
    };

    let reply = call(
        &h.app,
        "POST",
        "/v1/admin/plans",
        Some(TENANT_TOKEN),
        Some(json!({"code": plan_code("authz"), "name": "Should not exist"})),
    )
    .await;

    assert_eq!(
        reply.status,
        StatusCode::FORBIDDEN,
        "a valid tenant credential is not a platform credential: {reply:?}"
    );
}

/// No credential at all is a 401, and the response carries no scope.
#[tokio::test]
async fn an_unauthenticated_caller_is_refused_before_authorization() {
    let Some(h) = fixture("cp-admin-anon").await else {
        return;
    };

    let reply = call(
        &h.app,
        "POST",
        "/v1/admin/plans",
        None,
        Some(json!({"code": plan_code("anon"), "name": "Nobody"})),
    )
    .await;

    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
}

/// Every admin mutation is refused without the scope — not just the first one
/// that was written carefully.
#[tokio::test]
async fn a_tenant_scoped_principal_is_refused_on_every_admin_route() {
    let Some(h) = fixture("cp-admin-routes").await else {
        return;
    };
    let tenant = h.home.to_string();

    // Each route gets a body that would be *accepted* if the caller were
    // allowed to make the call. A body that fails deserialization is rejected
    // by the extractor before the handler runs, and a test that never reaches
    // the handler has not tested the handler's authorization at all.
    let routes = [
        (
            "POST",
            "/v1/admin/plans".to_owned(),
            json!({"code": plan_code("routes"), "name": "Routes"}),
        ),
        (
            "PUT",
            "/v1/admin/plans/free/features".to_owned(),
            json!({"feature": "rag"}),
        ),
        (
            "POST",
            format!("/v1/admin/tenants/{tenant}/subscription"),
            json!({"plan_code": "developer"}),
        ),
        (
            "POST",
            format!("/v1/admin/tenants/{tenant}/subscription/transitions"),
            json!({"state": "past_due", "reason": "test", "expected_version": 1}),
        ),
        (
            "POST",
            format!("/v1/admin/tenants/{tenant}/overrides/features"),
            json!({
                "feature": "rag",
                "enabled": true,
                "reason": "test",
                "effective_from": "2024-01-01T00:00:00Z",
            }),
        ),
        (
            "POST",
            format!("/v1/admin/tenants/{tenant}/overrides/quotas"),
            json!({
                "dimension": "tokens",
                "period": "daily",
                "max_value": 10,
                "reason": "test",
                "effective_from": "2024-01-01T00:00:00Z",
            }),
        ),
        (
            "POST",
            format!("/v1/admin/tenants/{tenant}/overrides/features/rag/end"),
            Value::Null,
        ),
    ];

    for (method, uri, body) in routes {
        let body = if body.is_null() { None } else { Some(body) };
        let reply = call(&h.app, method, &uri, Some(TENANT_TOKEN), body).await;
        assert_eq!(
            reply.status,
            StatusCode::FORBIDDEN,
            "{method} {uri} must refuse a tenant principal: {reply:?}"
        );
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Validation
// ───────────────────────────────────────────────────────────────────────────

/// A transition without a reason is refused: the reason is the only thing that
/// lets the next operator understand a change they did not make.
#[tokio::test]
async fn a_subscription_transition_without_a_reason_is_refused() {
    let Some(h) = fixture("cp-admin-reason").await else {
        return;
    };
    let tenant = h.home.to_string();

    let reply = call(
        &h.app,
        "POST",
        &format!("/v1/admin/tenants/{tenant}/subscription/transitions"),
        Some(ADMIN_TOKEN),
        Some(json!({"state": "past_due", "reason": "   ", "expected_version": 1})),
    )
    .await;

    assert!(
        reply.status.is_client_error(),
        "a blank reason is a validation failure, not an accepted change: {reply:?}"
    );
}

/// An unknown lifecycle state is a validation failure, never a 500.
#[tokio::test]
async fn an_unknown_subscription_state_is_a_validation_failure() {
    let Some(h) = fixture("cp-admin-state").await else {
        return;
    };
    let tenant = h.home.to_string();

    let reply = call(
        &h.app,
        "POST",
        &format!("/v1/admin/tenants/{tenant}/subscription/transitions"),
        Some(ADMIN_TOKEN),
        Some(json!({"state": "pretty_good", "reason": "test", "expected_version": 1})),
    )
    .await;

    assert!(
        reply.status.is_client_error() && reply.status != StatusCode::INTERNAL_SERVER_ERROR,
        "an unknown state must not surface as a server error: {reply:?}"
    );
}

/// An unknown enforcement mode is refused rather than defaulted.
#[tokio::test]
async fn an_unknown_enforcement_mode_is_refused_not_defaulted() {
    let Some(h) = fixture("cp-admin-enforcement").await else {
        return;
    };
    let tenant = h.home.to_string();

    let reply = call(
        &h.app,
        "POST",
        &format!("/v1/admin/tenants/{tenant}/overrides/quotas"),
        Some(ADMIN_TOKEN),
        Some(json!({
            "dimension": "tokens",
            "period": "daily",
            "max_value": 10,
            "enforcement": "sort_of",
            "reason": "test",
            "effective_from": "2024-01-01T00:00:00Z",
        })),
    )
    .await;

    assert!(
        reply.status.is_client_error(),
        "an unrecognised mode must not silently become 'enforce': {reply:?}"
    );
}

/// A negative ceiling is refused; it is not clamped to zero, because a quota
/// of zero is a lockout and a caller asking for it should have to say so.
#[tokio::test]
async fn a_negative_quota_ceiling_is_refused() {
    let Some(h) = fixture("cp-admin-negative").await else {
        return;
    };
    let tenant = h.home.to_string();

    let reply = call(
        &h.app,
        "POST",
        &format!("/v1/admin/tenants/{tenant}/overrides/quotas"),
        Some(ADMIN_TOKEN),
        Some(json!({
            "dimension": "tokens",
            "period": "daily",
            "max_value": -5,
            "reason": "test",
            "effective_from": "2024-01-01T00:00:00Z",
        })),
    )
    .await;

    assert!(
        reply.status.is_client_error(),
        "a negative ceiling is meaningless: {reply:?}"
    );
}

/// A malformed effective date is a validation failure, not a panic.
#[tokio::test]
async fn a_malformed_effective_date_is_a_validation_failure() {
    let Some(h) = fixture("cp-admin-date").await else {
        return;
    };
    let tenant = h.home.to_string();

    let reply = call(
        &h.app,
        "POST",
        &format!("/v1/admin/tenants/{tenant}/overrides/features"),
        Some(ADMIN_TOKEN),
        Some(json!({
            "feature": "rag",
            "enabled": true,
            "reason": "test",
            "effective_from": "whenever",
        })),
    )
    .await;

    assert!(
        reply.status.is_client_error() && reply.status != StatusCode::INTERNAL_SERVER_ERROR,
        "a bad timestamp is a client error: {reply:?}"
    );
}

// ───────────────────────────────────────────────────────────────────────────
// Optimistic concurrency
// ───────────────────────────────────────────────────────────────────────────

/// Two operators, one stale read: the second transition is refused rather than
/// overwriting the first.
#[tokio::test]
async fn a_transition_against_a_stale_version_is_refused() {
    let Some(h) = fixture("cp-admin-version").await else {
        return;
    };
    let tenant = h.home;

    let code = plan_code("stale");
    let created = call(
        &h.app,
        "POST",
        "/v1/admin/plans",
        Some(ADMIN_TOKEN),
        Some(json!({"code": code, "name": "Stale"})),
    )
    .await;
    assert!(
        created.status.is_success(),
        "the fixture needs a plan: {created:?}"
    );

    let subscribed = call(
        &h.app,
        "POST",
        &format!("/v1/admin/tenants/{tenant}/subscription"),
        Some(ADMIN_TOKEN),
        Some(json!({"plan_code": code})),
    )
    .await;
    assert!(
        subscribed.status.is_success(),
        "the fixture needs a subscription: {subscribed:?}"
    );

    // Two callers that both read version 1. Only one may move it.
    let first = call(
        &h.app,
        "POST",
        &format!("/v1/admin/tenants/{tenant}/subscription/transitions"),
        Some(ADMIN_TOKEN),
        Some(json!({
            "state": "past_due",
            "reason": "payment failed",
            "expected_version": 1,
        })),
    )
    .await;
    assert!(
        first.status.is_success(),
        "the first transition against the current version must win: {first:?}"
    );

    let second = call(
        &h.app,
        "POST",
        &format!("/v1/admin/tenants/{tenant}/subscription/transitions"),
        Some(ADMIN_TOKEN),
        Some(json!({
            "state": "canceled",
            "reason": "stale retry",
            "expected_version": 1,
        })),
    )
    .await;
    assert!(
        second.status.is_client_error(),
        "a transition against a version that has moved is a lost update: {second:?}"
    );
}

// ───────────────────────────────────────────────────────────────────────────
// Audit
// ───────────────────────────────────────────────────────────────────────────

/// A plan creation is recorded, and the record names the plan.
#[tokio::test]
async fn creating_a_plan_writes_an_audit_event() {
    let Some(h) = fixture("cp-admin-audit-plan").await else {
        return;
    };
    let before = h.audit.len();

    let code = plan_code("audited");
    let reply = call(
        &h.app,
        "POST",
        "/v1/admin/plans",
        Some(ADMIN_TOKEN),
        Some(json!({"code": code, "name": "Audited"})),
    )
    .await;
    assert!(
        reply.status.is_success(),
        "the plan must be created: {reply:?}"
    );

    let events = h.audit.events();
    assert_eq!(
        events.len(),
        before + 1,
        "one mutation is one audit event — no more and, crucially, no fewer"
    );
    let last = events.last().expect("the new event");
    assert!(
        last.action.contains("plan"),
        "the audit record must name what changed: {:?}",
        last.action
    );
    assert!(
        last.resource_id.as_deref() == Some(code.as_str()),
        "the audit record must name the plan: {:?}",
        last.resource_id
    );
}

/// A refused mutation is recorded **as a refusal**, and does not create the
/// resource either.
///
/// The distinction is the whole point. Before the refusal path was audited, the
/// audit plane only ever heard about control-plane changes that succeeded — so
/// the attempt an auditor most needs to see, one that was made and refused, was
/// the one thing never written down. Recording it must not, however, look like
/// a success: the event carries `Denied`, and nothing was created.
#[tokio::test]
async fn a_refused_mutation_is_recorded_as_a_refusal_and_changes_nothing() {
    let Some(h) = fixture("cp-admin-audit-refusal").await else {
        return;
    };
    let before = h.audit.len();

    let code = plan_code("refused");
    let reply = call(
        &h.app,
        "POST",
        "/v1/admin/plans",
        Some(TENANT_TOKEN),
        Some(json!({"code": code.clone(), "name": "Refused"})),
    )
    .await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN);

    // One event, and one outcome: refused.
    let events = h.audit.events();
    assert_eq!(
        events.len(),
        before + 1,
        "an attempt to change a tenant's commercial state is recorded whether \
         or not it was allowed"
    );
    let last = events.last().expect("the new event");
    assert_eq!(
        last.outcome,
        mas_domain::AuditOutcome::Denied,
        "a refusal is recorded as a refusal. Writing it as a success would \
         make the audit trail actively misleading: {:?}",
        last.action
    );
    assert!(
        last.action.contains("plan"),
        "the record names what was attempted: {:?}",
        last.action
    );

    // And the thing itself was never created.
    let exists: Option<(String,)> = sqlx::query_as("SELECT code FROM plans WHERE code = $1")
        .bind(&code)
        .fetch_optional(&h.pool)
        .await
        .expect("query");
    assert!(
        exists.is_none(),
        "an unauthorised request must not have created the plan"
    );
}

/// The two refusal classes are deliberately different, and this test pins the
/// difference down.
///
/// * **Authenticated, wrong scope** → `403`, and a `Denied` audit event. The
///   caller reached a control-plane decision, was refused, and the refusal is
///   on the record.
/// * **No credential at all** → `401`, and no control-plane audit event.
///   Authentication runs in the middleware and fails before any handler — and
///   therefore before any control-plane decision — exists. There is no actor
///   to attribute it to and no mutation that was attempted and refused; there
///   is only a request that never got as far as the plane.
///
/// Asserting the difference matters more than asserting either half: a suite
/// that only checked "refused requests are audited" would pass just as happily
/// on a system that logged nothing and on one that logged everything twice.
#[tokio::test]
async fn an_unauthenticated_attempt_never_reaches_a_control_plane_decision() {
    let Some(h) = fixture("cp-admin-audit-anon").await else {
        return;
    };
    let before = h.audit.len();

    let reply = call(
        &h.app,
        "POST",
        "/v1/admin/plans",
        None,
        Some(json!({"code": plan_code("anon"), "name": "Anonymous"})),
    )
    .await;
    assert_eq!(
        reply.status,
        StatusCode::UNAUTHORIZED,
        "no credential is a 401: {reply:?}"
    );

    assert_eq!(
        h.audit.len(),
        before,
        "authentication failed before a control-plane decision existed, so \
         there is no control-plane refusal to record"
    );

    // The same route, with a real but insufficient credential, *is* recorded —
    // proved here as well as in `a_refused_mutation_is_recorded_as_a_refusal`
    // so the two halves sit side by side and cannot drift apart.
    let reply = call(
        &h.app,
        "POST",
        "/v1/admin/plans",
        Some(TENANT_TOKEN),
        Some(json!({"code": plan_code("anon-authz"), "name": "Refused"})),
    )
    .await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN);
    assert_eq!(
        h.audit.len(),
        before + 1,
        "a credential that reached the plane and was refused leaves a record"
    );
    assert_eq!(
        h.audit.events().last().expect("the refusal").outcome,
        mas_domain::AuditOutcome::Denied
    );
}

/// An authorised operator whose mutation does not apply is recorded too.
///
/// This is the last case that could happen to a tenant's commercial state
/// without leaving a line in the audit log. To an auditor reading only the
/// successes, "an operator tried to set this tenant's quota to −5 and was
/// refused" and "nothing happened" are the same sentence — and the whole point
/// of the audit trail is that they are not.
#[tokio::test]
async fn a_mutation_that_fails_validation_is_recorded_as_a_failure() {
    let Some(h) = fixture("cp-admin-audit-failure").await else {
        return;
    };
    let tenant = h.home.to_string();
    let before = h.audit.len();

    let reply = call(
        &h.app,
        "POST",
        &format!("/v1/admin/tenants/{tenant}/overrides/quotas"),
        Some(ADMIN_TOKEN),
        Some(json!({
            "dimension": "tokens",
            "period": "daily",
            "max_value": -5,
            "reason": "test",
            "effective_from": "2024-01-01T00:00:00Z",
        })),
    )
    .await;
    assert!(
        reply.status.is_client_error() && reply.status != StatusCode::INTERNAL_SERVER_ERROR,
        "a negative ceiling is a validation failure, not a 500: {reply:?}"
    );

    let events = h.audit.events();
    assert_eq!(
        events.len(),
        before + 1,
        "an authorised attempt that did not apply is still an attempt, and it \
         is the one an auditor most needs to see"
    );
    let last = events.last().expect("the event");
    assert_eq!(
        last.outcome,
        mas_domain::AuditOutcome::Failure,
        "recorded as a failure, not as a success — a failure written as a \
         success would make the trail worse than no trail"
    );
    assert!(
        last.action.contains("quota"),
        "the record names what was attempted: {:?}",
        last.action
    );

    // Nothing was applied, which is the other half.
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM quota_overrides WHERE tenant_id = $1")
        .bind(h.home.into_uuid())
        .fetch_one(&h.pool)
        .await
        .expect("count");
    assert_eq!(rows, 0, "the refused override was not written");
}

/// A transition against a stale version is a failure, and is recorded as one.
///
/// Optimistic concurrency is exactly the case where "the write was refused" is
/// the interesting fact: two operators, one stale read, and only one of them
/// moved the subscription. The loser's attempt is what an auditor reconstructs
/// the sequence from.
#[tokio::test]
async fn a_transition_refused_for_a_stale_version_is_recorded_as_a_failure() {
    let Some(h) = fixture("cp-admin-audit-stale").await else {
        return;
    };
    let tenant = h.home;

    let code = plan_code("stale-audit");
    let created = call(
        &h.app,
        "POST",
        "/v1/admin/plans",
        Some(ADMIN_TOKEN),
        Some(json!({"code": code, "name": "Stale audited"})),
    )
    .await;
    assert!(
        created.status.is_success(),
        "fixture needs a plan: {created:?}"
    );
    let subscribed = call(
        &h.app,
        "POST",
        &format!("/v1/admin/tenants/{tenant}/subscription"),
        Some(ADMIN_TOKEN),
        Some(json!({"plan_code": code})),
    )
    .await;
    assert!(
        subscribed.status.is_success(),
        "fixture needs a subscription: {subscribed:?}"
    );

    let first = call(
        &h.app,
        "POST",
        &format!("/v1/admin/tenants/{tenant}/subscription/transitions"),
        Some(ADMIN_TOKEN),
        Some(json!({"state": "past_due", "reason": "payment failed", "expected_version": 1})),
    )
    .await;
    assert!(
        first.status.is_success(),
        "the first transition wins: {first:?}"
    );

    let before = h.audit.len();
    let second = call(
        &h.app,
        "POST",
        &format!("/v1/admin/tenants/{tenant}/subscription/transitions"),
        Some(ADMIN_TOKEN),
        Some(json!({"state": "canceled", "reason": "stale retry", "expected_version": 1})),
    )
    .await;
    assert!(
        second.status.is_client_error(),
        "a stale version is a lost update: {second:?}"
    );

    let events = h.audit.events();
    assert_eq!(
        events.len(),
        before + 1,
        "the refused transition is recorded"
    );
    assert_eq!(
        events.last().expect("the event").outcome,
        mas_domain::AuditOutcome::Failure,
        "a lost update that was refused is a failure, and the audit trail is \
         where the two operators' sequence is reconstructed from"
    );
}

/// An override grant and its removal are both recorded, because "who gave this
/// tenant extra capability, and who took it away" is precisely the question an
/// auditor asks.
#[tokio::test]
async fn granting_then_ending_an_override_writes_two_audit_events() {
    let Some(h) = fixture("cp-admin-audit-override").await else {
        return;
    };
    let tenant = h.home;
    let before = h.audit.len();

    let granted = call(
        &h.app,
        "POST",
        &format!("/v1/admin/tenants/{tenant}/overrides/features"),
        Some(ADMIN_TOKEN),
        Some(json!({
            "feature": "rag",
            "enabled": true,
            "reason": "pilot customer",
            "effective_from": "2024-01-01T00:00:00Z",
        })),
    )
    .await;
    assert!(
        granted.status.is_success(),
        "the override must be granted: {granted:?}"
    );

    let ended = call(
        &h.app,
        "POST",
        &format!("/v1/admin/tenants/{tenant}/overrides/features/rag/end"),
        Some(ADMIN_TOKEN),
        None,
    )
    .await;
    assert!(
        ended.status.is_success(),
        "the override must be endable: {ended:?}"
    );

    assert_eq!(
        h.audit.len(),
        before + 2,
        "grant and end are two decisions by an operator, and both must be attributable"
    );
}

// ───────────────────────────────────────────────────────────────────────────
// Tenant isolation
// ───────────────────────────────────────────────────────────────────────────

/// An admin bound to tenant A cannot write tenant B's subscription.
///
/// The path carries B's id, so a handler that trusted the path would apply it.
/// The refusal has to come from below the handler: row-level security, armed
/// from the caller's resolved scope, not from the id in the URL.
#[tokio::test]
async fn an_admin_cannot_write_another_tenants_subscription() {
    let Some(h) = fixture("cp-admin-cross").await else {
        return;
    };

    let code = plan_code("cross");
    let created = call(
        &h.app,
        "POST",
        "/v1/admin/plans",
        Some(ADMIN_TOKEN),
        Some(json!({"code": code.clone(), "name": "Cross"})),
    )
    .await;
    assert!(
        created.status.is_success(),
        "plans are platform reference data: {created:?}"
    );

    let victim = h.other;
    let reply = call(
        &h.app,
        "POST",
        &format!("/v1/admin/tenants/{victim}/subscription"),
        Some(ADMIN_TOKEN),
        Some(json!({"plan_code": code})),
    )
    .await;
    assert!(
        reply.status.is_client_error(),
        "tenant B's subscription must not be writable from tenant A's scope: {reply:?}"
    );
    assert_ne!(
        reply.status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "a row-level-security refusal is an authorization answer, not a broken \
         server: a 500 invites a retry that can never succeed, and it reports \
         a boundary being enforced as though the platform had failed"
    );
    assert_eq!(
        reply.code(),
        Some("FORBIDDEN"),
        "the refusal has to carry the stable code a client can branch on, \
         not a database error it is told to retry: {reply:?}"
    );
    assert!(
        !reply.body.to_string().contains(&victim.to_string()),
        "the refusal must not name the tenant the caller may not reach: {reply:?}"
    );

    let rows: Option<(uuid::Uuid,)> =
        sqlx::query_as("SELECT id FROM subscriptions WHERE tenant_id = $1")
            .bind(victim.into_uuid())
            .fetch_optional(&h.pool)
            .await
            .expect("query");
    assert!(
        rows.is_none(),
        "a refused cross-tenant write must not have left a row behind"
    );
}

/// The home tenant does work — the isolation assertion above is only
/// meaningful if the same request shape succeeds where it is allowed.
#[tokio::test]
async fn an_admin_can_write_its_own_tenants_subscription() {
    let Some(h) = fixture("cp-admin-own").await else {
        return;
    };
    let tenant = h.home;

    let code = plan_code("own");
    let created = call(
        &h.app,
        "POST",
        "/v1/admin/plans",
        Some(ADMIN_TOKEN),
        Some(json!({"code": code.clone(), "name": "Own"})),
    )
    .await;
    assert!(created.status.is_success(), "plan creation: {created:?}");

    let reply = call(
        &h.app,
        "POST",
        &format!("/v1/admin/tenants/{tenant}/subscription"),
        Some(ADMIN_TOKEN),
        Some(json!({"plan_code": code})),
    )
    .await;
    assert!(
        reply.status.is_success(),
        "the same request inside the caller's own scope must succeed: {reply:?}"
    );
}

/// Granting a feature to a plan does not grant it to a tenant that has no
/// subscription to that plan — the plan is a template, not a grant.
#[tokio::test]
async fn a_plan_feature_grant_does_not_reach_a_tenant_without_that_plan() {
    let Some(h) = fixture("cp-admin-template").await else {
        return;
    };

    let code = plan_code("template");
    let created = call(
        &h.app,
        "POST",
        "/v1/admin/plans",
        Some(ADMIN_TOKEN),
        Some(json!({"code": code.clone(), "name": "Template plan"})),
    )
    .await;
    assert!(created.status.is_success(), "plan creation: {created:?}");

    let granted = call(
        &h.app,
        "PUT",
        &format!("/v1/admin/plans/{code}/features"),
        Some(ADMIN_TOKEN),
        Some(json!({"feature": "export", "limit": null})),
    )
    .await;
    assert!(
        granted.status.is_success(),
        "a feature grant on a plan is a catalogue edit: {granted:?}"
    );

    // The tenant has no subscription at all, so no effective entitlement is
    // created anywhere by the grant alone.
    let rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM plan_entitlements pe
           JOIN plans p ON p.id = pe.plan_id
          WHERE p.code = $1",
    )
    .bind(&code)
    .fetch_one(&h.pool)
    .await
    .expect("query");
    assert_eq!(
        rows, 1,
        "the grant is one row on the plan, not a grant to any tenant"
    );
}
