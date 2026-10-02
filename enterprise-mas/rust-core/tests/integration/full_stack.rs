// Cross-crate full-stack integration: a REAL HTTP server (axum bound to a
// loopback port) carrying `mas-api`'s production router and in-memory
// application services, driven by the REAL `mas-cli` typed client — no
// mocks on either side. Compiled into `mas-api`'s test target via
// `crates/api/tests/root_integration.rs` (the root `tests/` tree stays the
// single source of truth while cargo requires a package anchor).
//
// The scenario mirrors the onboarding path:
//   seed org → tenant → project (service-level, like `mas-api --dev-inmemory`)
//   → POST /v1/agents                 (handler-level registration)
//   → POST /v1/workflows (+graph PUT) (fixture graph, topological response)
//   → execution submit (+replay)      (via `MasApiClient` two-phase decode)
//   → list/get/transitions            (scope guard + contract assertions)

use std::time::Duration;

use mas_application::context::ServiceContext;
use mas_common::ids::TenantId;
use mas_common::timestamps::Timestamp;
use mas_domain::IsolationMode;
use uuid::Uuid;

use mas_api::router::router;
use mas_api::state::test_support::inmemory_state;
use mas_api::state::AppState;
use mas_cli::client::{MasApiClient, Outcome};
use mas_cli::{ClientConfig, Format};

struct FixtureScope {
    tenant: Uuid,
    organization: Uuid,
    project: Uuid,
}

/// Seeds one org → tenant → project through the application services (the
/// same calls `mas-api --dev-inmemory` makes) and returns the ids.
async fn seed_scope(state: &AppState) -> FixtureScope {
    let system =
        ServiceContext::for_service("root-it", "root-it-1", &Timestamp::now()).expect("ctx");
    let organization = state
        .tenancy
        .register_organization(&system, "Acme Ltd.", "Acme", "acme-root-it")
        .await
        .expect("org");
    let org_scoped = system.clone().with_scope(TenantId::new(), organization.id);
    let tenant = state
        .tenancy
        .register_tenant(
            &org_scoped,
            "Prod",
            "prod-root-it",
            mas_common::enums::Environment::Production,
            IsolationMode::SharedRls,
        )
        .await
        .expect("tenant");
    let tenant_scoped = system.clone().with_scope(tenant.id, organization.id);
    let project = state
        .tenancy
        .register_project(&tenant_scoped, "Core", "core-root-it")
        .await
        .expect("project");
    FixtureScope {
        tenant: Uuid::from(tenant.id),
        organization: Uuid::from(organization.id),
        project: Uuid::from(project.id),
    }
}

/// Binds the production router on an ephemeral loopback port, serving until
/// the test process exits. Returns the base URL.
async fn serve_test_api(state: AppState) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let app = router(state);
    tokio::spawn(async move { axum::serve(listener, app).await });
    format!("http://{addr}")
}

fn client_for(base_url: &str, scope: Option<&FixtureScope>) -> MasApiClient {
    let (tenant, organization) = scope
        .map(|s| (Some(s.tenant), Some(s.organization)))
        .unwrap_or((None, None));
    MasApiClient::new(ClientConfig {
        base_url: base_url.to_owned(),
        token: "test-token".to_owned(),
        tenant,
        organization,
        correlation_id: "root-it-corr".to_owned(),
        request_timeout: Duration::from_secs(10),
    })
    .expect("client")
}

fn ok<T>(outcome: Outcome<T>) -> T {
    match outcome {
        Outcome::Ok(resp) => resp.data,
        Outcome::Err(err) => panic!("expected ok, got {} ({})", err.message, err.code),
    }
}

#[tokio::test]
async fn full_stack_through_real_http_and_real_cli_client() {
    let state = inmemory_state();
    let scope = seed_scope(&state).await;
    let base_url = serve_test_api(state).await;
    let client = client_for(&base_url, Some(&scope));

    // 1. Register an agent (scoped route — headers flow through the client).
    let agent_body = serde_json::json!({
        "project_id": scope.project,
        "name": "Copilot",
        "slug": "copilot",
        "kind": "standard",
    });
    let agent_created: serde_json::Value = ok(client.post("agents", &agent_body, None).await);
    let agent_id = agent_created["id"].as_str().expect("agent id").to_owned();
    assert_eq!(agent_created["status"], "draft");

    // 2. Create a workflow, then push the fixture graph (topological order
    //    proves the application pipeline ran, not just storage).
    let workflow_body = serde_json::json!({"project_id": scope.project, "name": "flow-1"});
    let workflow_created: serde_json::Value =
        ok(client.post("workflows", &workflow_body, None).await);
    let workflow_id = workflow_created["id"].as_str().expect("workflow id").to_owned();

    let graph_fixture: serde_json::Value = serde_json::from_str(include_str!("../fixtures/workflow_graph.json"
    ))
    .expect("fixture parses");
    // PUT is not in the minimal client surface; call it via the raw
    // builder to keep the client surface honest.
    let raw = reqwest::Client::new()
        .put(format!("{base_url}/v1/workflows/{workflow_id}/graph"))
        .bearer_auth("test-token")
        .header("x-tenant-id", scope.tenant.to_string())
        .header("x-organization-id", scope.organization.to_string())
        .json(&graph_fixture)
        .send()
        .await
        .expect("graph PUT");
    let put_status = raw.status();
    let put_body = raw.text().await.unwrap_or_default();
    assert!(put_status.is_success(), "graph PUT: {put_status} — {put_body}");

    // 3. Submit an execution — first through, then idempotent replay.
    let idempotency_key = "root-it-idem-1";
    let submit_body = serde_json::json!({
        "workflow_id": workflow_id,
        "input": {"question": "what is two plus two"},
        "idempotency_key": idempotency_key,
    });
    let submitted: serde_json::Value = ok(
        client
            .post(
                &format!("projects/{}/executions", scope.project),
                &submit_body,
                Some(idempotency_key),
            )
            .await,
    );
    let execution_id = submitted["id"].as_str().expect("execution id").to_owned();
    assert_eq!(submitted["status"], "pending");

    let replay = client
        .post::<_, serde_json::Value>(
            &format!("projects/{}/executions", scope.project),
            &submit_body,
            Some(idempotency_key),
        )
        .await;
    let Outcome::Ok(replay_resp) = replay else {
        panic!("replay must decode");
    };
    assert!(
        replay_resp.idempotent_replay,
        "second submit with the same key is a replay",
    );
    assert_eq!(replay_resp.data["id"].as_str(), Some(execution_id.as_str()));

    // 4. Transitions: start → running; the list reflects it; foreign scope
    //    answers 404 (existence hidden).
    let started: serde_json::Value =
        ok(client.post_empty(&format!("executions/{execution_id}/start")).await);
    assert_eq!(started["status"], "running");

    let list: Vec<serde_json::Value> = ok(client.get("executions").await);
    assert_eq!(list.len(), 1, "one execution visible in scope");

    let foreign_client = client_for(
        &base_url,
        Some(&FixtureScope {
            tenant: Uuid::new_v4(),
            organization: Uuid::new_v4(),
            project: Uuid::new_v4(),
        }),
    );
    let foreign = foreign_client
        .get::<serde_json::Value>(&format!("executions/{execution_id}"))
        .await;
    let Outcome::Err(failure) = foreign else {
        panic!("foreign scope must not see the execution");
    };
    assert_eq!(failure.code, "RESOURCE_NOT_FOUND", "{failure:?}");

    // 5. Pause → resume rounds the lifecycle out (agent assigned only at
    //    submit in a fuller wiring; agent_id stays None here by design).
    let paused: serde_json::Value =
        ok(client.post_empty(&format!("executions/{execution_id}/pause")).await);
    assert_eq!(paused["status"], "paused");
    let resumed = client
        .post_empty::<serde_json::Value>(&format!("executions/{execution_id}/resume"))
        .await;
    assert!(matches!(resumed, Outcome::Ok(_)), "resume ok: {resumed:?}");

    let _ = agent_id; // published at this level; publish/supersede is an api-level test.
    let _ = Format::Brief; // marker: CLI formats also covered by unit tests
}

#[tokio::test]
async fn scope_headers_are_mandatory_from_the_client_side_too() {
    let state = inmemory_state();
    let _scope = seed_scope(&state).await;
    let base_url = serve_test_api(state).await;

    // Server-side: no headers at all → 400/401 with a stable code.
    let no_scope = client_for(&base_url, None);
    let outcome = no_scope.get::<serde_json::Value>("executions").await;
    let Outcome::Err(failure) = outcome else {
        panic!("server must reject scope-less calls");
    };
    assert!(
        matches!(
            failure.code.as_str(),
            "VALIDATION_FAILED" | "UNAUTHORIZED" | "FORBIDDEN"
        ),
        "scope contract on the wire: {failure:?}",
    );
}
