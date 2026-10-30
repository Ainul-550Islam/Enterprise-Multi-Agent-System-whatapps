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
use mas_application::{
    audit::InMemoryAuditSink,
    connector_service::ConnectorService,
    context::ServiceContext,
    integration_service::IntegrationService,
    stores::{ConnectorStorePort, InMemoryServices},
    tool_service::{RegisterTool, ToolService},
    webhook_service::{DeliveryCursor, DeliveryRecord, WebhookAdminPort, WebhookService},
};
use mas_common::ids::{EventId, SecretReferenceId, WebhookId};
use mas_common::ids::{OrganizationId, TenantId};
use mas_common::timestamps::Timestamp;
use mas_contracts::tool::{ToolPageResponse, ToolResponse};
use mas_domain::MembershipRole;
use mas_domain::{
    value_objects::Slug, Agent, AgentKind, Connector, ConnectorAuthMode, Project,
    SafetyClassification, ToolKind, ToolRuntime,
};
use mas_integrations::{
    connectors::ConnectionProbePort,
    durable_webhooks::OutboundWebhookEnqueuePort,
    registry::{ConnectorRegistryPort, InMemoryConnectorRegistry, InMemoryProviderRegistry},
    secrets::{SecretReferenceLookupPort, SecretReferenceRecord},
};
use mas_security::{
    Action, AuthorizationRequest, AuthorizationService, PrincipalKind, ResourceKind, ScopeSet,
    SecurityPrincipal, TenantMembership,
};
use serde_json::json;
use std::sync::Arc;
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

// --- Foreign-tenant id-hiding fixtures (mirror the PostgreSQL contracts). ---

/// Probe port that must never be reached: id visibility is decided before
/// any connection attempt, so a foreign connector can never trigger egress.
#[derive(Debug)]
struct NeverProbes;

#[async_trait::async_trait]
impl ConnectionProbePort for NeverProbes {
    async fn probe(&self, _connector: &Connector) -> mas_common::result::Result<()> {
        panic!("a foreign connector id must never reach the probe path");
    }
}

/// Secret lookup that resolves nothing; foreign/missing ids return `None`.
#[derive(Debug)]
struct NeverLooksUpSecrets;

#[async_trait::async_trait]
impl SecretReferenceLookupPort for NeverLooksUpSecrets {
    async fn get(
        &self,
        _id: SecretReferenceId,
    ) -> mas_common::result::Result<Option<SecretReferenceRecord>> {
        Ok(None)
    }
}

/// Fan-out port that never enqueues; the covered paths must not reach it.
#[derive(Debug)]
struct NeverFanout;

#[async_trait::async_trait]
impl OutboundWebhookEnqueuePort for NeverFanout {
    async fn enqueue_event(
        &self,
        _tenant_id: TenantId,
        _event_id: EventId,
        _event_type: &str,
        _body: &[u8],
    ) -> mas_common::result::Result<u64> {
        Ok(0)
    }
}

/// Delivery store keyed by `(tenant, endpoint)`, mirroring the
/// `PostgresWebhookAdmin` contract: an endpoint that is not visible under the
/// calling tenant is `RESOURCE_NOT_FOUND`, never an empty page.
#[derive(Debug, Default)]
struct TenantBoundDeliveries {
    pages: std::sync::Mutex<std::collections::BTreeMap<(TenantId, WebhookId), Vec<DeliveryRecord>>>,
}

impl TenantBoundDeliveries {
    fn seed(&self, tenant: TenantId, endpoint: WebhookId, records: Vec<DeliveryRecord>) {
        self.pages
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert((tenant, endpoint), records);
    }
}

#[async_trait::async_trait]
impl WebhookAdminPort for TenantBoundDeliveries {
    async fn list_deliveries(
        &self,
        tenant: TenantId,
        endpoint: WebhookId,
        after: Option<DeliveryCursor>,
        limit: u16,
    ) -> mas_common::result::Result<Vec<DeliveryRecord>> {
        let pages = self.pages.lock().unwrap_or_else(|error| error.into_inner());
        let records = pages.get(&(tenant, endpoint)).ok_or_else(|| {
            mas_common::error::AppError::not_found("webhook", endpoint.to_string())
        })?;
        let mut page: Vec<DeliveryRecord> = records
            .iter()
            .filter(|record| {
                after.is_none_or(|cursor| {
                    record.created_at < cursor.created_at
                        || (record.created_at == cursor.created_at && record.id < cursor.id)
                })
            })
            .cloned()
            .collect();
        page.truncate(usize::from(limit));
        Ok(page)
    }

    async fn replay_dead_letter(
        &self,
        tenant: TenantId,
        endpoint: WebhookId,
        delivery: uuid::Uuid,
        _actor: Option<mas_common::ids::UserId>,
    ) -> mas_common::result::Result<DeliveryRecord> {
        let pages = self.pages.lock().unwrap_or_else(|error| error.into_inner());
        let visible = pages.get(&(tenant, endpoint)).ok_or_else(|| {
            mas_common::error::AppError::not_found("webhook", endpoint.to_string())
        })?;
        let record = visible
            .iter()
            .find(|record| record.id == delivery)
            .cloned()
            .ok_or_else(|| {
                mas_common::error::AppError::not_found("webhook delivery", delivery.to_string())
            })?;
        if record.state != "dead_lettered" {
            return Err(mas_common::error::AppError::conflict(
                "only dead-lettered webhook deliveries may be replayed",
            ));
        }
        Ok(record)
    }
}

fn scoped_context(
    service: &str,
    correlation: &str,
    tenant: TenantId,
    organization: OrganizationId,
) -> ServiceContext {
    ServiceContext::for_service(service, correlation, &Timestamp::now())
        .expect("service context")
        .with_scope(tenant, organization)
}

#[tokio::test]
async fn foreign_tenant_tool_ids_are_hidden_as_not_found() {
    let tenant_a = TenantId::new();
    let tenant_b = TenantId::new();
    let organization = OrganizationId::new();
    let store = Arc::new(InMemoryServices::new());
    let audit = Arc::new(InMemoryAuditSink::new());
    let engine = Arc::new(mas_policy::engine::PolicyEngine::new(Arc::new(
        mas_policy::store::InMemoryPolicyStore::new(),
    )));
    let service = ToolService::new(
        Box::new(store.clone()),
        Box::new(store.clone()),
        engine,
        Box::new(audit.clone()),
    );

    // Seed a tool that exists only in tenant B's scope.
    let seed_ctx = scoped_context("tooling-test", "seed-foreign-tool", tenant_b, organization);
    let tool = service
        .register(
            &seed_ctx,
            RegisterTool {
                project_id: None,
                name: "foreign-tool".to_owned(),
                description: None,
                kind: ToolKind::Internal,
                input_schema: json!({"type": "object"}),
                output_schema: None,
                runtime: ToolRuntime {
                    endpoint: None,
                    timeout_ms: 30_000,
                    sandbox_required: false,
                    settings: serde_json::Map::new(),
                },
                safety: SafetyClassification::Standard,
            },
        )
        .await
        .expect("seed tool");

    let ctx_a = scoped_context("tooling-test", "foreign-tool-read", tenant_a, organization);
    let read = service
        .get(&ctx_a, tool.id)
        .await
        .expect_err("foreign tool id is hidden from tenant A");
    assert_eq!(read.error_code(), "RESOURCE_NOT_FOUND");

    let update = service
        .update(&ctx_a, tool.id, None, None, None, None, None, None, None)
        .await
        .expect_err("foreign tool update is hidden from tenant A");
    assert_eq!(update.error_code(), "RESOURCE_NOT_FOUND");
}

#[tokio::test]
async fn foreign_tenant_agent_tool_permissions_are_hidden_as_not_found() {
    use mas_application::stores::{AgentStorePort, ProjectStorePort};

    let tenant_a = TenantId::new();
    let tenant_b = TenantId::new();
    let organization = OrganizationId::new();
    let store = Arc::new(InMemoryServices::new());
    let audit = Arc::new(InMemoryAuditSink::new());
    let engine = Arc::new(mas_policy::engine::PolicyEngine::new(Arc::new(
        mas_policy::store::InMemoryPolicyStore::new(),
    )));
    let service = ToolService::new(
        Box::new(store.clone()),
        Box::new(store.clone()),
        engine,
        Box::new(audit.clone()),
    );

    // Seed a project and agent that exist only in tenant B's scope.
    let project = Project::create(
        tenant_b,
        organization,
        "Foreign Project",
        Slug::new("foreign-project").expect("slug"),
    )
    .expect("project");
    ProjectStorePort::save(store.as_ref(), &project)
        .await
        .expect("seed project");
    let agent = Agent::create(
        tenant_b,
        organization,
        project.id,
        "foreign-agent",
        Slug::new("foreign-agent").expect("slug"),
        AgentKind::Standard,
    )
    .expect("agent");
    AgentStorePort::save(store.as_ref(), &agent)
        .await
        .expect("seed agent");

    let ctx_a = scoped_context(
        "tooling-test",
        "foreign-agent-permissions",
        tenant_a,
        organization,
    );
    let error = service
        .replace_agent_permissions(&ctx_a, agent.id, Vec::new())
        .await
        .expect_err("foreign agent id is hidden from tenant A");
    assert_eq!(error.error_code(), "RESOURCE_NOT_FOUND");
    assert!(
        audit.is_empty(),
        "a refused foreign replacement must not record a success audit"
    );
}

#[tokio::test]
async fn foreign_tenant_connector_ids_are_hidden_as_not_found() {
    let tenant_a = TenantId::new();
    let tenant_b = TenantId::new();
    let organization = OrganizationId::new();
    let audit = Arc::new(InMemoryAuditSink::new());

    // The connector exists only in tenant B's scope, seeded into both the
    // probe registry (integration plane) and the disable lifecycle store.
    let connector = Connector::register(
        tenant_b,
        organization,
        "stripe",
        "foreign-stripe",
        ConnectorAuthMode::None,
    )
    .expect("connector");
    let connector_id = connector.id;
    let registry = InMemoryConnectorRegistry::new();
    registry.save(&connector).await.expect("seed registry");

    let integrations = Arc::new(IntegrationService::new(
        Box::new(InMemoryProviderRegistry::new()),
        Box::new(registry),
        Box::new(NeverProbes),
        Box::new(InMemoryServices::new()),
        Box::new(NeverLooksUpSecrets),
        Box::new(NeverFanout),
        Box::new(audit.clone()),
    ));
    let lifecycle_store = InMemoryServices::new();
    ConnectorStorePort::save(&lifecycle_store, &connector)
        .await
        .expect("seed lifecycle store");
    let service = ConnectorService::new(
        Box::new(InMemoryProviderRegistry::new()),
        integrations,
        Box::new(lifecycle_store),
        Box::new(audit.clone()),
    );

    let ctx_a = scoped_context(
        "tooling-test",
        "foreign-connector-probe",
        tenant_a,
        organization,
    );
    let probe = service
        .probe(&ctx_a, connector_id)
        .await
        .expect_err("foreign connector probe is hidden from tenant A");
    assert_eq!(probe.error_code(), "RESOURCE_NOT_FOUND");

    let disable = service
        .disable(&ctx_a, connector_id)
        .await
        .expect_err("foreign connector disable is hidden from tenant A");
    assert_eq!(disable.error_code(), "RESOURCE_NOT_FOUND");
    assert!(
        audit.is_empty(),
        "refused foreign connector operations must not record a success audit"
    );
}

#[tokio::test]
async fn foreign_tenant_webhook_delivery_ids_are_hidden_as_not_found() {
    let tenant_a = TenantId::new();
    let tenant_b = TenantId::new();
    let organization = OrganizationId::new();
    let endpoint = WebhookId::new();
    let empty_endpoint = WebhookId::new();

    let delivery = DeliveryRecord {
        id: uuid::Uuid::now_v7(),
        tenant_id: tenant_b,
        endpoint_id: endpoint,
        event_id: EventId::new(),
        state: "dead_lettered".to_owned(),
        attempt_count: 3,
        last_status_code: Some(500),
        next_attempt_at: None,
        payload_hash: "a".repeat(64),
        last_error: Some("upstream 500".to_owned()),
        created_at: Timestamp::now(),
    };
    let delivery_id = delivery.id;
    let fixture = TenantBoundDeliveries::default();
    fixture.seed(tenant_b, endpoint, vec![delivery]);
    // Own endpoint, no deliveries yet: still an empty200 page, never a 404.
    fixture.seed(tenant_b, empty_endpoint, Vec::new());

    let audit = Arc::new(InMemoryAuditSink::new());
    let service = WebhookService::new(Box::new(fixture), Box::new(audit.clone()));

    // Controls: the owning tenant sees both its empty and its populated page.
    let own_empty = service
        .deliveries(
            &scoped_context("tooling-test", "own-empty", tenant_b, organization),
            empty_endpoint,
            None,
            10,
        )
        .await
        .expect("own empty endpoint stays an empty page");
    assert!(own_empty.items.is_empty());
    let own_page = service
        .deliveries(
            &scoped_context("tooling-test", "own-page", tenant_b, organization),
            endpoint,
            None,
            10,
        )
        .await
        .expect("own endpoint page");
    assert_eq!(own_page.items.len(), 1);

    // Foreign tenant: the endpoint id is hidden as not-found for both routes.
    let ctx_a = scoped_context("tooling-test", "foreign-deliveries", tenant_a, organization);
    let deliveries = service
        .deliveries(&ctx_a, endpoint, None, 10)
        .await
        .expect_err("foreign delivery listing is hidden from tenant A");
    assert_eq!(deliveries.error_code(), "RESOURCE_NOT_FOUND");

    let replay = service
        .replay(&ctx_a, endpoint, delivery_id)
        .await
        .expect_err("foreign delivery replay is hidden from tenant A");
    assert_eq!(replay.error_code(), "RESOURCE_NOT_FOUND");

    // Control: the owning tenant can replay its own dead-lettered delivery
    // exactly once and the success is audited.
    let replayed = service
        .replay(
            &scoped_context("tooling-test", "own-replay", tenant_b, organization),
            endpoint,
            delivery_id,
        )
        .await
        .expect("own dead-letter replay");
    assert_eq!(replayed.id, delivery_id);
    assert!(!audit.is_empty(), "successful replay must be audited");
}
