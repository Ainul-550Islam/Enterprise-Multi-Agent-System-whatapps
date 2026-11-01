//! PURPOSE: Real-PostgreSQL coverage for tool persistence, permission replacement,
//! and forced-RLS tenant isolation.
//!
//! Owning layer: `mas-persistence` integration tests.
//!
//! Production responsibility: verify that the production tool repositories
//! preserve tool state and atomically replace scoped grants under tenant RLS.
//!
//! Data ownership: fixtures are isolated by generated IDs and live only in the
//! explicitly configured test database.
//!
//! MUST NOT: run against production, silently use an in-memory store, or claim
//! forced-RLS coverage when the runtime role is not configured.

mod support;

use mas_application::stores::{AgentStorePort, ToolStorePort};
use mas_common::ids::{OrganizationId, ProjectId, TenantId};
use mas_domain::{Agent, AgentKind, Slug, Tool, ToolKind, ToolPermission};
use mas_persistence::repositories::services::PgServices;
use mas_persistence::repositories::tools::PostgresToolStore;
use mas_persistence::scoped_request::with_authoritative_scope;
use mas_security::AuthoritativeScope;
use sqlx::{PgPool, Row};
use std::collections::BTreeSet;

#[derive(Clone, Copy)]
struct FixtureScope {
    organization: OrganizationId,
    tenant: TenantId,
    project: ProjectId,
}

async fn seed_scope(pool: &PgPool, label: &str) -> FixtureScope {
    let suffix = uuid::Uuid::now_v7().simple().to_string();
    let organization = OrganizationId::new();
    let tenant = TenantId::new();
    let project = ProjectId::new();
    let mut connection = pool.acquire().await.expect("migrator connection");
    sqlx::query(
        "INSERT INTO organizations (id, legal_name, display_name, slug) VALUES ($1, $2, $3, $4)",
    )
    .bind(organization.into_uuid())
    .bind(format!("Tool Test {label}"))
    .bind(format!("Tool Test {label}"))
    .bind(format!("tool-{label}-{suffix}"))
    .execute(connection.as_mut())
    .await
    .expect("organization seed");
    sqlx::query("INSERT INTO tenants (id, organization_id, name, slug, status, environment, isolation_mode) VALUES ($1, $2, $3, $4, 'active', 'development', 'shared_rls')")
        .bind(tenant.into_uuid())
        .bind(organization.into_uuid())
        .bind(format!("Tool Test {label}"))
        .bind(format!("tool-{label}-{suffix}"))
        .execute(connection.as_mut()).await.expect("tenant seed");
    sqlx::query("INSERT INTO projects (id, tenant_id, organization_id, name, slug) VALUES ($1, $2, $3, $4, $5)")
        .bind(project.into_uuid())
        .bind(tenant.into_uuid())
        .bind(organization.into_uuid())
        .bind(format!("Tool Project {label}"))
        .bind(format!("tool-{label}-{suffix}"))
        .execute(connection.as_mut()).await.expect("project seed");
    FixtureScope {
        organization,
        tenant,
        project,
    }
}

fn scope(fixture: FixtureScope) -> AuthoritativeScope {
    AuthoritativeScope::resolved(fixture.tenant, fixture.organization, "tools-live-pg")
        .with_project(fixture.project)
}

#[tokio::test]
async fn live_tool_crud_permission_replace_and_cross_tenant_rls() {
    let Some(migrator) = support::live_pool("tools-live-pg-migrator").await else {
        return;
    };
    let Some(app_pool) = support::live_app_pool("tools-live-pg-app").await else {
        eprintln!("tools_live_pg: skipping forced-RLS assertions (set MAS_TEST_DATABASE_APP_URL)");
        return;
    };

    let tenant_a = seed_scope(&migrator, "a").await;
    let tenant_b = seed_scope(&migrator, "b").await;
    let store = PostgresToolStore::new(app_pool.clone());
    let agents = PgServices::new(app_pool.clone());

    let (tool_a, agent_a) = with_authoritative_scope(&app_pool, scope(tenant_a), || async {
        let tool = Tool::register(
            tenant_a.tenant,
            tenant_a.organization,
            Some(tenant_a.project),
            "Scoped Tool A",
            ToolKind::Internal,
            serde_json::json!({"type":"object","properties":{}}),
            None,
        )
        .expect("tool aggregate");
        store.save(&tool).await.expect("save tool A");
        let loaded = store
            .get(tool.id)
            .await
            .expect("get tool A")
            .expect("tool exists");
        assert_eq!(loaded.version, tool.version);
        assert_eq!(loaded.project_id, Some(tenant_a.project));
        assert_eq!(
            store
                .list_page(tenant_a.tenant, Some(tenant_a.project), None, 20)
                .await
                .expect("tool page")
                .len(),
            1
        );

        let agent = Agent::create(
            tenant_a.tenant,
            tenant_a.organization,
            tenant_a.project,
            "Tool Test Agent A",
            Slug::new(&format!("tool-agent-{}", uuid::Uuid::now_v7().simple()))
                .expect("agent slug"),
            AgentKind::Standard,
        )
        .expect("agent aggregate");
        AgentStorePort::save(&agents, &agent)
            .await
            .expect("save agent");
        let grant = ToolPermission::new(
            tool.id,
            tenant_a.tenant,
            Some(tenant_a.project),
            Some(agent.id),
            None,
            BTreeSet::from(["invoke".to_owned()]),
        )
        .expect("grant");
        store
            .replace_agent_permissions(tenant_a.tenant, agent.id, std::slice::from_ref(&grant))
            .await
            .expect("replace grants");
        assert_eq!(
            store
                .permissions(tenant_a.tenant, tool.id)
                .await
                .expect("read grants")
                .len(),
            1
        );

        let duplicate_subject = ToolPermission::new(
            tool.id,
            tenant_a.tenant,
            Some(tenant_a.project),
            Some(agent.id),
            None,
            BTreeSet::from(["inspect".to_owned()]),
        )
        .expect("second grant");
        assert!(store
            .replace_agent_permissions(tenant_a.tenant, agent.id, &[grant, duplicate_subject])
            .await
            .is_err());
        let after_failed_replace = store
            .permissions(tenant_a.tenant, tool.id)
            .await
            .expect("grants after rollback");
        assert_eq!(
            after_failed_replace.len(),
            1,
            "failed replacement must retain the prior grant set"
        );
        assert!(after_failed_replace[0].allowed_actions.contains("invoke"));
        Ok((tool, agent))
    })
    .await
    .expect("tenant A scoped work");

    let tool_b = with_authoritative_scope(&app_pool, scope(tenant_b), || async {
        assert!(
            store
                .get(tool_a.id)
                .await
                .expect("foreign lookup")
                .is_none(),
            "foreign tool ID must be hidden by forced RLS"
        );
        let tool = Tool::register(
            tenant_b.tenant,
            tenant_b.organization,
            Some(tenant_b.project),
            "Scoped Tool B",
            ToolKind::Internal,
            serde_json::json!({"type":"object","properties":{}}),
            None,
        )
        .expect("tool B aggregate");
        store.save(&tool).await.expect("save tool B");
        let visible = store
            .list_page(tenant_b.tenant, Some(tenant_b.project), None, 20)
            .await
            .expect("tenant B page");
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].tenant_id, tenant_b.tenant);
        Ok(tool)
    })
    .await
    .expect("tenant B scoped work");

    assert_ne!(tool_a.tenant_id, tool_b.tenant_id);
    assert_eq!(agent_a.tenant_id, tenant_a.tenant);

    with_authoritative_scope(&app_pool, scope(tenant_a), || async {
        let mut scoped = mas_persistence::scoped_request::connection().await?;
        let row = sqlx::query("SELECT count(*)::bigint AS count FROM tools WHERE tenant_id = $1")
            .bind(tenant_b.tenant.into_uuid())
            .fetch_one(scoped.connection().as_mut())
            .await
            .map_err(|error| mas_common::error::AppError::database(error.to_string()))?;
        let count: i64 = row
            .try_get("count")
            .map_err(|error| mas_common::error::AppError::database(error.to_string()))?;
        assert_eq!(
            count, 0,
            "RLS must filter direct SQL reads as well as repository reads"
        );
        Ok(())
    })
    .await
    .expect("tenant A scoped RLS query");
}
