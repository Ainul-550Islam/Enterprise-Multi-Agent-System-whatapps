//! Live PostgreSQL regression for organization filtering before LIMIT.
//! Requires a migrated database and both privileged fixture and RLS app DSNs.

use mas_application::durable::DurableExecutionPort;
use mas_common::ids::{OrganizationId, ProjectId, TenantId, WorkflowId};
use mas_common::timestamps::Timestamp;
use mas_domain::Execution;
use mas_persistence::durable::PgDurableExecution;
use mas_persistence::rows::ExecutionRow;
use mas_persistence::scoped_request::with_authoritative_scope;
use mas_security::AuthoritativeScope;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[tokio::test]
async fn postgres_filters_organization_before_limit_inside_rls_transaction() {
    let admin_url = std::env::var("MAS_TEST_DATABASE_URL").expect("MAS_TEST_DATABASE_URL required");
    let app_url =
        std::env::var("MAS_TEST_DATABASE_APP_URL").expect("MAS_TEST_DATABASE_APP_URL required");
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&admin_url)
        .await
        .expect("admin pool");
    let app = PgPoolOptions::new()
        .max_connections(2)
        .connect(&app_url)
        .await
        .expect("app pool");
    let org_a = OrganizationId::new();
    let org_b = OrganizationId::new();
    let tenant = TenantId::new();
    let project = ProjectId::new();
    let workflow = WorkflowId::new();
    let slug = format!("test-{}", Uuid::new_v4().simple());
    for (org, suffix) in [(org_a, "a"), (org_b, "b")] {
        sqlx::query("INSERT INTO organizations (id, legal_name, display_name, slug) VALUES ($1, 'Test', 'Test', $2)")
            .bind(org.into_uuid()).bind(format!("{slug}-{suffix}"))
            .execute(&admin).await.expect("insert organization");
    }
    sqlx::query("INSERT INTO tenants (id, organization_id, name, slug, environment) VALUES ($1, $2, 'Test', $3, 'development')")
        .bind(tenant.into_uuid()).bind(org_a.into_uuid()).bind(&slug)
        .execute(&admin).await.expect("insert tenant");
    sqlx::query("INSERT INTO projects (id, tenant_id, organization_id, name, slug) VALUES ($1,$2,$3,'Test',$4)")
        .bind(project.into_uuid()).bind(tenant.into_uuid()).bind(org_a.into_uuid()).bind(&slug)
        .execute(&admin).await.expect("insert project");
    sqlx::query("INSERT INTO workflows (id, tenant_id, organization_id, project_id, name, slug, spec) VALUES ($1,$2,$3,$4,'Test',$5,'{}')")
        .bind(workflow.into_uuid()).bind(tenant.into_uuid()).bind(org_a.into_uuid()).bind(project.into_uuid()).bind(&slug)
        .execute(&admin).await.expect("insert workflow");

    // Existing schema permits independently referenced organization ids on an
    // execution. Include such a row to prove the query is defensive even if
    // legacy or malformed data contains a conflicting organization.
    let mut a = Execution::new(
        tenant,
        org_a,
        project,
        Some(workflow),
        None,
        serde_json::json!({}),
        "scope-test-a",
        "test",
    )
    .expect("a");
    let mut b = Execution::new(
        tenant,
        org_b,
        project,
        Some(workflow),
        None,
        serde_json::json!({}),
        "scope-test-b",
        "test",
    )
    .expect("b");
    a.created_at = Timestamp::from_unix_seconds(1_700_000_000).expect("time");
    b.created_at = Timestamp::from_unix_seconds(1_700_000_001).expect("time");
    for execution in [&a, &b] {
        let row = ExecutionRow::from_domain(execution).expect("execution row");
        sqlx::query("INSERT INTO executions (id,tenant_id,organization_id,project_id,workflow_id,status,input,state,correlation_id,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)")
            .bind(row.id).bind(row.tenant_id).bind(row.organization_id).bind(row.project_id)
            .bind(row.workflow_id).bind(row.status).bind(row.input).bind(row.state)
            .bind(row.correlation_id).bind(row.created_at).bind(row.updated_at)
            .execute(&admin).await.expect("insert execution");
    }
    let store = PgDurableExecution::new(app.clone());
    let scope = AuthoritativeScope::resolved(tenant, org_a, "test");
    let (all, visible, foreign) = with_authoritative_scope(&app, scope, || async {
        Ok((
            store.list_executions(tenant, 1).await?,
            store.list_executions_scoped(tenant, Some(org_a), 1).await?,
            store
                .list_executions_scoped(TenantId::new(), Some(org_a), 1)
                .await?,
        ))
    })
    .await
    .expect("scoped queries");
    assert_eq!(all.len(), 1);
    assert_eq!(
        all[0].id, b.id,
        "newer foreign organization consumes unfiltered limit"
    );
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id, a.id, "organization is filtered before limit");
    assert!(foreign.is_empty());

    sqlx::query("DELETE FROM tenants WHERE id=$1")
        .bind(tenant.into_uuid())
        .execute(&admin)
        .await
        .expect("delete tenant");
    sqlx::query("DELETE FROM organizations WHERE id = $1 OR id = $2")
        .bind(org_a.into_uuid())
        .bind(org_b.into_uuid())
        .execute(&admin)
        .await
        .expect("delete organizations");
}
