//! Live RLS regression: a stale metadata writer must not revive a tombstone.
//! Both fixture-admin and restricted application DSNs are required.

use mas_application::stores::DocumentStorePort;
use mas_common::ids::{DataSourceId, OrganizationId, ProjectId, TenantId, UserId};
use mas_domain::{Document, DocumentStatus};
use mas_persistence::repositories::PostgresDocumentStore;
use mas_persistence::scoped_request::with_authoritative_scope;
use mas_security::AuthoritativeScope;
use uuid::Uuid;

mod support;

#[tokio::test]
async fn deleted_document_cannot_be_revived_by_a_stale_writer() {
    let Some(admin) = support::live_pool("document-tombstone-admin").await else {
        return;
    };
    let Some(app) = support::live_app_pool("document-tombstone-app").await else {
        return;
    };
    let tenant = TenantId::new();
    let organization = OrganizationId::new();
    let project = ProjectId::new();
    let source = DataSourceId::new();
    let user = UserId::new();
    let slug = format!("test-{}", Uuid::new_v4().simple());
    sqlx::query("INSERT INTO organizations (id, legal_name, display_name, slug) VALUES ($1, 'Test', 'Test', $2)")
        .bind(organization.into_uuid()).bind(&slug).execute(&admin).await.expect("organization");
    sqlx::query("INSERT INTO tenants (id, organization_id, name, slug, environment) VALUES ($1,$2,'Test',$3,'development')")
        .bind(tenant.into_uuid()).bind(organization.into_uuid()).bind(&slug).execute(&admin).await.expect("tenant");
    sqlx::query("INSERT INTO projects (id, tenant_id, organization_id, name, slug) VALUES ($1,$2,$3,'Test',$4)")
        .bind(project.into_uuid()).bind(tenant.into_uuid()).bind(organization.into_uuid()).bind(&slug).execute(&admin).await.expect("project");
    sqlx::query("INSERT INTO users (id, email, display_name) VALUES ($1,$2,'Test')")
        .bind(user.into_uuid())
        .bind(format!("{slug}@example.test"))
        .execute(&admin)
        .await
        .expect("user");
    sqlx::query("INSERT INTO data_sources (id,tenant_id,project_id,name,kind,status,created_by) VALUES ($1,$2,$3,'Test','upload','active',$4)")
        .bind(source.into_uuid()).bind(tenant.into_uuid()).bind(project.into_uuid()).bind(user.into_uuid()).execute(&admin).await.expect("source");
    let store = PostgresDocumentStore::new(app.clone());
    let scope = AuthoritativeScope::resolved(tenant, organization, "test");
    let mut document = Document::new(
        tenant,
        project,
        source,
        "Test",
        "text/plain",
        4,
        "a".repeat(64),
        user,
        serde_json::json!({}),
        Default::default(),
    )
    .expect("document");
    with_authoritative_scope(&app, scope.clone(), || async {
        store.save(&document).await
    })
    .await
    .expect("save pending");
    document.begin_processing().expect("processing");
    with_authoritative_scope(&app, scope.clone(), || async {
        store.save(&document).await
    })
    .await
    .expect("save processing");
    let mut stale = document.clone();
    stale.mark_ready(1).expect("ready");
    document.mark_deleted().expect("deleted");
    with_authoritative_scope(&app, scope.clone(), || async {
        store.save(&document).await
    })
    .await
    .expect("save deleted");
    assert!(
        with_authoritative_scope(&app, scope.clone(), || async { store.save(&stale).await })
            .await
            .is_err(),
        "stale write must fail"
    );
    with_authoritative_scope(&app, scope.clone(), || async {
        store.save(&document).await
    })
    .await
    .expect("idempotent delete");
    let fetched = with_authoritative_scope(&app, scope, || async { store.get(document.id).await })
        .await
        .expect("get")
        .expect("document");
    assert_eq!(fetched.status, DocumentStatus::Deleted);
    sqlx::query("DELETE FROM tenants WHERE id=$1")
        .bind(tenant.into_uuid())
        .execute(&admin)
        .await
        .expect("cleanup tenant");
    sqlx::query("DELETE FROM users WHERE id=$1")
        .bind(user.into_uuid())
        .execute(&admin)
        .await
        .expect("cleanup user");
    sqlx::query("DELETE FROM organizations WHERE id=$1")
        .bind(organization.into_uuid())
        .execute(&admin)
        .await
        .expect("cleanup organization");
}

#[tokio::test]
async fn database_rejects_cross_project_and_cross_tenant_catalogue_links() {
    let Some(pool) = support::live_pool("document-catalogue-constraint").await else {
        return;
    };
    let mut tx = pool.begin().await.expect("transaction");
    let org = Uuid::new_v4();
    let tenant_a = Uuid::new_v4();
    let tenant_b = Uuid::new_v4();
    let project_a = Uuid::new_v4();
    let project_a2 = Uuid::new_v4();
    let project_b = Uuid::new_v4();
    let user = Uuid::new_v4();
    let source = Uuid::new_v4();
    let slug = format!("test-{}", Uuid::new_v4().simple());
    sqlx::query(
        "INSERT INTO organizations (id,legal_name,display_name,slug) VALUES ($1,'Test','Test',$2)",
    )
    .bind(org)
    .bind(&slug)
    .execute(&mut *tx)
    .await
    .expect("org");
    for (tenant, suffix) in [(tenant_a, "a"), (tenant_b, "b")] {
        sqlx::query("INSERT INTO tenants (id,organization_id,name,slug,environment) VALUES ($1,$2,'Test',$3,'development')")
            .bind(tenant).bind(org).bind(format!("{slug}-{suffix}")).execute(&mut *tx).await.expect("tenant");
    }
    for (project, tenant, suffix) in [
        (project_a, tenant_a, "a"),
        (project_a2, tenant_a, "a2"),
        (project_b, tenant_b, "b"),
    ] {
        sqlx::query("INSERT INTO projects (id,tenant_id,organization_id,name,slug) VALUES ($1,$2,$3,'Test',$4)")
            .bind(project).bind(tenant).bind(org).bind(format!("{slug}-{suffix}")).execute(&mut *tx).await.expect("project");
    }
    sqlx::query("INSERT INTO users (id,email,display_name) VALUES ($1,$2,'Test')")
        .bind(user)
        .bind(format!("{slug}@example.test"))
        .execute(&mut *tx)
        .await
        .expect("user");
    sqlx::query("INSERT INTO data_sources (id,tenant_id,project_id,name,kind,status,created_by) VALUES ($1,$2,$3,'Valid','upload','active',$4)")
        .bind(source).bind(tenant_a).bind(project_a).bind(user).execute(&mut *tx).await.expect("valid source");
    sqlx::query("SAVEPOINT bad_source")
        .execute(&mut *tx)
        .await
        .expect("savepoint");
    let error = sqlx::query("INSERT INTO data_sources (id,tenant_id,project_id,name,kind,status,created_by) VALUES ($1,$2,$3,'Bad','upload','active',$4)")
        .bind(Uuid::new_v4()).bind(tenant_a).bind(project_b).bind(user).execute(&mut *tx).await.expect_err("cross-tenant source must fail");
    assert_eq!(
        error.as_database_error().and_then(|e| e.constraint()),
        Some("data_sources_tenant_project_fk")
    );
    sqlx::query("ROLLBACK TO SAVEPOINT bad_source")
        .execute(&mut *tx)
        .await
        .expect("rollback source");
    sqlx::query("SAVEPOINT bad_document")
        .execute(&mut *tx)
        .await
        .expect("savepoint");
    let error = sqlx::query("INSERT INTO documents (id,tenant_id,project_id,source_id,title,mime_type,size_bytes,content_sha256,created_by) VALUES ($1,$2,$3,$4,'Bad','text/plain',4,$5,$6)")
        .bind(Uuid::new_v4()).bind(tenant_a).bind(project_a2).bind(source).bind("a".repeat(64)).bind(user).execute(&mut *tx).await.expect_err("cross-project document must fail");
    assert_eq!(
        error.as_database_error().and_then(|e| e.constraint()),
        Some("documents_tenant_project_source_fk")
    );
    sqlx::query("ROLLBACK TO SAVEPOINT bad_document")
        .execute(&mut *tx)
        .await
        .expect("rollback document");
    sqlx::query("INSERT INTO documents (id,tenant_id,project_id,source_id,title,mime_type,size_bytes,content_sha256,created_by) VALUES ($1,$2,$3,$4,'Good','text/plain',4,$5,$6)")
        .bind(Uuid::new_v4()).bind(tenant_a).bind(project_a).bind(source).bind("a".repeat(64)).bind(user).execute(&mut *tx).await.expect("matching document");
    tx.rollback().await.expect("cleanup");
}
