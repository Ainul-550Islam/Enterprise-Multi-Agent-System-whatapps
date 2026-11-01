//! PURPOSE: Live PostgreSQL round trip for `PgServices` project listing (`ProjectStorePort::list`), the exact first store read the built-in assistant pipeline performs.
//!
//! Owning layer: `mas-persistence` (tests).
//! Production responsibility: prove against a real database that a
//! tenant-scoped project list decodes every column (citext slug, jsonb
//! config, nullable description — including a row left at the schema
//! default `'{}'::jsonb`) and returns only rows visible under the armed
//! scope — the store path `POST /v1/assistant/messages` hits before it
//! touches agents or executions. The cross-tenant half runs on the app pool
//! so row-level security is actually exercised (a superuser pool bypasses
//! RLS by definition, and asserting isolation there would prove nothing).
//! Data ownership: fixtures are seeded by this file against the migrator
//! pool; no production data is read.
//! MUST NOT: run without `MAS_TEST_DATABASE_URL` (it skips cleanly instead);
//! assert on rows the seed did not create; claim an RLS guarantee from a
//! superuser connection.

mod support;

use mas_application::stores::ProjectStorePort;
use mas_common::ids::{OrganizationId, ProjectId, TenantId};
use mas_persistence::repositories::services::PgServices;
use mas_persistence::scoped_request::with_authoritative_scope;
use mas_security::AuthoritativeScope;
use sqlx::PgPool;

/// The org → tenant → project fixture this file seeds.
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
    .bind(format!("ProjectList Test {label}"))
    .bind(format!("ProjectList Test {label}"))
    .bind(format!("project-list-{label}-{suffix}"))
    .execute(connection.as_mut())
    .await
    .expect("organization seed");
    sqlx::query("INSERT INTO tenants (id, organization_id, name, slug, status, environment, isolation_mode) VALUES ($1, $2, $3, $4, 'active', 'development', 'shared_rls')")
        .bind(tenant.into_uuid()).bind(organization.into_uuid()).bind(format!("ProjectList Test {label}"))
        .bind(format!("project-list-{label}-{suffix}")).execute(connection.as_mut()).await.expect("tenant seed");
    // Note: no `config` column — the row keeps the schema default
    // `'{}'::jsonb`, which is exactly the hydration edge this test pins.
    sqlx::query("INSERT INTO projects (id, tenant_id, organization_id, name, slug) VALUES ($1, $2, $3, $4, $5)")
        .bind(project.into_uuid()).bind(tenant.into_uuid()).bind(organization.into_uuid())
        .bind(format!("ProjectList Project {label}")).bind(format!("project-list-{label}-{suffix}"))
        .execute(connection.as_mut()).await.expect("project seed");
    FixtureScope {
        organization,
        tenant,
        project,
    }
}

fn scope(fixture: FixtureScope) -> AuthoritativeScope {
    AuthoritativeScope::resolved(fixture.tenant, fixture.organization, "project-list-live-pg")
        .with_project(fixture.project)
}

#[tokio::test]
async fn project_list_round_trips_in_scope_and_stays_tenant_bound() {
    let Some(pool) = support::live_pool("project-list-live-pg").await else {
        return;
    };

    let fixture = seed_scope(&pool, "a").await;
    let stranger = seed_scope(&pool, "b").await;
    let services = PgServices::new(pool.clone());

    // The same shape as production: one RLS-armed transaction, the project
    // store reading through the scoped connection. The seeded row keeps the
    // schema-default config `'{}'`, so this also proves a default-config row
    // hydrates (the assistant pipeline lists projects before anything else).
    let projects = with_authoritative_scope(&pool, scope(fixture), || async {
        ProjectStorePort::list(&services, fixture.tenant).await
    })
    .await
    .expect("tenant-scoped project list must decode against real PostgreSQL");

    assert_eq!(
        projects.len(),
        1,
        "the scoped tenant has exactly the seeded project, got: {projects:?}"
    );
    let listed = &projects[0];
    assert_eq!(listed.id, fixture.project);
    assert_eq!(listed.tenant_id, fixture.tenant);
    assert_eq!(listed.organization_id, fixture.organization);
    assert_eq!(listed.name, "ProjectList Project a");
    assert_eq!(
        listed.config.default_resource_limits,
        mas_domain::value_objects::ResourceLimits::default(),
        "schema-default config hydrates to the documented default limits"
    );
    assert!(listed.config.labels.is_empty());

    // Cross-tenant denial is an RLS property, so it must run on the app pool:
    // a superuser migrator connection bypasses row-level security and would
    // make the assertion vacuously pass or misleadingly fail.
    let Some(app_pool) = support::live_app_pool("project-list-live-pg-app").await else {
        eprintln!(
            "project_list_live_pg: skipping forced-RLS assertion (set MAS_TEST_DATABASE_APP_URL)"
        );
        return;
    };
    let app_services = PgServices::new(app_pool.clone());

    // Positive control on the app pool: the owning scope still reads its row.
    let owned = with_authoritative_scope(&app_pool, scope(fixture), || async {
        ProjectStorePort::list(&app_services, fixture.tenant).await
    })
    .await
    .expect("app-pool tenant-scoped project list");
    assert_eq!(
        owned.len(),
        1,
        "the owning scope must read its project through the app pool"
    );

    // Another tenant's scope must not observe this tenant's project: both the
    // scope GUC and the query predicate point at the stranger's tenant while
    // the row belongs to `fixture`.
    let foreign = with_authoritative_scope(&app_pool, scope(stranger), || async {
        ProjectStorePort::list(&app_services, fixture.tenant).await
    })
    .await
    .expect("foreign-scoped project list query");
    assert!(
        foreign.is_empty(),
        "another tenant's scope must never see this tenant's project, got: {foreign:?}"
    );
}
