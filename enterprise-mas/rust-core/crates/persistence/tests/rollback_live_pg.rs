//! PURPOSE: Live-PostgreSQL verification that agent rollback publishes a new
//! immutable version and that rollback history is append-only and tenant scoped.
//!
//! Owning layer: `mas-persistence` integration tests.
//!
//! Production responsibility: exercise the existing rollback and workflow
//! version repositories under the same forced-RLS runtime role used in service.
//!
//! Data ownership: test-only tenants and agent/version rows in the explicitly
//! configured live database.
//!
//! MUST NOT: mutate production data or treat an unscoped/migrator read as proof
//! of tenant isolation.

mod support;

use mas_application::stores::{AgentStorePort, RollbackStorePort};
use mas_common::enums::AgentStatus;
use mas_common::ids::{OrganizationId, ProjectId, TenantId, UserId};
use mas_domain::{
    Agent, AgentKind, AgentVersion, AuditActor, RollbackRecord, RollbackResourceKind, Slug,
};
use mas_persistence::repositories::agents::AgentVersionStore;
use mas_persistence::repositories::rollbacks::PostgresRollbackStore;
use mas_persistence::repositories::services::PgServices;
use mas_persistence::scoped_request::with_authoritative_scope;
use mas_security::AuthoritativeScope;
use sqlx::PgPool;

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
    .bind(format!("Rollback Test {label}"))
    .bind(format!("Rollback Test {label}"))
    .bind(format!("rollback-{label}-{suffix}"))
    .execute(connection.as_mut())
    .await
    .expect("organization seed");
    sqlx::query("INSERT INTO tenants (id, organization_id, name, slug, status, environment, isolation_mode) VALUES ($1, $2, $3, $4, 'active', 'development', 'shared_rls')")
        .bind(tenant.into_uuid()).bind(organization.into_uuid()).bind(format!("Rollback Test {label}"))
        .bind(format!("rollback-{label}-{suffix}")).execute(connection.as_mut()).await.expect("tenant seed");
    sqlx::query("INSERT INTO projects (id, tenant_id, organization_id, name, slug) VALUES ($1, $2, $3, $4, $5)")
        .bind(project.into_uuid()).bind(tenant.into_uuid()).bind(organization.into_uuid())
        .bind(format!("Rollback Project {label}")).bind(format!("rollback-{label}-{suffix}"))
        .execute(connection.as_mut()).await.expect("project seed");
    FixtureScope {
        organization,
        tenant,
        project,
    }
}

fn scope(fixture: FixtureScope) -> AuthoritativeScope {
    AuthoritativeScope::resolved(fixture.tenant, fixture.organization, "rollback-live-pg")
        .with_project(fixture.project)
}

#[tokio::test]
async fn rollback_appends_a_forward_version_and_hides_foreign_history() {
    let Some(migrator) = support::live_pool("rollback-live-pg-migrator").await else {
        return;
    };
    let Some(app_pool) = support::live_app_pool("rollback-live-pg-app").await else {
        eprintln!(
            "rollback_live_pg: skipping forced-RLS assertions (set MAS_TEST_DATABASE_APP_URL)"
        );
        return;
    };
    let tenant_a = seed_scope(&migrator, "a").await;
    let tenant_b = seed_scope(&migrator, "b").await;
    let agents = PgServices::new(app_pool.clone());
    let versions = AgentVersionStore::new(app_pool.clone());
    let rollbacks = PostgresRollbackStore::new(app_pool.clone());
    const CHECKSUM_1: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    const CHECKSUM_2: &str = "2222222222222222222222222222222222222222222222222222222222222222";
    const CHECKSUM_3: &str = "3333333333333333333333333333333333333333333333333333333333333333";

    let rollback_id = with_authoritative_scope(&app_pool, scope(tenant_a), || async {
        let mut agent = Agent::create(
            tenant_a.tenant,
            tenant_a.organization,
            tenant_a.project,
            "Rollback Test Agent",
            Slug::new(&format!("rollback-agent-{}", uuid::Uuid::now_v7().simple()))
                .expect("agent slug"),
            AgentKind::Standard,
        )
        .expect("agent aggregate");
        agent.status = AgentStatus::Active;
        agent.current_version = Some(2);
        AgentStorePort::save(&agents, &agent).await?;

        let published_by = UserId::new();
        let source = AgentVersion::publish(
            agent.id,
            1,
            CHECKSUM_1,
            serde_json::json!({"model":"source"}),
            published_by,
        )?;
        let current = AgentVersion::publish(
            agent.id,
            2,
            CHECKSUM_2,
            serde_json::json!({"model":"current"}),
            published_by,
        )?;
        versions.save(&source, tenant_a.tenant).await?;
        versions.save(&current, tenant_a.tenant).await?;

        let version_three = AgentVersion::publish(
            agent.id,
            3,
            CHECKSUM_3,
            source.configuration_snapshot().clone(),
            published_by,
        )?
        .as_rollback_of(source.id());
        agent.current_version = Some(3);
        let record = RollbackRecord::new(
            tenant_a.tenant,
            RollbackResourceKind::Agent,
            *agent.id.as_uuid(),
            2,
            3,
            AuditActor::system(),
            "restore immutable source configuration",
            format!("rollback-{}", uuid::Uuid::now_v7()),
        )?;
        RollbackStorePort::commit_agent_rollback(&rollbacks, &agent, &version_three, &record)
            .await?;

        let stored_versions = versions.list_for_agent(agent.id).await?;
        assert_eq!(
            stored_versions.len(),
            3,
            "rollback must append, not overwrite, a version"
        );
        let appended = stored_versions
            .iter()
            .find(|version| version.version_number() == 3)
            .expect("version 3");
        assert_eq!(appended.rollback_of(), Some(source.id()));
        assert!(
            versions.get(source.id()).await?.is_some(),
            "source snapshot remains immutable and readable"
        );

        assert!(
            RollbackStorePort::append(&rollbacks, &record)
                .await
                .is_err(),
            "duplicate idempotency key must not rewrite history"
        );
        let history = RollbackStorePort::list(&rollbacks, tenant_a.tenant, 50).await?;
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].from_version, 2);
        assert_eq!(history[0].to_version, 3);
        Ok(record.id)
    })
    .await
    .expect("tenant A rollback scope");

    with_authoritative_scope(&app_pool, scope(tenant_b), || async {
        assert!(
            RollbackStorePort::get(&rollbacks, rollback_id)
                .await?
                .is_none(),
            "foreign rollback id must read as not found"
        );
        assert!(RollbackStorePort::list(&rollbacks, tenant_b.tenant, 50)
            .await?
            .is_empty());
        Ok(())
    })
    .await
    .expect("tenant B rollback scope");
}
