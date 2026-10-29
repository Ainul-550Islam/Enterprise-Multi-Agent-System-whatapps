//! PURPOSE: Live-PostgreSQL checks for hourly execution rollups, exact window
//! boundaries, and tenant-isolated analytics reads.
//!
//! Owning layer: `mas-persistence` integration tests.
//!
//! Production responsibility: exercise the existing rollup refresh function
//! and analytics repository against real RLS-enforced PostgreSQL roles.
//!
//! Data ownership: generated test tenants and execution/usage rows in the
//! explicitly configured live test database.
//!
//! MUST NOT: run against production or claim runtime isolation with a
//! BYPASSRLS/migrator-only role.

mod support;

use mas_application::stores::{AgentStorePort, AnalyticsStorePort, ExecutionStorePort};
use mas_common::ids::{OrganizationId, ProjectId, TenantId};
use mas_common::timestamps::Timestamp;
use mas_domain::value_objects::Slug;
use mas_domain::{Agent, AgentKind, Execution, TransitionActor, TransitionFailure};
use mas_persistence::repositories::analytics::PostgresAnalyticsStore;
use mas_persistence::repositories::services::PgServices;
use mas_persistence::scoped_request::with_authoritative_scope;
use mas_security::AuthoritativeScope;
use sqlx::{PgPool, Row};

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
    .bind(format!("Analytics Test {label}"))
    .bind(format!("Analytics Test {label}"))
    .bind(format!("analytics-{label}-{suffix}"))
    .execute(connection.as_mut())
    .await
    .expect("organization seed");
    sqlx::query("INSERT INTO tenants (id, organization_id, name, slug, status, environment, isolation_mode) VALUES ($1, $2, $3, $4, 'active', 'development', 'shared_rls')")
        .bind(tenant.into_uuid()).bind(organization.into_uuid()).bind(format!("Analytics Test {label}"))
        .bind(format!("analytics-{label}-{suffix}")).execute(connection.as_mut()).await.expect("tenant seed");
    sqlx::query("INSERT INTO projects (id, tenant_id, organization_id, name, slug) VALUES ($1, $2, $3, $4, $5)")
        .bind(project.into_uuid()).bind(tenant.into_uuid()).bind(organization.into_uuid())
        .bind(format!("Analytics Project {label}")).bind(format!("analytics-{label}-{suffix}"))
        .execute(connection.as_mut()).await.expect("project seed");
    FixtureScope {
        organization,
        tenant,
        project,
    }
}

fn scope(fixture: FixtureScope) -> AuthoritativeScope {
    AuthoritativeScope::resolved(fixture.tenant, fixture.organization, "analytics-live-pg")
        .with_project(fixture.project)
}

#[tokio::test]
async fn hourly_rollups_honor_windows_and_kpi_reads_are_tenant_scoped() {
    let Some(migrator) = support::live_pool("analytics-live-pg-migrator").await else {
        return;
    };
    let Some(app_pool) = support::live_app_pool("analytics-live-pg-app").await else {
        eprintln!(
            "analytics_live_pg: skipping forced-RLS assertions (set MAS_TEST_DATABASE_APP_URL)"
        );
        return;
    };
    let tenant_a = seed_scope(&migrator, "a").await;
    let tenant_b = seed_scope(&migrator, "b").await;
    let base: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT date_trunc('hour', now())")
            .fetch_one(&migrator)
            .await
            .expect("hour boundary");
    let end = base + chrono::Duration::hours(2);

    let services = PgServices::new(app_pool.clone());
    let mut analytics_agents = Vec::new();
    for (fixture, label) in [(tenant_a, "a"), (tenant_b, "b")] {
        let agent_id = with_authoritative_scope(&app_pool, scope(fixture), || async {
            let agent = Agent::create(
                fixture.tenant,
                fixture.organization,
                fixture.project,
                format!("Analytics Agent {label}"),
                Slug::new(&format!(
                    "analytics-agent-{label}-{}",
                    uuid::Uuid::now_v7().simple()
                ))?,
                AgentKind::Standard,
            )?;
            AgentStorePort::save(&services, &agent).await?;
            let actor = TransitionActor::system("analytics-live-pg")?;
            let executions = if label == "a" {
                vec![
                    ("completed", 9_i64, 10_i64),
                    ("failed", 19_i64, 20_i64),
                    ("completed", 64_i64, 65_i64),
                    ("completed", 119_i64, 120_i64),
                ]
            } else {
                vec![("completed", 9_i64, 10_i64)]
            };
            for (status, start_offset, finish_offset) in executions {
                let mut execution = Execution::new(
                    fixture.tenant,
                    fixture.organization,
                    fixture.project,
                    None,
                    Some(agent.id),
                    serde_json::json!({"query":"analytics fixture"}),
                    format!("analytics-{}-{}", label, uuid::Uuid::now_v7()),
                    "analytics-live-pg",
                )?;
                execution.start(&actor)?;
                if status == "completed" {
                    execution.complete(&actor, Some(serde_json::json!({"ok":true})))?;
                } else {
                    execution.fail(
                        &actor,
                        TransitionFailure::terminal("test_failure", "analytics fixture failure")?,
                    )?;
                }
                execution.started_at = Some(Timestamp::from_datetime(
                    base + chrono::Duration::minutes(start_offset),
                ));
                execution.finished_at = Some(Timestamp::from_datetime(
                    base + chrono::Duration::minutes(finish_offset),
                ));
                execution.created_at =
                    Timestamp::from_datetime(base + chrono::Duration::minutes(start_offset));
                execution.updated_at = execution.finished_at.expect("finish set");
                ExecutionStorePort::save(&services, &execution).await?;
            }
            Ok(agent.id)
        })
        .await
        .expect("seed scoped execution aggregates");
        analytics_agents.push((fixture.tenant, agent_id));
    }

    for (fixture, minute, quantity, cost, key) in [
        (tenant_a, 20_i64, 40_i64, 300_i64, "a-bucket-one"),
        (tenant_a, 70_i64, 7_i64, 70_i64, "a-bucket-two"),
        (tenant_b, 20_i64, 900_i64, 900_i64, "b-bucket-one"),
    ] {
        let agent_id = analytics_agents
            .iter()
            .find(|(tenant, _)| *tenant == fixture.tenant)
            .map(|(_, agent_id)| agent_id)
            .expect("tenant analytics agent");
        let dimensions = serde_json::json!({"agent_id": agent_id.to_string()});
        sqlx::query("INSERT INTO usage_records (tenant_id, organization_id, project_id, resource, quantity, unit, dimensions, occurred_at, cost_micros, metering_key) VALUES ($1, $2, $3, 'tokens', $4, 'tokens', $5, $6, $7, $8)")
            .bind(fixture.tenant.into_uuid()).bind(fixture.organization.into_uuid())
            .bind(fixture.project.into_uuid()).bind(quantity).bind(dimensions)
            .bind(base + chrono::Duration::minutes(minute)).bind(cost).bind(key)
            .execute(&migrator).await.expect("usage seed");
    }

    let affected: i64 = sqlx::query_scalar("SELECT refresh_execution_rollups_hourly($1, $2, $3)")
        .bind(tenant_a.tenant.into_uuid())
        .bind(base)
        .bind(end)
        .fetch_one(&migrator)
        .await
        .expect("refresh rollup");
    assert_eq!(
        affected, 2,
        "only populated hourly buckets inside the half-open range are refreshed"
    );

    let store = PostgresAnalyticsStore::new(app_pool.clone());
    let snapshot_a = with_authoritative_scope(&app_pool, scope(tenant_a), || async {
        let snapshot = store.snapshot(tenant_a.tenant, Timestamp::from_datetime(base), Timestamp::from_datetime(end)).await?;
        assert_eq!(snapshot.executions.len(), 3, "finish exactly at window_end is excluded");
        assert_eq!(snapshot.usage_records.len(), 2);
        let mut scoped = mas_persistence::scoped_request::connection().await?;
        let rows = sqlx::query("SELECT bucket, tasks_completed, tasks_failed, tokens, cost_micros, p95_ms FROM execution_rollups_hourly WHERE tenant_id = $1 ORDER BY bucket")
            .bind(tenant_a.tenant.into_uuid()).fetch_all(scoped.connection().as_mut()).await
            .map_err(|error| mas_common::error::AppError::database(error.to_string()))?;
        assert_eq!(rows.len(), 2);
        let first = &rows[0];
        assert_eq!(first.try_get::<i64, _>("tasks_completed").expect("completed"), 1);
        assert_eq!(first.try_get::<i64, _>("tasks_failed").expect("failed"), 1);
        assert_eq!(first.try_get::<i64, _>("tokens").expect("tokens"), 40);
        assert_eq!(first.try_get::<i64, _>("cost_micros").expect("cost"), 300);
        assert_eq!(first.try_get::<i64, _>("p95_ms").expect("p95"), 60_000);
        let second = &rows[1];
        assert_eq!(second.try_get::<i64, _>("tasks_completed").expect("second bucket"), 1);
        assert_eq!(second.try_get::<i64, _>("tokens").expect("second bucket tokens"), 7);
        assert_eq!(second.try_get::<i64, _>("cost_micros").expect("second bucket cost"), 70);
        Ok(snapshot)
    }).await.expect("tenant A analytics scope");
    assert_eq!(snapshot_a.executions.len(), 3);

    let snapshot_b = with_authoritative_scope(&app_pool, scope(tenant_b), || async {
        let snapshot = store
            .snapshot(
                tenant_b.tenant,
                Timestamp::from_datetime(base),
                Timestamp::from_datetime(end),
            )
            .await?;
        assert_eq!(snapshot.executions.len(), 1);
        assert_eq!(snapshot.usage_records.len(), 1);
        let mut scoped = mas_persistence::scoped_request::connection().await?;
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM execution_rollups_hourly WHERE tenant_id = $1",
        )
        .bind(tenant_a.tenant.into_uuid())
        .fetch_one(scoped.connection().as_mut())
        .await
        .map_err(|error| mas_common::error::AppError::database(error.to_string()))?;
        assert_eq!(count, 0, "tenant B must not see tenant A's rollup rows");
        Ok(snapshot)
    })
    .await
    .expect("tenant B analytics scope");
    assert_eq!(snapshot_b.executions.len(), 1);
}
