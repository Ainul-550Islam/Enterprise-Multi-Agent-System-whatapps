//! PURPOSE: Live PostgreSQL proof for tenant usage persistence, bounded windows, RLS isolation, and filtered audit keyset reads.
//!
//! Owning layer: `mas-persistence` integration tests.
//! Production responsibility: exercise real PostgreSQL and the non-BYPASSRLS runtime role; skip only with a loud environment notice.
//! Data ownership: tests create isolated UUID-scoped fixtures and append-only facts in the configured test database.
//! MUST NOT: pass using the migrator role for RLS assertions, touch production databases, or conceal a missing runtime test URL.

#[path = "support/mod.rs"]
mod support;

use mas_application::{
    audit_query_service::{AuditPosition, AuditReadFilter, AuditReadPort},
    stores::UsageStorePort,
};
use mas_common::{
    ids::{OrganizationId, TenantId},
    timestamps::Timestamp,
};
use mas_domain::{
    quota::QuotaDimension,
    usage::{ActorKind, ActorRef, UsageRecord},
};
use mas_persistence::{
    repositories::{audit::PostgresAuditLog, usage::PostgresUsageStore},
    scoped_request::with_authoritative_scope,
};
use mas_security::AuthoritativeScope;
use sqlx::PgPool;
use std::time::Duration;

async fn fixture_scope(pool: &PgPool) -> (TenantId, OrganizationId) {
    let organization = OrganizationId::new();
    let tenant = TenantId::new();
    let suffix = uuid::Uuid::now_v7().simple().to_string();
    sqlx::query(
        "INSERT INTO organizations (id, legal_name, display_name, slug) VALUES ($1,$2,$3,$4)",
    )
    .bind(organization.into_uuid())
    .bind("Live test organization")
    .bind("Live test organization")
    .bind(format!("org-{suffix}"))
    .execute(pool)
    .await
    .expect("seed organization");
    sqlx::query("INSERT INTO tenants (id, organization_id, name, slug, status, environment, isolation_mode) VALUES ($1,$2,$3,$4,'active','development','shared_rls')")
        .bind(tenant.into_uuid()).bind(organization.into_uuid()).bind("Live test tenant").bind(format!("tenant-{suffix}"))
        .execute(pool).await.expect("seed tenant");
    (tenant, organization)
}

#[tokio::test]
async fn usage_append_windows_tenant_isolation_and_audit_keysets_use_live_postgres() {
    let Some(migrator) = support::live_pool("mas-usage-live-test").await else {
        return;
    };
    let Some(app) = support::live_app_pool("mas-usage-live-app-test").await else {
        eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nusage_live_pg: skipping RLS assertions (set MAS_TEST_DATABASE_APP_URL to a non-BYPASSRLS mas_app connection)\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        return;
    };
    let (tenant_a, organization_a) = fixture_scope(&migrator).await;
    let (tenant_b, organization_b) = fixture_scope(&migrator).await;
    let store = PostgresUsageStore::new(app.clone());
    let start = Timestamp::now()
        .checked_sub(Duration::from_secs(2))
        .expect("start");
    let end = Timestamp::now()
        .checked_add(Duration::from_secs(2))
        .expect("end");
    let mut included = UsageRecord::new(
        tenant_a,
        organization_a,
        None,
        ActorRef::new(ActorKind::User, "live-user-a").expect("actor"),
        QuotaDimension::Tokens,
        7,
        None,
    )
    .expect("usage");
    included.occurred_at = start;
    let mut boundary = UsageRecord::new(
        tenant_a,
        organization_a,
        None,
        ActorRef::new(ActorKind::User, "live-user-a").expect("actor"),
        QuotaDimension::Tokens,
        11,
        None,
    )
    .expect("usage");
    boundary.occurred_at = end;
    let foreign = UsageRecord::new(
        tenant_b,
        organization_b,
        None,
        ActorRef::new(ActorKind::User, "live-user-b").expect("actor"),
        QuotaDimension::Tokens,
        13,
        None,
    )
    .expect("usage");
    let scope_a = AuthoritativeScope::resolved(tenant_a, organization_a, "usage-live-test");
    with_authoritative_scope(&app, scope_a, || async {
        store.append(&included).await?;
        store.append(&boundary).await?;
        let window = store.list(tenant_a, start, end).await?;
        assert_eq!(window.len(), 1, "from is inclusive and to is exclusive");
        assert_eq!(window[0].quantity, 7);
        assert!(
            store.get(foreign.id).await?.is_none(),
            "foreign tenant id is hidden as absent"
        );
        let mut executor = mas_persistence::scoped_request::connection().await?;
        let mutation =
            sqlx::query("UPDATE usage_records SET quantity=99 WHERE tenant_id=$1 AND id=$2")
                .bind(tenant_a.into_uuid())
                .bind(included.id.into_uuid())
                .execute(executor.connection().as_mut())
                .await;
        assert!(
            mutation.is_err(),
            "the runtime role must not mutate append-only usage facts"
        );
        Ok(())
    })
    .await
    .expect("tenant A RLS transaction");
    let scope_b = AuthoritativeScope::resolved(tenant_b, organization_b, "usage-live-test-b");
    with_authoritative_scope(&app, scope_b, || async {
        assert!(
            store.get(included.id).await?.is_none(),
            "tenant B cannot observe tenant A usage"
        );
        store.append(&foreign).await?;
        Ok(())
    })
    .await
    .expect("tenant B RLS transaction");
    with_authoritative_scope(
        &app,
        AuthoritativeScope::resolved(tenant_a, organization_a, "usage-live-test-a-second"),
        || async {
            assert!(
                store.get(foreign.id).await?.is_none(),
                "tenant A cannot observe tenant B usage"
            );
            Ok(())
        },
    )
    .await
    .expect("reverse tenant isolation transaction");

    let event_a = uuid::Uuid::now_v7();
    let event_b = uuid::Uuid::now_v7();
    for (id, action, actor) in [
        (event_a, "usage.test.read", "live-user-a"),
        (event_b, "policy.test.publish", "live-user-a"),
    ] {
        sqlx::query("INSERT INTO audit_events (id, tenant_id, organization_id, actor, action, resource_type, outcome, correlation_id, metadata, occurred_at) VALUES ($1,$2,$3,$4,$5,'usage','success',$6,'{}'::jsonb,$7)")
            .bind(id).bind(tenant_a.into_uuid()).bind(organization_a.into_uuid()).bind(serde_json::json!({"kind":"user","id":actor}))
            .bind(action).bind(format!("audit-{id}")).bind(start.into_datetime())
            .execute(&migrator).await.expect("seed audit row");
    }
    let audit = PostgresAuditLog::new(app.clone());
    let scope = AuthoritativeScope::resolved(tenant_a, organization_a, "audit-live-test");
    with_authoritative_scope(&app, scope, || async {
        let filter = AuditReadFilter {
            actor: Some("live-user-a".to_owned()),
            action: Some("usage.test".to_owned()),
            ..AuditReadFilter::default()
        };
        let first = audit.read_page(tenant_a, &filter, None, 1).await?;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].action, "usage.test.read");
        let after = AuditPosition {
            occurred_at_ms: first[0].occurred_at.to_unix_ms(),
            id: first[0].id.to_string(),
        };
        let next = audit.read_page(tenant_a, &filter, Some(&after), 10).await?;
        assert!(
            next.is_empty(),
            "filtered keyset query must not leak an adjacent policy event"
        );
        Ok(())
    })
    .await
    .expect("audit RLS transaction");
}
