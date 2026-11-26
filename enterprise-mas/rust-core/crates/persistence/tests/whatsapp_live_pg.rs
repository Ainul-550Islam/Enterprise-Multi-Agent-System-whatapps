//! PURPOSE: Live-Postgres WhatsApp repository suite (channel ownership,
//! wamid idempotency, cross_tenant read refusal).
//!
//! Owning layer: persistence integration tests.
//!
//! Production responsibility: prove the repository behaves as the service
//! contract claims against a real database with RLS enforced — dedup on
//! `wamid` (provider redeliveries must not double-insert) and tenant hiding
//! on every read path.
//!
//! Must NOT: run without `MAS_TEST_DATABASE_URL` (it skips cleanly instead),
//! or claim in-memory coverage as live coverage.
//!
//! Gated by `MAS_TEST_DATABASE_URL` (migrator role) and
//! `MAS_TEST_DATABASE_APP_URL` (RLS app role) — see `tests/support/mod.rs`.

mod support;

use mas_common::enums::Environment;
use mas_common::ids::{OrganizationId, TenantId};
use mas_common::timestamps::Timestamp;
use mas_domain::value_objects::Slug;
use mas_domain::whatsapp::{
    WhatsAppChannel, WhatsAppChannelStatus, WhatsAppDirection, WhatsAppMessage,
    WhatsAppMessageStatus, WhatsAppMessageType,
};
use mas_domain::{IsolationMode, Organization, Tenant};
use mas_persistence::repositories::tenancy::{OrganizationStore, TenantStore};
use mas_persistence::repositories::whatsapp::PostgresWhatsAppStore;

struct Scope {
    organization: Organization,
    tenant: Tenant,
}

async fn seed_scope(pool: &sqlx::PgPool, label: &str) -> Scope {
    let mut conn = pool.acquire().await.expect("connection");
    let suffix = &uuid::Uuid::now_v7().to_string().replace('-', "");
    let organization = Organization::create(
        format!("Org {label}"),
        format!("Org ({label})"),
        Slug::new(&format!("org-{label}-{suffix}")).expect("org slug"),
    )
    .expect("organization");
    OrganizationStore::insert_in(&mut conn, &organization)
        .await
        .expect("org insert");
    let tenant = Tenant::create(
        organization.id,
        format!("{label} tenant"),
        Slug::new(&format!("tenant-{label}-{suffix}")).expect("tenant slug"),
        Environment::Development,
        IsolationMode::SharedRls,
    )
    .expect("tenant");
    TenantStore::insert_in(&mut conn, &tenant)
        .await
        .expect("tenant insert");
    Scope {
        organization,
        tenant,
    }
}

async fn scoped<T, F, Fut>(
    pool: sqlx::PgPool,
    tenant: TenantId,
    organization: OrganizationId,
    work: F,
) -> T
where
    F: FnOnce(sqlx::PgPool) -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let authoritative =
        mas_security::AuthoritativeScope::resolved(tenant, organization, "whatsapp-live-pg-test");
    let inner = pool.clone();
    mas_persistence::scoped_request::with_authoritative_scope(
        &pool,
        authoritative,
        move || async move { Ok(work(inner).await) },
    )
    .await
    .expect("scoped transaction")
}

fn channel(tenant: TenantId, phone: &str, name: &str) -> WhatsAppChannel {
    WhatsAppChannel {
        id: mas_common::ids::WhatsAppChannelId::new(),
        tenant_id: tenant,
        display_name: name.to_owned(),
        phone_number_id: phone.to_owned(),
        waba_id: Some(format!("waba-{phone}")),
        api_version: "v21.0".to_owned(),
        status: WhatsAppChannelStatus::Active,
        verify_token_ref: "ref://whatsapp/verify".to_owned(),
        created_at: Timestamp::now(),
        updated_at: Timestamp::now(),
    }
}

fn message(
    tenant: TenantId,
    channel: mas_common::ids::WhatsAppChannelId,
    wamid: &str,
) -> WhatsAppMessage {
    WhatsAppMessage {
        id: mas_common::ids::WhatsAppMessageId::new(),
        tenant_id: tenant,
        channel_id: channel,
        direction: WhatsAppDirection::Inbound,
        wamid: Some(wamid.to_owned()),
        from_addr: Some("+15550001".to_owned()),
        to_addr: Some("+15550002".to_owned()),
        message_type: WhatsAppMessageType::Text,
        body_text: Some("hello".to_owned()),
        status: WhatsAppMessageStatus::Received,
        payload: serde_json::json!({"source": "whatsapp-live-pg"}),
        occurred_at: Timestamp::now(),
        created_at: Timestamp::now(),
        updated_at: Timestamp::now(),
    }
}

#[tokio::test]
async fn record_message_is_idempotent_per_wamid() {
    let Some(pool) = support::live_pool("whatsapp-wamid").await else {
        return;
    };
    let scope = seed_scope(&pool, "wamid").await;
    let suffix = uuid::Uuid::now_v7().to_string().replace('-', "");
    let row = scoped(pool.clone(), scope.tenant.id, scope.organization.id, {
        let phone = format!("pn-wamid-{suffix}");
        move |p| async move {
            let store = PostgresWhatsAppStore::new(p);
            store
                .create_channel(&channel(scope.tenant.id, &phone, "wamid line"))
                .await
                .expect("channel")
        }
    })
    .await;

    let wamid = format!("wamid.REDLIVERY-{suffix}");
    let msg = message(scope.tenant.id, row.id, &wamid);
    // first delivery records; the provider redelivery must not double-insert
    scoped(pool.clone(), scope.tenant.id, scope.organization.id, {
        let msg = msg.clone();
        move |p| async move {
            let store = PostgresWhatsAppStore::new(p);
            let (recorded, fresh) = store.record_message(&msg).await.expect("first record");
            assert!(fresh, "first delivery must be recorded");
            assert_eq!(recorded.id, msg.id);

            let redelivery = WhatsAppMessage {
                id: mas_common::ids::WhatsAppMessageId::new(),
                ..msg.clone()
            };
            let (again, fresh) = store.record_message(&redelivery).await.expect("redelivery");
            assert!(!fresh, "a wamid redelivery must not be a fresh row");
            assert_eq!(again.id, msg.id, "redelivery resolves to the original row");

            let by_wamid = store
                .get_message_by_wamid(scope.tenant.id, row.id, &wamid)
                .await
                .expect("lookup")
                .expect("row exists");
            assert_eq!(by_wamid.id, msg.id);
        }
    })
    .await;
}

#[tokio::test]
async fn message_gets_are_cross_tenant_hidden() {
    let Some(pool) = support::live_pool("whatsapp-x-tenant").await else {
        return;
    };
    let a = seed_scope(&pool, "wa-x-a").await;
    let b = seed_scope(&pool, "wa-x-b").await;
    let suffix = uuid::Uuid::now_v7().to_string().replace('-', "");
    let row = scoped(pool.clone(), a.tenant.id, a.organization.id, {
        let phone = format!("pn-x-{suffix}");
        move |p| async move {
            let store = PostgresWhatsAppStore::new(p);
            store
                .create_channel(&channel(a.tenant.id, &phone, "A"))
                .await
                .expect("channel")
        }
    })
    .await;
    let wamid = format!("wamid.X-{suffix}");
    let msg = message(a.tenant.id, row.id, &wamid);
    scoped(pool.clone(), a.tenant.id, a.organization.id, {
        let msg = msg.clone();
        move |p| async move {
            let store = PostgresWhatsAppStore::new(p);
            store.record_message(&msg).await.expect("record");
        }
    })
    .await;

    scoped(pool.clone(), b.tenant.id, b.organization.id, {
        let msg = msg.clone();
        let wamid = wamid.clone();
        move |p| async move {
            let store = PostgresWhatsAppStore::new(p);
            assert!(
                store
                    .get_message(b.tenant.id, msg.id)
                    .await
                    .expect("get")
                    .is_none(),
                "tenant B must not read tenant A's message by id"
            );
            assert!(
                store
                    .get_message_by_wamid(b.tenant.id, msg.channel_id, &wamid)
                    .await
                    .expect("get by wamid")
                    .is_none(),
                "tenant B must not read tenant A's message by wamid"
            );
        }
    })
    .await;
}

#[tokio::test]
async fn channel_owner_resolves_by_phone_number() {
    let Some(pool) = support::live_pool("whatsapp-owner").await else {
        return;
    };
    let scope = seed_scope(&pool, "owner").await;
    let suffix = uuid::Uuid::now_v7().to_string().replace('-', "");
    let phone = format!("pn-owner-{suffix}");
    let tenant_id = scope.tenant.id;
    let organization_id = scope.organization.id;
    scoped(pool.clone(), tenant_id, organization_id, {
        let phone = phone.clone();
        move |p| async move {
            PostgresWhatsAppStore::new(p)
                .create_channel(&channel(tenant_id, &phone, "owner line"))
                .await
                .expect("channel");
        }
    })
    .await;

    // Inbound webhook ownership resolution is deliberately pre-scope, so it
    // runs after the channel transaction commits and on the separate resolver
    // connection rather than reading its own uncommitted insert.
    let resolver = PostgresWhatsAppStore::new(pool);
    let owner = resolver
        .resolve_owner_by_phone_number(&phone)
        .await
        .expect("resolve");
    assert_eq!(
        owner,
        Some((tenant_id, organization_id)),
        "phone handle must resolve to the owning tenant"
    );
    let missing = resolver
        .resolve_owner_by_phone_number("pn-does-not-exist")
        .await
        .expect("resolve missing");
    assert!(missing.is_none(), "unknown handles resolve to nothing");
}
