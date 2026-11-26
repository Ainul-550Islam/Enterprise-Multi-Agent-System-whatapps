//! PURPOSE: The live-PostgreSQL half of the cross-tenant guarantee for the
//! WhatsApp and billing boundary tables (migrations 0055-0061).
//!
//! Owning layer: `mas-persistence` (tests).
//!
//! Production responsibility: prove against a real database what the static
//! contract test can only assert about source text:
//!
//!   1. tenant B cannot see, read or update tenant A's rows in any boundary
//!      table, under FORCE RLS with the `app.current_tenant` policy;
//!   2. a row's `tenant_id` is immutable (0061's trigger) -- no UPDATE can
//!      migrate a row between tenants;
//!   3. the pre-scope SECURITY DEFINER resolvers (0055/0057) map a provider
//!      handle to exactly its owning tenant, and to nothing else;
//!   4. one provider customer / phone number cannot be claimed by two
//!      tenants, and an event application claim is exactly-once per tenant.
//!
//! It must NOT:
//!   * run without `MAS_TEST_DATABASE_URL` (it skips loudly instead);
//!   * be pointed at a production database;
//!   * weaken an isolation assertion to accommodate a convenience fixture.
//!
//! Live PostgreSQL suite for the boundary-table isolation contract.
//!
//! Gated by `MAS_TEST_DATABASE_URL` (the owner/migrator role). The suite
//! applies the full migration set itself and seeds its own fixtures.

mod support;

use mas_common::enums::Environment;
use mas_common::ids::{OrganizationId, PlanId, TenantId};
use mas_common::timestamps::Timestamp;
use mas_domain::billing::{
    BillingCustomer, CheckoutSessionRecord, CheckoutState, EventApplication,
};
use mas_domain::value_objects::Slug;
use mas_domain::whatsapp::{
    WhatsAppChannel, WhatsAppChannelStatus, WhatsAppDirection, WhatsAppMessage,
    WhatsAppMessageStatus, WhatsAppMessageType,
};
use mas_domain::{IsolationMode, Organization, Tenant};
use mas_persistence::repositories::billing_checkout::{
    PostgresCheckoutSessionStore, PostgresEventApplicationStore,
};
use mas_persistence::repositories::billing_customers::PostgresBillingCustomerStore;
use mas_persistence::repositories::tenancy::{OrganizationStore, TenantStore};
use mas_persistence::repositories::whatsapp::PostgresWhatsAppStore;

/// The org -> tenant fixture, seeded through the tenancy stores.
struct Scope {
    organization: Organization,
    tenant: Tenant,
}

/// Seeds one organization and one tenant with fresh slugs.
async fn seed_scope(pool: &sqlx::PgPool, label: &str) -> Scope {
    let mut conn = pool.acquire().await.expect("connection");
    let suffix = &uuid::Uuid::now_v7().to_string().replace('-', "");
    let organization = Organization::create(
        format!("Acme {label}"),
        format!("Acme ({label})"),
        Slug::new(&format!("acme-{label}-{suffix}")).expect("org slug"),
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

/// Runs `work` inside one RLS-armed transaction for `tenant`, exactly as
/// production does.
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
        mas_security::AuthoritativeScope::resolved(tenant, organization, "cross-tenant-pg-test");
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
        waba_id: None,
        api_version: "v21.0".to_owned(),
        status: WhatsAppChannelStatus::Active,
        verify_token_ref: format!("env:MAS_WA_VERIFY_{phone}"),
        created_at: Timestamp::now(),
        updated_at: Timestamp::now(),
    }
}

fn customer(tenant: TenantId, handle: &str) -> BillingCustomer {
    BillingCustomer {
        id: mas_common::ids::BillingCustomerId::new(),
        tenant_id: tenant,
        provider: "stripe".to_owned(),
        provider_customer_id: handle.to_owned(),
        email: None,
        created_at: Timestamp::now(),
        updated_at: Timestamp::now(),
    }
}

fn checkout(tenant: TenantId, key: &str) -> CheckoutSessionRecord {
    CheckoutSessionRecord {
        id: mas_common::ids::CheckoutSessionId::new(),
        tenant_id: tenant,
        idempotency_key: key.to_owned(),
        plan_id: PlanId::new(),
        plan_code: "plan_pro".to_owned(),
        plan_price_id: None,
        provider: "stripe".to_owned(),
        provider_session_id: None,
        state: CheckoutState::Created,
        success_url: "https://example.test/ok".to_owned(),
        cancel_url: "https://example.test/cancel".to_owned(),
        created_at: Timestamp::now(),
        updated_at: Timestamp::now(),
    }
}

fn claim(tenant: TenantId, event_id: &str) -> EventApplication {
    EventApplication {
        id: mas_common::ids::EventApplicationId::new(),
        tenant_id: tenant,
        provider: "stripe".to_owned(),
        event_id: event_id.to_owned(),
        receipt_id: None,
        event_type: "checkout.session.completed".to_owned(),
        effect: "test_effect".to_owned(),
        applied_at: Timestamp::now(),
    }
}

#[tokio::test]
async fn whatsapp_rows_are_invisible_across_tenants() {
    let Some(pool) = support::live_pool("cross-tenant-whatsapp").await else {
        return;
    };
    let a = seed_scope(&pool, "wa-a").await;
    let b = seed_scope(&pool, "wa-b").await;
    let suffix = &uuid::Uuid::now_v7().to_string().replace('-', "");
    let phone_a = format!("pn-a{suffix}");
    let phone_b = format!("pn-b{suffix}");

    let row_a = scoped(pool.clone(), a.tenant.id, a.organization.id, {
        let phone = phone_a.clone();
        move |p| async move {
            let store = PostgresWhatsAppStore::new(p);
            store
                .create_channel(&channel(a.tenant.id, &phone, "A"))
                .await
                .expect("channel A")
        }
    })
    .await;
    let row_b = scoped(pool.clone(), b.tenant.id, b.organization.id, {
        let phone = phone_b.clone();
        move |p| async move {
            let store = PostgresWhatsAppStore::new(p);
            store
                .create_channel(&channel(b.tenant.id, &phone, "B"))
                .await
                .expect("channel B")
        }
    })
    .await;

    // Tenant A sees only its own channel.
    scoped(pool.clone(), a.tenant.id, a.organization.id, {
        let row_a = row_a.clone();
        let row_b = row_b.clone();
        move |p| async move {
            let store = PostgresWhatsAppStore::new(p);
            let list = store.list_channels(a.tenant.id).await.expect("list A");
            assert_eq!(list.len(), 1, "tenant A must see exactly its own channel");
            assert_eq!(list[0].id, row_a.id);
            assert!(
                store
                    .get_channel(a.tenant.id, row_b.id)
                    .await
                    .expect("get")
                    .is_none(),
                "tenant A must not read tenant B's channel by id"
            );
        }
    })
    .await;

    // Tenant B cannot update tenant A's channel.
    scoped(pool.clone(), b.tenant.id, b.organization.id, {
        let row_a = row_a.clone();
        move |p| async move {
            let store = PostgresWhatsAppStore::new(p);
            let updated = store
                .set_channel_status(b.tenant.id, row_a.id, WhatsAppChannelStatus::Paused)
                .await
                .expect("update attempt");
            assert!(
                updated.is_none(),
                "tenant B must not update tenant A's channel"
            );
        }
    })
    .await;

    // A message row follows its channel's tenant.
    let message = WhatsAppMessage {
        id: mas_common::ids::WhatsAppMessageId::new(),
        tenant_id: a.tenant.id,
        channel_id: row_a.id,
        direction: WhatsAppDirection::Inbound,
        wamid: Some(format!("wamid.{suffix}")),
        from_addr: Some("+15550001".to_owned()),
        to_addr: Some("+15550002".to_owned()),
        message_type: WhatsAppMessageType::Text,
        body_text: Some("hello".to_owned()),
        status: WhatsAppMessageStatus::Received,
        payload: serde_json::json!({"untrusted": true}),
        occurred_at: Timestamp::now(),
        created_at: Timestamp::now(),
        updated_at: Timestamp::now(),
    };
    scoped(pool.clone(), a.tenant.id, a.organization.id, {
        let message = message.clone();
        move |p| async move {
            let store = PostgresWhatsAppStore::new(p);
            let (recorded, fresh) = store.record_message(&message).await.expect("record");
            assert!(fresh, "the first delivery must be recorded");
            assert_eq!(recorded.id, message.id);
        }
    })
    .await;
    scoped(pool.clone(), b.tenant.id, b.organization.id, {
        let message = message.clone();
        move |p| async move {
            let store = PostgresWhatsAppStore::new(p);
            assert!(
                store
                    .get_message(b.tenant.id, message.id)
                    .await
                    .expect("get")
                    .is_none(),
                "tenant B must not read tenant A's message by id"
            );
        }
    })
    .await;
}

#[tokio::test]
async fn a_phone_number_cannot_be_claimed_by_two_tenants() {
    let Some(pool) = support::live_pool("cross-tenant-phone").await else {
        return;
    };
    let a = seed_scope(&pool, "pn-a").await;
    let b = seed_scope(&pool, "pn-b").await;
    let suffix = &uuid::Uuid::now_v7().to_string().replace('-', "");
    let phone = format!("pn-unique{suffix}");

    scoped(pool.clone(), a.tenant.id, a.organization.id, {
        let phone = phone.clone();
        move |p| async move {
            let store = PostgresWhatsAppStore::new(p);
            store
                .create_channel(&channel(a.tenant.id, &phone, "A"))
                .await
                .expect("first claim");
        }
    })
    .await;

    let conflict = scoped(pool.clone(), b.tenant.id, b.organization.id, {
        let phone = phone.clone();
        move |p| async move {
            let store = PostgresWhatsAppStore::new(p);
            store
                .create_channel(&channel(b.tenant.id, &phone, "B"))
                .await
        }
    })
    .await;
    assert!(
        conflict.is_err(),
        "the same phone number must not be claimable by two tenants"
    );

    // The pre-scope resolver maps the phone number to exactly tenant A.
    let resolved = PostgresWhatsAppStore::new(pool.clone())
        .resolve_owner_by_phone_number(&phone)
        .await
        .expect("resolve");
    assert_eq!(resolved, Some((a.tenant.id, a.organization.id)));
    let unknown = PostgresWhatsAppStore::new(pool.clone())
        .resolve_owner_by_phone_number("pn-does-not-exist")
        .await
        .expect("resolve unknown");
    assert_eq!(
        unknown, None,
        "an unknown phone number resolves to no tenant"
    );
}

#[tokio::test]
async fn billing_customer_maps_to_one_tenant_only() {
    let Some(pool) = support::live_pool("cross-tenant-billing").await else {
        return;
    };
    let a = seed_scope(&pool, "bc-a").await;
    let b = seed_scope(&pool, "bc-b").await;
    let suffix = &uuid::Uuid::now_v7().to_string().replace('-', "");
    let handle = format!("cus_{suffix}");

    scoped(pool.clone(), a.tenant.id, a.organization.id, {
        let row = customer(a.tenant.id, &handle);
        move |p| async move {
            let store = PostgresBillingCustomerStore::new(p);
            store.create_or_get(&row).await.expect("create customer A");
        }
    })
    .await;

    // The same provider customer claimed by another tenant is a conflict.
    let conflict = scoped(pool.clone(), b.tenant.id, b.organization.id, {
        let row = customer(b.tenant.id, &handle);
        move |p| async move {
            let store = PostgresBillingCustomerStore::new(p);
            store.create_or_get(&row).await
        }
    })
    .await;
    assert!(
        conflict.is_err(),
        "one provider customer must not map to two tenants"
    );

    // Pre-scope resolution maps the handle to exactly tenant A.
    let resolved = PostgresBillingCustomerStore::new(pool.clone())
        .resolve_owner("stripe", &handle)
        .await
        .expect("resolve");
    assert_eq!(resolved, Some((a.tenant.id, a.organization.id)));
    let unknown = PostgresBillingCustomerStore::new(pool.clone())
        .resolve_owner("stripe", "cus_unknown")
        .await
        .expect("resolve unknown");
    assert_eq!(unknown, None);
}

#[tokio::test]
async fn event_application_claim_is_exactly_once_per_tenant() {
    let Some(pool) = support::live_pool("cross-tenant-claim").await else {
        return;
    };
    let a = seed_scope(&pool, "ev-a").await;
    let suffix = &uuid::Uuid::now_v7().to_string().replace('-', "");
    let event_id = format!("evt_{suffix}");

    let (first, second) = scoped(pool.clone(), a.tenant.id, a.organization.id, {
        let event_id = event_id.clone();
        move |p| async move {
            let store = PostgresEventApplicationStore::new(p);
            let first = store
                .claim(&claim(a.tenant.id, &event_id))
                .await
                .expect("first claim");
            let second = store
                .claim(&claim(a.tenant.id, &event_id))
                .await
                .expect("second claim");
            (first, second)
        }
    })
    .await;
    assert!(first, "the first claim must insert");
    assert!(
        !second,
        "the second claim must be refused (already applied)"
    );

    // A claim is append-only evidence: it cannot be deleted.
    let deleted = sqlx::query("DELETE FROM billing_event_applications WHERE event_id = $1")
        .bind(&event_id)
        .execute(&pool)
        .await;
    assert!(deleted.is_err(), "a claim row must not be deletable");
}

#[tokio::test]
async fn checkout_idempotency_is_per_tenant() {
    let Some(pool) = support::live_pool("cross-tenant-checkout").await else {
        return;
    };
    let a = seed_scope(&pool, "ck-a").await;
    let b = seed_scope(&pool, "ck-b").await;
    let suffix = &uuid::Uuid::now_v7().to_string().replace('-', "");
    let key = format!("key-{suffix}");
    let plan_id = PlanId::new();
    let plan_code = format!("plan_checkout_{suffix}");
    sqlx::query("INSERT INTO plans (id, code, name) VALUES ($1, $2, $3)")
        .bind(plan_id.into_uuid())
        .bind(&plan_code)
        .bind("Checkout test plan")
        .execute(&pool)
        .await
        .expect("checkout plan fixture");

    scoped(pool.clone(), a.tenant.id, a.organization.id, {
        let mut row = checkout(a.tenant.id, &key);
        row.plan_id = plan_id;
        row.plan_code = plan_code.clone();
        move |p| async move {
            let store = PostgresCheckoutSessionStore::new(p);
            let (_, created_first) = store.create_or_get(&row).await.expect("first");
            assert!(created_first, "the first create must insert");
            let (existing, created_again) = store.create_or_get(&row).await.expect("again");
            assert!(
                !created_again,
                "a repeated idempotency key must not create twice"
            );
            assert_eq!(existing.idempotency_key, row.idempotency_key);
        }
    })
    .await;

    // The same key in another tenant is a different checkout.
    scoped(pool.clone(), b.tenant.id, b.organization.id, {
        let mut row = checkout(b.tenant.id, &key);
        row.plan_id = plan_id;
        row.plan_code = plan_code;
        move |p| async move {
            let store = PostgresCheckoutSessionStore::new(p);
            let (_, created) = store.create_or_get(&row).await.expect("tenant B");
            assert!(
                created,
                "the same key in another tenant is a separate checkout"
            );
        }
    })
    .await;
}

#[tokio::test]
async fn tenant_id_is_immutable() {
    let Some(pool) = support::live_pool("cross-tenant-immutable").await else {
        return;
    };
    let a = seed_scope(&pool, "im-a").await;
    let b = seed_scope(&pool, "im-b").await;
    let suffix = &uuid::Uuid::now_v7().to_string().replace('-', "");
    let phone = format!("pn-imm{suffix}");

    // Seed one channel as tenant A (owner connection, plain insert).
    let id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO whatsapp_channels (id, tenant_id, display_name, phone_number_id, verify_token_ref) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(id)
    .bind(a.tenant.id.as_uuid())
    .bind("immutable")
    .bind(&phone)
    .bind("env:X")
    .execute(&pool)
    .await
    .expect("seed channel");

    // Any UPDATE that moves the row to tenant B must be refused by 0061's
    // trigger -- even from the owner connection that bypasses the policy.
    let moved = sqlx::query("UPDATE whatsapp_channels SET tenant_id = $1 WHERE id = $2")
        .bind(b.tenant.id.as_uuid())
        .bind(id)
        .execute(&pool)
        .await;
    assert!(moved.is_err(), "tenant_id must be immutable (0061 trigger)");
}

#[tokio::test]
async fn boundary_tables_are_under_force_rls() {
    let Some(pool) = support::live_pool("cross-tenant-rls").await else {
        return;
    };
    for table in [
        "whatsapp_channels",
        "whatsapp_messages",
        "billing_customers",
        "plan_prices",
        "billing_checkout_sessions",
        "billing_event_applications",
    ] {
        let (enabled, forced): (bool, bool) = sqlx::query_as(
            "SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE oid = to_regclass($1)",
        )
        .bind(table)
        .fetch_one(&pool)
        .await
        .unwrap_or_else(|error| panic!("pg_class lookup for {table}: {error}"));
        assert!(enabled, "{table} must have ROW LEVEL SECURITY enabled");
        assert!(forced, "{table} must have ROW LEVEL SECURITY forced");
    }
}
